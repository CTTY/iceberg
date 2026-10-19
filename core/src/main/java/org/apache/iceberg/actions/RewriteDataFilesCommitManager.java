/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.actions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.CompactionMap;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Functionality used by RewriteDataFile Actions from different platforms to handle commits. */
public class RewriteDataFilesCommitManager {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesCommitManager.class);

  private final Table table;
  private final long startingSnapshotId;
  private final boolean useStartingSequenceNumber;
  private final Map<String, String> snapshotProperties;
  private final String branch;

  private boolean resolveConflicts = false;
  private int maxConflictingDeleteFiles = 0;
  private int maxConflictRetries = 0;
  private Runnable beforeCommitHook = null;

  // constructor used for testing
  public RewriteDataFilesCommitManager(Table table) {
    this(table, table.currentSnapshot().snapshotId());
  }

  public RewriteDataFilesCommitManager(Table table, long startingSnapshotId) {
    this(table, startingSnapshotId, RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER_DEFAULT);
  }

  public RewriteDataFilesCommitManager(
      Table table, long startingSnapshotId, boolean useStartingSequenceNumber) {
    this(table, startingSnapshotId, useStartingSequenceNumber, ImmutableMap.of());
  }

  public RewriteDataFilesCommitManager(
      Table table,
      long startingSnapshotId,
      boolean useStartingSequenceNumber,
      Map<String, String> snapshotProperties) {
    this(table, startingSnapshotId, useStartingSequenceNumber, snapshotProperties, null);
  }

  public RewriteDataFilesCommitManager(
      Table table,
      long startingSnapshotId,
      boolean useStartingSequenceNumber,
      Map<String, String> snapshotProperties,
      String branch) {
    this.table = table;
    this.startingSnapshotId = startingSnapshotId;
    this.useStartingSequenceNumber = useStartingSequenceNumber;
    this.snapshotProperties = snapshotProperties;
    this.branch = branch;
  }

  /**
   * Enables rebasing of position deletes committed concurrently with the rewrite.
   *
   * <p>When enabled, and when the rewritten groups carry {@link RewriteFileGroup#positionRuns()
   * position runs}, the commit does not fail on deletion vectors added to the rewritten files after
   * the starting snapshot. Instead, those deletion vectors are remapped onto the new files through
   * the recorded runs and committed together with the rewrite.
   *
   * <p>Rebasing moves the validation baseline forward, which skips the usual check for concurrent
   * equality deletes. With {@code useStartingSequenceNumber} the output files keep the starting
   * data sequence number and those deletes still apply, so they are ignored. Without it, a
   * concurrent equality delete is reported as a conflict, matching the existing behavior of {@code
   * RewriteFiles} in that mode.
   *
   * @param maxDeleteFiles maximum number of concurrently added delete files to rebase per commit
   * @param maxRetries maximum number of commit attempts when new deletes keep arriving
   * @return this for method chaining
   */
  public RewriteDataFilesCommitManager resolvePositionDeleteConflicts(
      int maxDeleteFiles, int maxRetries) {
    Preconditions.checkArgument(maxDeleteFiles > 0, "maxDeleteFiles must be positive");
    Preconditions.checkArgument(maxRetries > 0, "maxRetries must be positive");
    this.resolveConflicts = true;
    this.maxConflictingDeleteFiles = maxDeleteFiles;
    this.maxConflictRetries = maxRetries;
    return this;
  }

  /**
   * Hook invoked inside each commit attempt, after conflict detection and right before the commit,
   * when conflict resolution is enabled. Exists so tests can inject a concurrent commit between
   * detection and commit, or fail an attempt by throwing a {@link ValidationException}.
   */
  @VisibleForTesting
  public void beforeCommitAttempt(Runnable hook) {
    this.beforeCommitHook = hook;
  }

  /**
   * Perform a commit operation on the table adding and removing files as required for this set of
   * file groups
   *
   * @param fileGroups fileSets to commit
   */
  public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    DataFileSet rewrittenDataFiles = DataFileSet.create();
    DataFileSet addedDataFiles = DataFileSet.create();
    DeleteFileSet danglingDVs = DeleteFileSet.create();
    List<CompactionMap.Run> runs = Lists.newArrayList();
    boolean allGroupsTracked = true;
    for (RewriteFileGroup group : fileGroups) {
      rewrittenDataFiles.addAll(group.rewrittenFiles());
      addedDataFiles.addAll(group.addedFiles());
      danglingDVs.addAll(group.danglingDVs());
      if (group.positionRuns() != null) {
        runs.addAll(group.positionRuns());
      } else {
        allGroupsTracked = false;
      }
    }

    if (resolveConflicts && allGroupsTracked) {
      commitWithConflictResolution(
          rewrittenDataFiles, addedDataFiles, danglingDVs, CompactionMap.of(runs));
      return;
    }

    if (resolveConflicts) {
      LOG.warn(
          "Position delete conflict resolution is enabled but not all groups tracked positions;"
              + " committing without rebasing");
    }

    RewriteFiles rewrite = newRewrite(startingSnapshotId);
    rewrittenDataFiles.forEach(rewrite::deleteFile);
    addedDataFiles.forEach(rewrite::addFile);
    danglingDVs.forEach(rewrite::deleteFile);
    rewrite.commit();
  }

  /**
   * Commits the rewrite, rebasing deletion vectors added since the starting snapshot.
   *
   * <p>Each attempt resolves the window {@code (baseline, current]}, advances the validation
   * baseline to {@code current} and commits. If another writer lands a delete between detection and
   * commit, the commit fails validation and the next attempt resolves the new window.
   */
  private void commitWithConflictResolution(
      DataFileSet rewrittenDataFiles,
      DataFileSet addedDataFiles,
      DeleteFileSet plannedDanglingDVs,
      CompactionMap map) {
    PositionDeleteConflictResolver resolver =
        new PositionDeleteConflictResolver(
            table, map, rewrittenDataFiles, addedDataFiles, useStartingSequenceNumber);

    long baseline = startingSnapshotId;
    DeleteFileSet deletesToRemove = DeleteFileSet.of(plannedDanglingDVs);
    List<DeleteFile> deletesToAdd = Lists.newArrayList();

    ValidationException lastFailure = null;
    for (int attempt = 1; attempt <= maxConflictRetries; attempt++) {
      table.refresh();
      Snapshot current = branch != null ? table.snapshot(branch) : table.currentSnapshot();
      Preconditions.checkState(current != null, "Cannot commit rewrite: no current snapshot");

      if (current.snapshotId() == baseline && lastFailure != null) {
        // nothing new to rebase, so retrying would fail the same way
        throw lastFailure;
      }

      if (current.snapshotId() != baseline) {
        PositionDeleteConflictResolver.Resolution resolution =
            resolver.resolve(baseline, current, deletesToRemove, maxConflictingDeleteFiles);
        // the resolver recomputes the planned set against the new window, so replace, not merge
        deletesToRemove = DeleteFileSet.of(resolution.deletesToRemove());
        deletesToAdd.addAll(resolution.deletesToAdd());
        baseline = current.snapshotId();

        if (resolution.conflictingDeleteFiles() > 0) {
          LOG.info(
              "Attempt {}: rebased {} concurrent delete files, validating from snapshot {}",
              attempt,
              resolution.conflictingDeleteFiles(),
              baseline);
        }
      }

      RewriteFiles rewrite = newRewrite(baseline);
      rewrittenDataFiles.forEach(rewrite::deleteFile);
      addedDataFiles.forEach(rewrite::addFile);
      deletesToRemove.forEach(rewrite::deleteFile);
      deletesToAdd.forEach(rewrite::addFile);

      try {
        if (beforeCommitHook != null) {
          // part of the attempt: a ValidationException from the hook counts as a failed commit
          beforeCommitHook.run();
        }

        rewrite.commit();
        return;
      } catch (ValidationException e) {
        LOG.warn("Attempt {} to commit rewrite failed validation, retrying", attempt, e);
        lastFailure = e;
      }
    }

    throw new ValidationException(
        lastFailure,
        "Cannot commit rewrite after %s attempts to rebase concurrent deletes",
        maxConflictRetries);
  }

  private RewriteFiles newRewrite(long validateFromSnapshotId) {
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(validateFromSnapshotId);
    if (useStartingSequenceNumber) {
      // output files always keep the sequence number of the snapshot they were derived from,
      // even when validation starts from a later snapshot
      long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
      rewrite.dataSequenceNumber(sequenceNumber);
    }

    snapshotProperties.forEach(rewrite::set);

    if (branch != null) {
      rewrite.toBranch(branch);
    }

    return rewrite;
  }

  /**
   * Clean up a specified file set by removing any files created for that operation, should not
   * throw any exceptions
   *
   * @param fileGroup group of files which has already been rewritten
   */
  public void abortFileGroup(RewriteFileGroup fileGroup) {
    Preconditions.checkState(
        fileGroup.addedFiles() != null, "Cannot abort a fileGroup that was not rewritten");

    Tasks.foreach(fileGroup.addedFiles())
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((dataFile, exc) -> LOG.warn("Failed to delete: {}", dataFile.location(), exc))
        .run(dataFile -> table.io().deleteFile(dataFile.location()));
  }

  public void commitOrClean(Set<RewriteFileGroup> rewriteGroups) {
    try {
      commitFileGroups(rewriteGroups);
    } catch (CommitStateUnknownException e) {
      LOG.error(
          "Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewriteGroups,
          e);
      throw e;
    } catch (Exception e) {
      if (e instanceof CleanableFailure) {
        LOG.error(
            "Cannot commit groups {}, attempting to clean up written files", rewriteGroups, e);
        rewriteGroups.forEach(this::abortFileGroup);
      }

      throw e;
    }
  }

  /**
   * An async service which allows for committing multiple file groups as their rewrites complete.
   * The service also allows for partial-progress since commits can fail. Once the service has been
   * closed no new file groups should not be offered.
   *
   * @param rewritesPerCommit number of file groups to include in a commit
   * @return the service for handling commits
   */
  public CommitService service(int rewritesPerCommit) {
    return new CommitService(rewritesPerCommit);
  }

  public class CommitService extends BaseCommitService<RewriteFileGroup> {

    CommitService(int rewritesPerCommit) {
      super(table, rewritesPerCommit);
    }

    @Override
    protected void commitOrClean(Set<RewriteFileGroup> batch) {
      RewriteDataFilesCommitManager.this.commitOrClean(batch);
    }

    @Override
    protected void abortFileGroup(RewriteFileGroup group) {
      RewriteDataFilesCommitManager.this.abortFileGroup(group);
    }
  }
}
