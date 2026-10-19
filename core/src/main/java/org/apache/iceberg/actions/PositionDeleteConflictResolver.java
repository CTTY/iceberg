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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.CompactionMap;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rebases position deletes that were committed concurrently with a data file rewrite onto the
 * rewritten files, using the {@link CompactionMap} recorded by the rewrite.
 *
 * <p>Given a snapshot window {@code (baseline, current]}, the resolver finds deletion vectors added
 * in that window that reference files being replaced by the rewrite, remaps their positions through
 * the compaction map and writes new deletion vectors against the rewrite's output files. The caller
 * then removes the original DVs and adds the remapped ones in the same {@code REPLACE} commit.
 *
 * <p>Only v3 deletion vectors are rebased. Equality deletes match by value and need no remapping;
 * they are ignored when the rewrite keeps the starting data sequence number (so they still apply to
 * the output) and reported as conflicts otherwise. v2 position delete files are reported as
 * unresolvable conflicts.
 */
public class PositionDeleteConflictResolver {
  private static final Logger LOG = LoggerFactory.getLogger(PositionDeleteConflictResolver.class);

  /** Outcome of resolving one snapshot window. */
  public static class Resolution {
    private final Set<DeleteFile> deletesToRemove;
    private final List<DeleteFile> deletesToAdd;
    private final int conflictingDeleteFiles;

    Resolution(
        Set<DeleteFile> deletesToRemove,
        List<DeleteFile> deletesToAdd,
        int conflictingDeleteFiles) {
      this.deletesToRemove = deletesToRemove;
      this.deletesToAdd = deletesToAdd;
      this.conflictingDeleteFiles = conflictingDeleteFiles;
    }

    /** Delete files that must be removed by the rewrite commit (planned dangling + rebased). */
    public Set<DeleteFile> deletesToRemove() {
      return deletesToRemove;
    }

    /** New delete files referencing the rewrite's output files. */
    public List<DeleteFile> deletesToAdd() {
      return deletesToAdd;
    }

    /** Number of concurrently committed delete files that were rebased. */
    public int conflictingDeleteFiles() {
      return conflictingDeleteFiles;
    }
  }

  private final Table table;
  private final CompactionMap map;
  private final Map<String, DataFile> targetFilesByLocation;
  private final Set<String> sourceLocations;
  private final boolean outputKeepsStartingSequenceNumber;

  /**
   * @param table the table being rewritten
   * @param map position runs recorded by the rewrite
   * @param sourceFiles data files being replaced
   * @param targetFiles data files produced by the rewrite
   * @param outputKeepsStartingSequenceNumber whether the rewrite assigns the starting snapshot's
   *     data sequence number to its output files. When true, equality deletes committed during the
   *     rewrite keep applying to the output and are ignored. When false, the output receives a
   *     newer sequence number, concurrent equality deletes would stop applying to it, and any
   *     equality delete in the window is reported as an unresolvable conflict. This mirrors {@code
   *     MergingSnapshotProducer.validateNoNewDeletesForDataFiles}, which ignores equality deletes
   *     only when a data sequence number is set.
   */
  public PositionDeleteConflictResolver(
      Table table,
      CompactionMap map,
      Set<DataFile> sourceFiles,
      Set<DataFile> targetFiles,
      boolean outputKeepsStartingSequenceNumber) {
    this.table = table;
    this.map = map;
    this.outputKeepsStartingSequenceNumber = outputKeepsStartingSequenceNumber;
    this.sourceLocations = Sets.newHashSet();
    sourceFiles.forEach(file -> sourceLocations.add(file.location()));
    this.targetFilesByLocation = Maps.newHashMap();
    targetFiles.forEach(file -> targetFilesByLocation.put(file.location(), file));
  }

  /**
   * Resolves conflicts in the window {@code (baselineSnapshotId, current]}.
   *
   * @param baselineSnapshotId the last snapshot whose changes have already been accounted for
   * @param current the snapshot the rewrite is about to commit on top of
   * @param plannedDanglingDeletes DVs the planner saw on the source files at the starting snapshot
   * @param maxDeleteFiles maximum number of concurrently added delete files to rebase
   * @return what to remove and add in the rewrite commit
   * @throws ValidationException if the window contains changes that cannot be rebased
   */
  public Resolution resolve(
      long baselineSnapshotId,
      Snapshot current,
      Set<DeleteFile> plannedDanglingDeletes,
      int maxDeleteFiles) {
    Set<String> removedInWindow = Sets.newHashSet();
    // insertion order matters for deterministic output
    Map<String, DeleteFile> addedDVs = Maps.newLinkedHashMap();

    Iterable<Snapshot> window =
        SnapshotUtil.ancestorsBetween(current.snapshotId(), baselineSnapshotId, table::snapshot);

    // ancestorsBetween yields newest first; collect everything, then reconcile
    for (Snapshot snapshot : window) {
      for (DataFile removed : snapshot.removedDataFiles(table.io())) {
        ValidationException.check(
            !sourceLocations.contains(removed.location()),
            "Cannot rebase rewrite: source data file %s was removed by snapshot %s",
            removed.location(),
            snapshot.snapshotId());
      }

      for (DeleteFile removed : snapshot.removedDeleteFiles(table.io())) {
        removedInWindow.add(removed.location());
      }

      for (DeleteFile added : snapshot.addedDeleteFiles(table.io())) {
        classifyAddedDelete(snapshot, added, addedDVs);
      }
    }

    // a DV added and then replaced within the window is not live anymore
    addedDVs.keySet().removeIf(removedInWindow::contains);

    ValidationException.check(
        addedDVs.size() <= maxDeleteFiles,
        "Too many concurrently added delete files to rebase (%s > %s)",
        addedDVs.size(),
        maxDeleteFiles);

    // planned dangling DVs that were replaced by a concurrent writer no longer exist
    Set<DeleteFile> toRemove = DeleteFileSet.create();
    for (DeleteFile planned : plannedDanglingDeletes) {
      if (!removedInWindow.contains(planned.location())) {
        toRemove.add(planned);
      }
    }

    toRemove.addAll(addedDVs.values());

    List<DeleteFile> toAdd = rebase(addedDVs.values());

    LOG.info(
        "Rebased {} concurrently added DVs onto {} rewritten files, producing {} new DVs",
        addedDVs.size(),
        targetFilesByLocation.size(),
        toAdd.size());

    return new Resolution(toRemove, toAdd, addedDVs.size());
  }

  private void classifyAddedDelete(
      Snapshot snapshot, DeleteFile added, Map<String, DeleteFile> addedDVs) {
    if (added.content() == FileContent.EQUALITY_DELETES) {
      // applies by value, so there is nothing to remap; it commutes with the rewrite only if the
      // output files keep a sequence number below the delete's (data_seq < delete_seq)
      ValidationException.check(
          outputKeepsStartingSequenceNumber,
          "Cannot rebase rewrite: snapshot %s added equality delete %s and the rewrite assigns a"
              + " new data sequence number to its output, so the delete would no longer apply."
              + " Use use-starting-sequence-number=true to allow concurrent equality deletes.",
          snapshot.snapshotId(),
          added.location());
      return;
    }

    String referenced = added.referencedDataFile();
    if (ContentFileUtil.isDV(added)) {
      if (sourceLocations.contains(referenced)) {
        addedDVs.put(added.location(), added);
      }

      return;
    }

    // v2 position delete file: not supported by this resolver
    boolean mayConflict = referenced == null || sourceLocations.contains(referenced);
    ValidationException.check(
        !mayConflict,
        "Cannot rebase rewrite: snapshot %s added position delete file %s which is not a deletion"
            + " vector",
        snapshot.snapshotId(),
        added.location());
  }

  private List<DeleteFile> rebase(Iterable<DeleteFile> dvs) {
    // accumulate remapped positions per target file so each target receives exactly one DV
    Map<String, PositionDeleteIndex> positionsByTarget = Maps.newLinkedHashMap();

    for (DeleteFile dv : dvs) {
      String sourceFile = dv.referencedDataFile();
      Preconditions.checkState(
          map.containsSource(sourceFile),
          "Rewrite replaced %s but recorded no position runs for it",
          sourceFile);

      PositionDeleteIndex positions = readDV(dv);
      Map<String, PositionDeleteIndex> remapped = map.remap(sourceFile, positions);

      long mapped = remapped.values().stream().mapToLong(PositionDeleteIndex::cardinality).sum();
      LOG.debug(
          "Remapped DV {}: {} positions -> {} positions across {} target files ({} already"
              + " applied by the rewrite)",
          dv.location(),
          positions.cardinality(),
          mapped,
          remapped.size(),
          positions.cardinality() - mapped);

      for (Map.Entry<String, PositionDeleteIndex> entry : remapped.entrySet()) {
        PositionDeleteIndex existing = positionsByTarget.get(entry.getKey());
        if (existing == null) {
          positionsByTarget.put(entry.getKey(), entry.getValue());
        } else {
          existing.merge(entry.getValue());
        }
      }
    }

    if (positionsByTarget.isEmpty()) {
      return ImmutableList.of();
    }

    return writeDVs(positionsByTarget);
  }

  private List<DeleteFile> writeDVs(Map<String, PositionDeleteIndex> positionsByTarget) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);

    try (DVFileWriter closeable = writer) {
      for (Map.Entry<String, PositionDeleteIndex> entry : positionsByTarget.entrySet()) {
        DataFile target = targetFilesByLocation.get(entry.getKey());
        Preconditions.checkState(
            target != null, "Compaction map references unknown target file %s", entry.getKey());
        PartitionSpec spec = table.specs().get(target.specId());
        closeable.delete(target.location(), entry.getValue(), spec, target.partition());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write rebased deletion vectors", e);
    }

    return Lists.newArrayList(writer.result().deleteFiles());
  }

  private PositionDeleteIndex readDV(DeleteFile dv) {
    InputFile inputFile = table.io().newInputFile(dv);
    long offset = dv.contentOffset();
    int length = dv.contentSizeInBytes().intValue();
    byte[] bytes = new byte[length];
    try {
      IOUtil.readFully(inputFile, offset, bytes, 0, length);
      return PositionDeleteIndex.deserialize(bytes, dv);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read deletion vector " + dv.location(), e);
    }
  }
}
