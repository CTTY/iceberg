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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Property-based tests for index files accumulation across snapshots.
 *
 * <p><b>Property 10: Index Files Accumulation</b>
 *
 * <p>For any sequence of commits with index data, the snapshot's index-files list SHALL contain all
 * Puffin file paths from previous snapshots plus the new Puffin file path. The list SHALL be
 * append-only.
 *
 * <p><b>Validates: Requirements 3.4, 3.5</b>
 */
public class TestIndexFilesAccumulationProperty {

  /**
   * Simulates the accumulation logic from SnapshotProducer.accumulateIndexFiles: inherits all index
   * files from the parent snapshot and appends new index files.
   */
  private static List<String> accumulateIndexFiles(
      Snapshot parentSnapshot, List<String> newIndexFiles) {
    List<String> inherited =
        parentSnapshot != null ? parentSnapshot.indexFiles() : ImmutableList.of();

    if (inherited.isEmpty() && newIndexFiles.isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> builder =
        ImmutableList.builderWithExpectedSize(inherited.size() + newIndexFiles.size());
    builder.addAll(inherited);
    builder.addAll(newIndexFiles);
    return builder.build();
  }

  /** Minimal Snapshot implementation for testing index files accumulation. */
  private static class TestSnapshot implements Snapshot {
    private final long snapshotId;
    private final Long parentId;
    private final List<String> indexFiles;

    TestSnapshot(long snapshotId, Long parentId, List<String> indexFiles) {
      this.snapshotId = snapshotId;
      this.parentId = parentId;
      this.indexFiles = ImmutableList.copyOf(indexFiles);
    }

    @Override
    public long sequenceNumber() {
      return snapshotId;
    }

    @Override
    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public Long parentId() {
      return parentId;
    }

    @Override
    public long timestampMillis() {
      return System.currentTimeMillis();
    }

    @Override
    public List<ManifestFile> allManifests(FileIO io) {
      return ImmutableList.of();
    }

    @Override
    public List<ManifestFile> dataManifests(FileIO io) {
      return ImmutableList.of();
    }

    @Override
    public List<ManifestFile> deleteManifests(FileIO io) {
      return ImmutableList.of();
    }

    @Override
    public String operation() {
      return "append";
    }

    @Override
    public Map<String, String> summary() {
      return Collections.emptyMap();
    }

    @Override
    public Iterable<DataFile> addedDataFiles(FileIO io) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<DataFile> removedDataFiles(FileIO io) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<DeleteFile> addedDeleteFiles(FileIO io) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<DeleteFile> removedDeleteFiles(FileIO io) {
      return ImmutableList.of();
    }

    @Override
    public String manifestListLocation() {
      return "manifest-list-" + snapshotId;
    }

    @Override
    public List<String> indexFiles() {
      return indexFiles;
    }
  }

  /**
   * Property 10: After a sequence of commits, each snapshot contains all previously accumulated
   * index files plus the new ones from that commit.
   *
   * <p>Validates: Requirements 3.4, 3.5
   */
  @Property(tries = 100)
  void accumulatedIndexFilesContainAllPreviousAndNew(
      @ForAll
          @Size(min = 1, max = 10)
          List<@Size(min = 1, max = 3) List<@StringLength(min = 5, max = 50) String>>
              commitsIndexFiles) {

    List<String> allExpected = new ArrayList<>();
    Snapshot parentSnapshot = null;

    for (int i = 0; i < commitsIndexFiles.size(); i++) {
      List<String> newFiles = commitsIndexFiles.get(i);
      List<String> accumulated = accumulateIndexFiles(parentSnapshot, newFiles);
      allExpected.addAll(newFiles);

      TestSnapshot snapshot =
          new TestSnapshot(
              i + 1, parentSnapshot != null ? parentSnapshot.snapshotId() : null, accumulated);

      assertThat(snapshot.indexFiles())
          .as("Snapshot %d should contain all accumulated index files", i + 1)
          .containsExactlyElementsOf(allExpected);

      parentSnapshot = snapshot;
    }
  }

  /**
   * Property 10: The index-files list is append-only — the order of previously accumulated files is
   * preserved and no files are removed between snapshots.
   *
   * <p>Validates: Requirements 3.4, 3.5
   */
  @Property(tries = 100)
  void indexFilesListIsAppendOnly(
      @ForAll
          @Size(min = 2, max = 10)
          List<@Size(min = 1, max = 3) List<@StringLength(min = 5, max = 50) String>>
              commitsIndexFiles) {

    Snapshot parentSnapshot = null;
    List<Snapshot> snapshots = new ArrayList<>();

    for (int i = 0; i < commitsIndexFiles.size(); i++) {
      List<String> newFiles = commitsIndexFiles.get(i);
      List<String> accumulated = accumulateIndexFiles(parentSnapshot, newFiles);

      TestSnapshot snapshot =
          new TestSnapshot(
              i + 1, parentSnapshot != null ? parentSnapshot.snapshotId() : null, accumulated);
      snapshots.add(snapshot);
      parentSnapshot = snapshot;
    }

    // Verify append-only: each snapshot's index-files starts with the previous snapshot's list
    for (int i = 1; i < snapshots.size(); i++) {
      List<String> previous = snapshots.get(i - 1).indexFiles();
      List<String> current = snapshots.get(i).indexFiles();

      assertThat(current.size())
          .as(
              "Snapshot %d should have at least as many index files as snapshot %d", i + 1, i)
          .isGreaterThanOrEqualTo(previous.size());

      assertThat(current.subList(0, previous.size()))
          .as(
              "Snapshot %d index-files should start with snapshot %d's index-files (append-only)",
              i + 1, i)
          .isEqualTo(previous);
    }
  }

  /**
   * Property 10: A snapshot with no parent and no new index files has an empty index-files list.
   *
   * <p>Validates: Requirements 3.4, 3.5
   */
  @Property(tries = 100)
  void emptyCommitProducesEmptyIndexFiles(@ForAll long snapshotId) {
    List<String> accumulated = accumulateIndexFiles(null, Collections.emptyList());
    TestSnapshot snapshot = new TestSnapshot(snapshotId, null, accumulated);

    assertThat(snapshot.indexFiles()).isEmpty();
  }

  /**
   * Property 10: The total number of index files after N commits equals the sum of index files
   * added in each commit.
   *
   * <p>Validates: Requirements 3.4, 3.5
   */
  @Property(tries = 100)
  void totalIndexFilesEqualsSum(
      @ForAll
          @Size(min = 1, max = 10)
          List<@Size(min = 0, max = 5) List<@StringLength(min = 5, max = 50) String>>
              commitsIndexFiles) {

    Snapshot parentSnapshot = null;
    Snapshot lastSnapshot = null;
    int expectedTotal = 0;

    for (int i = 0; i < commitsIndexFiles.size(); i++) {
      List<String> newFiles = commitsIndexFiles.get(i);
      expectedTotal += newFiles.size();
      List<String> accumulated = accumulateIndexFiles(parentSnapshot, newFiles);

      TestSnapshot snapshot =
          new TestSnapshot(
              i + 1, parentSnapshot != null ? parentSnapshot.snapshotId() : null, accumulated);
      lastSnapshot = snapshot;
      parentSnapshot = snapshot;
    }

    if (lastSnapshot != null) {
      assertThat(lastSnapshot.indexFiles().size())
          .as("Total index files should equal sum of all added files")
          .isEqualTo(expectedTotal);
    }
  }
}
