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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.index.BloomFilterIndex;
import org.apache.iceberg.index.IndexFilePruner;
import org.apache.iceberg.index.IndexPuffinWriter;
import org.apache.iceberg.index.IndexSpec;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Bloom filter index read-path integration.
 *
 * <p>These tests validate that IndexFilePruner correctly prunes FileScanTasks when integrated with
 * the scan planning pipeline. The tests use in-memory Puffin files to simulate the full flow of
 * writing indexes and then using them for pruning.
 *
 * <p>Validates Requirements 7.1, 7.2, 7.5, 7.6.
 */
public class TestSparkReadIndexIntegration {

  private static final long SNAPSHOT_ID = 1L;
  private static final long SEQUENCE_NUMBER = 1L;
  private static final String DATA_FILE_1 = "s3://bucket/table/data/part-00000.parquet";
  private static final String DATA_FILE_2 = "s3://bucket/table/data/part-00001.parquet";
  private static final String DATA_FILE_3 = "s3://bucket/table/data/part-00002.parquet";

  private static final Map<String, String> BLOOM_PROPS =
      ImmutableMap.of("fpp", "0.01", "expected-insertions", "100");

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  /**
   * Tests that IndexFilePruner correctly prunes files when an EQ predicate matches a value NOT in
   * the Bloom filter. Validates Requirement 7.1.
   */
  @Test
  public void testPruningWithEqualityPredicate() throws Exception {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String puffinPath = "s3://bucket/table/metadata/bloom-snap1-0.puffin";

    // Create a bloom filter for DATA_FILE_1 containing values 1-50
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    BloomFilterIndex index = (BloomFilterIndex) spec.createIndex();
    for (int i = 1; i <= 50; i++) {
      index.add(i, Types.IntegerType.get());
    }

    // Write the index to a Puffin file
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index, DATA_FILE_1);
    }
    fileIO.addFile(puffinPath, outputFile.toByteArray());

    // Load the pruner
    IndexFilePruner pruner = IndexFilePruner.load(ImmutableList.of(puffinPath), fileIO);
    assertThat(pruner.isEmpty()).isFalse();

    // Create mock file scan tasks
    FileScanTask task1 = new MockFileScanTask(DATA_FILE_1);
    FileScanTask task2 = new MockFileScanTask(DATA_FILE_2);

    // Test with a value that IS in the filter (should NOT prune)
    Expression eqPresent = Expressions.equal("id", 25);
    List<FileScanTask> result = pruner.pruneFiles(ImmutableList.of(task1, task2), eqPresent, SCHEMA);
    // task1 should be kept (value might be present), task2 has no index so kept
    assertThat(result).hasSize(2);

    // Test with a value that is NOT in the filter (should prune task1)
    Expression eqAbsent = Expressions.equal("id", 999);
    result = pruner.pruneFiles(ImmutableList.of(task1, task2), eqAbsent, SCHEMA);
    // task1 should be pruned (value definitely not present), task2 has no index so kept
    assertThat(result).hasSize(1);
    assertThat(result.get(0).file().location()).isEqualTo(DATA_FILE_2);
  }

  /**
   * Tests that IndexFilePruner correctly prunes files when an IN predicate has ALL values NOT in
   * the Bloom filter. Validates Requirement 7.2.
   */
  @Test
  public void testPruningWithInPredicate() throws Exception {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String puffinPath = "s3://bucket/table/metadata/bloom-snap1-0.puffin";

    // Create a bloom filter for DATA_FILE_1 containing values 1-50
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    BloomFilterIndex index = (BloomFilterIndex) spec.createIndex();
    for (int i = 1; i <= 50; i++) {
      index.add(i, Types.IntegerType.get());
    }

    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index, DATA_FILE_1);
    }
    fileIO.addFile(puffinPath, outputFile.toByteArray());

    IndexFilePruner pruner = IndexFilePruner.load(ImmutableList.of(puffinPath), fileIO);

    FileScanTask task1 = new MockFileScanTask(DATA_FILE_1);

    // IN with all values absent - should prune
    Expression inAbsent = Expressions.in("id", 999, 998, 997);
    List<FileScanTask> result = pruner.pruneFiles(ImmutableList.of(task1), inAbsent, SCHEMA);
    assertThat(result).isEmpty();

    // IN with one value present - should NOT prune
    Expression inPartial = Expressions.in("id", 25, 999, 998);
    result = pruner.pruneFiles(ImmutableList.of(task1), inPartial, SCHEMA);
    assertThat(result).hasSize(1);
  }

  /**
   * Tests that files without corresponding indexes are kept (not pruned). Validates Requirement
   * 7.5.
   */
  @Test
  public void testFilesWithoutIndexAreKept() throws Exception {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String puffinPath = "s3://bucket/table/metadata/bloom-snap1-0.puffin";

    // Create index only for DATA_FILE_1
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    BloomFilterIndex index = (BloomFilterIndex) spec.createIndex();
    index.add(1, Types.IntegerType.get());

    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index, DATA_FILE_1);
    }
    fileIO.addFile(puffinPath, outputFile.toByteArray());

    IndexFilePruner pruner = IndexFilePruner.load(ImmutableList.of(puffinPath), fileIO);

    // DATA_FILE_2 and DATA_FILE_3 have no index
    FileScanTask task2 = new MockFileScanTask(DATA_FILE_2);
    FileScanTask task3 = new MockFileScanTask(DATA_FILE_3);

    Expression eq = Expressions.equal("id", 999);
    List<FileScanTask> result = pruner.pruneFiles(ImmutableList.of(task2, task3), eq, SCHEMA);
    // Both should be kept since they have no index
    assertThat(result).hasSize(2);
  }

  /**
   * Tests that when no index files exist, all tasks are returned unchanged. Validates Requirement
   * 7.5.
   */
  @Test
  public void testNoIndexFilesReturnsAllTasks() {
    InMemoryFileIO fileIO = new InMemoryFileIO();

    // Load with empty index file list
    IndexFilePruner pruner = IndexFilePruner.load(Collections.emptyList(), fileIO);
    assertThat(pruner.isEmpty()).isTrue();

    FileScanTask task1 = new MockFileScanTask(DATA_FILE_1);
    FileScanTask task2 = new MockFileScanTask(DATA_FILE_2);

    Expression eq = Expressions.equal("id", 42);
    List<FileScanTask> result = pruner.pruneFiles(ImmutableList.of(task1, task2), eq, SCHEMA);
    assertThat(result).hasSize(2);
  }

  /**
   * Tests that bloom filter pruning integrates correctly with the scan pipeline by pruning files
   * after partition/min-max pruning. Validates Requirement 7.6.
   */
  @Test
  public void testPruningIntegrationWithMultipleFiles() throws Exception {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String puffinPath = "s3://bucket/table/metadata/bloom-snap1-0.puffin";

    // Create indexes for 3 data files with different value ranges
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);

    BloomFilterIndex index1 = (BloomFilterIndex) spec.createIndex();
    for (int i = 1; i <= 100; i++) {
      index1.add(i, Types.IntegerType.get());
    }

    BloomFilterIndex index2 = (BloomFilterIndex) spec.createIndex();
    for (int i = 101; i <= 200; i++) {
      index2.add(i, Types.IntegerType.get());
    }

    BloomFilterIndex index3 = (BloomFilterIndex) spec.createIndex();
    for (int i = 201; i <= 300; i++) {
      index3.add(i, Types.IntegerType.get());
    }

    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index1, DATA_FILE_1);
      writer.add(index2, DATA_FILE_2);
      writer.add(index3, DATA_FILE_3);
    }
    fileIO.addFile(puffinPath, outputFile.toByteArray());

    IndexFilePruner pruner = IndexFilePruner.load(ImmutableList.of(puffinPath), fileIO);

    FileScanTask task1 = new MockFileScanTask(DATA_FILE_1);
    FileScanTask task2 = new MockFileScanTask(DATA_FILE_2);
    FileScanTask task3 = new MockFileScanTask(DATA_FILE_3);
    List<FileScanTask> allTasks = ImmutableList.of(task1, task2, task3);

    // Query for value 150 - only in file 2
    Expression eq = Expressions.equal("id", 150);
    List<FileScanTask> result = pruner.pruneFiles(allTasks, eq, SCHEMA);
    // Files 1 and 3 should be pruned, only file 2 kept
    assertThat(result).hasSize(1);
    assertThat(result.get(0).file().location()).isEqualTo(DATA_FILE_2);
  }

  /**
   * Tests that pruning handles corrupt/missing Puffin files gracefully (fail-safe). Validates
   * Requirement 7.4.
   */
  @Test
  public void testFailSafeOnCorruptIndexFile() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    // Reference a non-existent Puffin file
    String missingPath = "s3://bucket/table/metadata/missing.puffin";

    // Should not throw, should return empty pruner (fail-safe)
    IndexFilePruner pruner = IndexFilePruner.load(ImmutableList.of(missingPath), fileIO);
    assertThat(pruner.isEmpty()).isTrue();

    FileScanTask task1 = new MockFileScanTask(DATA_FILE_1);
    Expression eq = Expressions.equal("id", 42);
    List<FileScanTask> result = pruner.pruneFiles(ImmutableList.of(task1), eq, SCHEMA);
    // File should be kept (fail-safe)
    assertThat(result).hasSize(1);
  }

  /**
   * Minimal mock FileScanTask for testing pruning logic without requiring a full Iceberg table.
   */
  private static class MockFileScanTask implements FileScanTask {
    private final MockDataFile dataFile;

    MockFileScanTask(String path) {
      this.dataFile = new MockDataFile(path);
    }

    @Override
    public DataFile file() {
      return dataFile;
    }

    @Override
    public List<org.apache.iceberg.DeleteFile> deletes() {
      return Collections.emptyList();
    }

    @Override
    public org.apache.iceberg.PartitionSpec spec() {
      return org.apache.iceberg.PartitionSpec.unpartitioned();
    }

    @Override
    public long start() {
      return 0;
    }

    @Override
    public long length() {
      return 1024;
    }

    @Override
    public Expression residual() {
      return Expressions.alwaysTrue();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this);
    }

    @Override
    public long sizeBytes() {
      return 1024;
    }

    @Override
    public long estimatedRowsCount() {
      return 100;
    }

    @Override
    public int filesCount() {
      return 1;
    }

    @Override
    public Schema schema() {
      return SCHEMA;
    }
  }

  /**
   * Minimal mock DataFile that provides a file location for pruning tests.
   */
  private static class MockDataFile implements DataFile {
    private final String path;

    MockDataFile(String path) {
      this.path = path;
    }

    @Override
    public Long pos() {
      return null;
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public CharSequence path() {
      return path;
    }

    @Override
    public org.apache.iceberg.FileFormat format() {
      return org.apache.iceberg.FileFormat.PARQUET;
    }

    @Override
    public org.apache.iceberg.StructLike partition() {
      return null;
    }

    @Override
    public long recordCount() {
      return 100;
    }

    @Override
    public long fileSizeInBytes() {
      return 1024;
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return null;
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, java.nio.ByteBuffer> lowerBounds() {
      return null;
    }

    @Override
    public Map<Integer, java.nio.ByteBuffer> upperBounds() {
      return null;
    }

    @Override
    public java.nio.ByteBuffer keyMetadata() {
      return null;
    }

    @Override
    public List<Long> splitOffsets() {
      return null;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return null;
    }

    @Override
    public Integer sortOrderId() {
      return null;
    }

    @Override
    public Long dataSequenceNumber() {
      return null;
    }

    @Override
    public Long fileSequenceNumber() {
      return null;
    }

    @Override
    public org.apache.iceberg.FileContent content() {
      return org.apache.iceberg.FileContent.DATA;
    }

    @Override
    public int specId() {
      return 0;
    }

    @Override
    public DataFile copy() {
      return this;
    }

    @Override
    public DataFile copyWithoutStats() {
      return this;
    }

    @Override
    public DataFile copyWithStats(java.util.Set<Integer> requestedColumnIds) {
      return this;
    }
  }
}
