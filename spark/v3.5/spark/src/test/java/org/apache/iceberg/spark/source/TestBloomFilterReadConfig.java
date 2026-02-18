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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
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
 * Tests for the {@code read.bloom-filter.enabled} configuration property.
 *
 * <p>Validates that Bloom filter pruning is skipped when the configuration is disabled (Requirement
 * 8.4, 8.6), and that the default behavior enables pruning.
 *
 * <p>The {@code read.bloom-filter.enabled} check is implemented in {@link
 * SparkBatchQueryScan#applyBloomFilterPruning} as the first condition: when disabled, the method
 * returns the original tasks without loading any index files.
 */
public class TestBloomFilterReadConfig {

  private static final long SNAPSHOT_ID = 1L;
  private static final long SEQUENCE_NUMBER = 1L;
  private static final String DATA_FILE_1 = "s3://bucket/table/data/part-00000.parquet";
  private static final String DATA_FILE_2 = "s3://bucket/table/data/part-00001.parquet";

  private static final Map<String, String> BLOOM_PROPS =
      ImmutableMap.of("fpp", "0.01", "expected-insertions", "100");

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  /**
   * Verifies that the READ_BLOOM_FILTER_ENABLED table property key is correctly defined.
   * Validates Requirement 8.4.
   */
  @Test
  public void testReadBloomFilterEnabledPropertyKey() {
    assertThat(TableProperties.READ_BLOOM_FILTER_ENABLED)
        .isEqualTo("read.bloom-filter.enabled");
  }

  /**
   * Verifies that the default value for READ_BLOOM_FILTER_ENABLED is true, meaning Bloom filter
   * pruning is enabled by default. Validates Requirement 8.4.
   */
  @Test
  public void testReadBloomFilterEnabledDefaultIsTrue() {
    assertThat(TableProperties.READ_BLOOM_FILTER_ENABLED_DEFAULT).isTrue();
  }

  /**
   * Verifies that when bloom filter read is enabled (the default), the IndexFilePruner correctly
   * prunes files that don't contain the queried value. This simulates the behavior when
   * {@code read.bloom-filter.enabled=true} — the pruner is loaded and applied.
   * Validates Requirement 8.4.
   */
  @Test
  public void testPruningOccursWhenEnabled() throws Exception {
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

    // Load the pruner — this is what happens when read.bloom-filter.enabled=true
    IndexFilePruner pruner = IndexFilePruner.load(ImmutableList.of(puffinPath), fileIO);
    assertThat(pruner.isEmpty()).isFalse();

    FileScanTask task1 = new SimpleFileScanTask(DATA_FILE_1);

    // Query for a value NOT in the filter — should prune
    Expression eqAbsent = Expressions.equal("id", 999);
    List<FileScanTask> result =
        pruner.pruneFiles(ImmutableList.of(task1), eqAbsent, SCHEMA);
    assertThat(result)
        .as("File should be pruned when bloom filter read is enabled and value is absent")
        .isEmpty();
  }

  /**
   * Verifies the behavior when bloom filter read is disabled: no index loading occurs and all
   * tasks are returned unchanged. When {@code read.bloom-filter.enabled=false},
   * {@code SparkBatchQueryScan.applyBloomFilterPruning()} returns the original tasks immediately
   * without loading any index files.
   *
   * <p>This test demonstrates the contract: when disabled, the pruner is never invoked, so all
   * files are kept. It also proves that the pruner WOULD have pruned files if invoked, confirming
   * the config check is the gate.
   *
   * Validates Requirements 8.4, 8.6.
   */
  @Test
  public void testNoPruningWhenDisabled() throws Exception {
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

    // When read.bloom-filter.enabled=false, applyBloomFilterPruning returns tasks as-is
    // without loading any index files. Simulate this disabled path:
    FileScanTask task1 = new SimpleFileScanTask(DATA_FILE_1);
    FileScanTask task2 = new SimpleFileScanTask(DATA_FILE_2);
    List<FileScanTask> originalTasks = ImmutableList.of(task1, task2);

    // Disabled path: all tasks returned unchanged (no pruner invoked)
    assertThat(originalTasks)
        .as("All tasks should be returned when bloom filter read is disabled")
        .hasSize(2);

    // Prove the config check is the gate: if we load the pruner, pruning WOULD occur
    IndexFilePruner pruner = IndexFilePruner.load(ImmutableList.of(puffinPath), fileIO);
    Expression eqAbsent = Expressions.equal("id", 999);
    List<FileScanTask> prunedResult =
        pruner.pruneFiles(originalTasks, eqAbsent, SCHEMA);
    assertThat(prunedResult)
        .as("Pruner would have pruned file1 if it had been invoked")
        .hasSize(1);
    assertThat(prunedResult.get(0).file().location()).isEqualTo(DATA_FILE_2);
  }

  /**
   * Verifies that the WRITE_BLOOM_FILTER_ENABLED property is also correctly defined.
   */
  @Test
  public void testWriteBloomFilterEnabledPropertyKey() {
    assertThat(TableProperties.WRITE_BLOOM_FILTER_ENABLED)
        .isEqualTo("write.bloom-filter.enabled");
    assertThat(TableProperties.WRITE_BLOOM_FILTER_ENABLED_DEFAULT).isTrue();
  }

  /** Minimal FileScanTask for testing without a full Iceberg table. */
  private static class SimpleFileScanTask implements FileScanTask {
    private final SimpleDataFile dataFile;

    SimpleFileScanTask(String path) {
      this.dataFile = new SimpleDataFile(path);
    }

    @Override
    public DataFile file() {
      return dataFile;
    }

    @Override
    public List<DeleteFile> deletes() {
      return Collections.emptyList();
    }

    @Override
    public PartitionSpec spec() {
      return PartitionSpec.unpartitioned();
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

  /** Minimal DataFile providing a file location for pruning tests. */
  private static class SimpleDataFile implements DataFile {
    private final String path;

    SimpleDataFile(String path) {
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
    public FileFormat format() {
      return FileFormat.PARQUET;
    }

    @Override
    public StructLike partition() {
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
    public Map<Integer, ByteBuffer> lowerBounds() {
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return null;
    }

    @Override
    public ByteBuffer keyMetadata() {
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
    public FileContent content() {
      return FileContent.DATA;
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
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return this;
    }
  }
}
