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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IndexFilePruner}. */
public class TestIndexFilePruner {

  private static final long SNAPSHOT_ID = 100L;
  private static final long SEQUENCE_NUMBER = 1L;
  private static final int COLUMN_ID = 1;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(COLUMN_ID, "name", Types.StringType.get()),
          Types.NestedField.required(2, "id", Types.IntegerType.get()));

  private static final String DATA_FILE_1 = "s3://bucket/table/data/part-00000.parquet";
  private static final String DATA_FILE_2 = "s3://bucket/table/data/part-00001.parquet";
  private static final String DATA_FILE_3 = "s3://bucket/table/data/part-00002.parquet";

  private IndexSpec createIndexSpec() {
    return new IndexSpec(
        "bloom_1_test1234",
        BloomFilterFactory.TYPE,
        COLUMN_ID,
        ImmutableMap.of("fpp", "0.01"));
  }

  private DataFile createDataFile(String path) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path)
        .withFileSizeInBytes(1024)
        .withRecordCount(100)
        .build();
  }

  private FileScanTask createFileScanTask(String path) {
    DataFile dataFile = createDataFile(path);
    return new BaseFileScanTask(
        dataFile,
        new DeleteFile[0],
        org.apache.iceberg.SchemaParser.toJson(SCHEMA),
        org.apache.iceberg.PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
        ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));
  }

  private InMemoryOutputFile writePuffinWithValues(String dataFilePath, String... values)
      throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
      for (String value : values) {
        index.add(value, Types.StringType.get());
      }
      writer.add(index, dataFilePath);
      writer.finish();
    }

    return outputFile;
  }

  @Test
  public void pruneFileWhereBloomFilterSaysDefinitelyNotPresent() throws IOException {
    // Create a bloom filter with "alice" and "bob" for DATA_FILE_1
    InMemoryOutputFile puffinFile = writePuffinWithValues(DATA_FILE_1, "alice", "bob");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile.location(), puffinFile.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(puffinFile.location()), fileIO);

    List<FileScanTask> tasks = Collections.singletonList(createFileScanTask(DATA_FILE_1));

    // Query for "charlie" which is definitely not in the bloom filter
    Expression filter = Expressions.equal("name", "charlie");
    List<FileScanTask> result = pruner.pruneFiles(tasks, filter, SCHEMA);

    assertThat(result).isEmpty();
  }

  @Test
  public void keepFileWhereBloomFilterSaysPossiblyPresent() throws IOException {
    InMemoryOutputFile puffinFile = writePuffinWithValues(DATA_FILE_1, "alice", "bob");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile.location(), puffinFile.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(puffinFile.location()), fileIO);

    List<FileScanTask> tasks = Collections.singletonList(createFileScanTask(DATA_FILE_1));

    // Query for "alice" which IS in the bloom filter
    Expression filter = Expressions.equal("name", "alice");
    List<FileScanTask> result = pruner.pruneFiles(tasks, filter, SCHEMA);

    assertThat(result).hasSize(1);
  }

  @Test
  public void keepFileWithNoIndex() throws IOException {
    // Create index only for DATA_FILE_1
    InMemoryOutputFile puffinFile = writePuffinWithValues(DATA_FILE_1, "alice");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile.location(), puffinFile.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(puffinFile.location()), fileIO);

    // DATA_FILE_2 has no index
    List<FileScanTask> tasks =
        Arrays.asList(createFileScanTask(DATA_FILE_1), createFileScanTask(DATA_FILE_2));

    Expression filter = Expressions.equal("name", "charlie");
    List<FileScanTask> result = pruner.pruneFiles(tasks, filter, SCHEMA);

    // DATA_FILE_1 should be pruned (charlie not in bloom), DATA_FILE_2 kept (no index)
    assertThat(result).hasSize(1);
    assertThat(result.get(0).file().location()).isEqualTo(DATA_FILE_2);
  }

  @Test
  public void emptyTaskList() throws IOException {
    InMemoryOutputFile puffinFile = writePuffinWithValues(DATA_FILE_1, "alice");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile.location(), puffinFile.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(puffinFile.location()), fileIO);

    List<FileScanTask> result =
        pruner.pruneFiles(Collections.emptyList(), Expressions.equal("name", "alice"), SCHEMA);

    assertThat(result).isEmpty();
  }

  @Test
  public void emptyIndexFilesList() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    IndexFilePruner pruner = IndexFilePruner.load(Collections.emptyList(), fileIO);

    assertThat(pruner.isEmpty()).isTrue();
    assertThat(pruner.indexCount()).isEqualTo(0);

    // All tasks should be kept when there are no indexes
    List<FileScanTask> tasks =
        Arrays.asList(createFileScanTask(DATA_FILE_1), createFileScanTask(DATA_FILE_2));

    List<FileScanTask> result =
        pruner.pruneFiles(tasks, Expressions.equal("name", "alice"), SCHEMA);

    assertThat(result).hasSize(2);
  }

  @Test
  public void failSafeOnCorruptedPuffinFile() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String corruptPath = "s3://bucket/table/metadata/corrupt.puffin";
    fileIO.addFile(corruptPath, new byte[] {0, 1, 2, 3, 4, 5});

    // Should not throw, and should keep all files
    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(corruptPath), fileIO);

    assertThat(pruner.isEmpty()).isTrue();

    List<FileScanTask> tasks = Collections.singletonList(createFileScanTask(DATA_FILE_1));
    List<FileScanTask> result =
        pruner.pruneFiles(tasks, Expressions.equal("name", "alice"), SCHEMA);

    assertThat(result).hasSize(1);
  }

  @Test
  public void failSafeOnMissingPuffinFile() {
    InMemoryFileIO fileIO = new InMemoryFileIO();

    IndexFilePruner pruner =
        IndexFilePruner.load(
            Collections.singletonList("s3://bucket/table/metadata/missing.puffin"), fileIO);

    assertThat(pruner.isEmpty()).isTrue();

    List<FileScanTask> tasks = Collections.singletonList(createFileScanTask(DATA_FILE_1));
    List<FileScanTask> result =
        pruner.pruneFiles(tasks, Expressions.equal("name", "alice"), SCHEMA);

    assertThat(result).hasSize(1);
  }

  @Test
  public void pruneWithInPredicate() throws IOException {
    InMemoryOutputFile puffinFile = writePuffinWithValues(DATA_FILE_1, "alice", "bob");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile.location(), puffinFile.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(puffinFile.location()), fileIO);

    List<FileScanTask> tasks = Collections.singletonList(createFileScanTask(DATA_FILE_1));

    // IN predicate with values not in the bloom filter - should prune
    Expression filter = Expressions.in("name", "charlie", "dave");
    List<FileScanTask> result = pruner.pruneFiles(tasks, filter, SCHEMA);

    assertThat(result).isEmpty();
  }

  @Test
  public void keepWithInPredicateWhenAnyValueMightBePresent() throws IOException {
    InMemoryOutputFile puffinFile = writePuffinWithValues(DATA_FILE_1, "alice", "bob");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile.location(), puffinFile.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(puffinFile.location()), fileIO);

    List<FileScanTask> tasks = Collections.singletonList(createFileScanTask(DATA_FILE_1));

    // IN predicate where "alice" IS in the bloom filter - should keep
    Expression filter = Expressions.in("name", "alice", "charlie");
    List<FileScanTask> result = pruner.pruneFiles(tasks, filter, SCHEMA);

    assertThat(result).hasSize(1);
  }

  @Test
  public void pruneMultipleFiles() throws IOException {
    // DATA_FILE_1 has "alice", DATA_FILE_2 has "bob"
    InMemoryOutputFile puffinFile1 = writePuffinWithValues(DATA_FILE_1, "alice");
    InMemoryOutputFile puffinFile2 = writePuffinWithValues(DATA_FILE_2, "bob");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile1.location(), puffinFile1.toByteArray());
    fileIO.addFile(puffinFile2.location(), puffinFile2.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(
            Arrays.asList(puffinFile1.location(), puffinFile2.location()), fileIO);

    List<FileScanTask> tasks =
        Arrays.asList(
            createFileScanTask(DATA_FILE_1),
            createFileScanTask(DATA_FILE_2),
            createFileScanTask(DATA_FILE_3));

    // Query for "alice" - DATA_FILE_2 should be pruned, DATA_FILE_1 and DATA_FILE_3 kept
    Expression filter = Expressions.equal("name", "alice");
    List<FileScanTask> result = pruner.pruneFiles(tasks, filter, SCHEMA);

    assertThat(result).hasSize(2);
    assertThat(result)
        .extracting(task -> task.file().location())
        .containsExactly(DATA_FILE_1, DATA_FILE_3);
  }

  @Test
  public void nullTaskListReturnsEmpty() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    IndexFilePruner pruner = IndexFilePruner.load(Collections.emptyList(), fileIO);

    List<FileScanTask> result =
        pruner.pruneFiles(null, Expressions.equal("name", "alice"), SCHEMA);

    assertThat(result).isEmpty();
  }

  @Test
  public void indexCountReflectsLoadedIndexes() throws IOException {
    InMemoryOutputFile puffinFile =
        writePuffinWithValues(DATA_FILE_1, "alice");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile.location(), puffinFile.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(Collections.singletonList(puffinFile.location()), fileIO);

    assertThat(pruner.indexCount()).isEqualTo(1);
    assertThat(pruner.isEmpty()).isFalse();
  }
  @Test
  public void filesPrunedTracksCount() throws IOException {
    // DATA_FILE_1 has "alice", DATA_FILE_2 has "bob"
    InMemoryOutputFile puffinFile1 = writePuffinWithValues(DATA_FILE_1, "alice");
    InMemoryOutputFile puffinFile2 = writePuffinWithValues(DATA_FILE_2, "bob");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile1.location(), puffinFile1.toByteArray());
    fileIO.addFile(puffinFile2.location(), puffinFile2.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(
            Arrays.asList(puffinFile1.location(), puffinFile2.location()), fileIO);

    // Before any pruning, filesPruned should be 0
    assertThat(pruner.filesPruned()).isEqualTo(0);

    List<FileScanTask> tasks =
        Arrays.asList(
            createFileScanTask(DATA_FILE_1),
            createFileScanTask(DATA_FILE_2),
            createFileScanTask(DATA_FILE_3));

    // Query for "alice" - DATA_FILE_2 should be pruned (bob != alice)
    Expression filter = Expressions.equal("name", "alice");
    pruner.pruneFiles(tasks, filter, SCHEMA);

    assertThat(pruner.filesPruned()).isEqualTo(1);
  }

  @Test
  public void filesPrunedResetsOnEachCall() throws IOException {
    InMemoryOutputFile puffinFile1 = writePuffinWithValues(DATA_FILE_1, "alice");
    InMemoryOutputFile puffinFile2 = writePuffinWithValues(DATA_FILE_2, "bob");

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(puffinFile1.location(), puffinFile1.toByteArray());
    fileIO.addFile(puffinFile2.location(), puffinFile2.toByteArray());

    IndexFilePruner pruner =
        IndexFilePruner.load(
            Arrays.asList(puffinFile1.location(), puffinFile2.location()), fileIO);

    List<FileScanTask> tasks =
        Arrays.asList(createFileScanTask(DATA_FILE_1), createFileScanTask(DATA_FILE_2));

    // First call: query for "charlie" - both files pruned
    pruner.pruneFiles(tasks, Expressions.equal("name", "charlie"), SCHEMA);
    assertThat(pruner.filesPruned()).isEqualTo(2);

    // Second call: query for "alice" - only DATA_FILE_2 pruned
    pruner.pruneFiles(tasks, Expressions.equal("name", "alice"), SCHEMA);
    assertThat(pruner.filesPruned()).isEqualTo(1);
  }

  @Test
  public void filesPrunedZeroWhenNoIndexes() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    IndexFilePruner pruner = IndexFilePruner.load(Collections.emptyList(), fileIO);

    List<FileScanTask> tasks =
        Arrays.asList(createFileScanTask(DATA_FILE_1), createFileScanTask(DATA_FILE_2));

    pruner.pruneFiles(tasks, Expressions.equal("name", "alice"), SCHEMA);

    assertThat(pruner.filesPruned()).isEqualTo(0);
  }
}
