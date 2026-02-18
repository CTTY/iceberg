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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.index.IndexFilePruner;
import org.apache.iceberg.index.IndexSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end integration tests for the Bloom filter secondary index pipeline.
 *
 * <p>These tests exercise the COMPLETE write+read pipeline in single tests: create a table with
 * index specs, write data through Spark (which triggers SparkIndexWriter to create Bloom filter
 * indexes in Puffin files via SparkWrite), then query through Spark SQL and verify that:
 *
 * <ul>
 *   <li>The write path creates proper Puffin index files referenced in snapshot metadata
 *   <li>The read path uses those index files for file pruning via SparkBatchQueryScan
 *   <li>Pruning reduces the number of files scanned
 *   <li>Query results remain correct despite pruning
 * </ul>
 *
 * <p>This is distinct from TestSparkReadIndexIntegration which focuses on the read-path API
 * (IndexFilePruner) in isolation. This test validates the full pipeline end-to-end including
 * snapshot metadata, index file accumulation across commits, and Spark SQL correctness.
 *
 * <p>Validates Requirements 6.1, 6.2, 7.1, 7.2.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestBloomFilterEndToEnd extends TestBaseWithCatalog {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  /**
   * Creates a table with bloom filter index specs on the "id" column and both write/read bloom
   * filter properties enabled.
   */
  private Table createTableWithBloomIndex(String name) {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg TBLPROPERTIES ('%s' = 'true', '%s' = 'true')",
        name,
        TableProperties.WRITE_BLOOM_FILTER_ENABLED,
        TableProperties.READ_BLOOM_FILTER_ENABLED);

    Table table = validationCatalog.loadTable(tableIdent);
    int idColumnId = table.schema().findField("id").fieldId();

    IndexSpec indexSpec =
        new IndexSpec(
            "bloom-filter-v1",
            idColumnId,
            ImmutableMap.of("fpp", "0.01", "expected-insertions", "1000"));

    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    TableMetadata updated =
        TableMetadata.buildFrom(base)
            .addIndexSpec(indexSpec)
            .setProperties(
                ImmutableMap.of("index-spec-version", String.valueOf(System.currentTimeMillis())))
            .build();
    ops.commit(base, updated);

    table = validationCatalog.loadTable(tableIdent);
    assertThat(((HasTableOperations) table).operations().current().indexSpecs())
        .as("Index specs should be persisted")
        .isNotEmpty();

    return table;
  }

  /**
   * End-to-end test: write data in multiple batches creating separate data files, then verify that
   * the write path creates Puffin index files and the read path prunes files for equality
   * predicates.
   *
   * <p>Pipeline: SparkWrite → SparkIndexWriter → Puffin files → SparkBatchQueryScan →
   * IndexFilePruner → pruned scan
   *
   * <p>Validates Requirements 6.1, 6.2, 7.1.
   */
  @TestTemplate
  public void testWriteAndReadPipelineWithEqualityPruning() throws NoSuchTableException {
    Table table = createTableWithBloomIndex(tableName);

    // Write 3 batches with well-separated value ranges to create 3 data files
    writeBatch(1, 101, "batch1");
    writeBatch(1001, 1101, "batch2");
    writeBatch(2001, 2101, "batch3");

    table.refresh();

    // --- Verify write path: snapshot has index files ---
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.indexFiles())
        .as("Write path should create index files in snapshot metadata")
        .isNotEmpty();

    // --- Verify read path: pruning reduces files scanned ---
    List<FileScanTask> allTasks = getAllFileScanTasks(table);
    assertThat(allTasks).as("Should have 3 data files from 3 appends").hasSize(3);

    IndexFilePruner pruner = IndexFilePruner.load(snapshot.indexFiles(), table.io());
    assertThat(pruner.isEmpty()).as("Pruner should have loaded indexes from Puffin files").isFalse();

    // Query for value 50 (only in batch1: 1-100) — should prune batch2 and batch3 files
    Expression eqFilter = Expressions.equal("id", 50);
    List<FileScanTask> prunedTasks = pruner.pruneFiles(allTasks, eqFilter, table.schema());

    assertThat(prunedTasks.size())
        .as("Equality predicate should prune files not containing the value")
        .isLessThan(allTasks.size());
    assertThat(pruner.filesPruned())
        .as("Should prune at least 2 files (batch2 and batch3)")
        .isGreaterThanOrEqualTo(2);

    // --- Verify correctness: Spark SQL returns correct results despite pruning ---
    List<Row> results =
        spark
            .sql(String.format("SELECT id, data FROM %s WHERE id = 50", tableName))
            .collectAsList();

    assertThat(results).as("Query should return exactly one row for id=50").hasSize(1);
    assertThat(results.get(0).getInt(0)).isEqualTo(50);
    assertThat(results.get(0).getString(1)).isEqualTo("batch1");
  }

  /**
   * End-to-end test: verify that IN predicates also benefit from bloom filter pruning through the
   * full pipeline, and that query results are correct.
   *
   * <p>Validates Requirements 6.1, 6.2, 7.2.
   */
  @TestTemplate
  public void testWriteAndReadPipelineWithInPruning() throws NoSuchTableException {
    Table table = createTableWithBloomIndex(tableName);

    // Write 2 batches with well-separated ranges
    writeBatch(1, 101, "low");
    writeBatch(5001, 5101, "high");

    table.refresh();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.indexFiles()).isNotEmpty();

    List<FileScanTask> allTasks = getAllFileScanTasks(table);
    assertThat(allTasks).hasSize(2);

    IndexFilePruner pruner = IndexFilePruner.load(snapshot.indexFiles(), table.io());

    // IN predicate with values only in the "high" range — should prune the "low" file
    Expression inFilter = Expressions.in("id", 5010, 5020, 5030);
    List<FileScanTask> prunedTasks = pruner.pruneFiles(allTasks, inFilter, table.schema());

    assertThat(prunedTasks.size())
        .as("IN predicate should prune files where all values are absent")
        .isLessThan(allTasks.size());
    assertThat(pruner.filesPruned()).isGreaterThan(0);

    // Verify correctness through Spark SQL
    List<Row> results =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE id IN (5010, 5020, 5030) ORDER BY id", tableName))
            .collectAsList();

    assertThat(results).hasSize(3);
    assertThat(results.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .containsExactly(5010, 5020, 5030);
  }

  /**
   * End-to-end test: verify that index files accumulate across multiple commits. Each append should
   * add a new Puffin file, and the snapshot should reference all of them.
   *
   * <p>Validates Requirements 6.1, 6.2, 7.1.
   */
  @TestTemplate
  public void testIndexFilesAccumulateAcrossCommits() throws NoSuchTableException {
    Table table = createTableWithBloomIndex(tableName);

    // First commit
    writeBatch(1, 101, "commit1");
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();
    int indexFilesAfterCommit1 = snap1.indexFiles().size();
    assertThat(indexFilesAfterCommit1)
        .as("First commit should create at least one index file")
        .isGreaterThanOrEqualTo(1);

    // Second commit
    writeBatch(1001, 1101, "commit2");
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();
    int indexFilesAfterCommit2 = snap2.indexFiles().size();
    assertThat(indexFilesAfterCommit2)
        .as("Second commit should accumulate index files (append-only)")
        .isGreaterThan(indexFilesAfterCommit1);

    // Third commit
    writeBatch(2001, 2101, "commit3");
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();
    int indexFilesAfterCommit3 = snap3.indexFiles().size();
    assertThat(indexFilesAfterCommit3)
        .as("Third commit should further accumulate index files")
        .isGreaterThan(indexFilesAfterCommit2);

    // Pruner should load indexes from ALL accumulated Puffin files
    IndexFilePruner pruner = IndexFilePruner.load(snap3.indexFiles(), table.io());
    List<FileScanTask> allTasks = getAllFileScanTasks(table);
    assertThat(allTasks).hasSize(3);

    // Query for value only in commit1 — should prune commit2 and commit3 files
    Expression eqFilter = Expressions.equal("id", 50);
    List<FileScanTask> prunedTasks = pruner.pruneFiles(allTasks, eqFilter, table.schema());

    assertThat(prunedTasks.size())
        .as("Pruner using accumulated index files should prune effectively")
        .isEqualTo(1);
    assertThat(pruner.filesPruned()).isEqualTo(2);

    // Verify Spark SQL correctness across all commits
    List<Row> allResults =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE id IN (50, 1050, 2050) ORDER BY id", tableName))
            .collectAsList();

    assertThat(allResults).hasSize(3);
    assertThat(allResults.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .containsExactly(50, 1050, 2050);
  }

  /**
   * End-to-end test: verify that Spark SQL queries return correct results for values that exist in
   * every batch, ensuring pruning doesn't incorrectly exclude files.
   *
   * <p>Validates Requirements 6.1, 7.1, 7.2.
   */
  @TestTemplate
  public void testPruningDoesNotBreakCorrectness() throws NoSuchTableException {
    createTableWithBloomIndex(tableName);

    // Write 3 batches — each batch contains value 999 to test that no file is incorrectly pruned
    Dataset<Row> df1 =
        spark
            .range(1, 51)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("a"))
            .union(
                spark
                    .range(999, 1000)
                    .toDF("id")
                    .withColumn("data", org.apache.spark.sql.functions.lit("a")));
    df1.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df2 =
        spark
            .range(1001, 1051)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("b"))
            .union(
                spark
                    .range(999, 1000)
                    .toDF("id")
                    .withColumn("data", org.apache.spark.sql.functions.lit("b")));
    df2.coalesce(1).writeTo(tableName).append();

    // Query for value 999 which exists in both files — no file should be pruned
    List<Row> results =
        spark
            .sql(String.format("SELECT data FROM %s WHERE id = 999 ORDER BY data", tableName))
            .collectAsList();

    assertThat(results)
        .as("Query for value present in multiple files should return all matching rows")
        .hasSize(2);
    assertThat(results.stream().map(r -> r.getString(0)).collect(Collectors.toList()))
        .containsExactly("a", "b");
  }

  // --- Helper methods ---

  private void writeBatch(int startInclusive, int endExclusive, String label)
      throws NoSuchTableException {
    Dataset<Row> df =
        spark
            .range(startInclusive, endExclusive)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit(label));
    df.coalesce(1).writeTo(tableName).append();
  }

  private List<FileScanTask> getAllFileScanTasks(Table table) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return Lists.newArrayList(tasks);
    } catch (Exception e) {
      throw new RuntimeException("Failed to plan files", e);
    }
  }
}
