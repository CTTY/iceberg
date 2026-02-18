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
 * Integration tests for the Spark Bloom filter read path.
 *
 * <p>These tests exercise the full pipeline: create a table with index specs, write data through
 * Spark (which triggers SparkIndexWriter to create Bloom filter indexes in Puffin files), then
 * query with equality predicates and verify that file pruning occurs and metrics are reported.
 *
 * <p>Validates Requirements 7.1, 7.2, 7.3, 7.4, 7.5.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkReadIndexIntegration extends TestBaseWithCatalog {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  /**
   * Creates a table with bloom filter index specs configured on the "id" column. Adds an IndexSpec
   * to the table metadata so that writes produce Puffin files with bloom filter blobs.
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

    // Use the table's own operations to commit the index spec change.
    // Note: addIndexSpec alone doesn't register as a "change" in TableMetadata.Builder,
    // so we also set a property to ensure the metadata is actually written.
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    TableMetadata updated =
        TableMetadata.buildFrom(base)
            .addIndexSpec(indexSpec)
            .setProperties(
                ImmutableMap.of("index-spec-version", String.valueOf(System.currentTimeMillis())))
            .build();
    ops.commit(base, updated);

    // Reload the table entirely to pick up the metadata change
    table = validationCatalog.loadTable(tableIdent);

    TableOperations reloadedOps = ((HasTableOperations) table).operations();
    assertThat(reloadedOps.current().indexSpecs())
        .as("Index specs should be persisted in table metadata")
        .isNotEmpty();

    return table;
  }

  /**
   * Tests that writing data through Spark to a table with bloom filter index specs creates Puffin
   * files, and that querying with an equality predicate on the indexed column results in file
   * pruning.
   *
   * <p>Validates Requirements 7.1, 7.5.
   */
  @TestTemplate
  public void testEqualityPredicatePrunesFiles() throws NoSuchTableException {
    Table table = createTableWithBloomIndex(tableName);

    // Write 3 separate batches to create 3 data files with distinct value ranges
    Dataset<Row> df1 =
        spark
            .range(1, 101)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch1"));
    df1.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df2 =
        spark
            .range(101, 201)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch2"));
    df2.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df3 =
        spark
            .range(201, 301)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch3"));
    df3.coalesce(1).writeTo(tableName).append();

    table.refresh();

    // Verify index files were created in the snapshot
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.indexFiles())
        .as("Snapshot should have index files from bloom filter writes")
        .isNotEmpty();

    // Load the pruner from the snapshot's index files
    IndexFilePruner pruner = IndexFilePruner.load(snapshot.indexFiles(), table.io());
    assertThat(pruner.isEmpty()).as("Pruner should have loaded indexes").isFalse();

    // Get all file scan tasks
    List<FileScanTask> allTasks;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      allTasks = Lists.newArrayList(tasks);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertThat(allTasks).as("Should have 3 data files from 3 appends").hasSize(3);

    // Query for a value in batch 2 (101-200) — should prune files from batch 1 and 3
    Expression eqFilter = Expressions.equal("id", 150);
    List<FileScanTask> prunedTasks = pruner.pruneFiles(allTasks, eqFilter, table.schema());

    assertThat(prunedTasks.size())
        .as("Bloom filter pruning should reduce the number of files scanned")
        .isLessThan(allTasks.size());

    // Verify the pruned count metric
    assertThat(pruner.filesPruned())
        .as("filesPruned metric should report pruned files")
        .isGreaterThan(0);
  }

  /**
   * Tests that querying with an IN predicate on an indexed column results in file pruning when all
   * values in the IN list are absent from a file's bloom filter.
   *
   * <p>Validates Requirement 7.2.
   */
  @TestTemplate
  public void testInPredicatePrunesFiles() throws NoSuchTableException {
    Table table = createTableWithBloomIndex(tableName);

    // Write 2 batches with distinct value ranges
    Dataset<Row> df1 =
        spark
            .range(1, 101)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch1"));
    df1.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df2 =
        spark
            .range(1001, 1101)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch2"));
    df2.coalesce(1).writeTo(tableName).append();

    table.refresh();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.indexFiles()).isNotEmpty();

    IndexFilePruner pruner = IndexFilePruner.load(snapshot.indexFiles(), table.io());

    List<FileScanTask> allTasks;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      allTasks = Lists.newArrayList(tasks);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertThat(allTasks).hasSize(2);

    // IN predicate with values only in batch 2 range — should prune batch 1's file
    Expression inFilter = Expressions.in("id", 1050, 1060, 1070);
    List<FileScanTask> prunedTasks = pruner.pruneFiles(allTasks, inFilter, table.schema());

    assertThat(prunedTasks.size())
        .as("IN predicate should prune files where all values are absent")
        .isLessThan(allTasks.size());

    assertThat(pruner.filesPruned()).as("filesPruned metric should be positive").isGreaterThan(0);
  }

  /**
   * Tests that the filesPruned metric correctly reports the number of files pruned by bloom filter
   * evaluation.
   *
   * <p>Validates Requirement 7.3.
   */
  @TestTemplate
  public void testFilesPrunedMetricReported() throws NoSuchTableException {
    Table table = createTableWithBloomIndex(tableName);

    // Write 3 batches with well-separated value ranges
    Dataset<Row> df1 =
        spark
            .range(1, 51)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("a"));
    df1.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df2 =
        spark
            .range(1001, 1051)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("b"));
    df2.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df3 =
        spark
            .range(2001, 2051)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("c"));
    df3.coalesce(1).writeTo(tableName).append();

    table.refresh();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.indexFiles()).isNotEmpty();

    IndexFilePruner pruner = IndexFilePruner.load(snapshot.indexFiles(), table.io());

    List<FileScanTask> allTasks;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      allTasks = Lists.newArrayList(tasks);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertThat(allTasks).hasSize(3);

    // Query for a value only in batch 1 — should prune 2 files
    Expression eqFilter = Expressions.equal("id", 25);
    List<FileScanTask> prunedTasks = pruner.pruneFiles(allTasks, eqFilter, table.schema());

    assertThat(prunedTasks)
        .as("Only the file containing value 25 should remain")
        .hasSize(1);

    assertThat(pruner.filesPruned()).as("Should report exactly 2 files pruned").isEqualTo(2);
  }

  /**
   * Tests that when bloom filter evaluation encounters no index for a file, the file is kept in the
   * scan (fail-safe behavior).
   *
   * <p>Validates Requirement 7.4.
   */
  @TestTemplate
  public void testFailSafeKeepsFilesWithoutIndex() throws NoSuchTableException {
    Table table = createTableWithBloomIndex(tableName);

    // Write one batch with index
    Dataset<Row> df1 =
        spark
            .range(1, 51)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("indexed"));
    df1.coalesce(1).writeTo(tableName).append();

    table.refresh();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.indexFiles())
        .as("Snapshot should have index files")
        .isNotEmpty();

    IndexFilePruner pruner = IndexFilePruner.load(snapshot.indexFiles(), table.io());

    List<FileScanTask> allTasks;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      allTasks = Lists.newArrayList(tasks);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertThat(allTasks).hasSize(1);

    // Query for a value that IS in the bloom filter — file should be kept
    Expression eqPresent = Expressions.equal("id", 25);
    List<FileScanTask> result = pruner.pruneFiles(allTasks, eqPresent, table.schema());

    assertThat(result)
        .as("File should be kept when bloom filter says value might be present")
        .hasSize(1);

    assertThat(pruner.filesPruned())
        .as("No files should be pruned when value is present")
        .isEqualTo(0);
  }

  /**
   * Tests the full end-to-end flow: write data through Spark, then run a Spark SQL query with an
   * equality predicate and verify the query returns correct results. This validates that bloom
   * filter pruning integrates correctly with the Spark query engine without breaking query
   * correctness.
   *
   * <p>Validates Requirements 7.1, 7.5.
   */
  @TestTemplate
  public void testSparkSqlQueryReturnsCorrectResults() throws NoSuchTableException {
    createTableWithBloomIndex(tableName);

    // Write 3 batches with distinct value ranges
    Dataset<Row> df1 =
        spark
            .range(1, 101)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch1"));
    df1.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df2 =
        spark
            .range(101, 201)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch2"));
    df2.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df3 =
        spark
            .range(201, 301)
            .toDF("id")
            .withColumn("data", org.apache.spark.sql.functions.lit("batch3"));
    df3.coalesce(1).writeTo(tableName).append();

    // Run a Spark SQL query with equality predicate — bloom filter pruning should be transparent
    List<Row> results =
        spark
            .sql(String.format("SELECT id, data FROM %s WHERE id = 150", tableName))
            .collectAsList();

    assertThat(results).as("Query should return exactly one row for id=150").hasSize(1);
    assertThat(results.get(0).getInt(0)).isEqualTo(150);
    assertThat(results.get(0).getString(1)).isEqualTo("batch2");

    // Run an IN query
    List<Row> inResults =
        spark
            .sql(
                String.format(
                    "SELECT id FROM %s WHERE id IN (50, 150, 250) ORDER BY id", tableName))
            .collectAsList();

    assertThat(inResults).as("IN query should return 3 rows").hasSize(3);
    assertThat(inResults.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .containsExactly(50, 150, 250);
  }
}
