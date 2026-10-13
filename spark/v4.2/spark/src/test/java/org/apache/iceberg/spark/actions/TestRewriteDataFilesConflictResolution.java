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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.FileRewritePlan;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.CompactionMap;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises rebasing of deletion vectors that are committed while a bin-pack rewrite is running.
 *
 * <p>The interleaving under test is driven explicitly rather than with threads:
 *
 * <ol>
 *   <li>write the source files (S0)
 *   <li>plan and execute the rewrite against S0, producing the output files and the compaction map
 *       without committing
 *   <li>commit a deletion vector against a source file (S1)
 *   <li>commit the rewrite with conflict resolution enabled
 * </ol>
 */
public class TestRewriteDataFilesConflictResolution extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final int MAX_DELETE_FILES = 100;
  private static final int MAX_RETRIES = 3;

  @TempDir private File tableDir;
  private String tableLocation;

  @BeforeAll
  public static void setupSpark() {
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  @AfterEach
  public void resetConf() {
    spark.conf().unset(SparkSQLProperties.COMPACTION_RESOLVE_POSITION_DELETE_CONFLICTS_ENABLED);
  }

  /**
   * The core scenario: D0 at S0, D1 and the map prepared from S0, DV0(D0) committed at S1, then the
   * prepared rewrite commits on top of S1.
   */
  @Test
  public void testRebaseDeletionVectorCommittedDuringRewrite() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 2, 200);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    List<DataFile> sourceFiles = dataFiles(table);
    assertThat(sourceFiles).hasSize(2);
    Map<String, Map<Long, String>> keysBySourcePosition = keysByFileAndPosition(table);

    // prepare D1 and the compaction map based on S0 (not committed)
    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);
    Set<DataFile> preparedFiles = addedFiles(groups);
    assertThat(preparedFiles).isNotEmpty();
    assertThat(groups).allSatisfy(group -> assertThat(group.positionRuns()).isNotEmpty());

    // concurrent transaction commits deletes against both source files -> S1
    DataFile d0 = sourceFiles.get(0);
    DataFile d1 = sourceFiles.get(1);
    List<Long> deletedInD0 = Lists.newArrayList(0L, 7L, 13L, 42L);
    List<Long> deletedInD1 = Lists.newArrayList(3L, 4L, 5L, 49L);
    commitDV(table, d0, deletedInD0);
    commitDV(table, d1, deletedInD1);
    Set<String> concurrentDVs =
        deleteFiles(table).stream().map(ContentFile::location).collect(Collectors.toSet());
    assertThat(concurrentDVs).hasSize(2);

    Set<String> expectedDeletedKeys = Sets.newHashSet();
    deletedInD0.forEach(
        pos -> expectedDeletedKeys.add(keysBySourcePosition.get(d0.location()).get(pos)));
    deletedInD1.forEach(
        pos -> expectedDeletedKeys.add(keysBySourcePosition.get(d1.location()).get(pos)));
    Set<String> expectedKeys = allKeys(keysBySourcePosition);
    expectedKeys.removeAll(expectedDeletedKeys);

    // the prepared rewrite commits with conflict resolution
    commitManager(table, startingSnapshotId, true).commitFileGroups(groups);

    table.refresh();
    Snapshot committed = table.currentSnapshot();
    assertThat(committed.operation()).isEqualTo("replace");

    // no row resurrection, no row loss
    assertThat(currentKeys(table)).isEqualTo(expectedKeys);

    // D1 is committed as prepared, not rewritten again; D0 is gone
    Set<String> currentDataFiles =
        dataFiles(table).stream().map(ContentFile::location).collect(Collectors.toSet());
    assertThat(currentDataFiles)
        .isEqualTo(preparedFiles.stream().map(ContentFile::location).collect(Collectors.toSet()));

    // the only live deletes are remapped DVs against D1, one per target file, and DV0 is gone
    List<DeleteFile> liveDeletes = deleteFiles(table);
    assertThat(liveDeletes)
        .isNotEmpty()
        .allSatisfy(dv -> assertThat(ContentFileUtil.isDV(dv)).isTrue());
    assertThat(
            liveDeletes.stream().map(DeleteFile::referencedDataFile).collect(Collectors.toList()))
        .doesNotHaveDuplicates()
        .allSatisfy(referenced -> assertThat(currentDataFiles).contains(referenced));
    assertThat(liveDeletes.stream().map(ContentFile::location))
        .doesNotContainAnyElementsOf(concurrentDVs);
    long remappedPositions = liveDeletes.stream().mapToLong(ContentFile::recordCount).sum();
    assertThat(remappedPositions).isEqualTo(deletedInD0.size() + deletedInD1.size());

    // the compaction map describes the actual layout: every surviving source row is found at the
    // mapped target position with the same key
    assertMapMatchesLayout(table, groups, keysBySourcePosition, expectedDeletedKeys);
  }

  @Test
  public void testConcurrentDeletionVectorFailsWithoutResolution() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 2, 200);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    DataFile d0 = dataFiles(table).get(0);

    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);
    commitDV(table, d0, Lists.newArrayList(1L, 2L));

    // upstream behavior: the rewrite cannot commit on top of a new delete for a replaced file
    assertThatThrownBy(
            () -> commitManager(table, startingSnapshotId, false).commitFileGroups(groups))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("found new position delete for replaced data file");
  }

  /**
   * The planner saw DV_old on D0 at S0 and the rewrite applied it. The concurrent writer replaces
   * DV_old with DV_new (a superset, as v3 requires). The commit must drop the stale planned
   * removal, remove DV_new and only rebase the positions the rewrite did not already apply.
   */
  @Test
  public void testConcurrentDeleteReplacesPlannedDanglingDV() throws IOException {
    Table table = createTable(3);
    // the bin-pack planner only rewrites groups with more than one input file
    writeRecords(table, 2, 200);
    DataFile d0 = dataFiles(table).get(0);
    Map<String, Map<Long, String>> keysBySourcePosition = keysByFileAndPosition(table);

    List<Long> oldDeletes = Lists.newArrayList(10L, 20L, 30L);
    commitDV(table, d0, oldDeletes);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    DeleteFile dvOld = Iterables.getOnlyElement(deleteFiles(table));

    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);
    assertThat(Iterables.getOnlyElement(groups).danglingDVs().stream().map(ContentFile::location))
        .containsExactly(dvOld.location());

    // v3: a new DV for the same file replaces the old one and carries all its positions
    List<Long> newDeletes = Lists.newArrayList(10L, 20L, 30L, 40L, 50L);
    commitDV(table, d0, newDeletes);
    assertThat(deleteFiles(table).stream().map(ContentFile::location))
        .hasSize(1)
        .doesNotContain(dvOld.location());

    commitManager(table, startingSnapshotId, true).commitFileGroups(groups);

    Set<String> expectedKeys = allKeys(keysBySourcePosition);
    newDeletes.forEach(
        pos -> expectedKeys.remove(keysBySourcePosition.get(d0.location()).get(pos)));
    assertThat(currentKeys(table)).isEqualTo(expectedKeys);

    // only the two positions that the rewrite had not applied are carried over
    DeleteFile remapped = Iterables.getOnlyElement(deleteFiles(table));
    assertThat(remapped.recordCount()).isEqualTo(2);
    assertThat(remapped.referencedDataFile())
        .isEqualTo(Iterables.getOnlyElement(addedFiles(groups)).location());
  }

  /**
   * A second writer lands between conflict detection and the commit attempt; the commit retries.
   */
  @Test
  public void testDeleteArrivingBetweenDetectionAndCommit() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 2, 200);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    List<DataFile> sourceFiles = dataFiles(table);
    DataFile d0 = sourceFiles.get(0);
    DataFile d1 = sourceFiles.get(1);
    Map<String, Map<Long, String>> keysBySourcePosition = keysByFileAndPosition(table);

    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);

    List<Long> firstDeletes = Lists.newArrayList(1L, 2L);
    commitDV(table, d0, firstDeletes);

    List<Long> secondDeletes = Lists.newArrayList(5L, 6L);
    int[] hookCalls = new int[1];
    RewriteDataFilesCommitManager commitManager = commitManager(table, startingSnapshotId, true);
    commitManager.beforeCommitAttempt(
        () -> {
          hookCalls[0]++;
          if (hookCalls[0] == 1) {
            // sneak a second transaction in after the first detection pass
            commitDV(table, d1, secondDeletes);
          }
        });

    commitManager.commitFileGroups(groups);

    assertThat(hookCalls[0]).as("first attempt must fail validation and retry").isEqualTo(2);

    Set<String> expectedKeys = allKeys(keysBySourcePosition);
    firstDeletes.forEach(
        pos -> expectedKeys.remove(keysBySourcePosition.get(d0.location()).get(pos)));
    secondDeletes.forEach(
        pos -> expectedKeys.remove(keysBySourcePosition.get(d1.location()).get(pos)));
    assertThat(currentKeys(table)).isEqualTo(expectedKeys);
    assertThat(deleteFiles(table).stream().mapToLong(ContentFile::recordCount).sum()).isEqualTo(4);
  }

  /**
   * A validation failure that is not explained by a new snapshot cannot be fixed by resolving
   * again, so the loop must propagate it instead of burning the remaining retries.
   */
  @Test
  public void testNoProgressFailurePropagatesWithoutRetry() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 2, 200);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    DataFile d0 = dataFiles(table).get(0);

    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);
    commitDV(table, d0, Lists.newArrayList(1L, 2L));

    int[] attempts = new int[1];
    RewriteDataFilesCommitManager commitManager = commitManager(table, startingSnapshotId, true);
    commitManager.beforeCommitAttempt(
        () -> {
          attempts[0]++;
          // fail the attempt without committing anything: the table does not move
          throw new ValidationException("injected failure without a new snapshot");
        });

    assertThatThrownBy(() -> commitManager.commitFileGroups(groups))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("injected failure");

    assertThat(attempts[0])
        .as("second attempt must stop before committing because nothing changed")
        .isEqualTo(1);
    assertThat(table.currentSnapshot().operation()).isEqualTo("delete");
  }

  /** Each partition is its own file group; runs and remapped DVs must stay partition-aligned. */
  @Test
  public void testPartitionedTable() throws IOException {
    Table table = createPartitionedTable(3);
    // 4 partitions x 2 files each
    writePartitionedRecords(table, 2, 400, 4);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    List<DataFile> sourceFiles = dataFiles(table);
    assertThat(sourceFiles).hasSize(8);
    Map<String, Map<Long, String>> keysBySourcePosition = keysByFileAndPosition(table);

    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);
    assertThat(groups).hasSize(4);
    Set<DataFile> preparedFiles = addedFiles(groups);
    assertThat(preparedFiles).hasSize(4);

    // deletes in two different partitions
    DataFile inPartitionA = sourceFiles.get(0);
    DataFile inPartitionB =
        sourceFiles.stream()
            .filter(f -> !f.partition().equals(inPartitionA.partition()))
            .findFirst()
            .orElseThrow();
    List<Long> deletedInA = Lists.newArrayList(0L, 3L, 17L);
    List<Long> deletedInB = Lists.newArrayList(5L, 6L);
    commitDV(table, inPartitionA, deletedInA);
    commitDV(table, inPartitionB, deletedInB);

    Set<String> expectedDeletedKeys = Sets.newHashSet();
    deletedInA.forEach(
        pos -> expectedDeletedKeys.add(keysBySourcePosition.get(inPartitionA.location()).get(pos)));
    deletedInB.forEach(
        pos -> expectedDeletedKeys.add(keysBySourcePosition.get(inPartitionB.location()).get(pos)));
    Set<String> expectedKeys = allKeys(keysBySourcePosition);
    expectedKeys.removeAll(expectedDeletedKeys);

    commitManager(table, startingSnapshotId, true).commitFileGroups(groups);

    assertThat(currentKeys(table)).isEqualTo(expectedKeys);
    assertThat(dataFiles(table).stream().map(ContentFile::location))
        .containsExactlyInAnyOrderElementsOf(
            preparedFiles.stream().map(ContentFile::location).collect(Collectors.toList()));

    // remapped DVs live in the partition of the file they reference
    Map<String, DataFile> currentByLocation =
        dataFiles(table).stream().collect(Collectors.toMap(ContentFile::location, f -> f));
    List<DeleteFile> liveDeletes = deleteFiles(table);
    assertThat(liveDeletes).hasSize(2);
    for (DeleteFile dv : liveDeletes) {
      DataFile target = currentByLocation.get(dv.referencedDataFile());
      assertThat(target).isNotNull();
      assertThat(dv.partition()).isEqualTo(target.partition());
    }

    assertMapMatchesLayout(table, groups, keysBySourcePosition, expectedDeletedKeys);
  }

  /**
   * With a tiny target file size the rewrite rolls to a new output file every 1000 rows, so a
   * single source file spans several output files and its runs must be split at the boundaries.
   */
  @Test
  public void testSourceFileSpansMultipleOutputFiles() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 2, 5000);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    List<DataFile> sourceFiles = dataFiles(table);
    Map<String, Map<Long, String>> keysBySourcePosition = keysByFileAndPosition(table);

    Set<RewriteFileGroup> groups =
        prepareRewrite(
            table,
            startingSnapshotId,
            ImmutableMap.of(
                SizeBasedFileRewritePlanner.REWRITE_ALL, "true",
                SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "1024"));
    Set<DataFile> preparedFiles = addedFiles(groups);
    assertThat(preparedFiles).as("rewrite must have rolled output files").hasSizeGreaterThan(2);

    // runs for one source file must reference more than one target file
    CompactionMap map =
        CompactionMap.of(
            groups.stream().flatMap(g -> g.positionRuns().stream()).collect(Collectors.toList()));
    DataFile d0 = sourceFiles.get(0);
    assertThat(map.runsFor(d0.location()).stream().map(CompactionMap.Run::targetFile).distinct())
        .hasSizeGreaterThan(1);

    // spread the deletes so they land in different output files
    List<Long> deleted = Lists.newArrayList(0L, 999L, 1000L, 1500L, 2499L);
    commitDV(table, d0, deleted);
    Set<String> expectedDeletedKeys =
        deleted.stream()
            .map(pos -> keysBySourcePosition.get(d0.location()).get(pos))
            .collect(Collectors.toSet());
    Set<String> expectedKeys = allKeys(keysBySourcePosition);
    expectedKeys.removeAll(expectedDeletedKeys);

    commitManager(table, startingSnapshotId, true).commitFileGroups(groups);

    assertThat(currentKeys(table)).isEqualTo(expectedKeys);
    List<DeleteFile> liveDeletes = deleteFiles(table);
    assertThat(liveDeletes).as("deletes spread across several output files").hasSizeGreaterThan(1);
    assertThat(liveDeletes.stream().mapToLong(ContentFile::recordCount).sum())
        .isEqualTo(deleted.size());
    assertMapMatchesLayout(table, groups, keysBySourcePosition, expectedDeletedKeys);
  }

  /** Equality deletes commute with the rewrite because output files keep the starting sequence. */
  @Test
  public void testEqualityDeleteCommittedDuringRewrite() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 2, 200);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    Set<String> keysBefore = currentKeys(table);
    String deletedKey = keysBefore.iterator().next();

    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);
    commitEqualityDelete(table, "c2", deletedKey);

    commitManager(table, startingSnapshotId, true).commitFileGroups(groups);

    Set<String> expectedKeys = Sets.newHashSet(keysBefore);
    expectedKeys.remove(deletedKey);
    assertThat(currentKeys(table)).isEqualTo(expectedKeys);
  }

  /** Removing a source file is a real data conflict, not a layout conflict; the rewrite fails. */
  @Test
  public void testSourceFileRemovedConcurrently() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 2, 200);
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    DataFile d0 = dataFiles(table).get(0);

    Set<RewriteFileGroup> groups = prepareRewrite(table, startingSnapshotId);
    table.newDelete().deleteFile(d0).commit();

    assertThatThrownBy(
            () -> commitManager(table, startingSnapshotId, true).commitFileGroups(groups))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("was removed by snapshot");
  }

  /** The whole action with the session conf enabled: wiring, tracking and a normal commit. */
  @Test
  public void testActionWithConflictResolutionEnabled() throws IOException {
    Table table = createTable(3);
    writeRecords(table, 3, 300);
    DataFile d0 = dataFiles(table).get(0);
    Map<String, Map<Long, String>> keysBySourcePosition = keysByFileAndPosition(table);
    commitDV(table, d0, Lists.newArrayList(0L, 1L));
    Set<String> expectedKeys = currentKeys(table);

    spark
        .conf()
        .set(SparkSQLProperties.COMPACTION_RESOLVE_POSITION_DELETE_CONFLICTS_ENABLED, "true");
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(3);
    assertThat(result.addedDataFilesCount()).isEqualTo(1);
    assertThat(currentKeys(table)).isEqualTo(expectedKeys);
    assertThat(deleteFiles(table)).as("planned dangling DV removed").isEmpty();
    assertThat(allKeys(keysBySourcePosition)).hasSize(expectedKeys.size() + 2);
  }

  @Test
  public void testActionRejectsV2TableWhenEnabled() {
    Table table = createTable(2);
    writeRecords(table, 2, 100);

    spark
        .conf()
        .set(SparkSQLProperties.COMPACTION_RESOLVE_POSITION_DELETE_CONFLICTS_ENABLED, "true");
    assertThatThrownBy(
            () ->
                SparkActions.get(spark)
                    .rewriteDataFiles(table)
                    .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("only deletion vectors (v3+) can be rebased");
  }

  @Test
  public void testActionRejectsNewSequenceNumberWhenEnabled() {
    Table table = createTable(3);
    writeRecords(table, 2, 100);

    spark
        .conf()
        .set(SparkSQLProperties.COMPACTION_RESOLVE_POSITION_DELETE_CONFLICTS_ENABLED, "true");
    assertThatThrownBy(
            () ->
                SparkActions.get(spark)
                    .rewriteDataFiles(table)
                    .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
                    .option(RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER, "false")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("starting sequence number");
  }

  // ---- helpers ----

  private Table createTable(int formatVersion) {
    Map<String, String> properties =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    return TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, tableLocation);
  }

  private Table createPartitionedTable(int formatVersion) {
    Map<String, String> properties =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    return TABLES.create(SCHEMA, spec, properties, tableLocation);
  }

  /**
   * c2 is the unique key in every test; c1 doubles as the partition value for partitioned tables.
   */
  private void writeRecords(Table table, int files, int numRecords) {
    writeRecords(table, files, numRecords, i -> i);
  }

  private void writePartitionedRecords(
      Table table, int filesPerPartition, int numRecords, int partitions) {
    writeRecords(table, filesPerPartition, numRecords, i -> i % partitions);
  }

  private void writeRecords(
      Table table, int files, int numRecords, java.util.function.IntUnaryOperator c1Of) {
    List<ThreeColumnRecord> records =
        IntStream.range(0, numRecords)
            .mapToObj(i -> new ThreeColumnRecord(c1Of.applyAsInt(i), "key" + i, "bar" + i))
            .collect(Collectors.toList());
    Dataset<Row> df =
        spark
            .createDataFrame(records, ThreeColumnRecord.class)
            .repartition(files)
            // partitioned writers need rows clustered by partition within each Spark task
            .sortWithinPartitions("c1");
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(tableLocation);
    table.refresh();
  }

  private Set<RewriteFileGroup> prepareRewrite(Table table, long startingSnapshotId) {
    return prepareRewrite(
        table,
        startingSnapshotId,
        ImmutableMap.of(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1"));
  }

  /**
   * Plans and executes a bin-pack rewrite of the whole table at the given snapshot without
   * committing.
   */
  private Set<RewriteFileGroup> prepareRewrite(
      Table table, long startingSnapshotId, Map<String, String> plannerOptions) {
    BinPackRewriteFilePlanner planner =
        new BinPackRewriteFilePlanner(table, Expressions.alwaysTrue(), startingSnapshotId, false);
    planner.init(plannerOptions);
    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    SparkBinPackFileRewriteRunner runner =
        new SparkBinPackFileRewriteRunner(spark, table).trackPositions(true);
    runner.init(ImmutableMap.of());

    Set<RewriteFileGroup> groups = Sets.newHashSet();
    try (CloseableIterable<RewriteFileGroup> planned = plan.groups()) {
      for (RewriteFileGroup group : planned) {
        group.setOutputFiles(runner.rewrite(group));
        groups.add(group);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    assertThat(groups).isNotEmpty();
    return groups;
  }

  private RewriteDataFilesCommitManager commitManager(
      Table table, long startingSnapshotId, boolean resolveConflicts) {
    RewriteDataFilesCommitManager commitManager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId, true);
    if (resolveConflicts) {
      commitManager.resolvePositionDeleteConflicts(MAX_DELETE_FILES, MAX_RETRIES);
    }

    return commitManager;
  }

  private void commitDV(Table table, DataFile dataFile, List<Long> positions) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try (DVFileWriter closeable = writer) {
      for (long pos : positions) {
        closeable.delete(dataFile.location(), pos, table.spec(), dataFile.partition());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    table.refresh();
    org.apache.iceberg.RowDelta rowDelta =
        table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    // v3 writers replace the existing DV of a data file rather than adding a second one
    deleteFiles(table).stream()
        .filter(dv -> dataFile.location().equals(dv.referencedDataFile()))
        .forEach(rowDelta::removeDeletes);
    rowDelta.commit();
  }

  private void commitEqualityDelete(Table table, String column, Object value) {
    Schema deleteSchema = table.schema().select(column);
    Record record = GenericRecord.create(deleteSchema).copy(ImmutableMap.of(column, value));
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
    EqualityDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .equalityFieldIds(
                ArrayUtil.toIntArray(
                    Lists.newArrayList(table.schema().findField(column).fieldId())))
            .equalityDeleteRowSchema(deleteSchema)
            .deleteFileFormat(FileFormat.PARQUET)
            .build()
            .newEqualityDeleteWriter(fileFactory.newOutputFile(), table.spec(), null);
    try (EqualityDeleteWriter<Record> closeable = writer) {
      closeable.write(record);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
  }

  private List<DataFile> dataFiles(Table table) {
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return Lists.newArrayList(tasks).stream()
          .map(FileScanTask::file)
          .sorted(Comparator.comparing(ContentFile::location))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<DeleteFile> deleteFiles(Table table) {
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return Lists.newArrayList(tasks).stream()
          .flatMap(task -> task.deletes().stream())
          // several DVs can share one Puffin file, so dedupe on location + blob offset
          .collect(
              Collectors.toMap(
                  df -> df.location() + "@" + df.contentOffset(), df -> df, (a, b) -> a))
          .values()
          .stream()
          .sorted(
              Comparator.comparing((DeleteFile df) -> df.location())
                  .thenComparingLong(df -> df.contentOffset()))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<DataFile> addedFiles(Set<RewriteFileGroup> groups) {
    return groups.stream()
        .flatMap(group -> group.addedFiles().stream())
        .collect(Collectors.toSet());
  }

  private Set<String> currentKeys(Table table) {
    table.refresh();
    return spark.read().format("iceberg").load(tableLocation).select("c2").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }

  /** Reads the current snapshot as file -> position -> key. */
  private Map<String, Map<Long, String>> keysByFileAndPosition(Table table) {
    table.refresh();
    return keysByFileAndPosition(table.currentSnapshot().snapshotId());
  }

  private Map<String, Map<Long, String>> keysByFileAndPosition(long snapshotId) {
    Map<String, Map<Long, String>> result = Maps.newHashMap();
    List<Row> rows =
        spark
            .read()
            .format("iceberg")
            .option("versionAsOf", snapshotId)
            .load(tableLocation)
            .select("_file", "_pos", "c2")
            .collectAsList();
    for (Row row : rows) {
      result
          .computeIfAbsent(row.getString(0), ignored -> Maps.newHashMap())
          .put(row.getLong(1), row.getString(2));
    }

    return result;
  }

  private static Set<String> allKeys(Map<String, Map<Long, String>> keysByFileAndPosition) {
    return keysByFileAndPosition.values().stream()
        .flatMap(positions -> positions.values().stream())
        .collect(Collectors.toSet());
  }

  /**
   * For every run, every source row that survived must be found in the committed table at the
   * mapped target position with the same key; deleted rows must be absent from the target.
   */
  private void assertMapMatchesLayout(
      Table table,
      Set<RewriteFileGroup> groups,
      Map<String, Map<Long, String>> sourceKeys,
      Set<String> deletedKeys) {
    Map<String, Map<Long, String>> targetKeys = keysByFileAndPosition(table);
    List<CompactionMap.Run> runs =
        groups.stream()
            .flatMap(group -> group.positionRuns().stream())
            .collect(Collectors.toList());
    CompactionMap map = CompactionMap.of(runs);

    long checked = 0;
    for (Map.Entry<String, Map<Long, String>> source : sourceKeys.entrySet()) {
      for (Map.Entry<Long, String> entry : source.getValue().entrySet()) {
        CompactionMap.Run run = map.runFor(source.getKey(), entry.getKey());
        assertThat(run).as("every S0 row must be covered by the map").isNotNull();
        long targetPos = run.targetStart() + (entry.getKey() - run.sourceStart());
        String targetKey =
            targetKeys.getOrDefault(run.targetFile(), Maps.newHashMap()).get(targetPos);
        if (deletedKeys.contains(entry.getValue())) {
          assertThat(targetKey)
              .as("deleted row must not be visible at %s:%s", run.targetFile(), targetPos)
              .isNull();
        } else {
          assertThat(targetKey).isEqualTo(entry.getValue());
        }

        checked++;
      }
    }

    assertThat(checked).isEqualTo(allKeys(sourceKeys).size());
  }
}
