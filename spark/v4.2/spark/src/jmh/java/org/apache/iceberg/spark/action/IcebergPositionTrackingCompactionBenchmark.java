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
package org.apache.iceberg.spark.action;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the overhead that position tracking adds to a bin-pack rewrite.
 *
 * <p>The same rewrite runs with position tracking off (baseline) and on. Tracking is the only
 * difference: there are no concurrent deletes, so the commit path never remaps anything and the
 * measured delta is the cost of exposing {@code _file}/{@code _pos} to the writer and recording
 * runs. {@code deleteGapsPerFile} controls how fragmented the mapping is: every gap splits a run,
 * so a high value approximates the worst case for an order-preserving rewrite.
 *
 * <p>To run this benchmark for spark-4.2: <code>
 *   ./gradlew :iceberg-spark:iceberg-spark-4.2_2.13:jmh
 *       -PjmhIncludeRegex=IcebergPositionTrackingCompactionBenchmark
 *       -PjmhOutputPath=benchmark/position-tracking-compaction-results.txt
 *       -PjmhJsonOutputPath=benchmark/position-tracking-compaction-results.json
 * </code>
 */
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class IcebergPositionTrackingCompactionBenchmark extends IcebergCompactionBenchmark {

  private static final String[] NAMESPACE = new String[] {"default"};
  private static final String NAME = "postrackbench";
  private static final Identifier IDENT = Identifier.of(NAMESPACE, NAME);
  private static final long TOTAL_ROWS = 2_000_000L;

  @Param({"false", "true"})
  private boolean trackPositions;

  @Param({"100", "1000"})
  private int numFiles;

  // deletion vectors applied during the rewrite; each deleted position splits a mapping run
  @Param({"0", "100"})
  private int deleteGapsPerFile;

  @Override
  protected String tableName() {
    return NAME;
  }

  @Benchmark
  @Threads(1)
  public void rewriteDataFiles() {
    spark()
        .conf()
        .set(
            SparkSQLProperties.COMPACTION_RESOLVE_POSITION_DELETE_CONFLICTS_ENABLED,
            String.valueOf(trackPositions));
    SparkActions.get(spark())
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
        .execute();
  }

  @Override
  protected final void initTable() {
    Schema schema =
        new Schema(
            required(1, "intCol", Types.IntegerType.get()),
            required(2, "stringCol", Types.StringType.get()),
            optional(3, "nullCol", Types.StringType.get()));

    SparkSessionCatalog<?> catalog;
    try {
      catalog =
          (SparkSessionCatalog<?>)
              Spark3Util.catalogAndIdentifier(spark(), "spark_catalog").catalog();
      catalog.dropTable(IDENT);
      catalog.createTable(
          IDENT,
          SparkSchemaUtil.convert(schema),
          new Transform[0],
          Map.of(TableProperties.FORMAT_VERSION, "3"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void appendData() {
    Dataset<Row> df =
        spark()
            .range(0, TOTAL_ROWS)
            .withColumn("intCol", col("id").cast("int"))
            .withColumn("stringCol", concat(lit("foo_"), col("id")))
            .withColumn("nullCol", lit(null).cast("string"))
            .drop("id")
            .repartition(numFiles);
    writeData(df);

    if (deleteGapsPerFile > 0) {
      addDeletionVectors();
    }
  }

  /** Commits one DV per data file with {@code deleteGapsPerFile} scattered positions. */
  private void addDeletionVectors() {
    Table table = table();
    List<DataFile> files = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> files.add(task.file()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    Random random = new Random(42);
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try (DVFileWriter closeable = writer) {
      for (DataFile file : files) {
        long rows = file.recordCount();
        for (int i = 0; i < deleteGapsPerFile && i < rows; i++) {
          closeable.delete(
              file.location(),
              Math.floorMod(random.nextLong(), rows),
              table.spec(),
              file.partition());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    RowDelta rowDelta = table.newRowDelta();
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();
  }
}
