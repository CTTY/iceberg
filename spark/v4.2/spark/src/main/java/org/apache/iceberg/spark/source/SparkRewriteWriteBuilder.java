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

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class SparkRewriteWriteBuilder implements WriteBuilder {

  private final SparkSession spark;
  private final Table table;
  private final Schema schema;
  private final String groupId;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo info;
  private final boolean caseSensitive;
  private final boolean checkNullability;
  private final boolean checkOrdering;
  private final boolean trackPositions;

  SparkRewriteWriteBuilder(
      SparkSession spark, Table table, Schema schema, String groupId, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.schema = schema;
    this.groupId = groupId;
    this.writeConf = new SparkWriteConf(spark, table, info.options());
    this.info = info;
    this.caseSensitive = writeConf.caseSensitive();
    this.checkNullability = writeConf.checkNullability();
    this.checkOrdering = writeConf.checkOrdering();
    this.trackPositions = FileRewriteCoordinator.get().tracksPositions(table, groupId);
  }

  @Override
  public Write build() {
    Schema writeSchema = validateWriteSchema();
    SparkUtil.validatePartitionTransforms(table.spec());
    String appId = spark.sparkContext().applicationId();

    StructType dsSchema = info.schema();
    SparkWrite.PositionTracking positionTracking = null;
    if (trackPositions) {
      positionTracking = positionTracking(dsSchema);
      // the position columns are consumed by the tracking writer and never reach the file writers
      writeSchema =
          TypeUtil.selectNot(
              writeSchema,
              ImmutableSet.of(
                  MetadataColumns.FILE_PATH.fieldId(), MetadataColumns.ROW_POSITION.fieldId()));
      dsSchema = new StructType(java.util.Arrays.copyOf(dsSchema.fields(), dsSchema.size() - 2));
    }

    return new SparkWrite(
        spark,
        table,
        null /* main branch */,
        writeConf,
        info,
        appId,
        writeSchema,
        dsSchema,
        writeConf.writeRequirements(),
        positionTracking) {

      @Override
      public BatchWrite toBatch() {
        return asRewrite(groupId);
      }

      @Override
      public StreamingWrite toStreaming() {
        throw new UnsupportedOperationException("Streaming writes are not supported for rewrites");
      }
    };
  }

  /**
   * The tracking writer reads the source file and position by ordinal from the incoming rows and
   * forwards the row unchanged to the file writers, which only look at the leading data columns.
   * That is only sound if the two tracking columns are the trailing fields of the dataset schema.
   */
  private static SparkWrite.PositionTracking positionTracking(StructType dsSchema) {
    StructField[] fields = dsSchema.fields();
    Preconditions.checkArgument(
        fields.length >= 2,
        "Position tracking requires %s and %s columns in the rewrite dataset",
        MetadataColumns.FILE_PATH.name(),
        MetadataColumns.ROW_POSITION.name());
    int fileOrdinal = fields.length - 2;
    int posOrdinal = fields.length - 1;
    Preconditions.checkArgument(
        fields[fileOrdinal].name().equals(MetadataColumns.FILE_PATH.name())
            && fields[posOrdinal].name().equals(MetadataColumns.ROW_POSITION.name()),
        "Position tracking requires %s and %s to be the trailing columns of the rewrite dataset,"
            + " got %s",
        MetadataColumns.FILE_PATH.name(),
        MetadataColumns.ROW_POSITION.name(),
        dsSchema);
    return new SparkWrite.PositionTracking(fileOrdinal, posOrdinal);
  }

  private Schema validateWriteSchema() {
    Schema writeSchema = SparkSchemaUtil.convert(schema, info.schema(), caseSensitive);
    TypeUtil.validateWriteSchema(schema, writeSchema, checkNullability, checkOrdering);
    return writeSchema;
  }
}
