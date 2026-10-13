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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PositionDeletesTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkRewriteTable extends BaseSparkTable implements SupportsRead, SupportsWrite {

  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);

  private final String groupId;
  private final Schema rewriteSchema;

  public SparkRewriteTable(Table table, String groupId) {
    this(table, groupId, rewriteSchema(table, groupId));
  }

  private SparkRewriteTable(Table table, String groupId, Schema rewriteSchema) {
    super(table, rewriteSchema);
    this.groupId = groupId;
    this.rewriteSchema = rewriteSchema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkStagedScanBuilder(spark(), table(), groupId, options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (table() instanceof PositionDeletesTable) {
      return new SparkPositionDeletesRewriteBuilder(spark(), table(), groupId, info);
    } else {
      return new SparkRewriteWriteBuilder(spark(), table(), rewriteSchema, groupId, info);
    }
  }

  private static Schema rewriteSchema(Table table, String groupId) {
    Schema schema =
        TableUtil.supportsRowLineage(table)
            ? MetadataColumns.schemaWithRowLineage(table.schema())
            : table.schema();

    if (FileRewriteCoordinator.get().tracksPositions(table, groupId)) {
      // expose the source file and row position as columns so they flow through the write; the
      // write builder strips them from the data schema before anything reaches the file writers
      return schemaWithPositionTracking(schema);
    }

    return schema;
  }

  static Schema schemaWithPositionTracking(Schema schema) {
    List<Types.NestedField> columns = Lists.newArrayList(schema.columns());
    columns.add(MetadataColumns.FILE_PATH);
    columns.add(MetadataColumns.ROW_POSITION);
    return new Schema(columns, schema.identifierFieldIds());
  }
}
