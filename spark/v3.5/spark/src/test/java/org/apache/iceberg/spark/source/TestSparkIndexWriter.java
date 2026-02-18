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
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.index.BloomFilterIndex;
import org.apache.iceberg.index.IndexSpec;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class TestSparkIndexWriter {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "count", Types.LongType.get()));

  private static final Map<String, String> BLOOM_PROPS =
      ImmutableMap.of("fpp", "0.01", "expected-insertions", "100");

  @Test
  public void testCreateIndexesFromSpecs() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    assertThat(writer.indexCount()).isEqualTo(1);
    assertThat(writer.dataFilePath()).isEqualTo("s3://bucket/data/part-00000.parquet");
  }

  @Test
  public void testWriteCollectsValuesForIndexedColumns() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    // Write rows with string values in column 2 (name)
    writer.write(createRow(1, "alice", 100L), SCHEMA);
    writer.write(createRow(2, "bob", 200L), SCHEMA);
    writer.write(createRow(3, "charlie", 300L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    assertThat(blobs).hasSize(1);

    Blob blob = blobs.get(0);
    assertThat(blob.type()).isEqualTo("bloom-filter-v1");
    assertThat(blob.inputFields()).containsExactly(2);
    assertThat(blob.properties()).containsKey("referenced-data-file");
    assertThat(blob.properties().get("referenced-data-file"))
        .isEqualTo("s3://bucket/data/part-00000.parquet");

    // Deserialize and verify the bloom filter contains the written values
    BloomFilterIndex index =
        SerializationUtil.deserializeFromBytes(blobBytes(blob));
    assertThat(index.mightContain("alice", Types.StringType.get())).isTrue();
    assertThat(index.mightContain("bob", Types.StringType.get())).isTrue();
    assertThat(index.mightContain("charlie", Types.StringType.get())).isTrue();
    // Value not added should likely not be present
    assertThat(index.mightContain("unknown_value_xyz", Types.StringType.get())).isFalse();
  }

  @Test
  public void testWriteSkipsNullValues() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    // Write a row with null name
    writer.write(createRowWithNullName(1, 100L), SCHEMA);
    writer.write(createRow(2, "alice", 200L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    assertThat(blobs).hasSize(1);

    BloomFilterIndex index =
        SerializationUtil.deserializeFromBytes(blobBytes(blobs.get(0)));
    assertThat(index.mightContain("alice", Types.StringType.get())).isTrue();
    // num-values should only count non-null values
    assertThat(index.metadata().get("num-values")).isEqualTo("1");
  }

  @Test
  public void testWriteWithIntegerColumn() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    writer.write(createRow(42, "alice", 100L), SCHEMA);
    writer.write(createRow(99, "bob", 200L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    assertThat(blobs).hasSize(1);

    BloomFilterIndex index =
        SerializationUtil.deserializeFromBytes(blobBytes(blobs.get(0)));
    assertThat(index.mightContain(42, Types.IntegerType.get())).isTrue();
    assertThat(index.mightContain(99, Types.IntegerType.get())).isTrue();
    assertThat(index.mightContain(999, Types.IntegerType.get())).isFalse();
  }

  @Test
  public void testWriteWithLongColumn() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 3, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    writer.write(createRow(1, "alice", 100L), SCHEMA);
    writer.write(createRow(2, "bob", 200L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    assertThat(blobs).hasSize(1);

    BloomFilterIndex index =
        SerializationUtil.deserializeFromBytes(blobBytes(blobs.get(0)));
    assertThat(index.mightContain(100L, Types.LongType.get())).isTrue();
    assertThat(index.mightContain(200L, Types.LongType.get())).isTrue();
    assertThat(index.mightContain(999L, Types.LongType.get())).isFalse();
  }

  @Test
  public void testMultipleIndexSpecs() {
    IndexSpec stringSpec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    IndexSpec intSpec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(
            ImmutableList.of(stringSpec, intSpec), "s3://bucket/data/part-00000.parquet");

    assertThat(writer.indexCount()).isEqualTo(2);

    writer.write(createRow(42, "alice", 100L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    assertThat(blobs).hasSize(2);

    // Verify both blobs reference the correct data file
    for (Blob blob : blobs) {
      assertThat(blob.properties().get("referenced-data-file"))
          .isEqualTo("s3://bucket/data/part-00000.parquet");
    }
  }

  @Test
  public void testFinishWithNoRows() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    List<Blob> blobs = writer.finish(1L, 1L);
    // Should still produce a blob (empty bloom filter)
    assertThat(blobs).hasSize(1);
    assertThat(blobs.get(0).properties().get("num-values")).isEqualTo("0");
  }

  @Test
  public void testEmptyIndexSpecs() {
    SparkIndexWriter writer =
        new SparkIndexWriter(Collections.emptyList(), "s3://bucket/data/part-00000.parquet");

    assertThat(writer.indexCount()).isEqualTo(0);

    writer.write(createRow(1, "alice", 100L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    assertThat(blobs).isEmpty();
  }

  @Test
  public void testBlobMetadataContainsSnapshotInfo() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    writer.write(createRow(1, "alice", 100L), SCHEMA);

    List<Blob> blobs = writer.finish(42L, 7L);
    Blob blob = blobs.get(0);

    assertThat(blob.snapshotId()).isEqualTo(42L);
    assertThat(blob.sequenceNumber()).isEqualTo(7L);
  }

  @Test
  public void testBlobMetadataContainsIndexMetadata() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    writer.write(createRow(1, "alice", 100L), SCHEMA);
    writer.write(createRow(2, "bob", 200L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    Map<String, String> props = blobs.get(0).properties();

    assertThat(props).containsKey("num-bits");
    assertThat(props).containsKey("num-hash-funcs");
    assertThat(props).containsKey("ndv");
    assertThat(props).containsKey("num-values");
    assertThat(props.get("num-values")).isEqualTo("2");
  }

  @Test
  public void testWriteWithUuidColumn() {
    Schema uuidSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "uuid_col", Types.UUIDType.get()));

    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    UUID testUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    InternalRow row = new GenericInternalRow(2);
    row.update(0, 1);
    row.update(1, UTF8String.fromString(testUuid.toString()));

    writer.write(row, uuidSchema);

    List<Blob> blobs = writer.finish(1L, 1L);
    assertThat(blobs).hasSize(1);

    BloomFilterIndex index =
        SerializationUtil.deserializeFromBytes(blobBytes(blobs.get(0)));
    assertThat(index.mightContain(testUuid, Types.UUIDType.get())).isTrue();
  }

  @Test
  public void testExtractValueForSupportedTypes() {
    // Test integer extraction
    InternalRow intRow = new GenericInternalRow(1);
    intRow.update(0, 42);
    assertThat(SparkIndexWriter.extractValue(intRow, 0, Types.IntegerType.get())).isEqualTo(42);

    // Test long extraction
    InternalRow longRow = new GenericInternalRow(1);
    longRow.update(0, 100L);
    assertThat(SparkIndexWriter.extractValue(longRow, 0, Types.LongType.get())).isEqualTo(100L);

    // Test string extraction
    InternalRow strRow = new GenericInternalRow(1);
    strRow.update(0, UTF8String.fromString("hello"));
    assertThat(SparkIndexWriter.extractValue(strRow, 0, Types.StringType.get()))
        .isEqualTo("hello");

    // Test UUID extraction
    UUID uuid = UUID.randomUUID();
    InternalRow uuidRow = new GenericInternalRow(1);
    uuidRow.update(0, UTF8String.fromString(uuid.toString()));
    assertThat(SparkIndexWriter.extractValue(uuidRow, 0, Types.UUIDType.get())).isEqualTo(uuid);
  }

  @Test
  public void testExtractValueForUnsupportedTypeReturnsNull() {
    InternalRow row = new GenericInternalRow(1);
    row.update(0, 3.14f);
    assertThat(SparkIndexWriter.extractValue(row, 0, Types.FloatType.get())).isNull();
  }

  @Test
  public void testColumnNotInSchemaIsSkipped() {
    // Index spec references column 99 which doesn't exist in the schema
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 99, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    // Should not throw — the column is simply not found in the ordinal mapping
    writer.write(createRow(1, "alice", 100L), SCHEMA);

    List<Blob> blobs = writer.finish(1L, 1L);
    // Blob is still produced but with 0 values
    assertThat(blobs).hasSize(1);
    assertThat(blobs.get(0).properties().get("num-values")).isEqualTo("0");
  }

  @Test
  public void testCloseIsIdempotent() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);
    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec), "s3://bucket/data/part-00000.parquet");

    // Should not throw
    writer.close();
    writer.close();
  }

  private static InternalRow createRow(int id, String name, long count) {
    InternalRow row = new GenericInternalRow(3);
    row.update(0, id);
    row.update(1, UTF8String.fromString(name));
    row.update(2, count);
    return row;
  }

  private static InternalRow createRowWithNullName(int id, long count) {
    InternalRow row = new GenericInternalRow(3);
    row.update(0, id);
    row.update(1, null);
    row.update(2, count);
    return row;
  }

  private static byte[] blobBytes(Blob blob) {
    byte[] bytes = new byte[blob.blobData().remaining()];
    blob.blobData().duplicate().get(bytes);
    return bytes;
  }
}
