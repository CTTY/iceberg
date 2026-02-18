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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.index.BloomFilterIndex;
import org.apache.iceberg.index.Index;
import org.apache.iceberg.index.IndexPuffinReader;
import org.apache.iceberg.index.IndexPuffinWriter;
import org.apache.iceberg.index.IndexSpec;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Spark write path with Bloom filter indexes.
 *
 * <p>Tests the full pipeline: SparkIndexWriter creates indexes during data file writes, blobs are
 * serialized and carried through TaskCommit, written to a Puffin file via IndexPuffinWriter, and
 * read back via IndexPuffinReader with deserialized bloom filters preserving their state.
 *
 * <p>Validates Requirements 6.1, 6.2, 6.3, 6.5.
 */
public class TestSparkWriteIndexIntegration {

  private static final long SNAPSHOT_ID = 1L;
  private static final long SEQUENCE_NUMBER = 1L;
  private static final String DATA_FILE_1 = "s3://bucket/table/data/part-00000.parquet";
  private static final String DATA_FILE_2 = "s3://bucket/table/data/part-00001.parquet";
  private static final String DATA_FILE_3 = "s3://bucket/table/data/part-00002.parquet";

  private static final Map<String, String> BLOOM_PROPS =
      ImmutableMap.of("fpp", "0.01", "expected-insertions", "100");

  private static final Schema INT_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final Schema MULTI_COL_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  // ---- TaskCommit Blob Carrying Tests ----

  @Test
  public void testTaskCommitCarriesIndexBlobs() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), DATA_FILE_1);

    InternalRow row = new GenericInternalRow(1);
    row.update(0, 42);
    writer.write(row, INT_SCHEMA);

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);
    SparkWrite.TaskCommit taskCommit =
        new SparkWrite.TaskCommit(new org.apache.iceberg.DataFile[0], blobs);

    assertThat(taskCommit.indexBlobs()).hasSize(1);
    assertThat(taskCommit.indexBlobs().get(0).type()).isEqualTo("bloom-filter-v1");
    assertThat(taskCommit.indexBlobs().get(0).properties())
        .containsKey("referenced-data-file");
  }

  @Test
  public void testTaskCommitWithNoIndexBlobs() {
    SparkWrite.TaskCommit taskCommit =
        new SparkWrite.TaskCommit(new org.apache.iceberg.DataFile[0]);

    assertThat(taskCommit.indexBlobs()).isEmpty();
    assertThat(taskCommit.files()).isEmpty();
  }

  @Test
  public void testTaskCommitWithNullIndexBlobs() {
    SparkWrite.TaskCommit taskCommit =
        new SparkWrite.TaskCommit(new org.apache.iceberg.DataFile[0], null);

    assertThat(taskCommit.indexBlobs()).isEmpty();
  }

  // ---- Multiple Data Files Produce Multiple Blobs (Req 6.1, 6.3) ----

  @Test
  public void testMultipleDataFilesProduceMultipleBlobs() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);

    // Simulate writing three separate data files, each with its own SparkIndexWriter
    List<Blob> allBlobs = new ArrayList<>();

    for (String dataFile : new String[] {DATA_FILE_1, DATA_FILE_2, DATA_FILE_3}) {
      SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), dataFile);
      for (int i = 0; i < 5; i++) {
        InternalRow row = new GenericInternalRow(1);
        row.update(0, i);
        writer.write(row, INT_SCHEMA);
      }
      allBlobs.addAll(writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER));
    }

    assertThat(allBlobs).hasSize(3);

    // Each blob should reference a different data file
    assertThat(allBlobs.get(0).properties().get("referenced-data-file")).isEqualTo(DATA_FILE_1);
    assertThat(allBlobs.get(1).properties().get("referenced-data-file")).isEqualTo(DATA_FILE_2);
    assertThat(allBlobs.get(2).properties().get("referenced-data-file")).isEqualTo(DATA_FILE_3);
  }

  // ---- Blob Metadata Correctness (Req 6.2, 6.5) ----

  @Test
  public void testBlobMetadataContainsRequiredFields() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), DATA_FILE_1);

    // Write 10 values
    for (int i = 0; i < 10; i++) {
      InternalRow row = new GenericInternalRow(1);
      row.update(0, i);
      writer.write(row, INT_SCHEMA);
    }

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);
    assertThat(blobs).hasSize(1);

    Blob blob = blobs.get(0);
    Map<String, String> props = blob.properties();

    // referenced-data-file
    assertThat(props).containsEntry("referenced-data-file", DATA_FILE_1);
    // num-values should be 10 (total values added)
    assertThat(props).containsEntry("num-values", "10");
    // ndv tracks insertions (incremented per add call)
    assertThat(props).containsEntry("ndv", "10");
    // num-bits and num-hash-funcs should be present
    assertThat(props).containsKey("num-bits");
    assertThat(props).containsKey("num-hash-funcs");
    // blob type
    assertThat(blob.type()).isEqualTo("bloom-filter-v1");
  }

  @Test
  public void testMultiColumnBlobMetadata() {
    IndexSpec spec1 = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    IndexSpec spec2 = new IndexSpec("bloom-filter-v1", 2, BLOOM_PROPS);

    SparkIndexWriter writer =
        new SparkIndexWriter(ImmutableList.of(spec1, spec2), DATA_FILE_1);

    InternalRow row = new GenericInternalRow(2);
    row.update(0, 42);
    row.update(1, UTF8String.fromString("alice"));
    writer.write(row, MULTI_COL_SCHEMA);

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);
    assertThat(blobs).hasSize(2);

    // Both blobs reference the same data file
    for (Blob blob : blobs) {
      assertThat(blob.properties().get("referenced-data-file")).isEqualTo(DATA_FILE_1);
      assertThat(blob.type()).isEqualTo("bloom-filter-v1");
      assertThat(blob.properties()).containsKey("ndv");
      assertThat(blob.properties()).containsKey("num-values");
    }
  }

  // ---- Full Write → Puffin → Read Round-Trip (Req 6.1, 6.2, 6.3) ----

  @Test
  public void testFullRoundTripSingleDataFile() throws IOException {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), DATA_FILE_1);

    // Write values through SparkIndexWriter
    for (int i = 0; i < 20; i++) {
      InternalRow row = new GenericInternalRow(1);
      row.update(0, i * 10);
      writer.write(row, INT_SCHEMA);
    }

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);

    // Write blobs to a Puffin file using IndexPuffinWriter
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec puffinSpec =
        new IndexSpec("bloom_1_test1234", "bloom-filter-v1", 1, ImmutableMap.of("fpp", "0.01"));

    try (IndexPuffinWriter puffinWriter =
        new IndexPuffinWriter(outputFile, puffinSpec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      for (Blob blob : blobs) {
        // Deserialize the blob data to get the Index, then re-add via IndexPuffinWriter
        byte[] blobBytes = new byte[blob.blobData().remaining()];
        blob.blobData().duplicate().get(blobBytes);
        BloomFilterIndex index = SerializationUtil.deserializeFromBytes(blobBytes);
        puffinWriter.add(index, blob.properties().get("referenced-data-file"));
      }
      puffinWriter.finish();
    }

    // Read back using IndexPuffinReader
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile.location(), outputFile.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(1);

    Index readIndex = reader.indexForDataFile(DATA_FILE_1);
    assertThat(readIndex).isNotNull();
    assertThat(readIndex).isInstanceOf(BloomFilterIndex.class);

    BloomFilterIndex bloomFilter = (BloomFilterIndex) readIndex;

    // All written values must be found (no false negatives)
    for (int i = 0; i < 20; i++) {
      assertThat(bloomFilter.mightContain(i * 10, Types.IntegerType.get()))
          .as("Bloom filter should contain value %d", i * 10)
          .isTrue();
    }

    // Values not written should not be found
    assertThat(bloomFilter.mightContain(99999, Types.IntegerType.get())).isFalse();
  }

  @Test
  public void testFullRoundTripMultipleDataFiles() throws IOException {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);

    // Simulate writing three data files with distinct value ranges
    String[] dataFiles = {DATA_FILE_1, DATA_FILE_2, DATA_FILE_3};
    List<Blob> allBlobs = new ArrayList<>();

    for (int fileIdx = 0; fileIdx < dataFiles.length; fileIdx++) {
      SparkIndexWriter writer =
          new SparkIndexWriter(ImmutableList.of(spec), dataFiles[fileIdx]);
      int base = fileIdx * 1000;
      for (int i = 0; i < 10; i++) {
        InternalRow row = new GenericInternalRow(1);
        row.update(0, base + i);
        writer.write(row, INT_SCHEMA);
      }
      allBlobs.addAll(writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER));
    }

    assertThat(allBlobs).hasSize(3);

    // Write all blobs to a single Puffin file
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec puffinSpec =
        new IndexSpec("bloom_1_test1234", "bloom-filter-v1", 1, ImmutableMap.of("fpp", "0.01"));

    try (IndexPuffinWriter puffinWriter =
        new IndexPuffinWriter(outputFile, puffinSpec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      for (Blob blob : allBlobs) {
        byte[] blobBytes = new byte[blob.blobData().remaining()];
        blob.blobData().duplicate().get(blobBytes);
        BloomFilterIndex index = SerializationUtil.deserializeFromBytes(blobBytes);
        puffinWriter.add(index, blob.properties().get("referenced-data-file"));
      }
      puffinWriter.finish();
    }

    // Read back
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile.location(), outputFile.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(3);

    // Verify each data file's bloom filter contains only its own values
    for (int fileIdx = 0; fileIdx < dataFiles.length; fileIdx++) {
      BloomFilterIndex bf = (BloomFilterIndex) reader.indexForDataFile(dataFiles[fileIdx]);
      assertThat(bf).isNotNull();

      int base = fileIdx * 1000;
      // Values written to this file must be found
      for (int i = 0; i < 10; i++) {
        assertThat(bf.mightContain(base + i, Types.IntegerType.get()))
            .as("File %s should contain value %d", dataFiles[fileIdx], base + i)
            .isTrue();
      }

      // Values from other files should not be found (ranges are 1000 apart)
      int otherBase = ((fileIdx + 1) % 3) * 1000;
      assertThat(bf.mightContain(otherBase, Types.IntegerType.get()))
          .as("File %s should not contain value %d from another file", dataFiles[fileIdx], otherBase)
          .isFalse();
    }
  }

  @Test
  public void testFullRoundTripWithStringValues() throws IOException {
    Schema stringSchema =
        new Schema(Types.NestedField.required(1, "name", Types.StringType.get()));
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);

    SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), DATA_FILE_1);

    String[] names = {"alice", "bob", "charlie", "diana", "eve"};
    for (String name : names) {
      InternalRow row = new GenericInternalRow(1);
      row.update(0, UTF8String.fromString(name));
      writer.write(row, stringSchema);
    }

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);

    // Write to Puffin
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec puffinSpec =
        new IndexSpec("bloom_1_test1234", "bloom-filter-v1", 1, ImmutableMap.of("fpp", "0.01"));

    try (IndexPuffinWriter puffinWriter =
        new IndexPuffinWriter(outputFile, puffinSpec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      for (Blob blob : blobs) {
        byte[] blobBytes = new byte[blob.blobData().remaining()];
        blob.blobData().duplicate().get(blobBytes);
        BloomFilterIndex index = SerializationUtil.deserializeFromBytes(blobBytes);
        puffinWriter.add(index, blob.properties().get("referenced-data-file"));
      }
      puffinWriter.finish();
    }

    // Read back
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile.location(), outputFile.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    BloomFilterIndex bf = (BloomFilterIndex) reader.indexForDataFile(DATA_FILE_1);
    assertThat(bf).isNotNull();

    for (String name : names) {
      assertThat(bf.mightContain(name, Types.StringType.get()))
          .as("Bloom filter should contain '%s'", name)
          .isTrue();
    }

    assertThat(bf.mightContain("nonexistent_xyz", Types.StringType.get())).isFalse();
  }

  // ---- Puffin File Metadata Verification ----

  @Test
  public void testPuffinFileBlobMetadataCompleteness() throws IOException {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), DATA_FILE_1);

    for (int i = 0; i < 15; i++) {
      InternalRow row = new GenericInternalRow(1);
      row.update(0, i);
      writer.write(row, INT_SCHEMA);
    }

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);

    // Write to Puffin and inspect blob metadata
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec puffinSpec =
        new IndexSpec("bloom_1_test1234", "bloom-filter-v1", 1, ImmutableMap.of("fpp", "0.01"));

    List<BlobMetadata> writtenMetadata;
    try (IndexPuffinWriter puffinWriter =
        new IndexPuffinWriter(outputFile, puffinSpec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      byte[] blobBytes = new byte[blobs.get(0).blobData().remaining()];
      blobs.get(0).blobData().duplicate().get(blobBytes);
      BloomFilterIndex index = SerializationUtil.deserializeFromBytes(blobBytes);
      puffinWriter.add(index, DATA_FILE_1);
      puffinWriter.finish();
      writtenMetadata = puffinWriter.writtenBlobsMetadata();
    }

    assertThat(writtenMetadata).hasSize(1);
    BlobMetadata meta = writtenMetadata.get(0);

    // Verify blob metadata fields
    assertThat(meta.type()).isEqualTo("bloom-filter-v1");
    assertThat(meta.inputFields()).containsExactly(1);
    assertThat(meta.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(meta.sequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(meta.properties()).containsEntry("referenced-data-file", DATA_FILE_1);
    assertThat(meta.properties()).containsEntry("num-values", "15");
    assertThat(meta.properties()).containsEntry("ndv", "15");
    assertThat(meta.properties()).containsKey("num-bits");
    assertThat(meta.properties()).containsKey("num-hash-funcs");

    // Verify file-level properties via raw Puffin reader
    try (PuffinReader puffinReader = Puffin.read(outputFile.toInputFile()).build()) {
      Map<String, String> fileProps = puffinReader.fileMetadata().properties();
      assertThat(fileProps).containsEntry("index-id", "bloom_1_test1234");
      assertThat(fileProps).containsEntry("fpp", "0.01");
    }
  }

  // ---- FPP Configuration Respected (Req 6.5) ----

  @Test
  public void testFppConfigurationRespected() {
    // Create index with a specific fpp
    Map<String, String> customFpp =
        ImmutableMap.of("fpp", "0.001", "expected-insertions", "100");
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, customFpp);

    SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), DATA_FILE_1);

    InternalRow row = new GenericInternalRow(1);
    row.update(0, 1);
    writer.write(row, INT_SCHEMA);

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);
    assertThat(blobs).hasSize(1);

    // Deserialize and verify the bloom filter was created with the custom fpp
    // A lower fpp (0.001 vs 0.01) should result in more bits
    byte[] blobBytes = new byte[blobs.get(0).blobData().remaining()];
    blobs.get(0).blobData().duplicate().get(blobBytes);
    BloomFilterIndex index = SerializationUtil.deserializeFromBytes(blobBytes);

    int numBitsLowFpp = Integer.parseInt(index.metadata().get("num-bits"));

    // Create another with higher fpp for comparison
    Map<String, String> highFpp =
        ImmutableMap.of("fpp", "0.1", "expected-insertions", "100");
    IndexSpec specHigh = new IndexSpec("bloom-filter-v1", 1, highFpp);
    SparkIndexWriter writerHigh =
        new SparkIndexWriter(ImmutableList.of(specHigh), DATA_FILE_2);

    InternalRow row2 = new GenericInternalRow(1);
    row2.update(0, 1);
    writerHigh.write(row2, INT_SCHEMA);

    List<Blob> blobsHigh = writerHigh.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);
    byte[] blobBytesHigh = new byte[blobsHigh.get(0).blobData().remaining()];
    blobsHigh.get(0).blobData().duplicate().get(blobBytesHigh);
    BloomFilterIndex indexHigh = SerializationUtil.deserializeFromBytes(blobBytesHigh);

    int numBitsHighFpp = Integer.parseInt(indexHigh.metadata().get("num-bits"));

    // Lower fpp should require more bits
    assertThat(numBitsLowFpp).isGreaterThan(numBitsHighFpp);
  }

  // ---- Serialization Round-Trip via TaskCommit ----

  @Test
  public void testSerializeDeserializeRoundTripViaTaskCommit() {
    IndexSpec spec = new IndexSpec("bloom-filter-v1", 1, BLOOM_PROPS);
    SparkIndexWriter writer = new SparkIndexWriter(ImmutableList.of(spec), DATA_FILE_1);

    for (int i = 0; i < 10; i++) {
      InternalRow row = new GenericInternalRow(1);
      row.update(0, i);
      writer.write(row, INT_SCHEMA);
    }

    List<Blob> blobs = writer.finish(SNAPSHOT_ID, SEQUENCE_NUMBER);
    SparkWrite.TaskCommit taskCommit =
        new SparkWrite.TaskCommit(new org.apache.iceberg.DataFile[0], blobs);

    // Deserialize the bloom filter from the TaskCommit blob
    Blob blob = taskCommit.indexBlobs().get(0);
    byte[] bytes = new byte[blob.blobData().remaining()];
    blob.blobData().duplicate().get(bytes);
    BloomFilterIndex index = SerializationUtil.deserializeFromBytes(bytes);

    // Verify all values are present
    for (int i = 0; i < 10; i++) {
      assertThat(index.mightContain(i, Types.IntegerType.get()))
          .as("Bloom filter should contain value %d", i)
          .isTrue();
    }

    assertThat(index.mightContain(9999, Types.IntegerType.get())).isFalse();

    // Verify metadata
    assertThat(index.type()).isEqualTo("bloom-filter-v1");
    assertThat(index.columnId()).isEqualTo(1);
    assertThat(index.metadata().get("num-values")).isEqualTo("10");
    assertThat(index.metadata().get("ndv")).isEqualTo("10");
  }
}
