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
import java.util.Map;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the write/read round-trip between {@link IndexPuffinWriter} and {@link
 * IndexPuffinReader}.
 *
 * <p>These tests verify that indexes written with the writer can be read back with the reader and
 * that the deserialized indexes preserve their functional state (e.g., mightContain returns true for
 * added values).
 */
public class TestIndexPuffinIntegration {

  private static final long SNAPSHOT_ID = 100L;
  private static final long SEQUENCE_NUMBER = 1L;
  private static final int COLUMN_ID = 5;
  private static final String DATA_FILE_1 = "s3://bucket/table/data/part-00000.parquet";
  private static final String DATA_FILE_2 = "s3://bucket/table/data/part-00001.parquet";

  private IndexSpec createIndexSpec() {
    return new IndexSpec(
        "bloom_5_test1234",
        BloomFilterFactory.TYPE,
        COLUMN_ID,
        ImmutableMap.of("fpp", "0.01"));
  }

  /**
   * Writes indexes to a Puffin file and registers it in an InMemoryFileIO for reading back.
   *
   * @return the output file that was written
   */
  private InMemoryOutputFile writeAndRegister(
      InMemoryFileIO fileIO, Map<String, BloomFilterIndex> dataFileToIndex) throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      for (Map.Entry<String, BloomFilterIndex> entry : dataFileToIndex.entrySet()) {
        writer.add(entry.getValue(), entry.getKey());
      }
      writer.finish();
    }

    fileIO.addFile(outputFile.location(), outputFile.toByteArray());
    return outputFile;
  }

  // ---- Write/Read Round-Trip Tests (Requirement 3.1) ----

  @Test
  public void roundTripPreservesBloomFilterState() throws IOException {
    BloomFilterIndex original = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    original.add("hello", Types.StringType.get());
    original.add("world", Types.StringType.get());
    original.add("iceberg", Types.StringType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile outputFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, original));

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    Index deserialized = reader.indexForDataFile(DATA_FILE_1);
    assertThat(deserialized).isNotNull();
    assertThat(deserialized).isInstanceOf(BloomFilterIndex.class);

    BloomFilterIndex bloomFilter = (BloomFilterIndex) deserialized;
    // Values that were added must be found (no false negatives)
    assertThat(bloomFilter.mightContain("hello", Types.StringType.get())).isTrue();
    assertThat(bloomFilter.mightContain("world", Types.StringType.get())).isTrue();
    assertThat(bloomFilter.mightContain("iceberg", Types.StringType.get())).isTrue();
    // A value never added should (very likely) not be found
    assertThat(bloomFilter.mightContain("nonexistent_value_xyz", Types.StringType.get())).isFalse();
  }

  @Test
  public void roundTripWithIntegerValues() throws IOException {
    BloomFilterIndex original = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    original.add(42, Types.IntegerType.get());
    original.add(100, Types.IntegerType.get());
    original.add(-1, Types.IntegerType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile outputFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, original));

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    BloomFilterIndex deserialized = (BloomFilterIndex) reader.indexForDataFile(DATA_FILE_1);
    assertThat(deserialized.mightContain(42, Types.IntegerType.get())).isTrue();
    assertThat(deserialized.mightContain(100, Types.IntegerType.get())).isTrue();
    assertThat(deserialized.mightContain(-1, Types.IntegerType.get())).isTrue();
    assertThat(deserialized.mightContain(999999, Types.IntegerType.get())).isFalse();
  }

  @Test
  public void roundTripWithLongValues() throws IOException {
    BloomFilterIndex original = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    original.add(Long.MAX_VALUE, Types.LongType.get());
    original.add(0L, Types.LongType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile outputFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, original));

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    BloomFilterIndex deserialized = (BloomFilterIndex) reader.indexForDataFile(DATA_FILE_1);
    assertThat(deserialized.mightContain(Long.MAX_VALUE, Types.LongType.get())).isTrue();
    assertThat(deserialized.mightContain(0L, Types.LongType.get())).isTrue();
    assertThat(deserialized.mightContain(12345L, Types.LongType.get())).isFalse();
  }

  @Test
  public void roundTripMultipleDataFiles() throws IOException {
    BloomFilterIndex index1 = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    index1.add("alpha", Types.StringType.get());

    BloomFilterIndex index2 = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    index2.add("beta", Types.StringType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile outputFile =
        writeAndRegister(
            fileIO, ImmutableMap.of(DATA_FILE_1, index1, DATA_FILE_2, index2));

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(2);

    BloomFilterIndex read1 = (BloomFilterIndex) reader.indexForDataFile(DATA_FILE_1);
    BloomFilterIndex read2 = (BloomFilterIndex) reader.indexForDataFile(DATA_FILE_2);

    assertThat(read1.mightContain("alpha", Types.StringType.get())).isTrue();
    assertThat(read1.mightContain("beta", Types.StringType.get())).isFalse();

    assertThat(read2.mightContain("beta", Types.StringType.get())).isTrue();
    assertThat(read2.mightContain("alpha", Types.StringType.get())).isFalse();
  }

  @Test
  public void roundTripPreservesIndexTypeAndColumnId() throws IOException {
    BloomFilterIndex original = new BloomFilterIndex(COLUMN_ID, 100, 0.01);
    original.add("test", Types.StringType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile outputFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, original));

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    Index deserialized = reader.indexForDataFile(DATA_FILE_1);
    assertThat(deserialized.type()).isEqualTo(BloomFilterFactory.TYPE);
    assertThat(deserialized.columnId()).isEqualTo(COLUMN_ID);
  }

  // ---- Blob Metadata Completeness Tests (Requirements 3.2, 3.3) ----

  @Test
  public void blobMetadataContainsAllRequiredFields() throws IOException {
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    index.add("value1", Types.StringType.get());
    index.add("value2", Types.StringType.get());

    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      BlobMetadata metadata = writer.add(index, DATA_FILE_1);

      // Requirement 3.2: blob metadata must include referenced-data-file, ndv, num-values
      assertThat(metadata.properties())
          .containsEntry(IndexPuffinWriter.REFERENCED_DATA_FILE, DATA_FILE_1);
      assertThat(metadata.properties()).containsKey("ndv");
      assertThat(metadata.properties()).containsKey("num-values");
      assertThat(metadata.properties()).containsKey("num-bits");
      assertThat(metadata.properties()).containsKey("num-hash-funcs");

      // Verify the blob type
      assertThat(metadata.type()).isEqualTo(BloomFilterFactory.TYPE);

      // Verify input fields contain the column ID
      assertThat(metadata.inputFields()).containsExactly(COLUMN_ID);

      // Verify snapshot and sequence number
      assertThat(metadata.snapshotId()).isEqualTo(SNAPSHOT_ID);
      assertThat(metadata.sequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    }
  }

  @Test
  public void blobMetadataReflectsActualIndexState() throws IOException {
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    index.add("a", Types.StringType.get());
    index.add("b", Types.StringType.get());
    index.add("c", Types.StringType.get());

    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      BlobMetadata metadata = writer.add(index, DATA_FILE_1);

      // num-values should reflect the 3 values added
      assertThat(metadata.properties().get("num-values")).isEqualTo("3");
      // ndv should reflect the 3 distinct values
      assertThat(metadata.properties().get("ndv")).isEqualTo("3");
    }
  }

  @Test
  public void fileLevelPropertiesContainIndexIdAndFpp() throws IOException {
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);
    index.add("test", Types.StringType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile outputFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, index));

    // Read back and verify file-level properties via raw Puffin reader
    try (org.apache.iceberg.puffin.PuffinReader puffinReader =
        org.apache.iceberg.puffin.Puffin.read(outputFile.toInputFile()).build()) {
      Map<String, String> fileProps = puffinReader.fileMetadata().properties();

      // Requirement 3.3: file-level properties must contain index-id and fpp
      assertThat(fileProps).containsEntry(IndexPuffinWriter.INDEX_ID_PROPERTY, "bloom_5_test1234");
      assertThat(fileProps).containsEntry(IndexPuffinWriter.FPP_PROPERTY, "0.01");
    }
  }

  // ---- Error Handling / Fail-Safe Tests (Requirement 5.7) ----

  @Test
  public void failSafeWithMixedGoodAndBadPuffinFiles() throws IOException {
    // Write a valid Puffin file with real data
    BloomFilterIndex goodIndex = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    goodIndex.add("present", Types.StringType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile goodFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, goodIndex));

    // Register a corrupted file
    String badPath = "s3://bucket/table/metadata/corrupted.puffin";
    fileIO.addFile(badPath, new byte[] {0x00, 0x01, 0x02, 0x03, 0x04});

    // Load both - bad file should be skipped, good file should load
    IndexPuffinReader reader =
        IndexPuffinReader.load(Arrays.asList(badPath, goodFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(1);
    BloomFilterIndex loaded = (BloomFilterIndex) reader.indexForDataFile(DATA_FILE_1);
    assertThat(loaded).isNotNull();
    assertThat(loaded.mightContain("present", Types.StringType.get())).isTrue();
  }

  @Test
  public void failSafeWithNonExistentFile() throws IOException {
    BloomFilterIndex goodIndex = new BloomFilterIndex(COLUMN_ID, 1000, 0.01);
    goodIndex.add("data", Types.StringType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile goodFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, goodIndex));

    // Load with a non-existent path alongside a good file
    IndexPuffinReader reader =
        IndexPuffinReader.load(
            Arrays.asList("s3://bucket/nonexistent.puffin", goodFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(1);
    assertThat(reader.indexForDataFile(DATA_FILE_1)).isNotNull();
  }

  @Test
  public void missingDataFileReturnsNull() throws IOException {
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);
    index.add("test", Types.StringType.get());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    InMemoryOutputFile outputFile =
        writeAndRegister(fileIO, ImmutableMap.of(DATA_FILE_1, index));

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    // A data file not in the index should return null, not throw
    assertThat(reader.indexForDataFile("s3://bucket/table/data/unknown.parquet")).isNull();
  }
}
