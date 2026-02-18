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
import java.util.List;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IndexPuffinWriter}. */
public class TestIndexPuffinWriter {

  private static final long SNAPSHOT_ID = 123456789L;
  private static final long SEQUENCE_NUMBER = 1L;
  private static final int COLUMN_ID = 5;
  private static final String DATA_FILE_1 = "s3://bucket/table/data/part-00000.parquet";
  private static final String DATA_FILE_2 = "s3://bucket/table/data/part-00001.parquet";

  private IndexSpec createIndexSpec() {
    return new IndexSpec(
        "bloom_5_a1b2c3d4",
        BloomFilterFactory.TYPE,
        COLUMN_ID,
        ImmutableMap.of("fpp", "0.01"));
  }

  @Test
  public void writeSingleBlob() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index, DATA_FILE_1);
      writer.finish();

      assertThat(writer.writtenBlobsMetadata()).hasSize(1);
      assertThat(writer.fileSize()).isGreaterThan(0);
      assertThat(writer.footerSize()).isGreaterThan(0);
    }
  }

  @Test
  public void writeMultipleBlobs() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();

    BloomFilterIndex index1 = new BloomFilterIndex(COLUMN_ID, 100, 0.01);
    BloomFilterIndex index2 = new BloomFilterIndex(COLUMN_ID, 200, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index1, DATA_FILE_1);
      writer.add(index2, DATA_FILE_2);
      writer.finish();

      assertThat(writer.writtenBlobsMetadata()).hasSize(2);
    }
  }

  @Test
  public void blobMetadataContainsReferencedDataFile() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      BlobMetadata metadata = writer.add(index, DATA_FILE_1);

      assertThat(metadata.properties())
          .containsEntry(IndexPuffinWriter.REFERENCED_DATA_FILE, DATA_FILE_1);
    }
  }

  @Test
  public void blobMetadataContainsIndexMetadata() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      BlobMetadata metadata = writer.add(index, DATA_FILE_1);

      assertThat(metadata.properties()).containsKey("ndv");
      assertThat(metadata.properties()).containsKey("num-values");
      assertThat(metadata.properties()).containsKey("num-bits");
      assertThat(metadata.properties()).containsKey("num-hash-funcs");
    }
  }

  @Test
  public void blobTypeMatchesIndex() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      BlobMetadata metadata = writer.add(index, DATA_FILE_1);

      assertThat(metadata.type()).isEqualTo(BloomFilterFactory.TYPE);
    }
  }

  @Test
  public void blobInputFieldsMatchColumnId() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      BlobMetadata metadata = writer.add(index, DATA_FILE_1);

      assertThat(metadata.inputFields()).containsExactly(COLUMN_ID);
    }
  }

  @Test
  public void fileLevelPropertiesContainIndexIdAndFpp() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index, DATA_FILE_1);
      writer.finish();
    }

    // Read back the Puffin file and verify file-level properties
    try (PuffinReader reader =
        Puffin.read(outputFile.toInputFile()).build()) {
      assertThat(reader.fileMetadata().properties())
          .containsEntry(IndexPuffinWriter.INDEX_ID_PROPERTY, "bloom_5_a1b2c3d4");
      assertThat(reader.fileMetadata().properties())
          .containsEntry(IndexPuffinWriter.FPP_PROPERTY, "0.01");
    }
  }

  @Test
  public void puffinFileReadableAfterWrite() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index, DATA_FILE_1);
      writer.finish();
    }

    try (PuffinReader reader =
        Puffin.read(outputFile.toInputFile()).build()) {
      List<BlobMetadata> blobs = reader.fileMetadata().blobs();
      assertThat(blobs).hasSize(1);

      BlobMetadata blobMeta = blobs.get(0);
      assertThat(blobMeta.type()).isEqualTo(BloomFilterFactory.TYPE);
      assertThat(blobMeta.snapshotId()).isEqualTo(SNAPSHOT_ID);
      assertThat(blobMeta.sequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
      assertThat(blobMeta.properties())
          .containsEntry(IndexPuffinWriter.REFERENCED_DATA_FILE, DATA_FILE_1);
    }
  }

  @Test
  public void defaultFppUsedWhenNotSpecified() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec =
        new IndexSpec("bloom_5_a1b2c3d4", BloomFilterFactory.TYPE, COLUMN_ID, ImmutableMap.of());
    BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      writer.add(index, DATA_FILE_1);
      writer.finish();
    }

    try (PuffinReader reader =
        Puffin.read(outputFile.toInputFile()).build()) {
      assertThat(reader.fileMetadata().properties())
          .containsEntry(
              IndexPuffinWriter.FPP_PROPERTY, String.valueOf(BloomFilterFactory.DEFAULT_FPP));
    }
  }
}
