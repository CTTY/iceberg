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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IndexPuffinReader}. */
public class TestIndexPuffinReader {

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

  private InMemoryOutputFile writePuffinFile(String... dataFilePaths) throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    IndexSpec spec = createIndexSpec();

    try (IndexPuffinWriter writer =
        new IndexPuffinWriter(outputFile, spec, SNAPSHOT_ID, SEQUENCE_NUMBER)) {
      for (String dataFilePath : dataFilePaths) {
        BloomFilterIndex index = new BloomFilterIndex(COLUMN_ID, 100, 0.01);
        writer.add(index, dataFilePath);
      }
      writer.finish();
    }

    return outputFile;
  }

  @Test
  public void loadSinglePuffinFile() throws IOException {
    InMemoryOutputFile outputFile = writePuffinFile(DATA_FILE_1);

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile.location(), outputFile.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(1);
    assertThat(reader.indexForDataFile(DATA_FILE_1)).isNotNull();
    assertThat(reader.indexForDataFile(DATA_FILE_1).type()).isEqualTo(BloomFilterFactory.TYPE);
    assertThat(reader.indexForDataFile(DATA_FILE_1).columnId()).isEqualTo(COLUMN_ID);
  }

  @Test
  public void loadMultipleBlobsFromSingleFile() throws IOException {
    InMemoryOutputFile outputFile = writePuffinFile(DATA_FILE_1, DATA_FILE_2);

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile.location(), outputFile.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(2);
    assertThat(reader.indexForDataFile(DATA_FILE_1)).isNotNull();
    assertThat(reader.indexForDataFile(DATA_FILE_2)).isNotNull();
  }

  @Test
  public void loadMultiplePuffinFiles() throws IOException {
    InMemoryOutputFile outputFile1 = writePuffinFile(DATA_FILE_1);
    InMemoryOutputFile outputFile2 = writePuffinFile(DATA_FILE_2);

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile1.location(), outputFile1.toByteArray());
    fileIO.addFile(outputFile2.location(), outputFile2.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(
            Arrays.asList(outputFile1.location(), outputFile2.location()), fileIO);

    assertThat(reader.size()).isEqualTo(2);
    assertThat(reader.indexForDataFile(DATA_FILE_1)).isNotNull();
    assertThat(reader.indexForDataFile(DATA_FILE_2)).isNotNull();
  }

  @Test
  public void emptyFilePathList() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    IndexPuffinReader reader = IndexPuffinReader.load(Collections.emptyList(), fileIO);

    assertThat(reader.isEmpty()).isTrue();
    assertThat(reader.size()).isEqualTo(0);
  }

  @Test
  public void nullFilePathList() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    IndexPuffinReader reader = IndexPuffinReader.load(null, fileIO);

    assertThat(reader.isEmpty()).isTrue();
  }

  @Test
  public void failSafeOnMissingFile() {
    InMemoryFileIO fileIO = new InMemoryFileIO();

    // Loading a non-existent file should not throw, just return empty
    IndexPuffinReader reader =
        IndexPuffinReader.load(
            Collections.singletonList("s3://bucket/table/metadata/nonexistent.puffin"), fileIO);

    assertThat(reader.isEmpty()).isTrue();
  }

  @Test
  public void failSafeOnCorruptedFile() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String corruptPath = "s3://bucket/table/metadata/corrupt.puffin";
    fileIO.addFile(corruptPath, new byte[] {0, 1, 2, 3, 4, 5});

    // Loading a corrupted file should not throw, just return empty
    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(corruptPath), fileIO);

    assertThat(reader.isEmpty()).isTrue();
  }

  @Test
  public void failSafeSkipsBadFileButLoadsGoodFile() throws IOException {
    InMemoryOutputFile goodFile = writePuffinFile(DATA_FILE_1);

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(goodFile.location(), goodFile.toByteArray());
    String badPath = "s3://bucket/table/metadata/bad.puffin";
    fileIO.addFile(badPath, new byte[] {0, 1, 2, 3});

    IndexPuffinReader reader =
        IndexPuffinReader.load(Arrays.asList(badPath, goodFile.location()), fileIO);

    assertThat(reader.size()).isEqualTo(1);
    assertThat(reader.indexForDataFile(DATA_FILE_1)).isNotNull();
  }

  @Test
  public void indexForDataFileReturnsNullForUnknownPath() throws IOException {
    InMemoryOutputFile outputFile = writePuffinFile(DATA_FILE_1);

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile.location(), outputFile.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    assertThat(reader.indexForDataFile("s3://bucket/table/data/unknown.parquet")).isNull();
  }

  @Test
  public void dataFileToIndexMapIsUnmodifiable() throws IOException {
    InMemoryOutputFile outputFile = writePuffinFile(DATA_FILE_1);

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(outputFile.location(), outputFile.toByteArray());

    IndexPuffinReader reader =
        IndexPuffinReader.load(Collections.singletonList(outputFile.location()), fileIO);

    Map<String, Index> map = reader.dataFileToIndex();
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> map.put("new-key", null))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
