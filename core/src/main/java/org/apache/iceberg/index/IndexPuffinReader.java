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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for reading {@link Index} objects from Puffin files.
 *
 * <p>Reads one or more Puffin files, deserializes the index blobs using Java serialization, and
 * builds a lookup map from data-file-path to {@link Index}. The data-file-path is extracted from
 * the {@code referenced-data-file} blob property written by {@link IndexPuffinWriter}.
 *
 * <p>All errors are handled with fail-safe behavior: if a Puffin file cannot be read or a blob
 * cannot be deserialized, a warning is logged and the file/blob is skipped. This ensures that index
 * loading failures never cause query failures.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * IndexPuffinReader indexReader = IndexPuffinReader.load(indexFilePaths, fileIO);
 * Map<String, Index> indexes = indexReader.dataFileToIndex();
 * }</pre>
 */
public class IndexPuffinReader {

  private static final Logger LOG = LoggerFactory.getLogger(IndexPuffinReader.class);

  private final Map<String, Index> dataFileToIndex;

  private IndexPuffinReader(Map<String, Index> dataFileToIndex) {
    this.dataFileToIndex = ImmutableMap.copyOf(dataFileToIndex);
  }

  /**
   * Loads index data from the given Puffin file paths.
   *
   * <p>For each Puffin file, reads the file metadata and all blobs, deserializes each blob into an
   * {@link Index} object, and maps it to the referenced data file path. If any error occurs while
   * reading a file or deserializing a blob, a warning is logged and that file/blob is skipped.
   *
   * @param indexFilePaths the list of Puffin file paths to read
   * @param io the FileIO instance for accessing the files
   * @return an IndexPuffinReader with the loaded data-file-path to Index mapping
   */
  public static IndexPuffinReader load(List<String> indexFilePaths, FileIO io) {
    Map<String, Index> dataFileToIndex = new HashMap<>();

    if (indexFilePaths == null || indexFilePaths.isEmpty()) {
      return new IndexPuffinReader(dataFileToIndex);
    }

    for (String path : indexFilePaths) {
      loadPuffinFile(path, io, dataFileToIndex);
    }

    return new IndexPuffinReader(dataFileToIndex);
  }

  private static void loadPuffinFile(
      String path, FileIO io, Map<String, Index> dataFileToIndex) {
    try (PuffinReader reader = Puffin.read(io.newInputFile(path)).build()) {
      FileMetadata fileMetadata = reader.fileMetadata();
      List<BlobMetadata> blobs = fileMetadata.blobs();

      for (Pair<BlobMetadata, ByteBuffer> pair : reader.readAll(blobs)) {
        loadBlob(pair.first(), pair.second(), path, dataFileToIndex);
      }
    } catch (Exception e) {
      LOG.warn("Failed to load index file: {}", path, e);
    }
  }

  private static void loadBlob(
      BlobMetadata blobMetadata,
      ByteBuffer blobData,
      String puffinPath,
      Map<String, Index> dataFileToIndex) {
    try {
      String dataFilePath =
          blobMetadata.properties().get(IndexPuffinWriter.REFERENCED_DATA_FILE);
      if (dataFilePath == null) {
        LOG.warn(
            "Blob in Puffin file {} missing '{}' property, skipping",
            puffinPath,
            IndexPuffinWriter.REFERENCED_DATA_FILE);
        return;
      }

      byte[] bytes = new byte[blobData.remaining()];
      blobData.get(bytes);

      Index index = SerializationUtil.deserializeFromBytes(bytes);
      dataFileToIndex.put(dataFilePath, index);
    } catch (Exception e) {
      LOG.warn("Failed to deserialize index blob from Puffin file: {}", puffinPath, e);
    }
  }

  /**
   * Returns an unmodifiable map from data-file-path to the corresponding {@link Index}.
   *
   * @return the data-file-path to Index lookup map
   */
  public Map<String, Index> dataFileToIndex() {
    return dataFileToIndex;
  }

  /**
   * Returns the {@link Index} for the given data file path, or {@code null} if no index exists.
   *
   * @param dataFilePath the data file path to look up
   * @return the Index for the data file, or null
   */
  public Index indexForDataFile(String dataFilePath) {
    return dataFileToIndex.get(dataFilePath);
  }

  /** Returns the number of data files that have loaded indexes. */
  public int size() {
    return dataFileToIndex.size();
  }

  /** Returns true if no indexes were loaded. */
  public boolean isEmpty() {
    return dataFileToIndex.isEmpty();
  }
}
