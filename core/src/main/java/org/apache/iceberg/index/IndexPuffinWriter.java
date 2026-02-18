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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializationUtil;

/**
 * Helper class for writing {@link Index} objects to a Puffin file.
 *
 * <p>Each index is serialized using Java serialization (since {@link Index} extends {@link
 * java.io.Serializable}) and written as a blob in the Puffin file. Blob properties include the
 * referenced data file path and index metadata (ndv, num-values, etc.). File-level properties
 * include the index-id and fpp from the {@link IndexSpec}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (IndexPuffinWriter writer = new IndexPuffinWriter(outputFile, indexSpec, snapshotId, sequenceNumber)) {
 *     writer.add(index1, "s3://bucket/table/data/part-00000.parquet");
 *     writer.add(index2, "s3://bucket/table/data/part-00001.parquet");
 *     writer.finish();
 * }
 * }</pre>
 */
public class IndexPuffinWriter implements Closeable {

  static final String REFERENCED_DATA_FILE = "referenced-data-file";
  static final String INDEX_ID_PROPERTY = "index-id";
  static final String FPP_PROPERTY = "fpp";

  private final PuffinWriter puffinWriter;
  private final long snapshotId;
  private final long sequenceNumber;
  private boolean finished;

  /**
   * Creates a new IndexPuffinWriter.
   *
   * @param outputFile the output file to write the Puffin data to
   * @param indexSpec the index specification defining the index type and properties
   * @param snapshotId the snapshot ID this index data belongs to
   * @param sequenceNumber the sequence number of the snapshot
   */
  public IndexPuffinWriter(
      OutputFile outputFile, IndexSpec indexSpec, long snapshotId, long sequenceNumber) {
    Preconditions.checkNotNull(outputFile, "outputFile is null");
    Preconditions.checkNotNull(indexSpec, "indexSpec is null");
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.finished = false;

    Map<String, String> fileProperties =
        ImmutableMap.of(
            INDEX_ID_PROPERTY,
            indexSpec.indexId(),
            FPP_PROPERTY,
            indexSpec.property(BloomFilterFactory.FPP_PROPERTY, String.valueOf(BloomFilterFactory.DEFAULT_FPP)));

    this.puffinWriter =
        Puffin.write(outputFile)
            .createdBy(IcebergBuild.fullVersion())
            .setAll(fileProperties)
            .compressBlobs(PuffinCompressionCodec.ZSTD)
            .build();
  }

  /**
   * Adds an index for a data file to the Puffin file.
   *
   * <p>The index is serialized using Java serialization and stored as a blob. The blob properties
   * include the referenced data file path and the index metadata (ndv, num-values, etc.).
   *
   * @param index the index to write
   * @param dataFilePath the path of the data file this index was built from
   * @return the blob metadata for the written blob
   */
  public BlobMetadata add(Index index, String dataFilePath) {
    Preconditions.checkNotNull(index, "index is null");
    Preconditions.checkNotNull(dataFilePath, "dataFilePath is null");
    Preconditions.checkState(!finished, "Writer already finished");

    byte[] serializedIndex = SerializationUtil.serializeToBytes(index);

    ImmutableMap.Builder<String, String> blobProperties = ImmutableMap.builder();
    blobProperties.put(REFERENCED_DATA_FILE, dataFilePath);
    blobProperties.putAll(index.metadata());

    Blob blob =
        new Blob(
            index.type(),
            ImmutableList.of(index.columnId()),
            snapshotId,
            sequenceNumber,
            ByteBuffer.wrap(serializedIndex),
            null,
            blobProperties.buildOrThrow());

    return puffinWriter.write(blob);
  }

  /**
   * Finishes writing the Puffin file. This writes the footer and closes the underlying stream.
   *
   * @throws IOException if an I/O error occurs
   */
  public void finish() throws IOException {
    Preconditions.checkState(!finished, "Writer already finished");
    puffinWriter.finish();
    finished = true;
  }

  /** Returns the location of the Puffin file being written. */
  public String location() {
    return puffinWriter.location();
  }

  /** Returns the file size after {@link #finish()} has been called. */
  public long fileSize() {
    return puffinWriter.fileSize();
  }

  /** Returns the footer size after {@link #finish()} has been called. */
  public long footerSize() {
    return puffinWriter.footerSize();
  }

  /** Returns the metadata for all blobs written so far. */
  public List<BlobMetadata> writtenBlobsMetadata() {
    return puffinWriter.writtenBlobsMetadata();
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      finish();
    }
  }
}
