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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.index.BloomFilterIndex;
import org.apache.iceberg.index.Index;
import org.apache.iceberg.index.IndexSpec;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates indexes during Spark data file writes.
 *
 * <p>Uses {@link IndexSpec} to create appropriate {@link Index} instances. For each row written,
 * values for indexed columns are extracted from the {@link InternalRow} and added to the
 * corresponding index. On finish, all indexes are serialized to Puffin {@link Blob} objects.
 *
 * <p>Errors during value extraction or index operations are logged and skipped to ensure graceful
 * degradation — index failures never block data file writes.
 */
public class SparkIndexWriter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(SparkIndexWriter.class);

  private final Map<Integer, Index> columnToIndex;
  private final List<IndexSpec> indexSpecs;
  private final String dataFilePath;
  // Cache: column ID -> ordinal position in the schema
  private Map<Integer, Integer> columnIdToOrdinal;

  /**
   * Creates a new SparkIndexWriter.
   *
   * @param indexSpecs the index specifications defining which columns to index
   * @param dataFilePath the path of the data file being written
   */
  public SparkIndexWriter(List<IndexSpec> indexSpecs, String dataFilePath) {
    Preconditions.checkNotNull(indexSpecs, "indexSpecs is null");
    Preconditions.checkNotNull(dataFilePath, "dataFilePath is null");
    this.indexSpecs = ImmutableList.copyOf(indexSpecs);
    this.dataFilePath = dataFilePath;
    this.columnToIndex = new HashMap<>();

    for (IndexSpec spec : indexSpecs) {
      try {
        columnToIndex.put(spec.columnId(), spec.createIndex());
      } catch (Exception e) {
        LOG.warn("Failed to create index for column {}: {}", spec.columnId(), e.getMessage(), e);
      }
    }
  }

  /**
   * Writes a row to all applicable indexes.
   *
   * <p>For each indexed column, the value is extracted from the row and added to the corresponding
   * index. Null values are skipped. Errors during extraction or indexing are logged and skipped.
   *
   * @param row the Spark internal row to index
   * @param schema the Iceberg schema for the row
   */
  public void write(InternalRow row, Schema schema) {
    if (columnToIndex.isEmpty()) {
      return;
    }

    ensureOrdinalMapping(schema);

    for (IndexSpec spec : indexSpecs) {
      int columnId = spec.columnId();
      Index index = columnToIndex.get(columnId);
      if (index == null) {
        continue;
      }

      try {
        Integer ordinal = columnIdToOrdinal.get(columnId);
        if (ordinal == null) {
          continue;
        }

        if (row.isNullAt(ordinal)) {
          continue;
        }

        Type colType = schema.findType(columnId);
        Object value = extractValue(row, ordinal, colType);
        if (value != null && index instanceof BloomFilterIndex) {
          ((BloomFilterIndex) index).add(value, colType);
        }
      } catch (Exception e) {
        LOG.warn(
            "Failed to index value for column {} in file {}: {}",
            columnId,
            dataFilePath,
            e.getMessage(),
            e);
      }
    }
  }

  /**
   * Finishes writing and serializes all indexes to Puffin blobs.
   *
   * <p>Each index is serialized using Java serialization and wrapped in a {@link Blob} with
   * metadata including the referenced data file path.
   *
   * @param snapshotId the snapshot ID for the blobs
   * @param sequenceNumber the sequence number for the blobs
   * @return a list of Puffin blobs, one per indexed column
   */
  public List<Blob> finish(long snapshotId, long sequenceNumber) {
    List<Blob> blobs = new ArrayList<>();

    for (IndexSpec spec : indexSpecs) {
      Index index = columnToIndex.get(spec.columnId());
      if (index == null) {
        continue;
      }

      try {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put("referenced-data-file", dataFilePath);
        properties.putAll(index.metadata());

        byte[] serializedIndex = SerializationUtil.serializeToBytes(index);

        Blob blob =
            new Blob(
                index.type(),
                ImmutableList.of(spec.columnId()),
                snapshotId,
                sequenceNumber,
                ByteBuffer.wrap(serializedIndex),
                PuffinCompressionCodec.ZSTD,
                properties.buildOrThrow());

        blobs.add(blob);
      } catch (Exception e) {
        LOG.warn(
            "Failed to serialize index for column {} in file {}: {}",
            spec.columnId(),
            dataFilePath,
            e.getMessage(),
            e);
      }
    }

    return blobs;
  }

  @Override
  public void close() {
    // No resources to release; indexes are in-memory
  }

  /** Returns the number of active indexes. */
  public int indexCount() {
    return columnToIndex.size();
  }

  /** Returns the data file path this writer is associated with. */
  public String dataFilePath() {
    return dataFilePath;
  }

  /**
   * Builds the column ID to ordinal position mapping from the schema. The mapping is cached and
   * only built once.
   */
  private void ensureOrdinalMapping(Schema schema) {
    if (columnIdToOrdinal != null) {
      return;
    }

    columnIdToOrdinal = new HashMap<>();
    List<Types.NestedField> columns = schema.columns();
    for (int i = 0; i < columns.size(); i++) {
      columnIdToOrdinal.put(columns.get(i).fieldId(), i);
    }
  }

  /**
   * Extracts a value from an InternalRow at the given ordinal position, converting from Spark's
   * internal representation to the Java type expected by the Bloom filter.
   *
   * @param row the Spark internal row
   * @param ordinal the column ordinal position
   * @param type the Iceberg type of the column
   * @return the extracted value, or null if the type is unsupported
   */
  static Object extractValue(InternalRow row, int ordinal, Type type) {
    switch (type.typeId()) {
      case STRING:
        return row.getUTF8String(ordinal).toString();
      case INTEGER:
        return row.getInt(ordinal);
      case LONG:
        return row.getLong(ordinal);
      case UUID:
        // Spark represents UUIDs as strings
        return UUID.fromString(row.getUTF8String(ordinal).toString());
      default:
        LOG.warn("Unsupported type for index extraction: {}", type);
        return null;
    }
  }
}
