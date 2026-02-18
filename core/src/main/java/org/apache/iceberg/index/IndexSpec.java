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

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * Represents an index specification for a single column.
 *
 * <p>Stored in table metadata under the "index-specs" field. The factory is resolved from the type
 * string and stored directly in the spec for easy access.
 */
public class IndexSpec implements Serializable {

  /**
   * Known index type constant for Bloom filter v1. Will be replaced with BloomFilterFactory.TYPE
   * once BloomFilterFactory is implemented.
   */
  public static final String BLOOM_FILTER_V1 = "bloom-filter-v1";

  private final String indexId;
  private final String type;
  private final int columnId;
  private final Map<String, String> properties;
  private final IndexFactory factory;

  /**
   * Creates a new index spec, auto-generating the index ID.
   *
   * @param type the index type (e.g., "bloom-filter-v1")
   * @param columnId the column ID this index is built on
   * @param properties type-specific configuration properties
   */
  public IndexSpec(String type, int columnId, Map<String, String> properties) {
    this(generateIndexId(type, columnId), type, columnId, properties);
  }

  /**
   * Creates an index spec with an explicit index ID. Used for deserialization from JSON.
   *
   * @param indexId the index ID
   * @param type the index type (e.g., "bloom-filter-v1")
   * @param columnId the column ID this index is built on
   * @param properties type-specific configuration properties
   */
  public IndexSpec(String indexId, String type, int columnId, Map<String, String> properties) {
    Preconditions.checkNotNull(indexId, "indexId is null");
    Preconditions.checkNotNull(type, "type is null");
    Preconditions.checkNotNull(properties, "properties is null");
    this.indexId = indexId;
    this.type = type;
    this.columnId = columnId;
    this.properties = ImmutableMap.copyOf(properties);
    this.factory = resolveFactory(type);
  }

  /**
   * Generates an index ID in the format {@code <type-prefix>_<column-id>_<uuid-suffix>}.
   *
   * <p>The type prefix is derived from the first segment of the type string split by "-". For
   * example, "bloom-filter-v1" produces prefix "bloom".
   *
   * @param type the index type string
   * @param columnId the column ID
   * @return a unique index ID
   */
  public static String generateIndexId(String type, int columnId) {
    String prefix = Iterables.get(Splitter.on('-').split(type), 0);
    return String.format(Locale.ROOT, "%s_%d_%s", prefix, columnId, UUID.randomUUID().toString().substring(0, 8));
  }

  private static IndexFactory resolveFactory(String type) {
    switch (type) {
      case BLOOM_FILTER_V1:
        return new BloomFilterFactory();
      default:
        throw new IllegalArgumentException("Unknown index type: " + type);
    }
  }

  /**
   * Creates a new index instance using this spec's factory.
   *
   * @return a new index for this spec's column and properties
   * @throws UnsupportedOperationException if the factory for this type is not yet implemented
   */
  public Index createIndex() {
    Preconditions.checkState(factory != null, "IndexFactory not available for type: %s", type);
    return factory.create(columnId, properties);
  }

  public String indexId() {
    return indexId;
  }

  public String type() {
    return type;
  }

  public int columnId() {
    return columnId;
  }

  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the value of a property, or the default value if not present.
   *
   * @param key the property key
   * @param defaultValue the default value if the key is not found
   * @return the property value or default
   */
  public String property(String key, String defaultValue) {
    return properties.getOrDefault(key, defaultValue);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexSpec that = (IndexSpec) o;
    return columnId == that.columnId
        && Objects.equals(indexId, that.indexId)
        && Objects.equals(type, that.type)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexId, type, columnId, properties);
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "IndexSpec[indexId=%s, type=%s, columnId=%d, properties=%s]",
        indexId, type, columnId, properties);
  }
}
