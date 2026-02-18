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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

/**
 * Serializes and deserializes {@link IndexSpec} to and from JSON.
 *
 * <p>JSON format:
 *
 * <pre>
 * {
 *   "index-id": "bloom_5_a1b2c3d4",
 *   "type": "bloom-filter-v1",
 *   "column-id": 5,
 *   "properties": {
 *     "fpp": "0.01"
 *   }
 * }
 * </pre>
 */
public class IndexSpecParser {

  static final String INDEX_ID = "index-id";
  static final String TYPE = "type";
  static final String COLUMN_ID = "column-id";
  static final String PROPERTIES = "properties";

  private IndexSpecParser() {}

  /**
   * Writes an {@link IndexSpec} to a {@link JsonGenerator}.
   *
   * @param spec the index spec to serialize
   * @param generator the JSON generator to write to
   * @throws IOException if an I/O error occurs
   */
  public static void toJson(IndexSpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(INDEX_ID, spec.indexId());
    generator.writeStringField(TYPE, spec.type());
    generator.writeNumberField(COLUMN_ID, spec.columnId());
    JsonUtil.writeStringMap(PROPERTIES, spec.properties(), generator);
    generator.writeEndObject();
  }

  /**
   * Converts an {@link IndexSpec} to a JSON string.
   *
   * @param spec the index spec to serialize
   * @return the JSON string representation
   */
  public static String toJson(IndexSpec spec) {
    return toJson(spec, false);
  }

  /**
   * Converts an {@link IndexSpec} to a JSON string with optional pretty printing.
   *
   * @param spec the index spec to serialize
   * @param pretty whether to pretty-print the JSON
   * @return the JSON string representation
   */
  public static String toJson(IndexSpec spec, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(spec, gen), pretty);
  }

  /**
   * Parses an {@link IndexSpec} from a JSON string.
   *
   * @param json the JSON string to parse
   * @return the parsed IndexSpec
   */
  public static IndexSpec fromJson(String json) {
    return JsonUtil.parse(json, IndexSpecParser::fromJson);
  }

  /**
   * Parses an {@link IndexSpec} from a {@link JsonNode}.
   *
   * @param json the JSON node to parse
   * @return the parsed IndexSpec
   */
  public static IndexSpec fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse index spec from non-object: %s", json);

    String indexId = JsonUtil.getString(INDEX_ID, json);
    String type = JsonUtil.getString(TYPE, json);
    int columnId = JsonUtil.getInt(COLUMN_ID, json);

    Map<String, String> properties;
    if (json.has(PROPERTIES)) {
      properties = JsonUtil.getStringMap(PROPERTIES, json);
    } else {
      properties = ImmutableMap.of();
    }

    return new IndexSpec(indexId, type, columnId, properties);
  }
}
