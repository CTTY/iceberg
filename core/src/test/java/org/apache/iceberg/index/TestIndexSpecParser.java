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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IndexSpecParser} JSON serialization and deserialization. */
public class TestIndexSpecParser {

  @Test
  public void jsonRoundTrip() {
    IndexSpec original =
        new IndexSpec("bloom_5_a1b2c3d4", "bloom-filter-v1", 5, ImmutableMap.of("fpp", "0.01"));

    String json = IndexSpecParser.toJson(original);
    IndexSpec parsed = IndexSpecParser.fromJson(json);

    assertThat(parsed).isEqualTo(original);
  }

  @Test
  public void allFieldsPreserved() {
    IndexSpec original =
        new IndexSpec(
            "bloom_42_deadbeef",
            "bloom-filter-v1",
            42,
            ImmutableMap.of("fpp", "0.05", "expected-insertions", "50000"));

    String json = IndexSpecParser.toJson(original);
    IndexSpec parsed = IndexSpecParser.fromJson(json);

    assertThat(parsed.indexId()).isEqualTo("bloom_42_deadbeef");
    assertThat(parsed.type()).isEqualTo("bloom-filter-v1");
    assertThat(parsed.columnId()).isEqualTo(42);
    assertThat(parsed.properties()).containsEntry("fpp", "0.05");
    assertThat(parsed.properties()).containsEntry("expected-insertions", "50000");
  }

  @Test
  public void emptyProperties() {
    IndexSpec original = new IndexSpec("bloom_1_abcd1234", "bloom-filter-v1", 1, ImmutableMap.of());

    String json = IndexSpecParser.toJson(original);
    IndexSpec parsed = IndexSpecParser.fromJson(json);

    assertThat(parsed).isEqualTo(original);
    assertThat(parsed.properties()).isEmpty();
  }

  @Test
  public void prettyPrint() {
    IndexSpec spec =
        new IndexSpec("bloom_5_a1b2c3d4", "bloom-filter-v1", 5, ImmutableMap.of("fpp", "0.01"));

    String pretty = IndexSpecParser.toJson(spec, true);

    assertThat(pretty).contains("\n");
    assertThat(pretty).contains("\"index-id\" : \"bloom_5_a1b2c3d4\"");
    assertThat(pretty).contains("\"type\" : \"bloom-filter-v1\"");
    assertThat(pretty).contains("\"column-id\" : 5");
    assertThat(pretty).contains("\"fpp\" : \"0.01\"");
  }

  @Test
  public void compactJson() {
    IndexSpec spec =
        new IndexSpec("bloom_5_a1b2c3d4", "bloom-filter-v1", 5, ImmutableMap.of("fpp", "0.01"));

    String compact = IndexSpecParser.toJson(spec, false);

    assertThat(compact).doesNotContain("\n");
  }

  @Test
  public void fromJsonString() {
    String json =
        "{\"index-id\":\"bloom_10_ff00ff00\",\"type\":\"bloom-filter-v1\","
            + "\"column-id\":10,\"properties\":{\"fpp\":\"0.001\"}}";

    IndexSpec parsed = IndexSpecParser.fromJson(json);

    assertThat(parsed.indexId()).isEqualTo("bloom_10_ff00ff00");
    assertThat(parsed.type()).isEqualTo("bloom-filter-v1");
    assertThat(parsed.columnId()).isEqualTo(10);
    assertThat(parsed.properties()).containsEntry("fpp", "0.001");
  }

  @Test
  public void fromJsonWithoutProperties() {
    String json =
        "{\"index-id\":\"bloom_3_aabbccdd\",\"type\":\"bloom-filter-v1\",\"column-id\":3}";

    IndexSpec parsed = IndexSpecParser.fromJson(json);

    assertThat(parsed.indexId()).isEqualTo("bloom_3_aabbccdd");
    assertThat(parsed.type()).isEqualTo("bloom-filter-v1");
    assertThat(parsed.columnId()).isEqualTo(3);
    assertThat(parsed.properties()).isEmpty();
  }
}
