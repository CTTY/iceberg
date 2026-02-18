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

import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import org.apache.iceberg.types.Types;

/**
 * Property-based tests for Bloom filter bitset serialization format.
 *
 * <p><b>Property 7: Bitset Serialization Format</b>
 *
 * <p>For any Bloom filter with m bits, serialization produces exactly ceil(m/8) bytes where bit
 * position i maps to byte i/8, bit i%8, with bit 0 as LSB of byte 0. Deserialization reconstructs
 * the identical bitset.
 *
 * <p><b>Validates: Requirements 2.7</b>
 */
public class TestBloomFilterSerializationProperty {

  /**
   * Property 7: Serialization round-trip preserves all membership queries.
   *
   * <p>Validates: Requirements 2.7
   */
  @Property(tries = 100)
  void serializationRoundTripPreservesMembership(
      @ForAll @Size(min = 1, max = 50) List<@StringLength(min = 1, max = 50) String> values) {
    int columnId = 1;
    BloomFilterIndex original = new BloomFilterIndex(columnId, values.size() + 10, 0.01);

    for (String value : values) {
      original.add(value, Types.StringType.get());
    }

    byte[] serialized = original.toBytes();
    BloomFilterIndex restored =
        BloomFilterIndex.fromBytes(serialized, columnId, original.numBits(), original.numHashFuncs());

    // Every value that was in the original must be in the restored filter
    for (String value : values) {
      assertThat(restored.mightContain(value, Types.StringType.get()))
          .as("Value '%s' lost after serialization round-trip", value)
          .isTrue();
    }
  }

  /**
   * Property 7: Serialized byte array has exactly ceil(m/8) bytes.
   *
   * <p>Validates: Requirements 2.7
   */
  @Property(tries = 100)
  void serializedSizeIsCeilMDiv8(
      @ForAll @Size(min = 1, max = 20) List<@StringLength(min = 1, max = 30) String> values) {
    BloomFilterIndex filter = new BloomFilterIndex(1, values.size() + 10, 0.01);

    for (String value : values) {
      filter.add(value, Types.StringType.get());
    }

    byte[] serialized = filter.toBytes();
    int expectedSize = (filter.numBits() + 7) / 8;
    assertThat(serialized).hasSize(expectedSize);
  }
}
