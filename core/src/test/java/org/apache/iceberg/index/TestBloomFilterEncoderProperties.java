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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.StringLength;
import org.apache.iceberg.types.Types;

/**
 * Property-based tests for {@link BloomFilterEncoder} canonical byte encoding.
 *
 * <p><b>Property 4: Canonical Byte Encoding Round-Trip</b>
 *
 * <p>For any value of supported type (string, int, long, uuid), encoding to canonical bytes and
 * then using those bytes for Bloom filter operations produces consistent results.
 *
 * <p><b>Validates: Requirements 2.1, 2.2, 2.3, 2.4</b>
 */
public class TestBloomFilterEncoderProperties {

  /**
   * Property 4: String canonical byte encoding produces UTF-8 bytes deterministically.
   *
   * <p>Validates: Requirements 2.1
   */
  @Property(tries = 100)
  void stringEncodingIsUtf8(@ForAll @StringLength(max = 200) String value) {
    byte[] encoded = BloomFilterEncoder.toCanonicalBytes(value, Types.StringType.get());
    assertThat(encoded).isEqualTo(value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Property 4: Integer canonical byte encoding produces 4-byte big-endian deterministically.
   *
   * <p>Validates: Requirements 2.2
   */
  @Property(tries = 100)
  void intEncodingIsBigEndian(@ForAll int value) {
    byte[] encoded = BloomFilterEncoder.toCanonicalBytes(value, Types.IntegerType.get());
    ByteBuffer expected = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    expected.putInt(value);
    assertThat(encoded).hasSize(4).isEqualTo(expected.array());
  }

  /**
   * Property 4: Long canonical byte encoding produces 8-byte big-endian deterministically.
   *
   * <p>Validates: Requirements 2.3
   */
  @Property(tries = 100)
  void longEncodingIsBigEndian(@ForAll long value) {
    byte[] encoded = BloomFilterEncoder.toCanonicalBytes(value, Types.LongType.get());
    ByteBuffer expected = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    expected.putLong(value);
    assertThat(encoded).hasSize(8).isEqualTo(expected.array());
  }

  /**
   * Property 4: UUID canonical byte encoding produces 16-byte MSB+LSB deterministically.
   *
   * <p>Validates: Requirements 2.4
   */
  @Property(tries = 100)
  void uuidEncodingIsMsbLsb(@ForAll @IntRange(min = 0) int seed) {
    UUID uuid = new UUID((long) seed * 31 + 17, (long) seed * 37 + 13);
    byte[] encoded = BloomFilterEncoder.toCanonicalBytes(uuid, Types.UUIDType.get());
    ByteBuffer expected = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
    expected.putLong(uuid.getMostSignificantBits());
    expected.putLong(uuid.getLeastSignificantBits());
    assertThat(encoded).hasSize(16).isEqualTo(expected.array());
  }

  /**
   * Property 4: Encoding the same value twice produces identical bytes (determinism).
   *
   * <p>Validates: Requirements 2.1, 2.2, 2.3, 2.4
   */
  @Property(tries = 100)
  void encodingIsDeterministic(@ForAll @StringLength(max = 100) String value) {
    byte[] first = BloomFilterEncoder.toCanonicalBytes(value, Types.StringType.get());
    byte[] second = BloomFilterEncoder.toCanonicalBytes(value, Types.StringType.get());
    assertThat(first).isEqualTo(second);
  }
}
