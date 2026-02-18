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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.ByteRange;
import net.jqwik.api.constraints.Size;

/**
 * Property-based tests for {@link BloomFilterEncoder} murmur3 hash computation.
 *
 * <p><b>Property 5: Murmur3 Hash Correctness</b>
 *
 * <p>For any byte array input, the murmur3_x64_128 hash with seed=0 produces deterministic 128-bit
 * output where h1 is the low 64 bits and h2 is the high 64 bits.
 *
 * <p><b>Validates: Requirements 2.5</b>
 */
public class TestBloomFilterHashProperties {

  /**
   * Property 5: Murmur3 hash is deterministic - same input always produces same output.
   *
   * <p>Validates: Requirements 2.5
   */
  @Property(tries = 100)
  void murmur3HashIsDeterministic(@ForAll @Size(max = 200) byte[] data) {
    long[] hash1 = BloomFilterEncoder.murmur3Hash128(data);
    long[] hash2 = BloomFilterEncoder.murmur3Hash128(data);
    assertThat(hash1).hasSize(2).isEqualTo(hash2);
  }

  /**
   * Property 5: Murmur3 hash returns exactly two 64-bit values (h1 and h2).
   *
   * <p>Validates: Requirements 2.5
   */
  @Property(tries = 100)
  void murmur3HashReturnsH1AndH2(@ForAll @Size(max = 200) byte[] data) {
    long[] hash = BloomFilterEncoder.murmur3Hash128(data);
    assertThat(hash).hasSize(2);
  }

  /**
   * Property 5: Bit positions computed from hash are all within valid range [0, m-1].
   *
   * <p>Validates: Requirements 2.5, 2.6
   */
  @Property(tries = 100)
  void bitPositionsAreInValidRange(
      @ForAll @Size(max = 100) byte[] data,
      @ForAll @ByteRange(min = 1, max = 20) byte k) {
    int m = 1000;
    long[] hash = BloomFilterEncoder.murmur3Hash128(data);
    int[] positions = BloomFilterEncoder.computeBitPositions(hash[0], hash[1], m, k);

    assertThat(positions).hasSize(k);
    for (int pos : positions) {
      assertThat(pos).isBetween(0, m - 1);
    }
  }
}
