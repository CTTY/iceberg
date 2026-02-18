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
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.hash.HashCode;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Encodes values into Bloom filters using the bloom-filter-v1 specification.
 *
 * <p>Canonical byte encoding:
 *
 * <ul>
 *   <li>String: UTF-8 bytes
 *   <li>Integer: 4-byte big-endian
 *   <li>Long: 8-byte big-endian
 *   <li>UUID: 16-byte MSB followed by LSB
 * </ul>
 *
 * <p>Hash function: murmur3_x64_128(bytes, seed=0)
 *
 * <ul>
 *   <li>h1 = low 64 bits
 *   <li>h2 = high 64 bits
 *   <li>pos_i = (h1 + i * h2) mod m, for i in [0, k-1]
 * </ul>
 */
public class BloomFilterEncoder {

  private BloomFilterEncoder() {}

  /**
   * Converts a value to its canonical byte representation for hashing.
   *
   * @param value the value to convert
   * @param type the Iceberg type of the value
   * @return the canonical byte representation
   * @throws IllegalArgumentException if the type is not supported
   */
  public static byte[] toCanonicalBytes(Object value, Type type) {
    if (type instanceof Types.StringType) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    } else if (type instanceof Types.IntegerType) {
      ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
      buf.putInt((Integer) value);
      return buf.array();
    } else if (type instanceof Types.LongType) {
      ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
      buf.putLong((Long) value);
      return buf.array();
    } else if (type instanceof Types.UUIDType) {
      UUID uuid = (UUID) value;
      ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
      buf.putLong(uuid.getMostSignificantBits());
      buf.putLong(uuid.getLeastSignificantBits());
      return buf.array();
    }

    throw new IllegalArgumentException("Unsupported type: " + type);
  }

  /**
   * Computes the murmur3_x64_128 hash of the given data with seed 0.
   *
   * @param data the byte array to hash
   * @return a two-element array where index 0 is h1 (low 64 bits) and index 1 is h2 (high 64 bits)
   */
  @SuppressWarnings("deprecation")
  public static long[] murmur3Hash128(byte[] data) {
    HashCode hashCode = Hashing.murmur3_128(0).hashBytes(data);

    // h1 is the low 64 bits, available directly via asLong()
    long h1 = hashCode.asLong();

    // h2 is the high 64 bits, extracted from bytes 8-15 (little-endian order in HashCode)
    byte[] bytes = hashCode.asBytes();
    long h2 =
        (bytes[8] & 0xFFL)
            | ((bytes[9] & 0xFFL) << 8)
            | ((bytes[10] & 0xFFL) << 16)
            | ((bytes[11] & 0xFFL) << 24)
            | ((bytes[12] & 0xFFL) << 32)
            | ((bytes[13] & 0xFFL) << 40)
            | ((bytes[14] & 0xFFL) << 48)
            | ((bytes[15] & 0xFFL) << 56);

    return new long[] {h1, h2};
  }

  /**
   * Computes the k bit positions for a Bloom filter using the enhanced double hashing scheme.
   *
   * <p>Each position is computed as: pos_i = (h1 + i * h2) mod m, for i in [0, k-1].
   *
   * @param h1 the first hash value (low 64 bits of murmur3_128)
   * @param h2 the second hash value (high 64 bits of murmur3_128)
   * @param m the number of bits in the Bloom filter
   * @param k the number of hash functions
   * @return an array of k bit positions, each in range [0, m-1]
   */
  public static int[] computeBitPositions(long h1, long h2, int m, int k) {
    int[] positions = new int[k];
    for (int i = 0; i < k; i++) {
      positions[i] = (int) Math.floorMod(h1 + (long) i * h2, m);
    }

    return positions;
  }

  /**
   * Calculates the optimal number of bits for a Bloom filter.
   *
   * <p>Uses the formula: m = ceil(-n * ln(fpp) / (ln(2)^2))
   *
   * @param n the expected number of distinct values
   * @param fpp the desired false positive probability (0.0 < fpp < 1.0)
   * @return the optimal number of bits
   */
  public static int optimalNumBits(long n, double fpp) {
    return (int) Math.ceil(-n * Math.log(fpp) / (Math.log(2) * Math.log(2)));
  }

  /**
   * Calculates the optimal number of hash functions for a Bloom filter.
   *
   * <p>Uses the formula: k = max(1, round(m / n * ln(2)))
   *
   * @param n the expected number of distinct values
   * @param m the number of bits in the Bloom filter
   * @return the optimal number of hash functions (at least 1)
   */
  public static int optimalNumHashFunctions(long n, int m) {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }
}
