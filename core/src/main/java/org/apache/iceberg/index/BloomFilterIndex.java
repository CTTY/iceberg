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

import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;

/**
 * In-memory Bloom filter implementation for bloom-filter-v1.
 *
 * <p>Implements the {@link Index} interface for integration with the index framework. Uses {@link
 * BloomFilterEncoder} for canonical byte encoding, hashing, and bit position computation.
 *
 * <p>The Bloom filter guarantees no false negatives: if a value was added, {@link
 * #mightContain(Object, Type)} will always return true. False positives are possible at a rate
 * controlled by the configured false positive probability (fpp).
 */
public class BloomFilterIndex implements Index {

  private static final long serialVersionUID = 1L;

  private final int columnId;
  private final BitSet bitset;
  private final int numBits;
  private final int numHashFuncs;
  private long ndv;
  private long numValues;

  /**
   * Creates a new Bloom filter index with optimal parameters.
   *
   * @param columnId the column ID this index is built on
   * @param expectedInsertions the expected number of distinct values to insert
   * @param fpp the desired false positive probability (0.0 &lt; fpp &lt; 1.0)
   */
  public BloomFilterIndex(int columnId, long expectedInsertions, double fpp) {
    this.columnId = columnId;
    this.numBits = BloomFilterEncoder.optimalNumBits(expectedInsertions, fpp);
    this.numHashFuncs =
        BloomFilterEncoder.optimalNumHashFunctions(expectedInsertions, this.numBits);
    this.bitset = new BitSet(numBits);
    this.ndv = 0;
    this.numValues = 0;
  }

  /**
   * Creates a Bloom filter index with explicit parameters. Used for deserialization via {@link
   * #fromBytes(byte[], int, int, int)}.
   */
  private BloomFilterIndex(int columnId, int numBits, int numHashFuncs) {
    this.columnId = columnId;
    this.numBits = numBits;
    this.numHashFuncs = numHashFuncs;
    this.bitset = new BitSet(numBits);
    this.ndv = 0;
    this.numValues = 0;
  }

  @Override
  public String type() {
    return BloomFilterFactory.TYPE;
  }

  @Override
  public int columnId() {
    return columnId;
  }

  /**
   * Adds a value to the Bloom filter.
   *
   * <p>Null values are skipped and not indexed. Each non-null value is converted to canonical bytes,
   * hashed, and the corresponding bit positions are set in the bitset.
   *
   * @param value the value to add (null values are ignored)
   * @param type the Iceberg type of the value
   */
  public void add(Object value, Type type) {
    if (value == null) {
      return;
    }

    byte[] bytes = BloomFilterEncoder.toCanonicalBytes(value, type);
    long[] hash = BloomFilterEncoder.murmur3Hash128(bytes);
    int[] positions = BloomFilterEncoder.computeBitPositions(hash[0], hash[1], numBits, numHashFuncs);

    for (int pos : positions) {
      bitset.set(pos);
    }

    numValues++;
    ndv++;
  }

  /**
   * Tests whether a value might be contained in the Bloom filter.
   *
   * <p>Returns true if the value is possibly present (may be a false positive), or false if the
   * value is definitely not present (no false negatives).
   *
   * @param value the value to test
   * @param type the Iceberg type of the value
   * @return true if the value might be present, false if definitely not present
   */
  public boolean mightContain(Object value, Type type) {
    byte[] bytes = BloomFilterEncoder.toCanonicalBytes(value, type);
    long[] hash = BloomFilterEncoder.murmur3Hash128(bytes);
    int[] positions = BloomFilterEncoder.computeBitPositions(hash[0], hash[1], numBits, numHashFuncs);

    for (int pos : positions) {
      if (!bitset.get(pos)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean prune(DataFile dataFile, Expression filter, Schema schema) {
    try {
      Expression bound;
      if (Binder.isBound(filter)) {
        bound = filter;
      } else {
        bound = Binder.bind(schema.asStruct(), filter, false);
      }

      return ExpressionVisitors.visit(bound, new BloomFilterVisitor(schema));
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public Map<String, String> metadata() {
    return ImmutableMap.of(
        "num-bits", String.valueOf(numBits),
        "num-hash-funcs", String.valueOf(numHashFuncs),
        "ndv", String.valueOf(ndv),
        "num-values", String.valueOf(numValues));
  }

  /**
   * Serializes the Bloom filter bitset to bytes.
   *
   * <p>The bitset is stored as ceil(numBits/8) bytes where bit position i maps to byte i/8, bit
   * i%8, with bit 0 as the LSB of byte 0.
   *
   * @return the serialized bitset bytes
   */
  public byte[] toBytes() {
    byte[] bytes = new byte[(numBits + 7) / 8];
    for (int i = 0; i < numBits; i++) {
      if (bitset.get(i)) {
        bytes[i / 8] = (byte) (bytes[i / 8] | (1 << (i % 8)));
      }
    }

    return bytes;
  }

  /**
   * Deserializes a Bloom filter from bytes.
   *
   * <p>Reconstructs the bitset from the byte array using the same encoding as {@link #toBytes()}.
   *
   * @param data the serialized bitset bytes
   * @param columnId the column ID this index is built on
   * @param numBits the number of bits in the Bloom filter (m)
   * @param numHashFuncs the number of hash functions (k)
   * @return a reconstructed BloomFilterIndex
   */
  public static BloomFilterIndex fromBytes(byte[] data, int columnId, int numBits, int numHashFuncs) {
    BloomFilterIndex index = new BloomFilterIndex(columnId, numBits, numHashFuncs);
    for (int i = 0; i < numBits; i++) {
      if ((data[i / 8] & (1 << (i % 8))) != 0) {
        index.bitset.set(i);
      }
    }

    return index;
  }

  /** Returns the number of bits in this Bloom filter. */
  public int numBits() {
    return numBits;
  }

  /** Returns the number of hash functions used by this Bloom filter. */
  public int numHashFuncs() {
    return numHashFuncs;
  }

  /** Returns the number of distinct values added to this Bloom filter. */
  public long ndv() {
    return ndv;
  }

  /** Returns the total number of non-null values added to this Bloom filter. */
  public long numValues() {
    return numValues;
  }

  /**
   * Visitor that evaluates bound expressions against this Bloom filter.
   *
   * <p>For EQ predicates on this column, tests the literal value. For IN predicates, tests all
   * values in the set. Returns true (prune) only if ALL literals are definitely not present. Returns
   * false (keep) for any other predicate type or on any error.
   */
  private class BloomFilterVisitor extends ExpressionVisitors.BoundVisitor<Boolean> {
    private final Schema schema;

    BloomFilterVisitor(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Boolean alwaysTrue() {
      return false;
    }

    @Override
    public Boolean alwaysFalse() {
      return false;
    }

    @Override
    public Boolean not(Boolean result) {
      return false;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      // prune if either side says prune (AND means both conditions must hold)
      return leftResult || rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      // prune only if both sides say prune (OR means at least one must hold)
      return leftResult && rightResult;
    }

    @Override
    public <T> Boolean isNull(Bound<T> expr) {
      return false;
    }

    @Override
    public <T> Boolean notNull(Bound<T> expr) {
      return false;
    }

    @Override
    public <T> Boolean isNaN(Bound<T> expr) {
      return false;
    }

    @Override
    public <T> Boolean notNaN(Bound<T> expr) {
      return false;
    }

    @Override
    public <T> Boolean lt(Bound<T> expr, Literal<T> lit) {
      return false;
    }

    @Override
    public <T> Boolean ltEq(Bound<T> expr, Literal<T> lit) {
      return false;
    }

    @Override
    public <T> Boolean gt(Bound<T> expr, Literal<T> lit) {
      return false;
    }

    @Override
    public <T> Boolean gtEq(Bound<T> expr, Literal<T> lit) {
      return false;
    }

    @Override
    public <T> Boolean eq(Bound<T> expr, Literal<T> lit) {
      if (expr.ref().fieldId() != columnId) {
        return false;
      }

      Type colType = schema.findType(columnId);
      return !mightContain(lit.value(), colType);
    }

    @Override
    public <T> Boolean notEq(Bound<T> expr, Literal<T> lit) {
      return false;
    }

    @Override
    public <T> Boolean in(Bound<T> expr, Set<T> literalSet) {
      if (expr.ref().fieldId() != columnId) {
        return false;
      }

      if (literalSet.isEmpty()) {
        return false;
      }

      Type colType = schema.findType(columnId);
      for (T value : literalSet) {
        if (mightContain(value, colType)) {
          return false;
        }
      }

      return true;
    }

    @Override
    public <T> Boolean notIn(Bound<T> expr, Set<T> literalSet) {
      return false;
    }

    @Override
    public <T> Boolean startsWith(Bound<T> expr, Literal<T> lit) {
      return false;
    }

    @Override
    public <T> Boolean notStartsWith(Bound<T> expr, Literal<T> lit) {
      return false;
    }
  }
}
