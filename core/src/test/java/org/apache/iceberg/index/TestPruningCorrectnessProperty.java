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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Tag;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

/**
 * Property-based tests for pruning correctness.
 *
 * <p><b>Property 14: Pruning Correctness</b>
 *
 * <p>For any data file and equality/IN predicate, the file SHALL be pruned if and only if {@code
 * Index.prune()} returns true. For Bloom filters, this means "definitely not present" for ALL
 * literal values in the predicate.
 *
 * <p><b>Validates: Requirements 5.3, 5.4, 5.5, 5.6</b>
 */
@Tag("Feature:bloom-filter-secondary-indexes")
@Tag("Property-14")
public class TestPruningCorrectnessProperty {

  private static final int COLUMN_ID = 1;

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(COLUMN_ID, "name", Types.StringType.get()));

  private static final DataFile DUMMY_DATA_FILE =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("s3://bucket/table/data/part-00000.parquet")
          .withFileSizeInBytes(1024)
          .withRecordCount(100)
          .build();

  /**
   * Property 14: No false negatives on prune — for any set of values added to a bloom filter,
   * querying with EQ for any added value should NOT prune the file.
   *
   * <p>Validates: Requirements 5.3, 5.4, 5.5, 5.6
   */
  @Property(tries = 100)
  void eqOnAddedValueShouldNotPrune(
      @ForAll @Size(min = 1, max = 50) List<@StringLength(min = 1, max = 100) String> values) {
    BloomFilterIndex filter = new BloomFilterIndex(COLUMN_ID, values.size() + 10, 0.01);

    for (String value : values) {
      filter.add(value, Types.StringType.get());
    }

    for (String value : values) {
      Expression eqFilter = Expressions.equal("name", value);
      boolean pruned = filter.prune(DUMMY_DATA_FILE, eqFilter, SCHEMA);

      assertThat(pruned)
          .as(
              "File should NOT be pruned when querying for added value '%s' (no false negatives)",
              value)
          .isFalse();
    }
  }

  /**
   * Property 14: Prune correctness for EQ — for any value NOT added to a bloom filter, if
   * mightContain returns false, then prune with EQ should return true.
   *
   * <p>Validates: Requirements 5.3, 5.4, 5.5, 5.6
   */
  @Property(tries = 100)
  void eqOnAbsentValueShouldPruneWhenDefinitelyNotPresent(
      @ForAll @Size(min = 1, max = 30) List<@StringLength(min = 1, max = 50) String> addedValues,
      @ForAll @Size(min = 1, max = 30) List<@StringLength(min = 1, max = 50) String> queryValues) {
    BloomFilterIndex filter = new BloomFilterIndex(COLUMN_ID, addedValues.size() + 10, 0.01);

    Set<String> addedSet = new HashSet<>(addedValues);
    for (String value : addedValues) {
      filter.add(value, Types.StringType.get());
    }

    for (String queryValue : queryValues) {
      if (addedSet.contains(queryValue)) {
        continue; // skip values that were added
      }

      boolean mightContain = filter.mightContain(queryValue, Types.StringType.get());
      Expression eqFilter = Expressions.equal("name", queryValue);
      boolean pruned = filter.prune(DUMMY_DATA_FILE, eqFilter, SCHEMA);

      if (!mightContain) {
        // If mightContain says definitely not present, prune must return true
        assertThat(pruned)
            .as(
                "File should be pruned when mightContain('%s') is false (definitely not present)",
                queryValue)
            .isTrue();
      } else {
        // If mightContain says possibly present (false positive), prune must return false
        assertThat(pruned)
            .as(
                "File should NOT be pruned when mightContain('%s') is true (possibly present)",
                queryValue)
            .isFalse();
      }
    }
  }

  /**
   * Property 14: IN predicate — keep if any value present. For any IN predicate where at least one
   * value was added to the bloom filter, the file should NOT be pruned.
   *
   * <p>Validates: Requirements 5.3, 5.4, 5.5, 5.6
   */
  @Property(tries = 100)
  void inPredicateKeepsFileWhenAnyValuePresent(
      @ForAll @Size(min = 1, max = 30) List<@StringLength(min = 1, max = 50) String> addedValues,
      @ForAll
          @Size(min = 0, max = 10)
          List<@StringLength(min = 1, max = 50) String>
              extraAbsentValues) {
    BloomFilterIndex filter = new BloomFilterIndex(COLUMN_ID, addedValues.size() + 10, 0.01);

    for (String value : addedValues) {
      filter.add(value, Types.StringType.get());
    }

    // Build an IN predicate that includes at least one added value plus some extra values
    List<String> inValues = new ArrayList<>();
    inValues.add(addedValues.get(0)); // at least one value that was added
    inValues.addAll(extraAbsentValues);

    Expression inFilter = Expressions.in("name", inValues);
    boolean pruned = filter.prune(DUMMY_DATA_FILE, inFilter, SCHEMA);

    assertThat(pruned)
        .as(
            "File should NOT be pruned when IN predicate contains at least one added value '%s'",
            addedValues.get(0))
        .isFalse();
  }

  /**
   * Property 14: IN predicate — prune if all values absent. For any IN predicate where ALL values
   * are definitely not present (mightContain returns false for all), the file should be pruned.
   *
   * <p>Validates: Requirements 5.3, 5.4, 5.5, 5.6
   */
  @Property(tries = 100)
  void inPredicatePrunesFileWhenAllValuesDefinitelyAbsent(
      @ForAll @Size(min = 1, max = 30) List<@StringLength(min = 1, max = 50) String> addedValues,
      @ForAll
          @Size(min = 1, max = 20)
          List<@StringLength(min = 1, max = 50) String>
              candidateQueryValues) {
    BloomFilterIndex filter = new BloomFilterIndex(COLUMN_ID, addedValues.size() + 10, 0.01);

    Set<String> addedSet = new HashSet<>(addedValues);
    for (String value : addedValues) {
      filter.add(value, Types.StringType.get());
    }

    // Filter candidate query values to only those that are definitely not present
    List<String> definitelyAbsent = new ArrayList<>();
    for (String candidate : candidateQueryValues) {
      if (!addedSet.contains(candidate)
          && !filter.mightContain(candidate, Types.StringType.get())) {
        definitelyAbsent.add(candidate);
      }
    }

    if (definitelyAbsent.isEmpty()) {
      return; // no suitable test values found, skip this iteration
    }

    Expression inFilter = Expressions.in("name", definitelyAbsent);
    boolean pruned = filter.prune(DUMMY_DATA_FILE, inFilter, SCHEMA);

    assertThat(pruned)
        .as(
            "File should be pruned when ALL values in IN predicate are definitely not present (%s)",
            definitelyAbsent)
        .isTrue();
  }
}
