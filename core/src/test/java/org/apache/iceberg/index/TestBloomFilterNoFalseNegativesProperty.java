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
import java.util.Set;
import java.util.UUID;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import org.apache.iceberg.types.Types;

/**
 * Property-based tests for the Bloom filter no-false-negatives guarantee.
 *
 * <p><b>Property 9: Bloom Filter No False Negatives</b>
 *
 * <p>For any value added to a Bloom filter, {@code mightContain(value)} SHALL return true. This is
 * the fundamental Bloom filter guarantee: no false negatives.
 *
 * <p><b>Validates: Requirements 5.5, 5.6</b>
 */
public class TestBloomFilterNoFalseNegativesProperty {

  /**
   * Property 9: Every string added to the Bloom filter is reported as possibly present.
   *
   * <p>Validates: Requirements 5.5, 5.6
   */
  @Property(tries = 100)
  void noFalseNegativesForStrings(
      @ForAll @Size(min = 1, max = 50) List<@StringLength(max = 100) String> values) {
    BloomFilterIndex filter = new BloomFilterIndex(1, values.size() + 10, 0.01);

    for (String value : values) {
      filter.add(value, Types.StringType.get());
    }

    for (String value : values) {
      assertThat(filter.mightContain(value, Types.StringType.get()))
          .as("Value '%s' was added but mightContain returned false", value)
          .isTrue();
    }
  }

  /**
   * Property 9: Every integer added to the Bloom filter is reported as possibly present.
   *
   * <p>Validates: Requirements 5.5, 5.6
   */
  @Property(tries = 100)
  void noFalseNegativesForIntegers(@ForAll @Size(min = 1, max = 50) Set<Integer> values) {
    BloomFilterIndex filter = new BloomFilterIndex(1, values.size() + 10, 0.01);

    for (Integer value : values) {
      filter.add(value, Types.IntegerType.get());
    }

    for (Integer value : values) {
      assertThat(filter.mightContain(value, Types.IntegerType.get()))
          .as("Value %d was added but mightContain returned false", value)
          .isTrue();
    }
  }

  /**
   * Property 9: Every long added to the Bloom filter is reported as possibly present.
   *
   * <p>Validates: Requirements 5.5, 5.6
   */
  @Property(tries = 100)
  void noFalseNegativesForLongs(@ForAll @Size(min = 1, max = 50) Set<Long> values) {
    BloomFilterIndex filter = new BloomFilterIndex(1, values.size() + 10, 0.01);

    for (Long value : values) {
      filter.add(value, Types.LongType.get());
    }

    for (Long value : values) {
      assertThat(filter.mightContain(value, Types.LongType.get()))
          .as("Value %d was added but mightContain returned false", value)
          .isTrue();
    }
  }

  /**
   * Property 9: Every UUID added to the Bloom filter is reported as possibly present.
   *
   * <p>Validates: Requirements 5.5, 5.6
   */
  @Property(tries = 100)
  void noFalseNegativesForUuids(@ForAll @Size(min = 1, max = 50) List<Long> seeds) {
    BloomFilterIndex filter = new BloomFilterIndex(1, seeds.size() + 10, 0.01);
    UUID[] uuids = new UUID[seeds.size()];

    for (int i = 0; i < seeds.size(); i++) {
      uuids[i] = new UUID(seeds.get(i), seeds.get(i) ^ 0xDEADBEEFL);
      filter.add(uuids[i], Types.UUIDType.get());
    }

    for (UUID uuid : uuids) {
      assertThat(filter.mightContain(uuid, Types.UUIDType.get()))
          .as("UUID %s was added but mightContain returned false", uuid)
          .isTrue();
    }
  }
}
