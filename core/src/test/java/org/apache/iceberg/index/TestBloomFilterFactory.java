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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BloomFilterFactory} FPP validation and defaults. */
public class TestBloomFilterFactory {

  private final BloomFilterFactory factory = new BloomFilterFactory();

  @Test
  public void validFppValues() {
    for (double fpp : new double[] {0.01, 0.001, 0.5, 0.1, 0.99}) {
      Index index = factory.create(1, ImmutableMap.of("fpp", String.valueOf(fpp)));
      assertThat(index).isNotNull();
      assertThat(index.type()).isEqualTo(BloomFilterFactory.TYPE);
    }
  }

  @Test
  public void fppZeroRejected() {
    assertThatThrownBy(() -> factory.create(1, ImmutableMap.of("fpp", "0.0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid false positive probability");
  }

  @Test
  public void fppOneRejected() {
    assertThatThrownBy(() -> factory.create(1, ImmutableMap.of("fpp", "1.0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid false positive probability");
  }

  @Test
  public void fppNegativeRejected() {
    assertThatThrownBy(() -> factory.create(1, ImmutableMap.of("fpp", "-0.5")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid false positive probability");
  }

  @Test
  public void fppGreaterThanOneRejected() {
    assertThatThrownBy(() -> factory.create(1, ImmutableMap.of("fpp", "1.5")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid false positive probability");
  }

  @Test
  public void defaultFppUsedWhenNotSpecified() {
    Index index = factory.create(1, ImmutableMap.of());
    assertThat(index).isNotNull();
    assertThat(index.type()).isEqualTo(BloomFilterFactory.TYPE);
    assertThat(index.columnId()).isEqualTo(1);
  }

  @Test
  public void typeReturnsBloomFilterV1() {
    assertThat(factory.type()).isEqualTo("bloom-filter-v1");
  }
}
