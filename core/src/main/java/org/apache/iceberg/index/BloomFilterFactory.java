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

import java.util.Map;

/**
 * Factory for creating {@link BloomFilterIndex} instances.
 *
 * <p>Reads configuration from the index spec properties:
 *
 * <ul>
 *   <li>{@code fpp} - false positive probability (default: 0.01)
 *   <li>{@code expected-insertions} - expected number of distinct values (default: 10000)
 * </ul>
 */
public class BloomFilterFactory implements IndexFactory {

  private static final long serialVersionUID = 1L;

  public static final String TYPE = "bloom-filter-v1";
  public static final String FPP_PROPERTY = "fpp";
  public static final double DEFAULT_FPP = 0.01;
  public static final String EXPECTED_INSERTIONS_PROPERTY = "expected-insertions";
  public static final long DEFAULT_EXPECTED_INSERTIONS = 10000L;

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Index create(int columnId, Map<String, String> properties) {
    double fpp =
        Double.parseDouble(properties.getOrDefault(FPP_PROPERTY, String.valueOf(DEFAULT_FPP)));

    if (fpp <= 0.0 || fpp >= 1.0) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid false positive probability (fpp): %s. Must be in range (0.0, 1.0).", fpp));
    }

    long expectedInsertions =
        Long.parseLong(
            properties.getOrDefault(
                EXPECTED_INSERTIONS_PROPERTY, String.valueOf(DEFAULT_EXPECTED_INSERTIONS)));
    return new BloomFilterIndex(columnId, expectedInsertions, fpp);
  }
}
