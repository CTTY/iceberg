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

import java.io.Serializable;
import java.util.Map;

/**
 * Factory for creating {@link Index} instances.
 *
 * <p>Each index type has its own factory implementation. The factory is stored directly in {@link
 * IndexSpec} for easy access.
 */
public interface IndexFactory extends Serializable {

  /** Returns the index type this factory handles (e.g., "bloom-filter-v1"). */
  String type();

  /**
   * Creates a new empty index for the given column.
   *
   * @param columnId the column ID to create the index for
   * @param properties type-specific configuration properties (e.g., fpp for Bloom filters)
   * @return a new index instance
   */
  Index create(int columnId, Map<String, String> properties);
}
