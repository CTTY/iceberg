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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;

/**
 * Base interface for all index types.
 *
 * <p>Implementations include Bloom filters, bitmap indexes, etc. Extends {@link Serializable} for
 * storage in Puffin files and distribution in Spark.
 */
public interface Index extends Serializable {

  /** Returns the index type identifier (e.g., "bloom-filter-v1"). */
  String type();

  /** Returns the column ID this index is built on. */
  int columnId();

  /**
   * Determines whether a data file should be pruned based on the filter expression.
   *
   * @param dataFile the data file to evaluate
   * @param filter the filter expression to evaluate
   * @param schema the table schema for type information
   * @return true if the file should be pruned (definitely no matching rows), false if the file
   *     should be kept (might contain matching rows)
   */
  boolean prune(DataFile dataFile, Expression filter, Schema schema);

  /** Returns metadata about this index (e.g., NDV, num-values). */
  Map<String, String> metadata();
}
