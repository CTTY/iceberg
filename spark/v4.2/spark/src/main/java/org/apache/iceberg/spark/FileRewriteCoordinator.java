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
package org.apache.iceberg.spark;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.CompactionMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;

public class FileRewriteCoordinator extends BaseFileRewriteCoordinator<DataFile> {

  private static final FileRewriteCoordinator INSTANCE = new FileRewriteCoordinator();

  // file sets whose rewrite must track row positions (source -> target) while writing
  private final Set<Pair<String, String>> positionTrackedSets = Sets.newConcurrentHashSet();
  // position runs reported by the write tasks of a tracked file set
  private final Map<Pair<String, String>, List<CompactionMap.Run>> positionRuns =
      Maps.newConcurrentMap();

  private FileRewriteCoordinator() {}

  public static FileRewriteCoordinator get() {
    return INSTANCE;
  }

  /**
   * Marks a file set so that its rewrite exposes {@code _file} and {@code _pos} to the writer and
   * records how positions move to the output files. Must be called before the rewrite table for the
   * file set is loaded.
   */
  public void enablePositionTracking(Table table, String fileSetId) {
    positionTrackedSets.add(toId(table, fileSetId));
  }

  public boolean tracksPositions(Table table, String fileSetId) {
    return positionTrackedSets.contains(toId(table, fileSetId));
  }

  /** Called from the rewrite batch write commit with the runs collected from all write tasks. */
  public void stagePositionRuns(Table table, String fileSetId, List<CompactionMap.Run> runs) {
    positionRuns.put(toId(table, fileSetId), ImmutableList.copyOf(runs));
  }

  /** Returns the staged runs for a tracked file set, or null if none were staged. */
  public List<CompactionMap.Run> fetchPositionRuns(Table table, String fileSetId) {
    return positionRuns.get(toId(table, fileSetId));
  }

  @Override
  public void clearRewrite(Table table, String fileSetId) {
    super.clearRewrite(table, fileSetId);
    Pair<String, String> id = toId(table, fileSetId);
    positionTrackedSets.remove(id);
    positionRuns.remove(id);
  }

  private Pair<String, String> toId(Table table, String setId) {
    return Pair.of(Spark3Util.baseTableUUID(table), setId);
  }
}
