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
package org.apache.iceberg.deletes;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * An in-memory description of how row positions moved during an order-preserving rewrite (bin-pack
 * or merge compaction).
 *
 * <p>The map is a set of {@link Run runs}. A run states that rows {@code [sourceStart, sourceStart
 * + length)} of {@code sourceFile} were written to {@code [targetStart, targetStart + length)} of
 * {@code targetFile}. Rows that were dropped by the rewrite (because a delete applied to them) are
 * simply not covered by any run.
 *
 * <p>The map is used to rebase position deletes that were committed against the source files while
 * the rewrite was running: {@code targetPos = targetStart + (sourcePos - sourceStart)}. It lives
 * only in the memory of the process performing the rewrite and is never persisted.
 */
public class CompactionMap implements Serializable {

  /** A contiguous range of rows moved from one source file to one target file. */
  public static class Run implements Serializable {
    private final String sourceFile;
    private final long sourceStart;
    private final long length;
    private final String targetFile;
    private final long targetStart;

    public Run(
        String sourceFile, long sourceStart, long length, String targetFile, long targetStart) {
      Preconditions.checkArgument(sourceFile != null, "Source file cannot be null");
      Preconditions.checkArgument(targetFile != null, "Target file cannot be null");
      Preconditions.checkArgument(sourceStart >= 0, "Source start must be >= 0: %s", sourceStart);
      Preconditions.checkArgument(targetStart >= 0, "Target start must be >= 0: %s", targetStart);
      Preconditions.checkArgument(length > 0, "Run length must be > 0: %s", length);
      this.sourceFile = sourceFile;
      this.sourceStart = sourceStart;
      this.length = length;
      this.targetFile = targetFile;
      this.targetStart = targetStart;
    }

    public String sourceFile() {
      return sourceFile;
    }

    public long sourceStart() {
      return sourceStart;
    }

    public long sourceEnd() {
      return sourceStart + length;
    }

    public long length() {
      return length;
    }

    public String targetFile() {
      return targetFile;
    }

    public long targetStart() {
      return targetStart;
    }

    boolean contains(long sourcePos) {
      return sourcePos >= sourceStart && sourcePos < sourceEnd();
    }

    long map(long sourcePos) {
      return targetStart + (sourcePos - sourceStart);
    }

    /** Returns true if {@code next} directly continues this run in both source and target. */
    boolean canMerge(Run next) {
      return sourceFile.equals(next.sourceFile)
          && targetFile.equals(next.targetFile)
          && next.sourceStart == sourceEnd()
          && next.targetStart == targetStart + length;
    }

    Run merge(Run next) {
      return new Run(sourceFile, sourceStart, length + next.length, targetFile, targetStart);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("source", sourceFile + "[" + sourceStart + ".." + sourceEnd() + ")")
          .add("target", targetFile + "[" + targetStart + ".." + (targetStart + length) + ")")
          .toString();
    }
  }

  private static final Comparator<Run> BY_SOURCE_START = Comparator.comparingLong(Run::sourceStart);

  // source file -> runs sorted by sourceStart, non-overlapping
  private final Map<String, List<Run>> runsBySource;

  private CompactionMap(Map<String, List<Run>> runsBySource) {
    this.runsBySource = runsBySource;
  }

  /**
   * Builds a map from an arbitrary collection of runs. Runs are grouped by source file, sorted and
   * merged when adjacent. Overlapping runs within one source file are rejected.
   */
  public static CompactionMap of(Iterable<Run> runs) {
    Map<String, List<Run>> grouped = Maps.newHashMap();
    for (Run run : runs) {
      grouped.computeIfAbsent(run.sourceFile(), ignored -> Lists.newArrayList()).add(run);
    }

    ImmutableMap.Builder<String, List<Run>> result = ImmutableMap.builder();
    for (Map.Entry<String, List<Run>> entry : grouped.entrySet()) {
      List<Run> sorted = entry.getValue();
      sorted.sort(BY_SOURCE_START);

      List<Run> merged = Lists.newArrayList();
      Run current = null;
      for (Run run : sorted) {
        if (current == null) {
          current = run;
        } else if (current.canMerge(run)) {
          current = current.merge(run);
        } else {
          Preconditions.checkArgument(
              run.sourceStart() >= current.sourceEnd(),
              "Overlapping runs for %s: %s and %s",
              entry.getKey(),
              current,
              run);
          merged.add(current);
          current = run;
        }
      }

      if (current != null) {
        merged.add(current);
      }

      result.put(entry.getKey(), ImmutableList.copyOf(merged));
    }

    return new CompactionMap(result.build());
  }

  public static CompactionMap empty() {
    return new CompactionMap(ImmutableMap.of());
  }

  public boolean isEmpty() {
    return runsBySource.isEmpty();
  }

  public Set<String> sourceFiles() {
    return runsBySource.keySet();
  }

  public boolean containsSource(String sourceFile) {
    return runsBySource.containsKey(sourceFile);
  }

  public List<Run> runsFor(String sourceFile) {
    return runsBySource.getOrDefault(sourceFile, Collections.emptyList());
  }

  public int runCount() {
    return runsBySource.values().stream().mapToInt(List::size).sum();
  }

  /**
   * Maps a single source position. Returns null if the position was not carried to any target file
   * (the row was deleted by the rewrite).
   */
  public Run runFor(String sourceFile, long sourcePos) {
    List<Run> runs = runsFor(sourceFile);
    int lo = 0;
    int hi = runs.size() - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      Run run = runs.get(mid);
      if (sourcePos < run.sourceStart()) {
        hi = mid - 1;
      } else if (sourcePos >= run.sourceEnd()) {
        lo = mid + 1;
      } else {
        return run;
      }
    }

    return null;
  }

  /**
   * Rebases a set of positions in {@code sourceFile} onto the target files of this map.
   *
   * <p>Positions are visited in ascending order and matched against the sorted runs with a single
   * forward cursor (merge join). Positions that fall between runs were deleted by the rewrite and
   * are dropped.
   *
   * @return target file path to the positions that now reference it; never contains empty indexes
   */
  public Map<String, PositionDeleteIndex> remap(String sourceFile, PositionDeleteIndex positions) {
    List<Run> runs = runsFor(sourceFile);
    Map<String, PositionDeleteIndex> result = Maps.newHashMap();
    if (runs.isEmpty() || positions.isEmpty()) {
      return result;
    }

    int[] cursor = new int[] {0};
    positions.forEach(
        pos -> {
          // advance past runs that end at or before this position
          while (cursor[0] < runs.size() && runs.get(cursor[0]).sourceEnd() <= pos) {
            cursor[0]++;
          }

          if (cursor[0] < runs.size()) {
            Run run = runs.get(cursor[0]);
            if (run.contains(pos)) {
              result
                  .computeIfAbsent(run.targetFile(), ignored -> new BitmapPositionDeleteIndex())
                  .delete(run.map(pos));
            }
          }
        });

    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("sourceFiles", runsBySource.size())
        .add("runs", runCount())
        .toString();
  }
}
