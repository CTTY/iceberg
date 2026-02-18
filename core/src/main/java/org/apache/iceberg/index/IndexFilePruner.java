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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prunes {@link FileScanTask}s using indexes loaded from Puffin files during scan planning.
 *
 * <p>The pruner loads indexes from one or more Puffin files via {@link IndexPuffinReader} and builds
 * a lookup map from data-file-path to {@link Index}. During pruning, each file scan task is
 * evaluated against its corresponding index. Files are pruned (excluded from the scan) only when the
 * index definitively indicates no matching rows exist.
 *
 * <p>All operations follow fail-safe behavior:
 *
 * <ul>
 *   <li>If an index file cannot be loaded, it is skipped with a warning
 *   <li>If a data file has no corresponding index, it is included in the scan
 *   <li>If index evaluation throws an exception, the file is included in the scan
 * </ul>
 *
 * <p>Usage:
 *
 * <pre>{@code
 * IndexFilePruner pruner = IndexFilePruner.load(indexFilePaths, fileIO);
 * List<FileScanTask> prunedTasks = pruner.pruneFiles(tasks, filter, schema);
 * }</pre>
 */
public class IndexFilePruner {

  private static final Logger LOG = LoggerFactory.getLogger(IndexFilePruner.class);

  private final Map<String, Index> dataFileToIndex;
  private int lastFilesPruned;

  private IndexFilePruner(Map<String, Index> dataFileToIndex) {
    this.dataFileToIndex = dataFileToIndex;
    this.lastFilesPruned = 0;
  }

  /**
   * Loads indexes from the given Puffin file paths and creates a pruner.
   *
   * <p>Delegates to {@link IndexPuffinReader#load(List, FileIO)} for the actual Puffin file reading
   * and index deserialization. All errors during loading are handled with fail-safe behavior by the
   * reader.
   *
   * @param indexFilePaths the list of Puffin file paths to load indexes from
   * @param io the FileIO instance for accessing the files
   * @return an IndexFilePruner with the loaded indexes
   */
  public static IndexFilePruner load(List<String> indexFilePaths, FileIO io) {
    IndexPuffinReader reader = IndexPuffinReader.load(indexFilePaths, io);
    return new IndexFilePruner(reader.dataFileToIndex());
  }

  /**
   * Prunes file scan tasks by evaluating the filter expression against loaded indexes.
   *
   * <p>For each task, the pruner looks up the index for the task's data file path. If an index
   * exists and its {@link Index#prune(org.apache.iceberg.DataFile, Expression, Schema)} method
   * returns true, the file is excluded from the result. Otherwise, the file is kept.
   *
   * <p>Fail-safe behavior ensures that files are never incorrectly excluded:
   *
   * <ul>
   *   <li>Files with no corresponding index are always included
   *   <li>Files where index evaluation throws an exception are always included
   * </ul>
   *
   * @param tasks the list of file scan tasks to evaluate
   * @param filter the filter expression to evaluate against indexes
   * @param schema the table schema for type information
   * @return the list of tasks that should not be pruned (i.e., may contain matching rows)
   */
  public List<FileScanTask> pruneFiles(
      List<FileScanTask> tasks, Expression filter, Schema schema) {
    if (tasks == null || tasks.isEmpty()) {
      return Collections.emptyList();
    }

    if (dataFileToIndex.isEmpty()) {
      return tasks;
    }

    List<FileScanTask> result = new ArrayList<>();
    int evaluated = 0;
    int pruned = 0;

    for (FileScanTask task : tasks) {
      String dataFilePath = task.file().location();
      Index index = dataFileToIndex.get(dataFilePath);

      if (index == null) {
        // No index for this file, keep it (fail-safe: Req 5.8)
        result.add(task);
        continue;
      }

      evaluated++;

      try {
        if (index.prune(task.file(), filter, schema)) {
          // Index says file can be pruned (definitely no matching rows)
          pruned++;
        } else {
          // Index says file might contain matching rows, keep it
          result.add(task);
        }
      } catch (Exception e) {
        // Fail-safe: on any error, include the file (Req 5.7)
        LOG.warn("Index evaluation failed for file: {}", dataFilePath, e);
        result.add(task);
      }
    }

    this.lastFilesPruned = pruned;

    LOG.debug(
        "Index pruning: evaluated {} files, pruned {} of {} total",
        evaluated,
        pruned,
        tasks.size());

    return result;
  }

  /** Returns the number of data files that have loaded indexes. */
  public int indexCount() {
    return dataFileToIndex.size();
  }

  /**
   * Returns the number of files pruned by the most recent call to {@link #pruneFiles}.
   *
   * <p>This metric is reset on each invocation of {@code pruneFiles} and reflects the count of
   * files that were excluded from the scan because the index definitively indicated no matching rows
   * exist.
   *
   * @return the number of files pruned in the last pruneFiles call, or 0 if pruneFiles has not been
   *     called
   */
  public int filesPruned() {
    return lastFilesPruned;
  }

  /** Returns true if no indexes were loaded. */
  public boolean isEmpty() {
    return dataFileToIndex.isEmpty();
  }
}
