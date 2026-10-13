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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.CompactionMap;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SparkWrite.TaskCommit;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Wraps a rewrite data writer and records where every input row ends up.
 *
 * <p>Each incoming row carries its source file and source position as trailing {@code _file} and
 * {@code _pos} columns. The wrapper counts output rows per partition, since the wrapped writer
 * assigns positions per output file and output files never span partitions. When the task commits,
 * the per-partition output offsets are split across the task's output files using their record
 * counts (files are produced in order within a partition) and reported as {@link CompactionMap.Run
 * runs} in the {@link TaskCommit}.
 *
 * <p>Runs are merged on the fly: consecutive rows from the same source file that stay consecutive
 * in the output collapse into one run, so a bin-pack of a file without deletes is a single run.
 */
class PositionTrackingDataWriter implements DataWriter<InternalRow> {

  private final DataWriter<InternalRow> delegate;
  private final int fileOrdinal;
  private final int posOrdinal;
  private final PartitionSpec spec;
  private final PartitionKey partitionKey; // null for unpartitioned specs
  private final InternalRowWrapper rowWrapper; // null for unpartitioned specs
  private final Map<StructLike, PartitionRuns> runsByPartition;
  private final PartitionRuns unpartitionedRuns;

  // cache the last source file so the common case (long stretches of one file) avoids re-decoding
  private UTF8String lastSourceFileUtf8 = null;
  private String lastSourceFile = null;

  PositionTrackingDataWriter(
      DataWriter<InternalRow> delegate,
      PartitionSpec spec,
      Schema dataSchema,
      StructType dataSparkType,
      int fileOrdinal,
      int posOrdinal) {
    this.delegate = delegate;
    this.spec = spec;
    this.fileOrdinal = fileOrdinal;
    this.posOrdinal = posOrdinal;
    if (spec.isUnpartitioned()) {
      this.partitionKey = null;
      this.rowWrapper = null;
      this.runsByPartition = null;
      this.unpartitionedRuns = new PartitionRuns();
    } else {
      this.partitionKey = new PartitionKey(spec, dataSchema);
      this.rowWrapper = new InternalRowWrapper(dataSparkType, dataSchema.asStruct());
      this.runsByPartition = StructLikeMap.create(spec.partitionType());
      this.unpartitionedRuns = null;
    }
  }

  @Override
  public void write(InternalRow row) throws IOException {
    String sourceFile = sourceFile(row);
    long sourcePos = row.getLong(posOrdinal);

    // the wrapped writer only reads the leading data columns, so the trailing tracking columns
    // can stay on the row
    delegate.write(row);

    runsFor(row).add(sourceFile, sourcePos);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    WriterCommitMessage message = delegate.commit();
    Preconditions.checkState(
        message instanceof TaskCommit,
        "Cannot track positions: unexpected commit message %s",
        message.getClass().getName());

    DataFile[] files = ((TaskCommit) message).files();
    List<CompactionMap.Run> runs = Lists.newArrayList();

    if (unpartitionedRuns != null) {
      unpartitionedRuns.resolve(Lists.newArrayList(files), runs);
    } else {
      Map<StructLike, List<DataFile>> filesByPartition = StructLikeMap.create(spec.partitionType());
      for (DataFile file : files) {
        filesByPartition
            .computeIfAbsent(file.partition(), ignored -> Lists.newArrayList())
            .add(file);
      }

      for (Map.Entry<StructLike, PartitionRuns> entry : runsByPartition.entrySet()) {
        List<DataFile> partitionFiles = filesByPartition.get(entry.getKey());
        Preconditions.checkState(
            partitionFiles != null,
            "Rows were written for partition %s but no output file was reported",
            entry.getKey());
        entry.getValue().resolve(partitionFiles, runs);
      }
    }

    return new TaskCommit(files, runs);
  }

  @Override
  public void abort() throws IOException {
    delegate.abort();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  private String sourceFile(InternalRow row) {
    UTF8String utf8 = row.getUTF8String(fileOrdinal);
    if (lastSourceFileUtf8 == null || !lastSourceFileUtf8.equals(utf8)) {
      this.lastSourceFileUtf8 = utf8.clone();
      this.lastSourceFile = utf8.toString();
    }

    return lastSourceFile;
  }

  private PartitionRuns runsFor(InternalRow row) {
    if (unpartitionedRuns != null) {
      return unpartitionedRuns;
    }

    partitionKey.partition(rowWrapper.wrap(row));
    PartitionRuns runs = runsByPartition.get(partitionKey);
    if (runs == null) {
      runs = new PartitionRuns();
      runsByPartition.put(partitionKey.copy(), runs);
    }

    return runs;
  }

  /** Output-relative runs for one partition, resolved to files once their boundaries are known. */
  private static class PartitionRuns {
    private final List<PendingRun> pending = Lists.newArrayList();
    private long outputPos = 0;

    void add(String sourceFile, long sourcePos) {
      if (!pending.isEmpty()) {
        PendingRun last = pending.get(pending.size() - 1);
        if (last.continuesWith(sourceFile, sourcePos, outputPos)) {
          last.length++;
          outputPos++;
          return;
        }
      }

      pending.add(new PendingRun(sourceFile, sourcePos, outputPos));
      outputPos++;
    }

    /**
     * Splits the output-relative runs across the partition's files. Files are in write order, so
     * cumulative record counts give each file's output range.
     */
    void resolve(List<DataFile> files, List<CompactionMap.Run> into) {
      long total = files.stream().mapToLong(DataFile::recordCount).sum();
      Preconditions.checkState(
          total == outputPos,
          "Tracked %s output rows but output files contain %s records",
          outputPos,
          total);

      int fileIdx = 0;
      long fileStart = 0;
      for (PendingRun run : pending) {
        long runStart = run.outputStart;
        long runEnd = run.outputStart + run.length;

        while (runStart < runEnd) {
          long fileEnd = fileStart + files.get(fileIdx).recordCount();
          if (runStart >= fileEnd) {
            fileStart = fileEnd;
            fileIdx++;
            continue;
          }

          long sliceEnd = Math.min(runEnd, fileEnd);
          long sliceLen = sliceEnd - runStart;
          into.add(
              new CompactionMap.Run(
                  run.sourceFile,
                  run.sourceStart + (runStart - run.outputStart),
                  sliceLen,
                  files.get(fileIdx).location(),
                  runStart - fileStart));
          runStart = sliceEnd;
        }
      }
    }
  }

  private static class PendingRun {
    private final String sourceFile;
    private final long sourceStart;
    private final long outputStart;
    private long length;

    PendingRun(String sourceFile, long sourceStart, long outputStart) {
      this.sourceFile = sourceFile;
      this.sourceStart = sourceStart;
      this.outputStart = outputStart;
      this.length = 1;
    }

    boolean continuesWith(String nextSourceFile, long nextSourcePos, long nextOutputPos) {
      return sourceFile.equals(nextSourceFile)
          && nextSourcePos == sourceStart + length
          && nextOutputPos == outputStart + length;
    }
  }
}
