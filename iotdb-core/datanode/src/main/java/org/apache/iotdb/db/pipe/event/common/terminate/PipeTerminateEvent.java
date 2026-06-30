/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.event.common.terminate;

import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link PipeTerminateEvent} is an {@link EnrichedEvent} that controls the termination of pipe,
 * that is, when the historical {@link PipeTsFileInsertionEvent}s are all processed, this will be
 * reported next and mark the {@link PipeDataNodeTask} as completed. WARNING: This event shall never
 * be discarded.
 */
public class PipeTerminateEvent extends EnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTerminateEvent.class);

  private final int dataRegionId;

  private final boolean shouldMark;

  private static final int TERMINATE_EXECUTOR_THREAD_COUNT =
      IoTDBDescriptor.getInstance().getConfig().getPipeTaskThreadCount();

  private static final int TERMINATE_EXECUTOR_QUEUE_SIZE =
      Math.max(1024, TERMINATE_EXECUTOR_THREAD_COUNT * 64);

  // Do not use call run policy to avoid deadlock
  private static final ExecutorService terminateExecutor = createTerminateExecutor();

  private static final ConcurrentMap<HistoricalTransferKey, HistoricalTransferSummaryCounter>
      HISTORICAL_TRANSFER_SUMMARY_COUNTER_MAP = new ConcurrentHashMap<>();

  private static ExecutorService createTerminateExecutor() {
    final WrappedThreadPoolExecutor executor =
        new WrappedThreadPoolExecutor(
            TERMINATE_EXECUTOR_THREAD_COUNT,
            TERMINATE_EXECUTOR_THREAD_COUNT,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(TERMINATE_EXECUTOR_QUEUE_SIZE),
            new IoTThreadFactory(ThreadName.PIPE_TERMINATE_EXECUTION_POOL.getName()),
            ThreadName.PIPE_TERMINATE_EXECUTION_POOL.getName());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  public PipeTerminateEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final int dataRegionId,
      final boolean shouldMark) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        null,
        null,
        null,
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
    this.dataRegionId = dataRegionId;
    this.shouldMark = shouldMark;

    addOnCommittedHook(this::markCompleted);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    return true;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return MinimumProgressIndex.INSTANCE;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userId,
      final String userName,
      final String cliHostname,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
    // Should record PipeTaskMeta, for the terminateEvent shall report progress to
    // notify the pipeTask it's completed.
    return new PipeTerminateEvent(pipeName, creationTime, pipeTaskMeta, dataRegionId, shouldMark);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return true;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return true;
  }

  public void markCompleted() {
    final HistoricalTransferSummary summary =
        snapshotAndClearHistoricalTransferSummary(pipeName, creationTime, dataRegionId);
    if (Objects.nonNull(summary)) {
      LOGGER.info(
          "Pipe {}@{}: terminate event committed for historical transfer. creationTime: {}, shouldMark: {}. {}",
          pipeName,
          dataRegionId,
          creationTime,
          shouldMark,
          summary.toReportMessage());
    }

    // To avoid deadlock
    if (shouldMark) {
      terminateExecutor.submit(
          () -> PipeDataNodeAgent.task().markCompleted(pipeName, creationTime, dataRegionId));
    }
  }

  @Override
  public String toString() {
    return String.format(
            "PipeTerminateEvent{dataRegionId=%s, shouldMark=%s}", dataRegionId, shouldMark)
        + " - "
        + super.toString();
  }

  public static void initializeHistoricalTransferSummary(
      final String pipeName,
      final long creationTime,
      final int dataRegionId,
      final long extractedHistoricalTsFileCount,
      final long extractedHistoricalDeletionCount) {
    HISTORICAL_TRANSFER_SUMMARY_COUNTER_MAP
        .computeIfAbsent(
            new HistoricalTransferKey(pipeName, creationTime, dataRegionId),
            ignored -> new HistoricalTransferSummaryCounter())
        .initialize(extractedHistoricalTsFileCount, extractedHistoricalDeletionCount);
  }

  public static void markHistoricalTsFileSkipped(
      final String pipeName, final long creationTime, final int dataRegionId) {
    getOrCreateHistoricalTransferSummaryCounter(pipeName, creationTime, dataRegionId)
        .skippedHistoricalTsFileCount
        .incrementAndGet();
  }

  public static void markHistoricalTsFileSplit(
      final String pipeName, final long creationTime, final int dataRegionId) {
    getOrCreateHistoricalTransferSummaryCounter(pipeName, creationTime, dataRegionId)
        .splitHistoricalTsFileCount
        .incrementAndGet();
  }

  public static void markHistoricalTsFileUnsplit(
      final String pipeName, final long creationTime, final int dataRegionId) {
    getOrCreateHistoricalTransferSummaryCounter(pipeName, creationTime, dataRegionId)
        .unsplitHistoricalTsFileCount
        .incrementAndGet();
  }

  public static HistoricalTransferSummary snapshotHistoricalTransferSummary(
      final String pipeName, final long creationTime, final int dataRegionId) {
    final HistoricalTransferSummaryCounter counter =
        HISTORICAL_TRANSFER_SUMMARY_COUNTER_MAP.get(
            new HistoricalTransferKey(pipeName, creationTime, dataRegionId));
    return Objects.nonNull(counter) ? counter.snapshot() : null;
  }

  public static void clearHistoricalTransferSummary(
      final String pipeName, final long creationTime, final int dataRegionId) {
    HISTORICAL_TRANSFER_SUMMARY_COUNTER_MAP.remove(
        new HistoricalTransferKey(pipeName, creationTime, dataRegionId));
  }

  private static HistoricalTransferSummary snapshotAndClearHistoricalTransferSummary(
      final String pipeName, final long creationTime, final int dataRegionId) {
    final HistoricalTransferSummaryCounter counter =
        HISTORICAL_TRANSFER_SUMMARY_COUNTER_MAP.remove(
            new HistoricalTransferKey(pipeName, creationTime, dataRegionId));
    return Objects.nonNull(counter) ? counter.snapshot() : null;
  }

  private static HistoricalTransferSummaryCounter getOrCreateHistoricalTransferSummaryCounter(
      final String pipeName, final long creationTime, final int dataRegionId) {
    return HISTORICAL_TRANSFER_SUMMARY_COUNTER_MAP.computeIfAbsent(
        new HistoricalTransferKey(pipeName, creationTime, dataRegionId),
        ignored -> new HistoricalTransferSummaryCounter());
  }

  public static final class HistoricalTransferSummary {

    private final long extractedHistoricalTsFileCount;
    private final long skippedHistoricalTsFileCount;
    private final long splitHistoricalTsFileCount;
    private final long unsplitHistoricalTsFileCount;
    private final long extractedHistoricalDeletionCount;

    private HistoricalTransferSummary(
        final long extractedHistoricalTsFileCount,
        final long skippedHistoricalTsFileCount,
        final long splitHistoricalTsFileCount,
        final long unsplitHistoricalTsFileCount,
        final long extractedHistoricalDeletionCount) {
      this.extractedHistoricalTsFileCount = extractedHistoricalTsFileCount;
      this.skippedHistoricalTsFileCount = skippedHistoricalTsFileCount;
      this.splitHistoricalTsFileCount = splitHistoricalTsFileCount;
      this.unsplitHistoricalTsFileCount = unsplitHistoricalTsFileCount;
      this.extractedHistoricalDeletionCount = extractedHistoricalDeletionCount;
    }

    public String toReportMessage() {
      return String.format(
          "historical summary: extractedTsFileCount=%s, skippedTsFileCount=%s, splitTsFileCount=%s, unsplitTsFileCount=%s, deletionCount=%s",
          extractedHistoricalTsFileCount,
          skippedHistoricalTsFileCount,
          splitHistoricalTsFileCount,
          unsplitHistoricalTsFileCount,
          extractedHistoricalDeletionCount);
    }
  }

  private static final class HistoricalTransferSummaryCounter {

    private final AtomicLong extractedHistoricalTsFileCount = new AtomicLong(0);
    private final AtomicLong skippedHistoricalTsFileCount = new AtomicLong(0);
    private final AtomicLong splitHistoricalTsFileCount = new AtomicLong(0);
    private final AtomicLong unsplitHistoricalTsFileCount = new AtomicLong(0);
    private final AtomicLong extractedHistoricalDeletionCount = new AtomicLong(0);

    private void initialize(
        final long extractedHistoricalTsFileCount, final long extractedHistoricalDeletionCount) {
      this.extractedHistoricalTsFileCount.set(extractedHistoricalTsFileCount);
      this.skippedHistoricalTsFileCount.set(0);
      this.splitHistoricalTsFileCount.set(0);
      this.unsplitHistoricalTsFileCount.set(0);
      this.extractedHistoricalDeletionCount.set(extractedHistoricalDeletionCount);
    }

    private HistoricalTransferSummary snapshot() {
      return new HistoricalTransferSummary(
          extractedHistoricalTsFileCount.get(),
          skippedHistoricalTsFileCount.get(),
          splitHistoricalTsFileCount.get(),
          unsplitHistoricalTsFileCount.get(),
          extractedHistoricalDeletionCount.get());
    }
  }

  private static final class HistoricalTransferKey {

    private final String pipeName;
    private final long creationTime;
    private final int dataRegionId;

    private HistoricalTransferKey(
        final String pipeName, final long creationTime, final int dataRegionId) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.dataRegionId = dataRegionId;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof HistoricalTransferKey)) {
        return false;
      }
      final HistoricalTransferKey that = (HistoricalTransferKey) obj;
      return creationTime == that.creationTime
          && dataRegionId == that.dataRegionId
          && Objects.equals(pipeName, that.pipeName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pipeName, creationTime, dataRegionId);
    }
  }
}
