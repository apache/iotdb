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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ProgressWALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.metric.ConsensusSubscriptionPrefetchingQueueMetrics;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutor;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutorManager;
import org.apache.iotdb.db.subscription.task.subtask.ConsensusPrefetchSubtask;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.WatermarkPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public class ConsensusPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusPrefetchingQueue.class);

  private final String consumerGroupId;
  private final String topicName;
  private final ConsensusGroupId consensusGroupId;

  private final IoTConsensusServerImpl serverImpl;

  private final ConsensusReqReader consensusReqReader;

  private final SubscriptionWalRetentionPolicy retentionPolicy;

  private final WakeableIndexedConsensusQueue pendingEntries;

  private static final int PENDING_QUEUE_CAPACITY = 4096;

  private final ConsensusLogToTabletConverter converter;

  private final ConsensusSubscriptionCommitManager commitManager;

  /**
   * Incremented on each seek to distinguish batches/events created before and after the seek. The
   * value is copied into commit contexts so stale events cannot be committed after a later seek.
   */
  private final AtomicLong seekGeneration;

  /** Internal WAL reader cursor used only for local replay positioning and deduplication. */
  private final AtomicLong nextExpectedSearchIndex;

  private final PriorityBlockingQueue<SubscriptionEvent> prefetchingQueue;

  private final Map<InFlightEventKey, SubscriptionEvent> inFlightEvents;

  private static final int MAX_PREFETCHING_QUEUE_SIZE =
      SubscriptionConfig.getInstance().getSubscriptionConsensusPrefetchingQueueCapacity();

  private final AtomicLong walGapSkippedEntries = new AtomicLong(0);

  /**
   * Guards queue state transitions that touch replay positioning, seek state, and writer buffers.
   */
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private volatile boolean isClosed = false;

  private volatile boolean closeRequested = false;

  private volatile boolean isActive = true;

  private volatile Set<Integer> activeWriterNodeIds = Collections.emptySet();

  private volatile Set<Integer> runtimeActiveWriterNodeIds = Collections.emptySet();

  private volatile int preferredWriterNodeId = -1;

  private volatile int previousPreferredWriterNodeId = -1;

  // ======================== Routing Runtime Version ========================

  private volatile long runtimeVersion = 0;

  private final AtomicLong runtimeVersionChangeCount = new AtomicLong(0);

  // ======================== Unified WAL / Release State ========================

  private volatile ProgressWALIterator subscriptionWALIterator;

  /**
   * Seek requests must not close/reset the WAL iterator from RPC threads because the prefetch
   * worker may be reading it concurrently. Instead, seek only records the latest desired reset and
   * the queue's next prefetch round applies it after observing the new seek generation.
   */
  private volatile long pendingSubscriptionWalResetSearchIndex = Long.MIN_VALUE;

  private volatile long pendingSubscriptionWalResetGeneration = Long.MIN_VALUE;

  // ======================== Watermark ========================

  /** Maximum data timestamp observed across all InsertNodes processed by this queue. */
  private volatile long maxObservedTimestamp = Long.MIN_VALUE;

  /** Wall-clock time (ms) of last watermark injection. 0 means never injected. */
  private volatile long lastWatermarkEmitTimeMs = 0;

  /** Number of entries accepted from realtime pending queue. */
  private final AtomicLong pendingPathAcceptedEntries = new AtomicLong(0);

  /** Number of entries accepted from WAL-backed paths (historical or catch-up). */
  private final AtomicLong walPathAcceptedEntries = new AtomicLong(0);

  private final Object prefetchBindingLock = new Object();

  private volatile ConsensusPrefetchSubtask prefetchSubtask;

  private volatile ConsensusSubscriptionPrefetchExecutor prefetchExecutor;

  /**
   * Whether the prefetch runtime has been initialized. Starts as false (dormant). Set to true on
   * the first poll with a region progress hint or when a seek installs a pending reset. This keeps
   * queue creation cheap: realtime entries can be buffered immediately while WAL replay state is
   * only built once the queue is actually activated.
   */
  private volatile boolean prefetchInitialized = false;

  private volatile PendingSeekRequest pendingSeekRequest;

  private final DeliveryBatchState lingerBatch = new DeliveryBatchState();

  private volatile long observedSeekGeneration;

  private volatile long lastStatsLogTimeMs = System.currentTimeMillis();

  private volatile long lastPendingAcceptedEntries = 0L;

  private volatile long lastWalAcceptedEntries = 0L;

  private volatile boolean pendingWalGapRetryRequested = false;

  private volatile long walGapWaitStartTimeMs = 0L;

  private volatile long lastWalGapWaitLogTimeMs = 0L;

  /** Fallback committed region progress from local persisted state. */
  private final RegionProgress fallbackCommittedRegionProgress;

  /** Recovery-time per-writer frontiers used to skip already committed entries after restart. */
  private final Map<WriterId, WriterProgress> recoveryWriterProgressByWriter =
      new ConcurrentHashMap<>();

  /**
   * Source-level dedup frontier for entries that have already been materialized into queue state.
   * The same request may first arrive through pendingEntries and later become visible from WAL; the
   * second path must not materialize it again.
   */
  private final Map<Integer, WriterProgress> materializedProgressByWriter =
      new ConcurrentHashMap<>();

  /**
   * Per-writer state used to release entries by writer progress and safe frontiers instead of a
   * region-level committed frontier.
   */
  private final Map<Integer, WriterState> writerStates = new ConcurrentHashMap<>();

  /**
   * Realtime writer buffers used by both pending replay and WAL catch-up so queue materialization
   * converges on the same per-writer representation before batch delivery.
   */
  private final Map<Integer, NavigableMap<Long, PreparedEntry>> realtimeEntriesByWriter =
      new ConcurrentHashMap<>();

  /**
   * Local tail position used only when initialization starts without any persisted region progress.
   */
  private final long fallbackTailSearchIndex;

  /** Local sequence used to represent the position immediately before a writer's first record. */
  private static final long BEFORE_FIRST_LOCAL_SEQ = -1L;

  /** Writer-progress metadata for the current pending/WAL batch being assembled. */
  private volatile long batchPhysicalTime = 0L;

  private volatile int batchWriterNodeId = -1;
  private volatile String orderMode = TopicConstant.ORDER_MODE_DEFAULT_VALUE;

  protected enum ReplayLocateStatus {
    FOUND,
    AT_END,
    LOCATE_MISS
  }

  protected static final class ReplayLocateDecision {
    private final ReplayLocateStatus status;
    private final long startSearchIndex;
    private final RegionProgress recoveryRegionProgress;
    private final String detail;

    private ReplayLocateDecision(
        final ReplayLocateStatus status,
        final long startSearchIndex,
        final RegionProgress recoveryRegionProgress,
        final String detail) {
      this.status = status;
      this.startSearchIndex = startSearchIndex;
      this.recoveryRegionProgress = recoveryRegionProgress;
      this.detail = detail;
    }

    static ReplayLocateDecision found(
        final long startSearchIndex,
        final RegionProgress recoveryRegionProgress,
        final String detail) {
      return new ReplayLocateDecision(
          ReplayLocateStatus.FOUND, startSearchIndex, recoveryRegionProgress, detail);
    }

    static ReplayLocateDecision atEnd(
        final long startSearchIndex,
        final RegionProgress recoveryRegionProgress,
        final String detail) {
      return new ReplayLocateDecision(
          ReplayLocateStatus.AT_END, startSearchIndex, recoveryRegionProgress, detail);
    }

    static ReplayLocateDecision locateMiss(
        final RegionProgress recoveryRegionProgress, final String detail) {
      return new ReplayLocateDecision(
          ReplayLocateStatus.LOCATE_MISS, Long.MIN_VALUE, recoveryRegionProgress, detail);
    }

    protected ReplayLocateStatus getStatus() {
      return status;
    }

    protected long getStartSearchIndex() {
      return startSearchIndex;
    }

    protected RegionProgress getRecoveryRegionProgress() {
      return recoveryRegionProgress;
    }

    protected String getDetail() {
      return detail;
    }
  }

  private static final class WakeableIndexedConsensusQueue
      extends LinkedBlockingDeque<IndexedConsensusRequest> {

    private final Runnable wakeupHook;

    private WakeableIndexedConsensusQueue(final int capacity, final Runnable wakeupHook) {
      super(capacity);
      this.wakeupHook = wakeupHook;
    }

    @Override
    public boolean offer(final IndexedConsensusRequest request) {
      final boolean offered = super.offer(request);
      if (offered) {
        wakeupHook.run();
      }
      return offered;
    }

    @Override
    public void put(final IndexedConsensusRequest request) throws InterruptedException {
      super.put(request);
      wakeupHook.run();
    }
  }

  private static final class PendingSeekRequest {

    private final long targetSearchIndex;
    private final RegionProgress committedRegionProgress;
    private final String seekReason;
    private final boolean previousPrefetchInitialized;
    private final long previousSeekGeneration;
    private final long targetSeekGeneration;

    private boolean completed = false;
    private RuntimeException failure;

    private PendingSeekRequest(
        final long targetSearchIndex,
        final RegionProgress committedRegionProgress,
        final String seekReason,
        final boolean previousPrefetchInitialized,
        final long previousSeekGeneration,
        final long targetSeekGeneration) {
      this.targetSearchIndex = targetSearchIndex;
      this.committedRegionProgress = committedRegionProgress;
      this.seekReason = seekReason;
      this.previousPrefetchInitialized = previousPrefetchInitialized;
      this.previousSeekGeneration = previousSeekGeneration;
      this.targetSeekGeneration = targetSeekGeneration;
    }

    private synchronized void complete() {
      completed = true;
      notifyAll();
    }

    private synchronized void fail(final RuntimeException failure) {
      this.failure = failure;
      completed = true;
      notifyAll();
    }

    private synchronized void awaitCompletion() {
      while (!completed) {
        try {
          wait(50L);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_INTERRUPTED_WHILE_WAITING_FOR_SEEK_APPLICATION_7C7ECAF2,
              e);
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  public ConsensusPrefetchingQueue(
      final String consumerGroupId,
      final String topicName,
      final String orderMode,
      final ConsensusGroupId consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final SubscriptionWalRetentionPolicy retentionPolicy,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final RegionProgress fallbackCommittedRegionProgress,
      final long tailStartSearchIndex,
      final long initialRuntimeVersion,
      final boolean initialActive) {
    this.consumerGroupId = consumerGroupId;
    this.topicName = topicName;
    this.consensusGroupId = consensusGroupId;
    this.serverImpl = serverImpl;
    this.consensusReqReader = serverImpl.getConsensusReqReader();
    this.retentionPolicy = retentionPolicy;
    this.converter = converter;
    this.commitManager = commitManager;
    this.fallbackCommittedRegionProgress = fallbackCommittedRegionProgress;
    this.fallbackTailSearchIndex = tailStartSearchIndex;
    this.runtimeVersion = initialRuntimeVersion;
    this.isActive = initialActive;
    this.orderMode = TopicConfig.normalizeOrderMode(orderMode);

    this.seekGeneration = new AtomicLong(0);
    this.nextExpectedSearchIndex = new AtomicLong(tailStartSearchIndex);

    this.prefetchingQueue = new PriorityBlockingQueue<>();
    this.inFlightEvents = new ConcurrentHashMap<>();
    this.observedSeekGeneration = seekGeneration.get();

    // Register pending queue early so we don't miss real-time writes
    this.pendingEntries =
        new WakeableIndexedConsensusQueue(PENDING_QUEUE_CAPACITY, this::requestPrefetch);
    serverImpl.registerSubscriptionQueue(pendingEntries, retentionPolicy);

    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_CREATED_DORMANT_CONSUMERGROUPID_863BC6D6,
        consumerGroupId,
        topicName,
        this.orderMode,
        consensusGroupId,
        fallbackCommittedRegionProgress,
        tailStartSearchIndex,
        initialRuntimeVersion,
        initialActive);

    // Register metrics
    ConsensusSubscriptionPrefetchingQueueMetrics.getInstance().register(this);
  }

  // ======================== Lock Operations ========================

  private void acquireReadLock() {
    lock.readLock().lock();
  }

  private void releaseReadLock() {
    lock.readLock().unlock();
  }

  private void acquireWriteLock() {
    lock.writeLock().lock();
  }

  private void releaseWriteLock() {
    lock.writeLock().unlock();
  }

  private void requestPrefetch() {
    if (closeRequested || isClosed) {
      return;
    }
    final ConsensusPrefetchSubtask subtask = ensurePrefetchSubtaskBound();
    if (Objects.nonNull(subtask)) {
      subtask.requestWakeupNow();
    }
  }

  private ConsensusPrefetchSubtask ensurePrefetchSubtaskBound() {
    if (closeRequested || isClosed) {
      return null;
    }

    final ConsensusSubscriptionPrefetchExecutor currentExecutor =
        ConsensusSubscriptionPrefetchExecutorManager.getInstance().getExecutor();
    if (Objects.isNull(currentExecutor)) {
      return null;
    }

    final ConsensusPrefetchSubtask currentSubtask = prefetchSubtask;
    if (Objects.nonNull(currentSubtask)
        && prefetchExecutor == currentExecutor
        && !currentSubtask.isClosed()) {
      return currentSubtask;
    }

    synchronized (prefetchBindingLock) {
      if (closeRequested || isClosed) {
        return null;
      }

      if (Objects.nonNull(prefetchSubtask)
          && prefetchExecutor == currentExecutor
          && !prefetchSubtask.isClosed()) {
        return prefetchSubtask;
      }

      final ConsensusPrefetchSubtask staleSubtask = prefetchSubtask;
      final ConsensusSubscriptionPrefetchExecutor staleExecutor = prefetchExecutor;
      if (Objects.nonNull(staleSubtask)
          && Objects.nonNull(staleExecutor)
          && (staleExecutor != currentExecutor || staleSubtask.isClosed())
          && !staleExecutor.isShutdown()) {
        staleExecutor.deregister(staleSubtask.getTaskId());
      }

      final ConsensusPrefetchSubtask newSubtask = new ConsensusPrefetchSubtask(this);
      if (!currentExecutor.register(newSubtask)) {
        return null;
      }
      prefetchExecutor = currentExecutor;
      prefetchSubtask = newSubtask;
      return newSubtask;
    }
  }

  private Pair<ConsensusSubscriptionPrefetchExecutor, ConsensusPrefetchSubtask>
      detachPrefetchSubtask() {
    synchronized (prefetchBindingLock) {
      final Pair<ConsensusSubscriptionPrefetchExecutor, ConsensusPrefetchSubtask> detached =
          new Pair<>(prefetchExecutor, prefetchSubtask);
      prefetchExecutor = null;
      prefetchSubtask = null;
      return detached;
    }
  }

  private boolean shouldRecoverPrefetchBindingAfterEmptyPoll() {
    if (!prefetchInitialized || isClosed || closeRequested || pendingSeekRequest != null) {
      return false;
    }

    final ConsensusSubscriptionPrefetchExecutor currentExecutor =
        ConsensusSubscriptionPrefetchExecutorManager.getInstance().getExecutor();
    if (Objects.isNull(currentExecutor)) {
      return false;
    }

    final ConsensusPrefetchSubtask currentSubtask = prefetchSubtask;
    final boolean bindingMissing =
        Objects.isNull(currentSubtask)
            || currentSubtask.isClosed()
            || Objects.isNull(prefetchExecutor)
            || prefetchExecutor.isShutdown()
            || prefetchExecutor != currentExecutor;
    if (!bindingMissing) {
      return false;
    }

    return hasImmediatePrefetchableWork()
        || hasHistoricalWalLag()
        || !lingerBatch.isEmpty()
        || !inFlightEvents.isEmpty()
        || computeWatermarkDelayMs() > 0L;
  }

  // ======================== Poll ========================

  public SubscriptionEvent poll(final String consumerId) {
    return poll(consumerId, null);
  }

  public SubscriptionEvent poll(final String consumerId, final RegionProgress regionProgress) {
    acquireReadLock();
    try {
      if (isClosed || closeRequested || !isActive) {
        return null;
      }
      if (!prefetchInitialized) {
        initPrefetch(regionProgress);
      }
      if (pendingSeekRequest != null) {
        return null;
      }
      final SubscriptionEvent event = pollInternal(consumerId);
      if (Objects.nonNull(event) && prefetchingQueue.size() < MAX_PREFETCHING_QUEUE_SIZE) {
        requestPrefetch();
      } else if (Objects.isNull(event) && shouldRecoverPrefetchBindingAfterEmptyPoll()) {
        requestPrefetch();
      }
      return event;
    } finally {
      releaseReadLock();
    }
  }

  private synchronized void initPrefetch(final RegionProgress regionProgress) {
    if (prefetchInitialized) {
      return; // double-check under synchronization
    }

    final RegionProgress committedRegionProgress = resolveCommittedRegionProgressForInit();
    final boolean useConsumerHint =
        shouldUseConsumerRegionProgressHint(regionProgress, committedRegionProgress);
    final RegionProgress recoveryRegionProgress =
        useConsumerHint
            ? mergeRecoveryRegionProgress(committedRegionProgress, regionProgress)
            : committedRegionProgress;
    final String progressSource =
        useConsumerHint
            ? Objects.nonNull(committedRegionProgress)
                    && !committedRegionProgress.getWriterPositions().isEmpty()
                ? "merged committed region progress with consumer topic progress hint"
                : "consumer topic progress hint"
            : "committed region progress fallback";
    final ReplayLocateDecision resolvedStart =
        resolveInitReplayStartDecision(recoveryRegionProgress, progressSource);

    clearRecoveryWriterProgress();
    final RegionProgress effectiveRecoveryRegionProgress =
        resolvedStart.getRecoveryRegionProgress();
    if (Objects.nonNull(effectiveRecoveryRegionProgress)
        && !effectiveRecoveryRegionProgress.getWriterPositions().isEmpty()) {
      installRecoveryWriterProgress(effectiveRecoveryRegionProgress);
    }

    this.nextExpectedSearchIndex.set(resolvedStart.getStartSearchIndex());
    if (consensusReqReader instanceof WALNode) {
      this.subscriptionWALIterator =
          new ProgressWALIterator(
              (WALNode) consensusReqReader, resolvedStart.getStartSearchIndex());
    }
    this.prefetchInitialized = true;
    this.observedSeekGeneration = seekGeneration.get();
    this.lingerBatch.reset();
    resetBatchWriterProgress();

    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_INITIALIZED_STARTSEARCHINDEX_69B53EE6,
        this,
        resolvedStart.getStartSearchIndex(),
        resolvedStart.getDetail(),
        recoveryWriterProgressByWriter.size());

    requestPrefetch();
  }

  private ReplayLocateDecision resolveInitReplayStartDecision(
      final RegionProgress recoveryRegionProgress, final String progressSource) {
    if (Objects.isNull(recoveryRegionProgress)
        || recoveryRegionProgress.getWriterPositions().isEmpty()) {
      return ReplayLocateDecision.found(
          fallbackTailSearchIndex,
          new RegionProgress(Collections.emptyMap()),
          progressSource + " (tail start without progress)");
    }
    if (!(consensusReqReader instanceof WALNode)) {
      throw new IllegalStateException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_RECOVER_FROM_NON_EMPTY_C1B367EF,
              this,
              recoveryRegionProgress));
    }

    final ReplayLocateDecision replayTarget =
        locateReplayStartForRegionProgress(recoveryRegionProgress, true);
    switch (replayTarget.getStatus()) {
      case FOUND:
      case AT_END:
        return new ReplayLocateDecision(
            replayTarget.getStatus(),
            replayTarget.getStartSearchIndex(),
            replayTarget.getRecoveryRegionProgress(),
            progressSource + " (" + replayTarget.getDetail() + ")");
      case LOCATE_MISS:
      default:
        throw new IllegalStateException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_INITIALIZE_REPLAY_START_E02DE40E,
                this,
                recoveryRegionProgress,
                replayTarget.getDetail()));
    }
  }

  private boolean shouldUseConsumerRegionProgressHint(
      final RegionProgress regionProgress, final RegionProgress committedRegionProgress) {
    if (Objects.isNull(regionProgress) || regionProgress.getWriterPositions().isEmpty()) {
      return false;
    }
    if (Objects.isNull(committedRegionProgress)
        || committedRegionProgress.getWriterPositions().isEmpty()) {
      return true;
    }
    for (final Map.Entry<WriterId, WriterProgress> entry :
        regionProgress.getWriterPositions().entrySet()) {
      if (Objects.isNull(entry.getKey()) || Objects.isNull(entry.getValue())) {
        continue;
      }
      final WriterProgress committedWriterProgress =
          committedRegionProgress.getWriterPositions().get(entry.getKey());
      if (Objects.isNull(committedWriterProgress)
          || compareWriterProgress(entry.getValue(), committedWriterProgress) > 0) {
        return true;
      }
    }
    return false;
  }

  private RegionProgress mergeRecoveryRegionProgress(
      final RegionProgress committedRegionProgress, final RegionProgress consumerRegionProgress) {
    if (Objects.isNull(committedRegionProgress)
        || committedRegionProgress.getWriterPositions().isEmpty()) {
      return consumerRegionProgress;
    }
    if (Objects.isNull(consumerRegionProgress)
        || consumerRegionProgress.getWriterPositions().isEmpty()) {
      return committedRegionProgress;
    }

    final Map<WriterId, WriterProgress> mergedWriterProgress = new LinkedHashMap<>();
    committedRegionProgress
        .getWriterPositions()
        .forEach(
            (writerId, writerProgress) -> {
              if (Objects.nonNull(writerId) && Objects.nonNull(writerProgress)) {
                mergedWriterProgress.put(writerId, writerProgress);
              }
            });
    consumerRegionProgress
        .getWriterPositions()
        .forEach(
            (writerId, writerProgress) -> {
              if (Objects.isNull(writerId) || Objects.isNull(writerProgress)) {
                return;
              }
              mergedWriterProgress.merge(
                  writerId,
                  writerProgress,
                  (committedWriterProgress, consumerWriterProgress) ->
                      compareWriterProgress(consumerWriterProgress, committedWriterProgress) > 0
                          ? consumerWriterProgress
                          : committedWriterProgress);
            });
    return new RegionProgress(mergedWriterProgress);
  }

  protected RegionProgress resolveCommittedRegionProgressForInit() {
    commitManager.getOrCreateState(consumerGroupId, topicName, consensusGroupId);
    final RegionProgress latestCommittedRegionProgress =
        commitManager.getCommittedRegionProgress(consumerGroupId, topicName, consensusGroupId);
    if (Objects.nonNull(latestCommittedRegionProgress)
        && !latestCommittedRegionProgress.getWriterPositions().isEmpty()) {
      return latestCommittedRegionProgress;
    }
    return Objects.nonNull(fallbackCommittedRegionProgress)
            && !fallbackCommittedRegionProgress.getWriterPositions().isEmpty()
        ? fallbackCommittedRegionProgress
        : null;
  }

  private void installRecoveryWriterProgress(final RegionProgress regionProgress) {
    recoveryWriterProgressByWriter.clear();
    recoveryWriterProgressByWriter.putAll(regionProgress.getWriterPositions());
    regionProgress
        .getWriterPositions()
        .keySet()
        .forEach(writerId -> trackWriterState(writerId.getNodeId()));
  }

  private void clearRecoveryWriterProgress() {
    recoveryWriterProgressByWriter.clear();
  }

  private boolean shouldSkipForRecoveryProgress(final IndexedConsensusRequest request) {
    if (recoveryWriterProgressByWriter.isEmpty()) {
      return false;
    }
    return isRequestCoveredByRegionProgress(request, recoveryWriterProgressByWriter, true);
  }

  private boolean hasComparableWriterProgress(final IndexedConsensusRequest request) {
    return request.getNodeId() >= 0
        && request.getPhysicalTime() > 0
        && request.getProgressLocalSeq() >= 0;
  }

  private WriterId toWriterId(final IndexedConsensusRequest request) {
    return new WriterId(consensusGroupId.toString(), request.getNodeId());
  }

  private WriterProgress toWriterProgress(final IndexedConsensusRequest request) {
    return new WriterProgress(request.getPhysicalTime(), request.getProgressLocalSeq());
  }

  private boolean isRequestCoveredByRegionProgress(
      final IndexedConsensusRequest request,
      final Map<WriterId, WriterProgress> regionProgressByWriter,
      final boolean seekAfter) {
    if (!hasComparableWriterProgress(request)) {
      return false;
    }
    final WriterProgress committedProgress = regionProgressByWriter.get(toWriterId(request));
    if (Objects.isNull(committedProgress)) {
      return false;
    }
    final int cmp = compareWriterProgress(toWriterProgress(request), committedProgress);
    return seekAfter ? cmp <= 0 : cmp < 0;
  }

  private WriterProgress decrementWriterProgress(final WriterProgress writerProgress) {
    return new WriterProgress(
        writerProgress.getPhysicalTime(),
        writerProgress.getLocalSeq() > 0L
            ? writerProgress.getLocalSeq() - 1L
            : BEFORE_FIRST_LOCAL_SEQ);
  }

  protected ReplayLocateDecision scanReplayStartForRequests(
      final Iterator<IndexedConsensusRequest> requests,
      final RegionProgress regionProgress,
      final boolean seekAfter) {
    final Map<WriterId, WriterProgress> requestedWriterProgress = new LinkedHashMap<>();
    if (Objects.nonNull(regionProgress)) {
      regionProgress
          .getWriterPositions()
          .forEach(
              (writerId, writerProgress) -> {
                if (Objects.nonNull(writerId) && Objects.nonNull(writerProgress)) {
                  requestedWriterProgress.put(writerId, writerProgress);
                }
              });
    }
    final Map<WriterId, WriterProgress> effectiveRecoveryWriterProgress =
        new LinkedHashMap<>(requestedWriterProgress);
    final Set<WriterId> exactVisibleWriterIds = new LinkedHashSet<>();
    Long firstUncoveredReplayableSearchIndex = null;
    boolean sawBlockingNonReplayableUncovered = false;

    while (requests.hasNext()) {
      final IndexedConsensusRequest request = requests.next();
      if (!hasComparableWriterProgress(request)) {
        continue;
      }

      final WriterId writerId = toWriterId(request);
      final WriterProgress requestProgress = toWriterProgress(request);
      final WriterProgress storedWriterProgress = requestedWriterProgress.get(writerId);
      if (!seekAfter
          && Objects.nonNull(storedWriterProgress)
          && compareWriterProgress(requestProgress, storedWriterProgress) == 0) {
        exactVisibleWriterIds.add(writerId);
      }

      if (isRequestCoveredByRegionProgress(request, requestedWriterProgress, seekAfter)) {
        continue;
      }

      if (request.getSearchIndex() >= 0) {
        if (Objects.isNull(firstUncoveredReplayableSearchIndex)) {
          firstUncoveredReplayableSearchIndex = request.getSearchIndex();
        }
      } else if (Objects.isNull(firstUncoveredReplayableSearchIndex)) {
        sawBlockingNonReplayableUncovered = true;
      }
    }

    if (!seekAfter && !exactVisibleWriterIds.isEmpty()) {
      for (final WriterId writerId : exactVisibleWriterIds) {
        final WriterProgress writerProgress = requestedWriterProgress.get(writerId);
        if (Objects.nonNull(writerProgress)) {
          effectiveRecoveryWriterProgress.put(writerId, decrementWriterProgress(writerProgress));
        }
      }
    }
    final RegionProgress effectiveRecoveryRegionProgress =
        new RegionProgress(effectiveRecoveryWriterProgress);

    if (sawBlockingNonReplayableUncovered) {
      return ReplayLocateDecision.locateMiss(
          effectiveRecoveryRegionProgress,
          "uncovered non-replayable WAL records appear before the first local replayable record");
    }
    if (Objects.nonNull(firstUncoveredReplayableSearchIndex)) {
      return ReplayLocateDecision.found(
          firstUncoveredReplayableSearchIndex,
          effectiveRecoveryRegionProgress,
          "resolved first uncovered replayable WAL record");
    }
    return ReplayLocateDecision.atEnd(
        consensusReqReader.getCurrentSearchIndex(),
        computeTailRegionProgress(),
        "all locally replayable WAL records are already covered");
  }

  protected ReplayLocateDecision locateReplayStartForRegionProgress(
      final RegionProgress regionProgress, final boolean seekAfter) {
    if (!(consensusReqReader instanceof WALNode)) {
      return ReplayLocateDecision.locateMiss(
          regionProgress, "WAL access is unavailable for region-level replay lookup");
    }

    final WALNode walNode = (WALNode) consensusReqReader;
    try (final ProgressWALIterator iterator = new ProgressWALIterator(walNode, Long.MIN_VALUE)) {
      final ReplayLocateDecision decision =
          scanReplayStartForRequests(iterator, regionProgress, seekAfter);
      if (iterator.hasIncompleteScan()) {
        return ReplayLocateDecision.locateMiss(
            regionProgress,
            "replay lookup did not complete: " + iterator.getIncompleteScanDetail());
      }
      return decision;
    } catch (final IOException e) {
      return ReplayLocateDecision.locateMiss(
          regionProgress, "failed to close replay lookup iterator: " + e.getMessage());
    }
  }

  private boolean shouldTrackMaterializedProgress(final IndexedConsensusRequest request) {
    return hasComparableWriterProgress(request);
  }

  private boolean shouldSkipForMaterializedProgress(final IndexedConsensusRequest request) {
    if (!shouldTrackMaterializedProgress(request)) {
      return false;
    }
    final WriterProgress materializedProgress =
        materializedProgressByWriter.get(request.getNodeId());
    return Objects.nonNull(materializedProgress)
        && compareWriterProgress(toWriterProgress(request), materializedProgress) <= 0;
  }

  private void markMaterializedProgress(final IndexedConsensusRequest request) {
    if (!shouldTrackMaterializedProgress(request)) {
      return;
    }
    materializedProgressByWriter.merge(
        request.getNodeId(),
        toWriterProgress(request),
        (existing, candidate) ->
            compareWriterProgress(candidate, existing) > 0 ? candidate : existing);
  }

  private int compareWriterProgress(
      final WriterProgress leftProgress, final WriterProgress rightProgress) {
    int cmp = Long.compare(leftProgress.getPhysicalTime(), rightProgress.getPhysicalTime());
    if (cmp != 0) {
      return cmp;
    }
    return Long.compare(leftProgress.getLocalSeq(), rightProgress.getLocalSeq());
  }

  private WriterState trackWriterState(final int writerNodeId) {
    return writerStates.computeIfAbsent(writerNodeId, ignored -> new WriterState());
  }

  private void refreshWriterSafeFrontiers() {
    final Map<Integer, Long> safePts =
        serverImpl.getWriterSafeFrontierTracker().snapshotEffectiveSafePts();
    for (final Map.Entry<Integer, Long> entry : safePts.entrySet()) {
      final WriterState writerState = trackWriterState(entry.getKey());
      writerState.effectiveSafePt = Math.max(writerState.effectiveSafePt, entry.getValue());
    }
  }

  private <T extends WriterBufferedEntry> PriorityQueue<WriterFrontier> buildWriterFrontiers(
      final Map<Integer, ?> writerEntriesByWriter, final Function<Integer, T> headSupplier) {
    refreshWriterSafeFrontiers();
    final PriorityQueue<WriterFrontier> frontiers = new PriorityQueue<>();
    final boolean useActiveWriterBarriers = shouldUseActiveWriterBarriers();
    final Set<Integer> writerNodeIds = ConcurrentHashMap.newKeySet();
    final Set<Integer> seenActiveWriterNodeIds = ConcurrentHashMap.newKeySet();
    writerNodeIds.addAll(writerStates.keySet());
    writerNodeIds.addAll(writerEntriesByWriter.keySet());
    for (final int writerNodeId : writerNodeIds) {
      final WriterState writerState = writerStates.get(writerNodeId);
      if (Objects.nonNull(writerState) && writerState.closed) {
        continue;
      }
      final T head = headSupplier.apply(writerNodeId);
      if (Objects.nonNull(head)) {
        if (isWriterRuntimeActive(writerNodeId)) {
          seenActiveWriterNodeIds.add(writerNodeId);
        }
        frontiers.add(WriterFrontier.forHead(writerNodeId, head));
        continue;
      }
      if (Objects.nonNull(writerState)
          && writerState.effectiveSafePt > 0
          && useActiveWriterBarriers
          && isWriterRuntimeActive(writerNodeId)) {
        seenActiveWriterNodeIds.add(writerNodeId);
        frontiers.add(WriterFrontier.forBarrier(writerNodeId, writerState.effectiveSafePt));
      }
    }
    if (useActiveWriterBarriers) {
      for (final Integer activeWriterNodeId : activeWriterNodeIds) {
        if (!seenActiveWriterNodeIds.contains(activeWriterNodeId)) {
          frontiers.add(WriterFrontier.forBarrier(activeWriterNodeId, Long.MIN_VALUE));
          break;
        }
      }
    }
    return frontiers;
  }

  private boolean shouldUseActiveWriterBarriers() {
    return !TopicConstant.ORDER_MODE_PER_WRITER_VALUE.equals(orderMode);
  }

  private void bufferRealtimeEntry(final PreparedEntry entry) {
    realtimeEntriesByWriter
        .computeIfAbsent(entry.writerNodeId, ignored -> new TreeMap<>())
        .put(entry.localSeq, entry);
  }

  private PreparedEntry peekRealtimeEntry(final int writerNodeId) {
    final NavigableMap<Long, PreparedEntry> writerEntries =
        realtimeEntriesByWriter.get(writerNodeId);
    if (Objects.isNull(writerEntries) || writerEntries.isEmpty()) {
      return null;
    }
    final Map.Entry<Long, PreparedEntry> firstEntry = writerEntries.firstEntry();
    return Objects.nonNull(firstEntry) ? firstEntry.getValue() : null;
  }

  private void removeRealtimeEntry(final int writerNodeId, final long localSeq) {
    final NavigableMap<Long, PreparedEntry> writerEntries =
        realtimeEntriesByWriter.get(writerNodeId);
    if (Objects.isNull(writerEntries)) {
      return;
    }
    writerEntries.remove(localSeq);
    if (writerEntries.isEmpty()) {
      realtimeEntriesByWriter.remove(writerNodeId);
    }
  }

  private PriorityQueue<WriterFrontier> buildRealtimeWriterFrontiers() {
    return buildWriterFrontiers(realtimeEntriesByWriter, this::peekRealtimeEntry);
  }

  private SubscriptionEvent pollInternal(final String consumerId) {
    final long size = prefetchingQueue.size();
    if (size == 0) {
      LOGGER.debug(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCHING_QUEUE_IS_EMPTY_FOR_22836B5E,
          this,
          consumerId,
          pendingEntries.size(),
          nextExpectedSearchIndex.get(),
          isClosed,
          prefetchInitialized,
          Objects.nonNull(prefetchSubtask) && prefetchSubtask.isScheduledOrRunning());
      return null;
    }

    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLLING_QUEUE_SIZE_CONSUMERID_FCA0AAD3,
        this,
        size,
        consumerId);
    long count = 0;

    SubscriptionEvent event;
    try {
      while (count++ < size
          && Objects.nonNull(
              event =
                  prefetchingQueue.poll(
                      SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs(),
                      TimeUnit.MILLISECONDS))) {
        // Metadata events (currently WATERMARK) are fire-and-forget:
        // skip inFlightEvents tracking so they are not recycled and re-delivered indefinitely.
        if (event.getCurrentResponse().getResponseType()
            == SubscriptionPollResponseType.WATERMARK.getType()) {
          return event;
        }

        if (event.isCommitted()) {
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_COMMITTED_EVENT_BROKEN_INVARIANT_E478FA3C,
              this,
              event);
          continue;
        }

        if (!event.pollable()) {
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_NON_POLLABLE_EVENT_BROKEN_E9551325,
              this,
              event);
          event.nack();
          continue;
        }

        // Mark as polled before updating inFlightEvents
        event.recordLastPolledTimestamp();
        inFlightEvents.put(new InFlightEventKey(consumerId, event.getCommitContext()), event);
        event.recordLastPolledConsumerId(consumerId);
        return event;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INTERRUPTED_WHILE_POLLING_B7CFF5FD,
          this,
          e);
    }

    return null;
  }

  public SubscriptionEvent pollTablets(
      final String consumerId, final SubscriptionCommitContext commitContext, final int offset) {
    acquireReadLock();
    try {
      if (isClosed || closeRequested || pendingSeekRequest != null) {
        return null;
      }
      final SubscriptionEvent event =
          inFlightEvents.get(new InFlightEventKey(consumerId, commitContext));
      if (Objects.isNull(event)) {
        if (isCommitContextOutdated(commitContext)) {
          return generateOutdatedErrorResponse();
        }
        return generateErrorResponse(
            String.format(
                "ConsensusPrefetchingQueue %s: no in-flight event for consumer %s, commit context %s",
                this, consumerId, commitContext));
      }
      if (Objects.isNull(event.getCurrentResponse())
          || event.getCurrentResponse().getResponseType()
              != SubscriptionPollResponseType.TABLETS.getType()
          || !(event.getCurrentResponse().getPayload() instanceof TabletsPayload)) {
        return generateErrorResponse(
            String.format(
                DataNodeMiscMessages.SUBSCRIPTION_CONSENSUS_UNEXPECTED_IN_FLIGHT_RESPONSE_FMT,
                this,
                consumerId,
                commitContext,
                offset));
      }

      final TabletsPayload payload = (TabletsPayload) event.getCurrentResponse().getPayload();
      // Consensus tablet events are emitted as one TabletsPayload. A non-positive nextOffset means
      // the client already received the complete batch, so any follow-up poll with an offset is
      // stale or malformed rather than a valid continuation request.
      if (payload.getNextOffset() <= 0) {
        return generateErrorResponse(
            String.format(
                "ConsensusPrefetchingQueue %s: no remaining tablet chunk for consumer %s, "
                    + "commit context %s, requested offset %s, current nextOffset %s",
                this, consumerId, commitContext, offset, payload.getNextOffset()));
      }
      if (offset != payload.getNextOffset()) {
        return generateErrorResponse(
            String.format(
                "ConsensusPrefetchingQueue %s: inconsistent tablet offset for consumer %s, "
                    + "commit context %s, requested offset %s, current nextOffset %s",
                this, consumerId, commitContext, offset, payload.getNextOffset()));
      }
      return generateErrorResponse(
          String.format(
              "ConsensusPrefetchingQueue %s: offset-based tablet continuation is not supported "
                  + "for consensus subscription, consumer %s, commit context %s, offset %s",
              this, consumerId, commitContext, offset));
    } finally {
      releaseReadLock();
    }
  }

  // ======================== Prefetch Round Drive ========================

  private static final long WAL_GAP_RETRY_SLEEP_MS = 10L;
  private static final long WAL_GAP_WAIT_LOG_INTERVAL_MS = 5_000L;

  private static final long PREFETCH_STATS_LOG_INTERVAL_MS = 5_000L;

  public PrefetchRoundResult drivePrefetchOnce() {
    if (applyPendingSeekRequestIfNecessary()) {
      return closeRequested ? PrefetchRoundResult.dormant() : PrefetchRoundResult.rescheduleNow();
    }

    acquireReadLock();
    try {
      if (isClosed || closeRequested || !prefetchInitialized) {
        return PrefetchRoundResult.dormant();
      }

      logPeriodicStatsIfNecessary();

      final long currentSeekGeneration = seekGeneration.get();
      if (currentSeekGeneration != observedSeekGeneration) {
        resetRoundStateForSeek(currentSeekGeneration);
      }

      applyPendingSubscriptionWalReset(observedSeekGeneration);
      recycleInFlightEvents();

      if (!isActive || prefetchingQueue.size() >= MAX_PREFETCHING_QUEUE_SIZE) {
        return computeIdleRoundResult();
      }

      final SubscriptionConfig config = SubscriptionConfig.getInstance();
      final int maxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
      final int batchMaxDelayMs = config.getSubscriptionConsensusBatchMaxDelayInMs();
      final int maxTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
      final long maxBatchBytes = config.getSubscriptionConsensusBatchMaxSizeInBytes();

      final List<IndexedConsensusRequest> batch = drainPendingEntries(maxWalEntries);
      if (!batch.isEmpty()) {
        LOGGER.debug(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_DRAINED_ENTRIES_FROM_PENDINGENTRIES_2D4E0BE7,
            this,
            batch.size(),
            batch.get(0).getSearchIndex(),
            batch.get(batch.size() - 1).getSearchIndex(),
            nextExpectedSearchIndex.get(),
            prefetchingQueue.size());

        final boolean batchAccepted =
            accumulateFromPending(
                batch, lingerBatch, observedSeekGeneration, maxTablets, maxBatchBytes);
        if (!batchAccepted) {
          if (pendingWalGapRetryRequested) {
            // Once a drained batch hits an unresolved WAL gap, the affected suffix falls back to
            // the WAL path on later rounds instead of being requeued into the bounded pending path.
            return PrefetchRoundResult.rescheduleAfter(WAL_GAP_RETRY_SLEEP_MS);
          }
          resetRoundStateForSeek(seekGeneration.get());
          return PrefetchRoundResult.rescheduleNow();
        }
      }

      if (batch.isEmpty() && lingerBatch.isEmpty()) {
        tryCatchUpFromWAL(observedSeekGeneration);
      }

      if (!drainBufferedRealtimeWriters(
          lingerBatch, observedSeekGeneration, maxTablets, maxBatchBytes)) {
        resetRoundStateForSeek(seekGeneration.get());
        return PrefetchRoundResult.rescheduleNow();
      }

      if (!lingerBatch.isEmpty() && lingerBatch.firstTabletTimeMs > 0L) {
        final long lingerElapsedMs = System.currentTimeMillis() - lingerBatch.firstTabletTimeMs;
        if (lingerElapsedMs >= batchMaxDelayMs) {
          if (seekGeneration.get() != observedSeekGeneration) {
            resetRoundStateForSeek(seekGeneration.get());
            return PrefetchRoundResult.rescheduleNow();
          }
          LOGGER.debug(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_TIME_BASED_FLUSH_TABLETS_LINGERED_10A4EBA8,
              this,
              lingerBatch.tablets.size(),
              lingerElapsedMs,
              batchMaxDelayMs);
          flushBatch(lingerBatch, observedSeekGeneration);
        }
      }

      maybeInjectWatermark();
      return computeIdleRoundResult();
    } catch (final Throwable fatal) {
      LOGGER.error(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_ROUND_FAILED_TYPE_MESSAGE_63BC909B,
          this,
          fatal.getClass().getName(),
          fatal.getMessage(),
          fatal);
      if (fatal instanceof VirtualMachineError) {
        markClosed();
        return PrefetchRoundResult.dormant();
      }
      return PrefetchRoundResult.rescheduleAfter(100L);
    } finally {
      releaseReadLock();
    }
  }

  private void logPeriodicStatsIfNecessary() {
    final long nowMs = System.currentTimeMillis();
    if (nowMs - lastStatsLogTimeMs < PREFETCH_STATS_LOG_INTERVAL_MS) {
      return;
    }

    final long currentPendingAcceptedEntries = pendingPathAcceptedEntries.get();
    final long currentWalAcceptedEntries = walPathAcceptedEntries.get();
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PERIODIC_STATS_LAG_PENDINGDELTA_D75375D0,
        this,
        getLag(),
        currentPendingAcceptedEntries - lastPendingAcceptedEntries,
        currentWalAcceptedEntries - lastWalAcceptedEntries,
        currentPendingAcceptedEntries,
        currentWalAcceptedEntries,
        pendingEntries.size(),
        prefetchingQueue.size(),
        inFlightEvents.size(),
        realtimeEntriesByWriter.size(),
        hasReadableWalEntries(),
        isActive,
        Objects.nonNull(prefetchSubtask) && prefetchSubtask.isScheduledOrRunning());
    lastStatsLogTimeMs = nowMs;
    lastPendingAcceptedEntries = currentPendingAcceptedEntries;
    lastWalAcceptedEntries = currentWalAcceptedEntries;
  }

  private void resetRoundStateForSeek(final long newSeekGeneration) {
    restorePendingSubscriptionWalCursor(newSeekGeneration);
    lingerBatch.reset();
    resetBatchWriterProgress();
    observedSeekGeneration = newSeekGeneration;
  }

  private List<IndexedConsensusRequest> drainPendingEntries(final int maxWalEntries) {
    final List<IndexedConsensusRequest> batch = new ArrayList<>();
    IndexedConsensusRequest next;
    while (batch.size() < maxWalEntries && (next = pendingEntries.poll()) != null) {
      batch.add(next);
    }
    return batch;
  }

  private PrefetchRoundResult computeIdleRoundResult() {
    if (isClosed || !prefetchInitialized || !isActive) {
      return PrefetchRoundResult.dormant();
    }
    if (prefetchingQueue.size() >= MAX_PREFETCHING_QUEUE_SIZE) {
      return PrefetchRoundResult.dormant();
    }
    if (hasImmediatePrefetchableWork()) {
      return PrefetchRoundResult.rescheduleNow();
    }
    long delayMs = Long.MAX_VALUE;
    if (hasHistoricalWalLag()) {
      delayMs = Math.min(delayMs, WAL_GAP_RETRY_SLEEP_MS);
    }
    if (!lingerBatch.isEmpty() && lingerBatch.firstTabletTimeMs > 0L) {
      final long lingerDelayMs =
          SubscriptionConfig.getInstance().getSubscriptionConsensusBatchMaxDelayInMs()
              - (System.currentTimeMillis() - lingerBatch.firstTabletTimeMs);
      delayMs = Math.min(delayMs, Math.max(1L, lingerDelayMs));
    }

    final long watermarkDelayMs = computeWatermarkDelayMs();
    if (watermarkDelayMs > 0L) {
      delayMs = Math.min(delayMs, watermarkDelayMs);
    }

    if (!inFlightEvents.isEmpty()) {
      delayMs =
          Math.min(
              delayMs,
              SubscriptionConfig.getInstance().getSubscriptionRecycleUncommittedEventIntervalMs());
    }

    return delayMs == Long.MAX_VALUE
        ? PrefetchRoundResult.dormant()
        : PrefetchRoundResult.rescheduleAfter(delayMs);
  }

  private long computeWatermarkDelayMs() {
    if (maxObservedTimestamp == Long.MIN_VALUE) {
      return -1L;
    }
    final long intervalMs =
        SubscriptionConfig.getInstance().getSubscriptionConsensusWatermarkIntervalMs();
    if (intervalMs <= 0L) {
      return -1L;
    }
    if (lastWatermarkEmitTimeMs == 0L) {
      return 1L;
    }
    final long elapsedMs = System.currentTimeMillis() - lastWatermarkEmitTimeMs;
    return elapsedMs >= intervalMs ? 1L : Math.max(1L, intervalMs - elapsedMs);
  }

  private boolean hasImmediatePrefetchableWork() {
    return !pendingEntries.isEmpty()
        || !realtimeEntriesByWriter.isEmpty()
        || hasReadableWalEntries();
  }

  private boolean hasHistoricalWalLag() {
    return hasUnreadWalEntriesBehindCursor();
  }

  private boolean hasUnreadWalEntriesBehindCursor() {
    final long currentSearchIndex = consensusReqReader.getCurrentSearchIndex();
    // Local search indexes start at 1; 0 is the empty-WAL sentinel.
    return currentSearchIndex > 0 && nextExpectedSearchIndex.get() <= currentSearchIndex;
  }

  private static boolean hasLocalSearchIndex(final IndexedConsensusRequest request) {
    return request.getSearchIndex() >= 0;
  }

  private boolean isBeforeLocalCursor(final IndexedConsensusRequest request) {
    return hasLocalSearchIndex(request) && request.getSearchIndex() < nextExpectedSearchIndex.get();
  }

  private void advanceLocalCursorIfPresent(final IndexedConsensusRequest request) {
    if (hasLocalSearchIndex(request)) {
      nextExpectedSearchIndex.set(request.getSearchIndex() + 1);
    }
  }

  private boolean appendRealtimeRequest(
      final IndexedConsensusRequest request,
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes,
      final boolean fromPending) {
    final PreparedEntry preparedEntry = prepareEntry(request);
    if (Objects.isNull(preparedEntry)) {
      return true;
    }
    if (!appendPreparedEntryViaRealtimeWriter(
        batchState, preparedEntry, expectedSeekGeneration, maxTablets, maxBatchBytes)) {
      return false;
    }
    if (fromPending) {
      markAcceptedFromPending();
    } else {
      markAcceptedFromWal();
    }
    return true;
  }

  /**
   * Accumulates tablets from pending entries into the linger buffer. When pending replay outruns
   * the local WAL reader, this method backfills the local-index gap from WAL before continuing.
   *
   * @return false if the batch became stale because seek generation changed while flushing
   */
  private boolean accumulateFromPending(
      final List<IndexedConsensusRequest> batch,
      final DeliveryBatchState lingerBatch,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {

    int processedCount = 0;
    int skippedCount = 0;

    for (int index = 0; index < batch.size(); index++) {
      final IndexedConsensusRequest request = batch.get(index);
      final long searchIndex = request.getSearchIndex();

      // Only local-indexed requests participate in the internal WAL read cursor.
      final long expected = nextExpectedSearchIndex.get();
      if (hasLocalSearchIndex(request) && searchIndex > expected) {
        LOGGER.debug(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_GAP_DETECTED_EXPECTED_GOT_FILLING_70DD08B3,
            this,
            expected,
            searchIndex,
            searchIndex - expected);
        if (!fillGapFromWAL(
            expected,
            searchIndex,
            lingerBatch,
            expectedSeekGeneration,
            maxTablets,
            maxBatchBytes)) {
          return false;
        }
      }

      if (isBeforeLocalCursor(request)) {
        skippedCount++;
        continue;
      }

      if (shouldSkipForRecoveryProgress(request)) {
        skippedCount++;
        advanceLocalCursorIfPresent(request);
        continue;
      }
      if (shouldSkipForMaterializedProgress(request)) {
        skippedCount++;
        advanceLocalCursorIfPresent(request);
        continue;
      }

      if (!appendRealtimeRequest(
          request, lingerBatch, expectedSeekGeneration, maxTablets, maxBatchBytes, true)) {
        return false;
      }
      markMaterializedProgress(request);
      processedCount++;
      advanceLocalCursorIfPresent(request);
    }

    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ACCUMULATE_COMPLETE_BATCHSIZE_FA3F3B41,
        this,
        batch.size(),
        processedCount,
        skippedCount,
        lingerBatch.tablets.size(),
        nextExpectedSearchIndex.get());

    return true;
  }

  /**
   * Fills a gap in the pending queue by reading entries from WAL so the internal local replay
   * cursor stays contiguous even when pending delivery jumps ahead of the WAL iterator.
   *
   * <p>Temporary WAL visibility lag is treated as a normal back-pressure condition: once a drained
   * pending batch encounters an unresolved local-index gap, the queue backs off and lets the
   * affected suffix fall back to the WAL path on later rounds. This keeps replay contiguous without
   * requeueing the drained batch back into the bounded pending queue.
   *
   * @return false if gap fill had to stop because the current batch became stale or the queue was
   *     interrupted/closed
   */
  private boolean fillGapFromWAL(
      final long fromIndex,
      final long toIndex,
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {
    pendingWalGapRetryRequested = false;
    resetSubscriptionWALPosition(fromIndex);
    if (seekGeneration.get() != expectedSeekGeneration || isClosed) {
      return false;
    }
    if (!pumpFromSubscriptionWAL(
        batchState, expectedSeekGeneration, Integer.MAX_VALUE, maxTablets, maxBatchBytes)) {
      return false;
    }

    final long nextExpected = nextExpectedSearchIndex.get();
    if (nextExpected >= toIndex) {
      walGapWaitStartTimeMs = 0L;
      lastWalGapWaitLogTimeMs = 0L;
      return true;
    }

    final long nowMs = System.currentTimeMillis();
    if (walGapWaitStartTimeMs == 0L) {
      walGapWaitStartTimeMs = nowMs;
    }
    if (lastWalGapWaitLogTimeMs == 0L
        || nowMs - lastWalGapWaitLogTimeMs >= WAL_GAP_WAIT_LOG_INTERVAL_MS) {
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_WAITING_MS_FOR_WAL_GAP_TO_BECOME_7D91C6C5,
          this,
          nowMs - walGapWaitStartTimeMs,
          nextExpected,
          toIndex,
          nextExpected,
          consensusReqReader.getCurrentSearchIndex(),
          expectedSeekGeneration);
      lastWalGapWaitLogTimeMs = nowMs;
    }
    onWalGapRetryScheduled();
    pendingWalGapRetryRequested = true;
    return false;
  }

  /**
   * Try catch-up from WAL when the pending queue was empty. This handles cold-start or scenarios
   * where the subscription started after data was already written.
   */
  private void tryCatchUpFromWAL(final long expectedSeekGeneration) {
    final SubscriptionConfig config = SubscriptionConfig.getInstance();
    final int maxTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
    final long maxBatchBytes = config.getSubscriptionConsensusBatchMaxSizeInBytes();
    final int maxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();

    final DeliveryBatchState batchState = new DeliveryBatchState();
    resetSubscriptionWALPosition(nextExpectedSearchIndex.get());
    final boolean accepted =
        pumpFromSubscriptionWAL(
            batchState, expectedSeekGeneration, maxWalEntries, maxTablets, maxBatchBytes);
    if (!accepted) {
      return;
    }

    if (!batchState.isEmpty()) {
      flushBatch(batchState, expectedSeekGeneration);
    }
  }

  private boolean pumpFromSubscriptionWAL(
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final int maxWalEntries,
      final int maxTablets,
      final long maxBatchBytes) {
    if (Objects.isNull(subscriptionWALIterator)) {
      return true;
    }

    subscriptionWALIterator.refresh();
    ensureSubscriptionWalReadable();

    int entriesRead = 0;
    while (entriesRead < maxWalEntries
        && subscriptionWALIterator.hasNext()
        && prefetchingQueue.size() < MAX_PREFETCHING_QUEUE_SIZE) {
      try {
        final IndexedConsensusRequest walEntry = subscriptionWALIterator.next();
        entriesRead++;

        if (isBeforeLocalCursor(walEntry)) {
          continue;
        }
        if (shouldSkipForRecoveryProgress(walEntry)) {
          advanceLocalCursorIfPresent(walEntry);
          continue;
        }
        if (shouldSkipForMaterializedProgress(walEntry)) {
          advanceLocalCursorIfPresent(walEntry);
          continue;
        }

        if (!appendRealtimeRequest(
            walEntry, batchState, expectedSeekGeneration, maxTablets, maxBatchBytes, false)) {
          return false;
        }
        markMaterializedProgress(walEntry);
        advanceLocalCursorIfPresent(walEntry);
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_READING_SUBSCRIPTION_WAL_A3888AC5,
            this,
            e);
        break;
      }
    }

    if (entriesRead > 0) {
      LOGGER.debug(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_READ_ENTRIES_14AA5096,
          this,
          entriesRead,
          nextExpectedSearchIndex.get());
    }
    return true;
  }

  private void ensureSubscriptionWalReadable() {
    if (Objects.isNull(subscriptionWALIterator)
        || subscriptionWALIterator.hasNext()
        || !(consensusReqReader instanceof WALNode)) {
      return;
    }

    final long currentWalIndex = consensusReqReader.getCurrentSearchIndex();
    if (nextExpectedSearchIndex.get() > currentWalIndex) {
      return;
    }

    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_EXHAUSTED_AT_E61AF763,
        this,
        nextExpectedSearchIndex.get(),
        currentWalIndex);
    ((WALNode) consensusReqReader).rollWALFile();
    resetSubscriptionWALPosition(nextExpectedSearchIndex.get());
    if (Objects.nonNull(subscriptionWALIterator)) {
      subscriptionWALIterator.refresh();
    }
  }

  private void resetSubscriptionWALPosition(final long startSearchIndex) {
    closeSubscriptionWALIterator();
    subscriptionWALIterator = createSubscriptionWALIterator(startSearchIndex);
  }

  protected ProgressWALIterator createSubscriptionWALIterator(final long startSearchIndex) {
    if (consensusReqReader instanceof WALNode) {
      return new ProgressWALIterator((WALNode) consensusReqReader, startSearchIndex);
    }
    return null;
  }

  protected void onWalGapRetryScheduled() {}

  private boolean hasReadableWalEntries() {
    return Objects.nonNull(subscriptionWALIterator) && subscriptionWALIterator.hasNext();
  }

  private void requestSubscriptionWalReset(
      final long targetSearchIndex, final long seekGenerationValue) {
    pendingSubscriptionWalResetSearchIndex = targetSearchIndex;
    pendingSubscriptionWalResetGeneration = seekGenerationValue;
  }

  private void applyPendingSubscriptionWalReset(final long observedSeekGeneration) {
    if (pendingSubscriptionWalResetGeneration != observedSeekGeneration
        || pendingSubscriptionWalResetSearchIndex == Long.MIN_VALUE) {
      return;
    }
    resetSubscriptionWALPosition(pendingSubscriptionWalResetSearchIndex);
    pendingSubscriptionWalResetSearchIndex = Long.MIN_VALUE;
    pendingSubscriptionWalResetGeneration = Long.MIN_VALUE;
  }

  private void restorePendingSubscriptionWalCursor(final long observedSeekGeneration) {
    if (pendingSubscriptionWalResetGeneration != observedSeekGeneration
        || pendingSubscriptionWalResetSearchIndex == Long.MIN_VALUE) {
      return;
    }
    // A seek can land in the middle of a prefetch iteration. Restore the local cursor to the
    // pending seek target before resuming under the new generation so stale in-flight work does
    // not permanently advance the historical replay frontier.
    nextExpectedSearchIndex.set(pendingSubscriptionWalResetSearchIndex);
  }

  private void closeSubscriptionWALIterator() {
    if (Objects.isNull(subscriptionWALIterator)) {
      return;
    }
    try {
      subscriptionWALIterator.close();
    } catch (final IOException e) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_CLOSING_SUBSCRIPTION_WAL_19711C01,
          this,
          e);
    } finally {
      subscriptionWALIterator = null;
    }
  }

  private PreparedEntry prepareEntry(final IndexedConsensusRequest indexedRequest) {
    final InsertNode insertNode =
        ConsensusLogToTabletConverter.deserializeToInsertNode(indexedRequest);
    if (Objects.isNull(insertNode)) {
      return null;
    }

    final long localSeq =
        indexedRequest.getProgressLocalSeq() >= 0
            ? indexedRequest.getProgressLocalSeq()
            : indexedRequest.getSearchIndex();
    final long searchIndex = indexedRequest.getSearchIndex();
    final long physicalTime =
        indexedRequest.getPhysicalTime() > 0
            ? indexedRequest.getPhysicalTime()
            : insertNode.getPhysicalTime();
    final int writerNodeId =
        indexedRequest.getNodeId() >= 0 ? indexedRequest.getNodeId() : insertNode.getNodeId();

    trackWriterState(writerNodeId);
    final long maxTs = extractMaxTime(insertNode);
    if (maxTs > maxObservedTimestamp) {
      maxObservedTimestamp = maxTs;
    }
    final List<Tablet> tablets = converter.convert(insertNode);
    return new PreparedEntry(tablets, physicalTime, writerNodeId, localSeq, searchIndex);
  }

  private static long estimateTabletSize(final Tablet tablet) {
    return PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
  }

  private void createAndEnqueueEvent(
      final List<Tablet> tablets, final long startSearchIndex, final long endSearchIndex) {
    createAndEnqueueEvent(
        tablets, startSearchIndex, endSearchIndex, endSearchIndex, seekGeneration.get());
  }

  private boolean createAndEnqueueEvent(
      final List<Tablet> tablets,
      final long startSearchIndex,
      final long endSearchIndex,
      final long commitLocalSeq,
      final long expectedSeekGeneration) {
    if (seekGeneration.get() != expectedSeekGeneration) {
      LOGGER.debug(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SKIP_STALE_EVENT_WITH_SEARCHINDEX_07A09B36,
          this,
          startSearchIndex,
          endSearchIndex,
          expectedSeekGeneration,
          seekGeneration.get());
      return false;
    }

    final SubscriptionCommitContext commitContext = buildWriterCommitContext(commitLocalSeq);
    final WriterId writerId = commitContext.getWriterId();
    final WriterProgress writerProgress = commitContext.getWriterProgress();
    commitManager.recordMapping(
        consumerGroupId, topicName, consensusGroupId, writerId, writerProgress);
    if (tablets.isEmpty()) {
      return commitManager.commit(
          consumerGroupId, topicName, consensusGroupId, writerId, writerProgress);
    }

    // nextOffset <= 0 means all tablets delivered in single batch
    // -tablets.size() indicates total count
    // Use Map<String, List<Tablet>> constructor with actual database name for table model;
    final TabletsPayload payload =
        new TabletsPayload(
            Collections.singletonMap(converter.getDatabaseName(), tablets), -tablets.size());

    final SubscriptionEvent event =
        new SubscriptionEvent(
            SubscriptionPollResponseType.TABLETS.getType(),
            payload,
            commitContext,
            SubscriptionAgent.broker().getColumnFilterMatcher(topicName).isTimeSelected(),
            getTimeSelectedByTable(converter.getDatabaseName(), tablets));

    prefetchingQueue.add(event);

    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ENQUEUED_EVENT_WITH_TABLETS_SEARCHINDEX_140FDDCB,
        this,
        tablets.size(),
        startSearchIndex,
        endSearchIndex,
        prefetchingQueue.size());

    // After enqueuing the data event, control metadata is handled separately from user data.
    return true;
  }

  private Map<String, Map<String, Boolean>> getTimeSelectedByTable(
      final String databaseName, final List<Tablet> tablets) {
    if (Objects.isNull(databaseName) || Objects.isNull(tablets) || tablets.isEmpty()) {
      return Collections.emptyMap();
    }
    final ColumnFilterMatcher matcher =
        SubscriptionAgent.broker().getColumnFilterMatcher(topicName);
    final Map<String, Boolean> tableMap = new HashMap<>();
    for (final Tablet tablet : tablets) {
      if (Objects.nonNull(tablet) && Objects.nonNull(tablet.getTableName())) {
        tableMap.put(
            tablet.getTableName(), matcher.isTimeSelected(databaseName, tablet.getTableName()));
      }
    }
    return tableMap.isEmpty()
        ? Collections.emptyMap()
        : Collections.singletonMap(databaseName, tableMap);
  }

  private SubscriptionCommitContext buildWriterCommitContext(final long localSeq) {
    final int effectiveNodeId =
        batchWriterNodeId >= 0
            ? batchWriterNodeId
            : IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    final WriterId writerId = new WriterId(consensusGroupId.toString(), effectiveNodeId);
    final WriterProgress writerProgress = new WriterProgress(batchPhysicalTime, localSeq);
    return new SubscriptionCommitContext(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        PipeDataNodeAgent.runtime().getRebootTimes(),
        topicName,
        consumerGroupId,
        seekGeneration.get(),
        writerId,
        writerProgress);
  }

  private void updateBatchWriterProgress(final long physicalTime, final int writerNodeId) {
    if (physicalTime > 0) {
      this.batchPhysicalTime = physicalTime;
    }
    if (writerNodeId >= 0) {
      this.batchWriterNodeId = writerNodeId;
    }
  }

  private void resetBatchWriterProgress() {
    this.batchPhysicalTime = 0L;
    this.batchWriterNodeId = -1;
  }

  private long estimateTabletsBytes(final List<Tablet> tablets) {
    long estimatedBytes = 0L;
    for (final Tablet tablet : tablets) {
      estimatedBytes += estimateTabletSize(tablet);
    }
    return estimatedBytes;
  }

  private boolean appendPreparedEntryViaRealtimeWriter(
      final DeliveryBatchState batchState,
      final PreparedEntry preparedEntry,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {
    bufferRealtimeEntry(preparedEntry);
    return drainRealtimeWriters(batchState, expectedSeekGeneration, maxTablets, maxBatchBytes);
  }

  private int getRealtimeBufferedEntryCount() {
    int count = 0;
    for (final NavigableMap<Long, PreparedEntry> writerEntries : realtimeEntriesByWriter.values()) {
      count += writerEntries.size();
    }
    return count;
  }

  private boolean drainBufferedRealtimeWriters(
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {
    while (!realtimeEntriesByWriter.isEmpty()) {
      final int bufferedBefore = getRealtimeBufferedEntryCount();
      if (!drainRealtimeWriters(batchState, expectedSeekGeneration, maxTablets, maxBatchBytes)) {
        return false;
      }

      final int bufferedAfter = getRealtimeBufferedEntryCount();
      if (bufferedAfter == 0 || prefetchingQueue.size() >= MAX_PREFETCHING_QUEUE_SIZE) {
        return true;
      }

      if (batchState.isEmpty()) {
        return true;
      }

      if (!flushBatch(batchState, expectedSeekGeneration)) {
        return false;
      }
    }
    return true;
  }

  private boolean canAppendWriterEntry(
      final DeliveryBatchState batchState,
      final WriterBufferedEntry entry,
      final long entryEstimatedBytes,
      final int maxEntries,
      final int maxTablets,
      final long maxBatchBytes) {
    final boolean wouldExceedEntryLimit =
        maxEntries != Integer.MAX_VALUE && batchState.entryCount >= maxEntries;
    final boolean wouldExceedTabletLimit =
        !batchState.isEmpty() && batchState.tablets.size() + entry.getTablets().size() > maxTablets;
    final boolean wouldExceedByteLimit =
        !batchState.isEmpty() && batchState.estimatedBytes + entryEstimatedBytes > maxBatchBytes;
    // Keep all consensus subscription modes on a single-writer commit/delivery shape so
    // SubscriptionCommitContext and RegionProgress remain per-writer.
    final boolean writerChanged =
        !batchState.isEmpty() && batchState.writerNodeId != entry.getWriterNodeId();
    return !(wouldExceedEntryLimit
        || wouldExceedTabletLimit
        || wouldExceedByteLimit
        || writerChanged);
  }

  private boolean drainRealtimeWriters(
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {
    return drainWriterEntries(
        batchState,
        this::buildRealtimeWriterFrontiers,
        this::peekRealtimeEntry,
        entry -> true,
        (writerNodeId, entry) -> removeRealtimeEntry(writerNodeId, entry.localSeq),
        Integer.MAX_VALUE,
        maxTablets,
        maxBatchBytes,
        true,
        expectedSeekGeneration);
  }

  private <T extends WriterBufferedEntry> boolean drainWriterEntries(
      final DeliveryBatchState batchState,
      final Supplier<PriorityQueue<WriterFrontier>> frontierSupplier,
      final Function<Integer, T> headSupplier,
      final Predicate<T> releasePredicate,
      final BiConsumer<Integer, T> removeHeadAction,
      final int maxEntries,
      final int maxTablets,
      final long maxBatchBytes,
      final boolean trackLingerTime,
      final long expectedSeekGeneration) {
    while (true) {
      final PriorityQueue<WriterFrontier> frontiers = frontierSupplier.get();
      if (frontiers.isEmpty()) {
        return true;
      }
      final WriterFrontier frontier = frontiers.peek();
      if (Objects.isNull(frontier) || frontier.isBarrier) {
        return true;
      }
      final T writerHead = headSupplier.apply(frontier.writerNodeId);
      if (Objects.isNull(writerHead)) {
        return true;
      }
      if (!releasePredicate.test(writerHead)) {
        return true;
      }

      if (writerHead.getTablets().isEmpty()) {
        if (!commitEmptyWriterEntry(batchState, writerHead, expectedSeekGeneration)) {
          return false;
        }
        removeHeadAction.accept(frontier.writerNodeId, writerHead);
        continue;
      }

      final long entryEstimatedBytes = estimateTabletsBytes(writerHead.getTablets());
      if (!canAppendWriterEntry(
          batchState, writerHead, entryEstimatedBytes, maxEntries, maxTablets, maxBatchBytes)) {
        return true;
      }

      removeHeadAction.accept(frontier.writerNodeId, writerHead);
      batchState.append(writerHead, entryEstimatedBytes, trackLingerTime);
    }
  }

  private boolean commitEmptyWriterEntry(
      final DeliveryBatchState batchState,
      final WriterBufferedEntry entry,
      final long expectedSeekGeneration) {
    if (!batchState.isEmpty() && !flushBatch(batchState, expectedSeekGeneration)) {
      return false;
    }

    updateBatchWriterProgress(entry.getPhysicalTime(), entry.getWriterNodeId());
    final boolean committed =
        createAndEnqueueEvent(
            Collections.emptyList(),
            entry.getSearchIndex(),
            entry.getSearchIndex(),
            entry.getLocalSeq(),
            expectedSeekGeneration);
    resetBatchWriterProgress();
    return committed;
  }

  private boolean flushBatch(
      final DeliveryBatchState batchState, final long expectedSeekGeneration) {
    updateBatchWriterProgress(batchState.physicalTime, batchState.writerNodeId);
    if (!createAndEnqueueEvent(
        new ArrayList<>(batchState.tablets),
        batchState.startSearchIndex,
        batchState.endSearchIndex,
        batchState.lastLocalSeq,
        expectedSeekGeneration)) {
      return false;
    }
    resetBatchWriterProgress();
    batchState.reset();
    return true;
  }

  // ======================== Commit (Ack/Nack) ========================

  /**
   * Refreshes the in-flight lease for an event held by a client-side processor.
   *
   * @return {@code true} if the lease was refreshed
   */
  public boolean refreshInFlightEventLease(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      return canAcceptCommitContext(commitContext, "refresh lease", true)
          && refreshInFlightEventLeaseInternal(consumerId, commitContext);
    } finally {
      releaseReadLock();
    }
  }

  private boolean refreshInFlightEventLeaseInternal(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean refreshed = new AtomicBoolean(false);
    inFlightEvents.compute(
        new InFlightEventKey(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            return null;
          }
          if (ev.isCommitted()) {
            ev.cleanUp(false);
            return null;
          }
          ev.recordLastPolledTimestamp();
          refreshed.set(true);
          return ev;
        });
    return refreshed.get();
  }

  private boolean canAcceptCommitContext(
      final SubscriptionCommitContext commitContext, final String action, final boolean silent) {
    if (isClosed || closeRequested || pendingSeekRequest != null) {
      return false;
    }
    if (Objects.isNull(commitContext) || !commitContext.hasWriterProgress()) {
      if (silent) {
        LOGGER.debug(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_WITHOUT_WRITER_PROGRESS_D84AA802,
            this,
            action,
            commitContext);
      } else {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_WITHOUT_WRITER_PROGRESS_D84AA802,
            this,
            action,
            commitContext);
      }
      return false;
    }
    if (!isActive) {
      if (silent) {
        LOGGER.debug(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_FOR_INACTIVE_QUEUE_COMMITCONTEXT_AE6D382C,
            this,
            action,
            commitContext,
            runtimeVersion);
      } else {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_FOR_INACTIVE_QUEUE_COMMITCONTEXT_AE6D382C,
            this,
            action,
            commitContext,
            runtimeVersion);
      }
      return false;
    }
    return true;
  }

  public boolean ack(final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      return canAcceptCommitContext(commitContext, "ack", false)
          && ackInternal(consumerId, commitContext);
    } finally {
      releaseReadLock();
    }
  }

  private boolean ackInternal(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final WriterId commitWriterId = extractCommitWriterId(commitContext);
    final WriterProgress commitWriterProgress = extractCommitWriterProgress(commitContext);
    final AtomicBoolean acked = new AtomicBoolean(false);
    inFlightEvents.compute(
        new InFlightEventKey(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            final boolean directCommitted =
                commitManager.commitWithoutOutstanding(
                    consumerGroupId,
                    topicName,
                    consensusGroupId,
                    commitWriterId,
                    commitWriterProgress);
            acked.set(directCommitted);
            if (!acked.get()) {
              LOGGER.warn(
                  DataNodePipeMessages
                      .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_99B8A8F3,
                  this,
                  commitContext);
            }
            return null;
          }

          if (ev.isCommitted()) {
            LOGGER.warn(
                DataNodePipeMessages
                    .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_EVENT_ALREADY_COMMITTED_AC34E829,
                this,
                commitContext);
            ev.cleanUp(false);
            return null;
          }

          final boolean committed =
              commitManager.commit(
                  consumerGroupId,
                  topicName,
                  consensusGroupId,
                  commitWriterId,
                  commitWriterProgress);
          if (!committed) {
            LOGGER.warn(
                DataNodePipeMessages
                    .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_ADVANCE_COMMIT_FRONTIER_56E606C0,
                this,
                commitContext);
            return ev;
          }

          ev.ack();
          ev.recordCommittedTimestamp();
          acked.set(true);
          ev.cleanUp(false);
          return null;
        });

    return acked.get();
  }

  public boolean nack(final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      return canAcceptCommitContext(commitContext, "nack", false)
          && nackInternal(consumerId, commitContext);
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Silent version of ack: returns false without logging if the commit context is not found. Used
   * in multi-region iteration where only one queue owns the event.
   */
  public boolean ackSilent(final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      if (!canAcceptCommitContext(commitContext, "ack", true)) {
        return false;
      }
      final WriterId commitWriterId = extractCommitWriterId(commitContext);
      final WriterProgress commitWriterProgress = extractCommitWriterProgress(commitContext);
      final AtomicBoolean acked = new AtomicBoolean(false);
      inFlightEvents.compute(
          new InFlightEventKey(consumerId, commitContext),
          (key, ev) -> {
            if (Objects.isNull(ev)) {
              final boolean directCommitted =
                  commitManager.commitWithoutOutstanding(
                      consumerGroupId,
                      topicName,
                      consensusGroupId,
                      commitWriterId,
                      commitWriterProgress);
              acked.set(directCommitted);
              return null;
            }
            if (ev.isCommitted()) {
              ev.cleanUp(false);
              return null;
            }
            final boolean committed =
                commitManager.commit(
                    consumerGroupId,
                    topicName,
                    consensusGroupId,
                    commitWriterId,
                    commitWriterProgress);
            if (!committed) {
              return ev;
            }
            ev.ack();
            ev.recordCommittedTimestamp();
            acked.set(true);
            ev.cleanUp(false);
            return null;
          });
      return acked.get();
    } finally {
      releaseReadLock();
    }
  }

  private WriterId extractCommitWriterId(final SubscriptionCommitContext commitContext) {
    final WriterId writerId = commitContext.getWriterId();
    return Objects.nonNull(writerId) ? writerId : new WriterId(consensusGroupId.toString(), -1);
  }

  private WriterProgress extractCommitWriterProgress(
      final SubscriptionCommitContext commitContext) {
    return commitContext.getWriterProgress();
  }

  /**
   * Silent version of nack: returns false without logging if the commit context is not found. Used
   * in multi-region iteration where only one queue owns the event.
   */
  public boolean nackSilent(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      if (!canAcceptCommitContext(commitContext, "nack", true)) {
        return false;
      }
      final AtomicBoolean nacked = new AtomicBoolean(false);
      inFlightEvents.compute(
          new InFlightEventKey(consumerId, commitContext),
          (key, ev) -> {
            if (Objects.isNull(ev)) {
              return null;
            }
            ev.nack();
            nacked.set(true);
            if (ev.isPoisoned()) {
              LOGGER.error(
                  DataNodePipeMessages
                      .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_NACKCOUNT_3A9255FB,
                  this,
                  ev.getNackCount(),
                  ev);
              ev.ack();
              ev.recordCommittedTimestamp();
              ev.cleanUp(false);
              return null;
            }
            prefetchingQueue.add(ev);
            return null;
          });
      return nacked.get();
    } finally {
      releaseReadLock();
    }
  }

  private boolean nackInternal(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean nacked = new AtomicBoolean(false);
    inFlightEvents.compute(
        new InFlightEventKey(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            LOGGER.warn(
                DataNodePipeMessages
                    .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_05F6C6E0,
                this,
                commitContext);
            return null;
          }

          ev.nack();
          nacked.set(true);
          if (ev.isPoisoned()) {
            LOGGER.error(
                DataNodePipeMessages
                    .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_NACKCOUNT_3A9255FB,
                this,
                ev.getNackCount(),
                ev);
            ev.ack();
            ev.recordCommittedTimestamp();
            ev.cleanUp(false);
            return null;
          }
          prefetchingQueue.add(ev);
          return null;
        });

    return nacked.get();
  }

  // ======================== Recycle ========================

  /** Recycles in-flight events that are pollable (timed out) back to the prefetching queue. */
  private void recycleInFlightEvents() {
    for (final InFlightEventKey key : new ArrayList<>(inFlightEvents.keySet())) {
      inFlightEvents.compute(
          key,
          (k, ev) -> {
            if (Objects.isNull(ev)) {
              return null;
            }
            if (ev.isCommitted()) {
              ev.cleanUp(false);
              return null;
            }
            if (ev.pollable()) {
              ev.nack();
              if (ev.isPoisoned()) {
                LOGGER.error(
                    DataNodePipeMessages
                        .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_DURING_23159F02,
                    this,
                    ev.getNackCount(),
                    ev);
                ev.ack();
                ev.recordCommittedTimestamp();
                ev.cleanUp(false);
                return null;
              }
              prefetchingQueue.add(ev);
              LOGGER.debug(
                  DataNodePipeMessages
                      .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RECYCLED_TIMED_OUT_EVENT_BACK_5E58639C,
                  this,
                  ev);
              return null;
            }
            return ev;
          });
    }
  }

  // ======================== Cleanup ========================

  public void cleanUp() {
    acquireWriteLock();
    try {
      prefetchingQueue.forEach(event -> event.cleanUp(true));
      prefetchingQueue.clear();

      inFlightEvents.values().forEach(event -> event.cleanUp(true));
      inFlightEvents.clear();

      realtimeEntriesByWriter.clear();
      writerStates.clear();
      clearRecoveryWriterProgress();
      materializedProgressByWriter.clear();
      pendingEntries.clear();
      lingerBatch.reset();
      resetBatchWriterProgress();
      pendingWalGapRetryRequested = false;
      walGapWaitStartTimeMs = 0L;
      lastWalGapWaitLogTimeMs = 0L;
      pendingSubscriptionWalResetSearchIndex = Long.MIN_VALUE;
      pendingSubscriptionWalResetGeneration = Long.MIN_VALUE;
      closeSubscriptionWALIterator();

    } finally {
      releaseWriteLock();
    }
  }

  // ======================== Seek ========================

  /**
   * Seeks to the earliest available WAL position. The actual position depends on WAL retention: if
   * old files have been reclaimed, the earliest available position may be later than 0.
   */
  public void seekToBeginning() {
    seekToResolvedPosition(0L, new RegionProgress(Collections.emptyMap()), "beginning");
  }

  /**
   * Seeks to the current WAL write position. After this, only newly written data will be consumed.
   */
  public void seekToEnd() {
    seekToResolvedPosition(
        consensusReqReader.getCurrentSearchIndex(), computeTailRegionProgress(), "end");
  }

  public void seekToRegionProgress(final RegionProgress regionProgress) {
    if (!(consensusReqReader instanceof WALNode)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_NOT_SUPPORTED_85477BAB,
          this);
      seekToBeginning();
      return;
    }
    final WALNode walNode = (WALNode) consensusReqReader;
    walNode.rollWALFile();

    final ReplayLocateDecision replayTarget =
        locateReplayStartForRegionProgress(regionProgress, false);
    switch (replayTarget.getStatus()) {
      case FOUND:
      case AT_END:
        LOGGER.info(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_WRITERCOUNT_3134A29B,
            this,
            regionProgress.getWriterPositions().size(),
            replayTarget.getStatus(),
            replayTarget.getStartSearchIndex());
        seekToResolvedPosition(
            replayTarget.getStartSearchIndex(),
            replayTarget.getRecoveryRegionProgress(),
            "regionProgress");
        return;
      case LOCATE_MISS:
      default:
        throw new IllegalStateException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKTOREGIONPROGRESS_2746E514,
                this,
                regionProgress,
                replayTarget.getDetail()));
    }
  }

  public void seekAfterRegionProgress(final RegionProgress regionProgress) {
    if (!(consensusReqReader instanceof WALNode)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_NOT_SUPPORTED_55F36BE8,
          this);
      seekToEnd();
      return;
    }
    final WALNode walNode = (WALNode) consensusReqReader;
    walNode.rollWALFile();

    final ReplayLocateDecision replayTarget =
        locateReplayStartForRegionProgress(regionProgress, true);
    switch (replayTarget.getStatus()) {
      case FOUND:
      case AT_END:
        LOGGER.info(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_WRITERCOUNT_C6B26D20,
            this,
            regionProgress.getWriterPositions().size(),
            replayTarget.getStatus(),
            replayTarget.getStartSearchIndex());
        seekToResolvedPosition(
            replayTarget.getStartSearchIndex(),
            replayTarget.getRecoveryRegionProgress(),
            "regionProgressAfter");
        return;
      case LOCATE_MISS:
      default:
        throw new IllegalStateException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKAFTERREGIONPROGRESS_48A500C3,
                this,
                regionProgress,
                replayTarget.getDetail()));
    }
  }

  private synchronized void seekToResolvedPosition(
      final long targetSearchIndex,
      final RegionProgress committedRegionProgress,
      final String seekReason) {
    final PendingSeekRequest request;

    acquireWriteLock();
    try {
      if (isClosed || closeRequested) {
        return;
      }
      // Fence old commit contexts immediately. The grouped reset itself is applied later by the
      // prefetch worker so WAL state and queue state still move under the queue's serial context.
      final boolean previousPrefetchInitialized = prefetchInitialized;
      final long previousSeekGeneration = seekGeneration.get();
      final long targetSeekGeneration = seekGeneration.incrementAndGet();
      request =
          new PendingSeekRequest(
              targetSearchIndex,
              committedRegionProgress,
              seekReason,
              previousPrefetchInitialized,
              previousSeekGeneration,
              targetSeekGeneration);
      pendingSeekRequest = request;
      prefetchInitialized = true;
    } finally {
      releaseWriteLock();
    }

    final ConsensusPrefetchSubtask subtask = ensurePrefetchSubtaskBound();
    if (Objects.isNull(subtask)) {
      failPendingSeekBeforeScheduling(request);
      request.awaitCompletion();
      return;
    }

    subtask.requestWakeupNow();
    request.awaitCompletion();
  }

  private boolean applyPendingSeekRequestIfNecessary() {
    final PendingSeekRequest request = pendingSeekRequest;
    if (Objects.isNull(request)) {
      return false;
    }

    acquireWriteLock();
    try {
      if (pendingSeekRequest != request) {
        return pendingSeekRequest != null;
      }
      pendingSeekRequest = null;
      if (isClosed || closeRequested) {
        request.fail(
            new IllegalStateException(
                String.format(
                    DataNodePipeMessages
                        .PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_WHILE_APPLYING_SEEK_2BB2B431,
                    this)));
        return true;
      }
      applySeekResetUnderWriteLock(request);
      request.complete();
      return true;
    } catch (final RuntimeException e) {
      request.fail(e);
      throw e;
    } finally {
      releaseWriteLock();
    }
  }

  public void abortPendingSeekForRuntimeStop() {
    final PendingSeekRequest requestToFail;

    acquireWriteLock();
    try {
      requestToFail = pendingSeekRequest;
      if (Objects.isNull(requestToFail)) {
        return;
      }
      pendingSeekRequest = null;
      prefetchInitialized = requestToFail.previousPrefetchInitialized;
      if (seekGeneration.get() == requestToFail.targetSeekGeneration) {
        seekGeneration.set(requestToFail.previousSeekGeneration);
      }
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ABORTED_PENDING_SEEK_DURING_RUNTIME_F9928604,
          this,
          requestToFail.seekReason,
          true,
          requestToFail.previousPrefetchInitialized,
          requestToFail.targetSeekGeneration,
          requestToFail.previousSeekGeneration);
    } finally {
      releaseWriteLock();
    }

    requestToFail.fail(
        new IllegalStateException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_RUNTIME_STOPPED_BEFORE_SEEK_7BCB4F4B,
                this,
                requestToFail.seekReason)));
  }

  private void failPendingSeekBeforeScheduling(final PendingSeekRequest request) {
    final boolean closing;

    acquireWriteLock();
    try {
      if (pendingSeekRequest != request) {
        return;
      }
      closing = isClosed || closeRequested;
      pendingSeekRequest = null;
      prefetchInitialized = request.previousPrefetchInitialized;
      if (seekGeneration.get() == request.targetSeekGeneration) {
        seekGeneration.set(request.previousSeekGeneration);
      }
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_SCHEDULE_SEEK_BECAUSE_9E407068,
          this,
          request.seekReason,
          closing
              ? DataNodePipeMessages.MESSAGE_THE_QUEUE_IS_CLOSING_AC6C2AB4
              : DataNodePipeMessages.MESSAGE_PREFETCH_RUNTIME_IS_UNAVAILABLE_F1721E89,
          true,
          request.previousPrefetchInitialized,
          request.targetSeekGeneration,
          request.previousSeekGeneration);
    } finally {
      releaseWriteLock();
    }

    request.fail(
        new IllegalStateException(
            String.format(
                closing
                    ? DataNodePipeMessages
                        .CONSENSUS_PREFETCHING_QUEUE_CLOSING_BEFORE_SEEK_SCHEDULED_FMT
                    : DataNodePipeMessages
                        .CONSENSUS_PREFETCHING_QUEUE_RUNTIME_UNAVAILABLE_FOR_SEEK_FMT,
                this,
                request.seekReason)));
  }

  private void applySeekResetUnderWriteLock(final PendingSeekRequest request) {
    // 1. Clean up all queued and in-flight events
    prefetchingQueue.forEach(event -> event.cleanUp(true));
    prefetchingQueue.clear();
    inFlightEvents.values().forEach(event -> event.cleanUp(true));
    inFlightEvents.clear();

    // 2. Discard stale pending entries from in-memory queue
    pendingEntries.clear();

    // 3. Reset per-writer release state and source-level dedup frontiers.
    realtimeEntriesByWriter.clear();
    writerStates.clear();
    clearRecoveryWriterProgress();
    materializedProgressByWriter.clear();
    if (Objects.nonNull(request.committedRegionProgress)
        && !request.committedRegionProgress.getWriterPositions().isEmpty()) {
      installRecoveryWriterProgress(request.committedRegionProgress);
    }

    // 4. Reset WAL read position. The target has been normalized by replay lookup: if the
    // requested progress points before the first retained replayable WAL entry, it is moved to that
    // first replayable searchIndex here.
    nextExpectedSearchIndex.set(request.targetSearchIndex);
    requestSubscriptionWalReset(request.targetSearchIndex, seekGeneration.get());
    lingerBatch.reset();
    resetBatchWriterProgress();
    observedSeekGeneration = seekGeneration.get();
    pendingWalGapRetryRequested = false;
    walGapWaitStartTimeMs = 0L;
    lastWalGapWaitLogTimeMs = 0L;

    // 5. Reset commit state to the writer progress immediately before the first re-delivered
    // entry so seek/rebind resumes from the intended frontier.
    commitManager.resetState(
        consumerGroupId, topicName, consensusGroupId, request.committedRegionProgress);

    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEK_APPLIED_TO_SEARCHINDEX_WRITERCOUNT_FA2C4327,
        this,
        request.seekReason,
        request.targetSearchIndex,
        Objects.nonNull(request.committedRegionProgress)
            ? request.committedRegionProgress.getWriterPositions().size()
            : 0,
        seekGeneration.get());
  }

  private RegionProgress computeTailRegionProgress() {
    if (!(consensusReqReader instanceof WALNode)) {
      return new RegionProgress(Collections.emptyMap());
    }

    final WALNode walNode = (WALNode) consensusReqReader;
    final Map<WriterId, WriterProgress> tailProgressByWriter = new LinkedHashMap<>();
    final File[] walFiles = WALFileUtils.listAllWALFiles(walNode.getLogDirectory());
    if (Objects.isNull(walFiles) || walFiles.length == 0) {
      mergeTailProgress(tailProgressByWriter, walNode.getCurrentWALMetaDataSnapshot());
      return new RegionProgress(tailProgressByWriter);
    }

    WALFileUtils.ascSortByVersionId(walFiles);
    final long liveVersionId = walNode.getCurrentWALFileVersion();
    final WALMetaData liveSnapshot = walNode.getCurrentWALMetaDataSnapshot();
    for (final File walFile : walFiles) {
      final long versionId = WALFileUtils.parseVersionId(walFile.getName());
      if (versionId == liveVersionId) {
        mergeTailProgress(tailProgressByWriter, liveSnapshot);
        continue;
      }
      try (final ProgressWALReader reader = new ProgressWALReader(walFile)) {
        mergeTailProgress(tailProgressByWriter, reader.getMetaData());
      } catch (final IOException e) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_READ_WAL_METADATA_FROM_A2ED50D1,
            this,
            walFile,
            e);
      }
    }
    return new RegionProgress(tailProgressByWriter);
  }

  private void mergeTailProgress(
      final Map<WriterId, WriterProgress> tailProgressByWriter, final WALMetaData metadata) {
    if (Objects.isNull(metadata)) {
      return;
    }
    final List<Long> physicalTimes = metadata.getPhysicalTimes();
    final List<Short> nodeIds = metadata.getNodeIds();
    final List<Long> localSeqs = metadata.getLocalSeqs();
    final int size = Math.min(Math.min(physicalTimes.size(), nodeIds.size()), localSeqs.size());
    for (int i = 0; i < size; i++) {
      final int writerNodeId = nodeIds.get(i);
      final long physicalTime = physicalTimes.get(i);
      final long localSeq = localSeqs.get(i);
      if (writerNodeId < 0 || physicalTime < 0L || localSeq < 0L) {
        continue;
      }

      final WriterId writerId = new WriterId(consensusGroupId.toString(), writerNodeId);
      final WriterProgress candidateProgress = new WriterProgress(physicalTime, localSeq);
      final WriterProgress currentProgress = tailProgressByWriter.get(writerId);
      if (Objects.isNull(currentProgress)
          || compareWriterProgress(candidateProgress, currentProgress) > 0) {
        tailProgressByWriter.put(writerId, candidateProgress);
      }
    }
  }

  /**
   * Extracts the maximum timestamp from an InsertNode. For row nodes this is the single timestamp;
   * for tablet nodes, {@code times} is sorted so the last element is the max. For composite nodes,
   * iterates over children.
   *
   * @return the maximum timestamp, or {@code Long.MIN_VALUE} if extraction fails
   */
  private long extractMaxTime(final InsertNode insertNode) {
    try {
      if (insertNode instanceof InsertRowNode) {
        return ((InsertRowNode) insertNode).getTime();
      }
      if (insertNode instanceof InsertTabletNode) {
        final InsertTabletNode tabletNode = (InsertTabletNode) insertNode;
        final int rowCount = tabletNode.getRowCount();
        return rowCount > 0 ? tabletNode.getTimes()[rowCount - 1] : Long.MIN_VALUE;
      }
      if (insertNode instanceof InsertMultiTabletsNode) {
        long max = Long.MIN_VALUE;
        for (final InsertTabletNode child :
            ((InsertMultiTabletsNode) insertNode).getInsertTabletNodeList()) {
          final int rowCount = child.getRowCount();
          if (rowCount > 0) {
            max = Math.max(max, child.getTimes()[rowCount - 1]);
          }
        }
        return max;
      }
      if (insertNode instanceof InsertRowsNode) {
        long max = Long.MIN_VALUE;
        for (final InsertRowNode row : ((InsertRowsNode) insertNode).getInsertRowNodeList()) {
          max = Math.max(max, row.getTime());
        }
        return max;
      }
      if (insertNode instanceof InsertRowsOfOneDeviceNode) {
        long max = Long.MIN_VALUE;
        for (final InsertRowNode row :
            ((InsertRowsOfOneDeviceNode) insertNode).getInsertRowNodeList()) {
          max = Math.max(max, row.getTime());
        }
        return max;
      }
      // Fallback: use getMinTime() which at least gets a timestamp
      return insertNode.getMinTime();
    } catch (final Exception e) {
      return Long.MIN_VALUE;
    }
  }

  /**
   * Checks whether it is time to inject a watermark event and does so if the configured interval
   * has elapsed. Called from prefetch rounds after processing data and during idle scheduling.
   */
  private void maybeInjectWatermark() {
    if (maxObservedTimestamp == Long.MIN_VALUE) {
      return; // No data observed yet, nothing to report
    }
    final long intervalMs =
        SubscriptionConfig.getInstance().getSubscriptionConsensusWatermarkIntervalMs();
    if (intervalMs <= 0) {
      return; // Watermark disabled
    }
    final long now = System.currentTimeMillis();
    if (now - lastWatermarkEmitTimeMs >= intervalMs) {
      injectWatermark(maxObservedTimestamp);
      lastWatermarkEmitTimeMs = now;
    }
  }

  /**
   * Injects a {@link SubscriptionPollResponseType#WATERMARK} event into the prefetching queue. The
   * committed mapping is deliberately NOT recorded because watermark events are metadata, not user
   * data.
   *
   * @param watermarkTimestamp the maximum data timestamp observed so far
   */
  private void injectWatermark(final long watermarkTimestamp) {
    final int dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    final SubscriptionCommitContext watermarkCtx = createNonCommittableSeekContext(dataNodeId);
    final SubscriptionEvent watermarkEvent =
        new SubscriptionEvent(
            SubscriptionPollResponseType.WATERMARK.getType(),
            new WatermarkPayload(watermarkTimestamp, dataNodeId),
            watermarkCtx);
    prefetchingQueue.add(watermarkEvent);

    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INJECTED_WATERMARK_WATERMARKTIMESTAMP_BF373164,
        this,
        watermarkTimestamp);
  }

  /** Returns the maximum observed data timestamp for metrics. */
  public long getMaxObservedTimestamp() {
    return maxObservedTimestamp;
  }

  private void markAcceptedFromPending() {
    pendingPathAcceptedEntries.incrementAndGet();
  }

  private void markAcceptedFromWal() {
    walPathAcceptedEntries.incrementAndGet();
  }

  public void close() {
    final PendingSeekRequest seekRequestToFail;
    final Pair<ConsensusSubscriptionPrefetchExecutor, ConsensusPrefetchSubtask> prefetchBinding;

    acquireWriteLock();
    try {
      if (isClosed || closeRequested) {
        return;
      }
      closeRequested = true;
      seekRequestToFail = pendingSeekRequest;
      pendingSeekRequest = null;
    } finally {
      releaseWriteLock();
    }

    // Stop receiving real-time in-memory writes before flushing close-time batches. Requests that
    // remain in pendingEntries have not advanced subscription progress and can be replayed from WAL
    // if this queue is created again.
    deregisterPendingEntriesFromConsensusServer();

    prefetchBinding = detachPrefetchSubtask();

    if (Objects.nonNull(seekRequestToFail)) {
      seekRequestToFail.fail(
          new IllegalStateException(
              String.format(
                  DataNodePipeMessages
                      .PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_BEFORE_SEEK_APPLIES_F893BB02,
                  this)));
    }

    if (Objects.nonNull(prefetchBinding.right)) {
      prefetchBinding.right.cancelPendingExecution();
      prefetchBinding.right.awaitIdle();
    }

    try {
      acquireWriteLock();
      try {
        if (!isClosed
            && pendingSeekRequest == null
            && seekGeneration.get() == observedSeekGeneration) {
          flushLingeringBatchOnCloseUnderWriteLock();
        }
        markClosed();
      } finally {
        releaseWriteLock();
      }

      // Deregister metrics after the queue is fully closed.
      ConsensusSubscriptionPrefetchingQueueMetrics.getInstance()
          .deregister(getPrefetchingQueueId());

      if (Objects.nonNull(prefetchBinding.left) && Objects.nonNull(prefetchBinding.right)) {
        if (!prefetchBinding.left.isShutdown()) {
          prefetchBinding.left.deregister(prefetchBinding.right.getTaskId());
        } else {
          prefetchBinding.right.close();
        }
      }

      try {
        cleanUp();
      } finally {
        // Persist progress before closing
        commitManager.persistAll();
      }
    } finally {
      closeRequested = false;
    }
  }

  private void deregisterPendingEntriesFromConsensusServer() {
    try {
      serverImpl.deregisterSubscriptionQueue(pendingEntries);
    } catch (final Exception e) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_DURING_DEREGISTER_34C332E7,
          this,
          e);
    }
  }

  private void flushLingeringBatchOnCloseUnderWriteLock() {
    if (lingerBatch.isEmpty()) {
      return;
    }
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FLUSHING_LINGERING_TABLETS_DURING_4C4AF235,
        this,
        lingerBatch.tablets.size());
    if (!flushBatch(lingerBatch, observedSeekGeneration)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_FLUSH_LINGERING_BATCH_F97D8AA7,
          this);
      lingerBatch.reset();
      resetBatchWriterProgress();
    }
  }

  private SubscriptionEvent generateErrorResponse(final String errorMessage) {
    return new SubscriptionEvent(
        SubscriptionPollResponseType.ERROR.getType(),
        new ErrorPayload(errorMessage, false),
        createNonCommittableContext(IoTDBDescriptor.getInstance().getConfig().getDataNodeId()));
  }

  private SubscriptionEvent generateOutdatedErrorResponse() {
    return new SubscriptionEvent(
        SubscriptionPollResponseType.ERROR.getType(),
        ErrorPayload.OUTDATED_ERROR_PAYLOAD,
        createNonCommittableContext(IoTDBDescriptor.getInstance().getConfig().getDataNodeId()));
  }

  /**
   * Shared subscription events still use {@link SubscriptionCommitContext#INVALID_COMMIT_ID} to
   * mark metadata and error payloads as non-committable. Consensus correctness never treats this
   * sentinel as a replay or commit frontier.
   */
  private SubscriptionCommitContext createNonCommittableContext(final int dataNodeId) {
    return new SubscriptionCommitContext(
        dataNodeId,
        PipeDataNodeAgent.runtime().getRebootTimes(),
        topicName,
        consumerGroupId,
        INVALID_COMMIT_ID);
  }

  private SubscriptionCommitContext createNonCommittableSeekContext(final int dataNodeId) {
    return new SubscriptionCommitContext(
        dataNodeId,
        PipeDataNodeAgent.runtime().getRebootTimes(),
        topicName,
        consumerGroupId,
        INVALID_COMMIT_ID,
        seekGeneration.get(),
        consensusGroupId.toString());
  }

  public boolean isCommitContextOutdated(final SubscriptionCommitContext commitContext) {
    return PipeDataNodeAgent.runtime().getRebootTimes() > commitContext.getRebootTimes()
        || seekGeneration.get() != commitContext.getSeekGeneration();
  }

  // ======================== Status ========================

  public boolean isClosed() {
    return isClosed;
  }

  public void markClosed() {
    isClosed = true;
  }

  // ======================== Routing Runtime Version Control ========================

  public long getWalGapSkippedEntries() {
    return walGapSkippedEntries.get();
  }

  public long getEpochChangeCount() {
    return runtimeVersionChangeCount.get();
  }

  // ======================== Leader Activation ========================

  /**
   * Activates or deactivates this queue. Only the preferred-writer (leader) node's queue should be
   * active. Inactive queues skip prefetching and return null on poll.
   */
  public void setActive(final boolean active) {
    this.isActive = active;
    LOGGER.info(
        DataNodePipeMessages.PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ISACTIVE_SET_TO_REGION_EC0AD7BA,
        this,
        active,
        consensusGroupId);
    if (active) {
      requestPrefetch();
    }
  }

  public boolean isActive() {
    return isActive;
  }

  public void setActiveWriterNodeIds(final Set<Integer> activeWriterNodeIds) {
    this.runtimeActiveWriterNodeIds =
        Collections.unmodifiableSet(
            new LinkedHashSet<>(Objects.requireNonNull(activeWriterNodeIds)));
    refreshEffectiveActiveWriterNodeIds();
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RUNTIMEACTIVEWRITERNODEIDS_EFFECTIVEACTIVEWRITERNODEIDS_246519D2,
        this,
        this.runtimeActiveWriterNodeIds,
        this.activeWriterNodeIds,
        consensusGroupId,
        orderMode,
        preferredWriterNodeId);
    requestPrefetch();
  }

  private void refreshEffectiveActiveWriterNodeIds() {
    final LinkedHashSet<Integer> effectiveWriterNodeIds = new LinkedHashSet<>();
    switch (orderMode) {
      case TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE:
        effectiveWriterNodeIds.addAll(runtimeActiveWriterNodeIds);
        if (effectiveWriterNodeIds.isEmpty() && preferredWriterNodeId >= 0) {
          effectiveWriterNodeIds.add(preferredWriterNodeId);
        }
        break;
      case TopicConstant.ORDER_MODE_PER_WRITER_VALUE:
        if (preferredWriterNodeId >= 0) {
          effectiveWriterNodeIds.add(preferredWriterNodeId);
        }
        break;
      case TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE:
      default:
        if (preferredWriterNodeId >= 0) {
          effectiveWriterNodeIds.add(preferredWriterNodeId);
        }
        if (previousPreferredWriterNodeId >= 0
            && previousPreferredWriterNodeId != preferredWriterNodeId
            && runtimeActiveWriterNodeIds.contains(previousPreferredWriterNodeId)) {
          effectiveWriterNodeIds.add(previousPreferredWriterNodeId);
        }
        break;
    }
    this.activeWriterNodeIds = Collections.unmodifiableSet(effectiveWriterNodeIds);
  }

  public void setPreferredWriterNodeId(final int preferredWriterNodeId) {
    if (this.preferredWriterNodeId != preferredWriterNodeId) {
      previousPreferredWriterNodeId = this.preferredWriterNodeId;
    } else {
      previousPreferredWriterNodeId = -1;
    }
    this.preferredWriterNodeId = preferredWriterNodeId;
    refreshEffectiveActiveWriterNodeIds();
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFERREDWRITERNODEID_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_B08E8180,
        this,
        this.preferredWriterNodeId,
        this.activeWriterNodeIds,
        consensusGroupId,
        orderMode);
    requestPrefetch();
  }

  public Set<Integer> getActiveWriterNodeIds() {
    return activeWriterNodeIds;
  }

  public void setOrderMode(final String orderMode) {
    final String normalizedOrderMode = TopicConfig.normalizeOrderMode(orderMode);
    if (Objects.equals(this.orderMode, normalizedOrderMode)) {
      return;
    }
    this.orderMode = normalizedOrderMode;
    refreshEffectiveActiveWriterNodeIds();
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ORDERMODE_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_CDD3C86E,
        this,
        this.orderMode,
        this.activeWriterNodeIds,
        consensusGroupId,
        preferredWriterNodeId,
        runtimeActiveWriterNodeIds);
    requestPrefetch();
  }

  public String getOrderMode() {
    return orderMode;
  }

  private boolean isWriterRuntimeActive(final int writerNodeId) {
    final Set<Integer> writerNodeIds = activeWriterNodeIds;
    return writerNodeIds.isEmpty() || writerNodeIds.contains(writerNodeId);
  }

  public void applyRuntimeState(final ConsensusRegionRuntimeState runtimeState) {
    Objects.requireNonNull(runtimeState, DataNodeMiscMessages.EXCEPTION_RUNTIMESTATE_D4D018BA);
    this.runtimeVersion = runtimeState.getRuntimeVersion();
    runtimeVersionChangeCount.incrementAndGet();
    LOGGER.info(
        DataNodePipeMessages.PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMEVERSION_36E05B80,
        this,
        runtimeState.getRuntimeVersion());
    setPreferredWriterNodeId(runtimeState.getPreferredWriterNodeId());
    setActiveWriterNodeIds(runtimeState.getActiveWriterNodeIds());
    // "active" decides whether this replica should serve subscription traffic on the current node.
    // In multi-writer mode, activeWriterNodeIds may intentionally include follower replicas for
    // ordering/watermark coordination, so it must not be reused as the local service-activation
    // signal.
    setActive(runtimeState.isActive());
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMESTATE_PREFERREDWRITERNODEID_D845E9D6,
        this,
        runtimeState,
        runtimeState.getPreferredWriterNodeId());
    if (runtimeState.isActive()) {
      requestPrefetch();
    }
  }

  public String getPrefetchingQueueId() {
    return consumerGroupId + "_" + topicName;
  }

  public long getSubscriptionUncommittedEventCount() {
    return inFlightEvents.size();
  }

  /** Exposes the current seek generation for runtime tests and metrics. */
  public long getCurrentSeekGeneration() {
    return seekGeneration.get();
  }

  public int getPrefetchedEventCount() {
    return prefetchingQueue.size();
  }

  public long getCurrentReadSearchIndex() {
    return nextExpectedSearchIndex.get();
  }

  public long getPendingPathAcceptedEntries() {
    return pendingPathAcceptedEntries.get();
  }

  public long getWalPathAcceptedEntries() {
    return walPathAcceptedEntries.get();
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public String getTopicName() {
    return topicName;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  /**
   * Returns the queue-local lag used by metrics.
   *
   * <p>Events that have already been materialized in memory are counted exactly. For data that is
   * still only in WAL, the exact number is not tracked by this queue and computing it would require
   * scanning WAL only for reporting. Therefore, unread WAL data is represented as one extra unit,
   * so the metric shows that this queue is not caught up without turning lag reporting into another
   * WAL reader.
   */
  public long getLag() {
    final long queuedLag =
        prefetchingQueue.size()
            + inFlightEvents.size()
            + pendingEntries.size()
            + getRealtimeBufferedEntryCount();
    final boolean hasUnreadWalEntries = hasUnreadWalEntriesBehindCursor();
    return queuedLag + (hasUnreadWalEntries ? 1 : 0);
  }

  // ======================== Stringify ========================

  public Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("consumerGroupId", consumerGroupId);
    result.put("topicName", topicName);
    result.put("consensusGroupId", consensusGroupId.toString());
    result.put("currentReadSearchIndex", String.valueOf(nextExpectedSearchIndex.get()));
    result.put("prefetchingQueueSize", String.valueOf(prefetchingQueue.size()));
    result.put("inFlightEventsSize", String.valueOf(inFlightEvents.size()));
    result.put("pendingEntriesSize", String.valueOf(pendingEntries.size()));
    result.put("pendingPathAcceptedEntries", String.valueOf(getPendingPathAcceptedEntries()));
    result.put("walPathAcceptedEntries", String.valueOf(getWalPathAcceptedEntries()));
    result.put("seekGeneration", String.valueOf(seekGeneration.get()));
    result.put("walGapSkippedEntries", String.valueOf(walGapSkippedEntries.get()));
    result.put("bufferedRealtimeEntryCount", String.valueOf(getRealtimeBufferedEntryCount()));
    result.put("lag", String.valueOf(getLag()));
    result.put("isClosed", String.valueOf(isClosed));
    result.put("isActive", String.valueOf(isActive));
    result.put("orderMode", orderMode);
    result.put("preferredWriterNodeId", String.valueOf(preferredWriterNodeId));
    result.put("activeWriterCount", String.valueOf(activeWriterNodeIds.size()));
    result.put("runtimeActiveWriterCount", String.valueOf(runtimeActiveWriterNodeIds.size()));
    result.put("recoveryWriterCount", String.valueOf(recoveryWriterProgressByWriter.size()));
    result.put("writerStateCount", String.valueOf(writerStates.size()));
    result.put("realtimeWriterCount", String.valueOf(realtimeEntriesByWriter.size()));
    return result;
  }

  @Override
  public String toString() {
    return "ConsensusPrefetchingQueue" + coreReportMessage();
  }

  // ======================== Inner Classes ========================

  private static final class InFlightEventKey {
    private final String consumerId;
    private final SubscriptionCommitContext commitContext;

    private InFlightEventKey(
        final String consumerId, final SubscriptionCommitContext commitContext) {
      this.consumerId = consumerId;
      this.commitContext = commitContext;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof InFlightEventKey)) {
        return false;
      }
      final InFlightEventKey that = (InFlightEventKey) obj;
      return Objects.equals(consumerId, that.consumerId)
          && Objects.equals(commitContext, that.commitContext);
    }

    @Override
    public int hashCode() {
      return Objects.hash(consumerId, commitContext);
    }
  }

  private interface WriterBufferedEntry {
    List<Tablet> getTablets();

    long getPhysicalTime();

    int getWriterNodeId();

    long getLocalSeq();

    long getSearchIndex();

    OrderingKey getOrderingKey();
  }

  private static final class DeliveryBatchState {

    private final List<Tablet> tablets = new ArrayList<>();
    private long startSearchIndex;
    private long endSearchIndex;
    private long estimatedBytes;
    private long firstTabletTimeMs;
    private long physicalTime;
    private int writerNodeId;
    private long lastLocalSeq;
    private int entryCount;

    private DeliveryBatchState() {
      reset();
    }

    private boolean isEmpty() {
      return tablets.isEmpty();
    }

    private void append(
        final WriterBufferedEntry entry,
        final long entryEstimatedBytes,
        final boolean trackLingerTime) {
      if (tablets.isEmpty()) {
        if (trackLingerTime) {
          firstTabletTimeMs = System.currentTimeMillis();
        }
        writerNodeId = entry.getWriterNodeId();
      }
      if (entry.getSearchIndex() >= 0) {
        if (startSearchIndex < 0) {
          startSearchIndex = entry.getSearchIndex();
        }
        endSearchIndex = entry.getSearchIndex();
      }
      tablets.addAll(entry.getTablets());
      estimatedBytes += entryEstimatedBytes;
      physicalTime = entry.getPhysicalTime();
      lastLocalSeq = entry.getLocalSeq();
      writerNodeId = entry.getWriterNodeId();
      entryCount++;
    }

    private void reset() {
      tablets.clear();
      startSearchIndex = -1L;
      endSearchIndex = -1L;
      estimatedBytes = 0L;
      firstTabletTimeMs = 0L;
      physicalTime = 0L;
      lastLocalSeq = -1L;
      writerNodeId = -1;
      entryCount = 0;
    }
  }

  private static final class WriterState {
    private long effectiveSafePt = 0L;
    private boolean closed = false;
  }

  private static final class PreparedEntry implements WriterBufferedEntry {
    private final List<Tablet> tablets;
    private final long physicalTime;
    private final int writerNodeId;
    private final long localSeq;
    private final long searchIndex;

    private PreparedEntry(
        final List<Tablet> tablets,
        final long physicalTime,
        final int writerNodeId,
        final long localSeq,
        final long searchIndex) {
      this.tablets = tablets;
      this.physicalTime = physicalTime;
      this.writerNodeId = writerNodeId;
      this.localSeq = localSeq;
      this.searchIndex = searchIndex;
    }

    @Override
    public List<Tablet> getTablets() {
      return tablets;
    }

    @Override
    public long getPhysicalTime() {
      return physicalTime;
    }

    @Override
    public int getWriterNodeId() {
      return writerNodeId;
    }

    @Override
    public long getLocalSeq() {
      return localSeq;
    }

    @Override
    public long getSearchIndex() {
      return searchIndex;
    }

    @Override
    public OrderingKey getOrderingKey() {
      return new OrderingKey(physicalTime, writerNodeId, localSeq);
    }
  }

  private static final class WriterFrontier implements Comparable<WriterFrontier> {
    private final int writerNodeId;
    private final OrderingKey orderingKey;
    private final boolean isBarrier;

    private WriterFrontier(
        final int writerNodeId, final OrderingKey orderingKey, final boolean isBarrier) {
      this.writerNodeId = writerNodeId;
      this.orderingKey = orderingKey;
      this.isBarrier = isBarrier;
    }

    private static WriterFrontier forHead(final int writerNodeId, final WriterBufferedEntry entry) {
      return new WriterFrontier(writerNodeId, entry.getOrderingKey(), false);
    }

    private static WriterFrontier forBarrier(final int writerNodeId, final long effectiveSafePt) {
      return new WriterFrontier(
          writerNodeId, new OrderingKey(effectiveSafePt, Integer.MIN_VALUE, Long.MIN_VALUE), true);
    }

    @Override
    public int compareTo(final WriterFrontier other) {
      int cmp = orderingKey.compareTo(other.orderingKey);
      if (cmp != 0) {
        return cmp;
      }
      if (isBarrier != other.isBarrier) {
        return isBarrier ? -1 : 1;
      }
      cmp = Integer.compare(writerNodeId, other.writerNodeId);
      return cmp;
    }
  }

  /** Composite ordering key (physicalTime, nodeId, localSeq) for writer ordering. */
  static final class OrderingKey implements Comparable<OrderingKey> {
    final long physicalTime;
    final int nodeId;
    final long localSeq;

    OrderingKey(final long physicalTime, final int nodeId, final long localSeq) {
      this.physicalTime = physicalTime;
      this.nodeId = nodeId;
      this.localSeq = localSeq;
    }

    @Override
    public int compareTo(final OrderingKey o) {
      int cmp = Long.compare(physicalTime, o.physicalTime);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Integer.compare(nodeId, o.nodeId);
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(localSeq, o.localSeq);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OrderingKey)) {
        return false;
      }
      final OrderingKey that = (OrderingKey) o;
      return physicalTime == that.physicalTime
          && nodeId == that.nodeId
          && localSeq == that.localSeq;
    }

    @Override
    public int hashCode() {
      return Objects.hash(physicalTime, nodeId, localSeq);
    }

    @Override
    public String toString() {
      return "(" + physicalTime + "," + nodeId + "," + localSeq + ")";
    }
  }
}
