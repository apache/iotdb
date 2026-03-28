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
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.metric.ConsensusSubscriptionPrefetchingQueueMetrics;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  private final String brokerId; // consumer group id
  private final String topicName;
  private final ConsensusGroupId consensusGroupId;

  private final IoTConsensusServerImpl serverImpl;

  private final ConsensusReqReader consensusReqReader;

  private volatile SteadyStateWalCursor steadyStateWalCursor;

  private final BlockingQueue<IndexedConsensusRequest> pendingEntries;

  private static final int PENDING_QUEUE_CAPACITY = 4096;

  private final ConsensusLogToTabletConverter converter;

  private final ConsensusSubscriptionCommitManager commitManager;

  private final AtomicLong seekGeneration;

  private final AtomicLong nextExpectedSearchIndex;

  private final PriorityBlockingQueue<SubscriptionEvent> prefetchingQueue;

  private final Map<Pair<String, SubscriptionCommitContext>, SubscriptionEvent> inFlightEvents;

  private static final int MAX_PREFETCHING_QUEUE_SIZE =
      SubscriptionConfig.getInstance().getSubscriptionConsensusPrefetchingQueueCapacity();

  private final AtomicLong walGapSkippedEntries = new AtomicLong(0);

  /**
   * Interval-based in-memory index for {@link #seekToTimestamp(long)}. Organized by searchIndex
   * intervals (each {@link #INTERVAL_SIZE} entries), recording the maximum data timestamp observed
   * within each interval. This design tolerates out-of-order timestamps: seek finds the first
   * interval whose maxTimestamp >= targetTimestamp, guaranteeing no data with timestamp >=
   * targetTimestamp is skipped (though earlier data within that interval may also be returned).
   *
   * <p>Key: interval start searchIndex (floor-aligned to INTERVAL_SIZE). Value: max data timestamp
   * seen in that interval.
   *
   * <p>This is analogous to Kafka's timeindex, which records maxTimestamp per segment rather than
   * timestamp闂備焦鍓氶崑鍛叏閻氼柆set mappings, making it immune to out-of-order producer timestamps.
   */
  private final NavigableMap<Long, Long> intervalMaxTimestampIndex = new ConcurrentSkipListMap<>();

  private static final int INTERVAL_SIZE = 100;

  private long currentIntervalStart = -1;

  private long currentIntervalMaxTimestamp = Long.MIN_VALUE;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private volatile boolean isClosed = false;

  private volatile boolean isActive = true;

  private volatile Set<Integer> activeWriterNodeIds = Collections.emptySet();

  private volatile Set<Integer> runtimeActiveWriterNodeIds = Collections.emptySet();

  private volatile int preferredWriterNodeId = -1;

  private volatile int previousPreferredWriterNodeId = -1;

  // ======================== Epoch Ordering ========================

  private volatile long runtimeVersion = 0;

  private final AtomicLong runtimeVersionChangeCount = new AtomicLong(0);

  // ======================== Historical Catch-up State ========================

  private volatile long lastReleasedPhysicalTime = 0;

  private volatile long lastReleasedLocalSeq = -1;

  private volatile ProgressWALIterator historicalWALIterator;

  private static final int HISTORICAL_LANE_BUFFER_MAX_SIZE = 1000;

  // ======================== Watermark ========================

  /** Maximum data timestamp observed across all InsertNodes processed by this queue. */
  private volatile long maxObservedTimestamp = Long.MIN_VALUE;

  /** Wall-clock time (ms) of last watermark injection. 0 means never injected. */
  private volatile long lastWatermarkEmitTimeMs = 0;

  /** Number of entries accepted from realtime pending queue. */
  private final AtomicLong pendingPathAcceptedEntries = new AtomicLong(0);

  /** Number of entries accepted from WAL-backed paths (historical or catch-up). */
  private final AtomicLong walPathAcceptedEntries = new AtomicLong(0);

  private final Thread prefetchThread;

  /**
   * Whether the prefetch loop has been initialized. Starts as false (dormant). Set to true on the
   * first poll with a region progress hint or when prefetch is explicitly triggered. This enables
   * lazy initialization: the queue captures pending entries from creation but defers WAL reader
   * setup and prefetch thread start until the consumer actually starts polling.
   */
  private volatile boolean prefetchInitialized = false;

  /** Fallback committed region progress from local persisted state. */
  private final RegionProgress fallbackCommittedRegionProgress;

  /** Recovery-time per-writer frontiers used to skip already committed entries after restart. */
  private final Map<WriterId, WriterProgress> recoveryWriterProgressByWriter =
      new ConcurrentHashMap<>();

  /**
   * Transitional lane state keyed by writer identity. This is the first step toward the target
   * per-writer lane model: release gating now reasons in terms of writer lanes and safe frontiers,
   * even though realtime/WAL intake still partially follows the older global-cursor structure.
   */
  private final Map<WriterLaneId, WriterLaneState> writerLanes = new ConcurrentHashMap<>();

  /**
   * Historical entries buffered per writer lane. This lets lane-frontier construction work directly
   * from lane-local state instead of rescanning the whole global sort buffer every time.
   */
  private final Map<WriterLaneId, NavigableMap<OrderingKey, SortableEntry>>
      historicalEntriesByLane = new ConcurrentHashMap<>();

  /** Number of historical entries currently buffered across all writer lanes. */
  private final AtomicLong historicalBufferedEntryCount = new AtomicLong(0);

  /**
   * Realtime lane buffers used by the non-Phase-A path. This is still a transitional structure, but
   * it already lets pending/WAL catch-up flow through per-writer lane state instead of directly
   * mutating batch state from a global region stream.
   */
  private final Map<WriterLaneId, NavigableMap<Long, PreparedEntry>> realtimeEntriesByLane =
      new ConcurrentHashMap<>();

  /** Fallback local tail position used when no precise global progress is available. */
  private final long fallbackTailSearchIndex;

  /** Writer-progress metadata for the current pending/WAL batch being assembled. */
  private volatile long batchPhysicalTime = 0L;

  private volatile int batchWriterNodeId = -1;
  private volatile long batchWriterEpoch = 0L;
  private volatile String orderMode = TopicConstant.ORDER_MODE_DEFAULT_VALUE;

  public ConsensusPrefetchingQueue(
      final String brokerId,
      final String topicName,
      final String orderMode,
      final ConsensusGroupId consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final RegionProgress fallbackCommittedRegionProgress,
      final long tailStartSearchIndex,
      final long initialEpoch,
      final boolean initialActive) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.consensusGroupId = consensusGroupId;
    this.serverImpl = serverImpl;
    this.consensusReqReader = serverImpl.getConsensusReqReader();
    this.converter = converter;
    this.commitManager = commitManager;
    this.fallbackCommittedRegionProgress = fallbackCommittedRegionProgress;
    this.fallbackTailSearchIndex = tailStartSearchIndex;
    this.runtimeVersion = initialEpoch;
    this.isActive = initialActive;
    this.orderMode = TopicConfig.normalizeOrderMode(orderMode);

    this.seekGeneration = new AtomicLong(0);
    this.nextExpectedSearchIndex = new AtomicLong(tailStartSearchIndex);
    // Defer WAL iterator creation until first poll.
    this.steadyStateWalCursor = null;

    this.prefetchingQueue = new PriorityBlockingQueue<>();
    this.inFlightEvents = new ConcurrentHashMap<>();

    // Register pending queue early so we don't miss real-time writes
    this.pendingEntries = new ArrayBlockingQueue<>(PENDING_QUEUE_CAPACITY);
    serverImpl.registerSubscriptionQueue(pendingEntries);

    // Prefetch thread is created but NOT started until first poll (lazy init)
    this.prefetchThread =
        new Thread(this::prefetchLoop, "ConsensusPrefetch-" + brokerId + "-" + topicName);
    this.prefetchThread.setDaemon(true);

    LOGGER.info(
        "ConsensusPrefetchingQueue created (dormant): brokerId={}, topicName={}, "
            + "orderMode={}, consensusGroupId={}, fallbackCommittedRegionProgress={}, "
            + "fallbackTailSearchIndex={}, initialEpoch={}, initialActive={}",
        brokerId,
        topicName,
        this.orderMode,
        consensusGroupId,
        fallbackCommittedRegionProgress,
        tailStartSearchIndex,
        initialEpoch,
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

  // ======================== Poll ========================

  public SubscriptionEvent poll(final String consumerId) {
    return poll(consumerId, null);
  }

  public SubscriptionEvent poll(final String consumerId, final RegionProgress regionProgress) {
    acquireReadLock();
    try {
      if (isClosed || !isActive) {
        return null;
      }
      if (!prefetchInitialized) {
        initPrefetch(regionProgress);
      }
      return pollInternal(consumerId);
    } finally {
      releaseReadLock();
    }
  }

  private synchronized void initPrefetch(final RegionProgress regionProgress) {
    if (prefetchInitialized) {
      return; // double-check under synchronization
    }

    long startSearchIndex = fallbackTailSearchIndex;
    final RegionProgress committedRegionProgress = resolveCommittedRegionProgressForInit();
    String progressSource = "tail fallback";

    clearRecoveryWriterProgress();

    if (Objects.nonNull(committedRegionProgress)) {
      installRecoveryWriterProgress(committedRegionProgress);
      progressSource = "committed region progress fallback";
    }

    if (shouldUseConsumerRegionProgressHint(regionProgress, committedRegionProgress)) {
      clearRecoveryWriterProgress();
      installRecoveryWriterProgress(regionProgress);
      progressSource = "consumer topic progress hint";
    }

    // Initialize WAL reader and iterators
    this.nextExpectedSearchIndex.set(startSearchIndex);
    resetSteadyStateWALPosition(startSearchIndex);

    // Initialize V3-based WAL iterator for historical catch-up
    if (consensusReqReader instanceof WALNode) {
      this.historicalWALIterator =
          new ProgressWALIterator(
              ((WALNode) consensusReqReader).getLogDirectory(), startSearchIndex);
    }

    // Start prefetch thread
    this.prefetchThread.start();
    this.prefetchInitialized = true;

    LOGGER.info(
        "ConsensusPrefetchingQueue {}: prefetch initialized, startSearchIndex={}, progressSource={}, recoveryWriterCount={}",
        this,
        startSearchIndex,
        progressSource,
        recoveryWriterProgressByWriter.size());
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

  protected RegionProgress resolveCommittedRegionProgressForInit() {
    commitManager.getOrCreateState(brokerId, topicName, consensusGroupId);
    final RegionProgress latestCommittedRegionProgress =
        commitManager.getCommittedRegionProgress(brokerId, topicName, consensusGroupId);
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
        .forEach(writerId -> trackWriterLane(writerId.getNodeId(), writerId.getWriterEpoch()));
  }

  private void clearRecoveryWriterProgress() {
    recoveryWriterProgressByWriter.clear();
  }

  private boolean shouldSkipForRecoveryProgress(final IndexedConsensusRequest request) {
    if (recoveryWriterProgressByWriter.isEmpty() || request.getNodeId() < 0) {
      return false;
    }
    final WriterId writerId =
        new WriterId(consensusGroupId.toString(), request.getNodeId(), request.getWriterEpoch());
    final WriterProgress committedProgress = recoveryWriterProgressByWriter.get(writerId);
    if (Objects.isNull(committedProgress)) {
      return false;
    }
    final long requestPhysicalTime = request.getPhysicalTime();
    final long requestLocalSeq = request.getProgressLocalSeq();
    if (requestPhysicalTime <= 0 || requestLocalSeq < 0) {
      return false;
    }
    return compareWriterProgress(
            new WriterProgress(requestPhysicalTime, requestLocalSeq), committedProgress)
        <= 0;
  }

  private int compareWriterProgress(
      final WriterProgress leftProgress, final WriterProgress rightProgress) {
    int cmp = Long.compare(leftProgress.getPhysicalTime(), rightProgress.getPhysicalTime());
    if (cmp != 0) {
      return cmp;
    }
    return Long.compare(leftProgress.getLocalSeq(), rightProgress.getLocalSeq());
  }

  private WriterLaneState trackWriterLane(final int writerNodeId, final long writerEpoch) {
    return writerLanes.computeIfAbsent(
        new WriterLaneId(writerNodeId, writerEpoch), ignored -> new WriterLaneState());
  }

  private void refreshWriterLaneSafeFrontiers() {
    final Map<WriterSafeFrontierTracker.WriterIdentity, Long> safePts =
        serverImpl.getWriterSafeFrontierTracker().snapshotEffectiveSafePts();
    for (final Map.Entry<WriterSafeFrontierTracker.WriterIdentity, Long> entry :
        safePts.entrySet()) {
      final WriterLaneState laneState =
          trackWriterLane(entry.getKey().getWriterNodeId(), entry.getKey().getWriterEpoch());
      laneState.effectiveSafePt = Math.max(laneState.effectiveSafePt, entry.getValue());
    }
  }

  private <T extends LaneBufferedEntry> PriorityQueue<LaneFrontier> buildLaneFrontiers(
      final Map<WriterLaneId, ?> laneEntriesByLane, final Function<WriterLaneId, T> headSupplier) {
    refreshWriterLaneSafeFrontiers();
    final PriorityQueue<LaneFrontier> frontiers = new PriorityQueue<>();
    final boolean useActiveWriterBarriers = shouldUseActiveWriterBarriers();
    final Set<WriterLaneId> laneIds = ConcurrentHashMap.newKeySet();
    final Set<Integer> seenActiveWriterNodeIds = ConcurrentHashMap.newKeySet();
    laneIds.addAll(writerLanes.keySet());
    laneIds.addAll(laneEntriesByLane.keySet());
    for (final WriterLaneId laneId : laneIds) {
      final WriterLaneState laneState = writerLanes.get(laneId);
      if (Objects.nonNull(laneState) && laneState.closed) {
        continue;
      }
      final T head = headSupplier.apply(laneId);
      if (Objects.nonNull(head)) {
        if (isLaneRuntimeActive(laneId)) {
          seenActiveWriterNodeIds.add(laneId.writerNodeId);
        }
        frontiers.add(LaneFrontier.forHead(laneId, head));
        continue;
      }
      if (Objects.nonNull(laneState)
          && laneState.effectiveSafePt > 0
          && useActiveWriterBarriers
          && isLaneRuntimeActive(laneId)) {
        seenActiveWriterNodeIds.add(laneId.writerNodeId);
        frontiers.add(LaneFrontier.forBarrier(laneId, laneState.effectiveSafePt));
      }
    }
    if (useActiveWriterBarriers) {
      for (final Integer activeWriterNodeId : activeWriterNodeIds) {
        if (!seenActiveWriterNodeIds.contains(activeWriterNodeId)) {
          frontiers.add(
              LaneFrontier.forBarrier(new WriterLaneId(activeWriterNodeId, 0L), Long.MIN_VALUE));
          break;
        }
      }
    }
    return frontiers;
  }

  private PriorityQueue<LaneFrontier> buildHistoricalLaneFrontiers() {
    return buildLaneFrontiers(historicalEntriesByLane, this::getHistoricalLaneHead);
  }

  private boolean isLaneBarrierBlockingRelease(final SortableEntry candidate) {
    final PriorityQueue<LaneFrontier> frontiers = buildHistoricalLaneFrontiers();
    if (frontiers.isEmpty()) {
      return false;
    }
    final LaneFrontier frontier = frontiers.peek();
    if (Objects.isNull(frontier)) {
      return false;
    }
    if (frontier.isBarrier) {
      return true;
    }
    return !frontier.laneId.equals(new WriterLaneId(candidate.nodeId, candidate.writerEpoch))
        || !frontier.orderingKey.equals(candidate.key);
  }

  private SortableEntry getHistoricalLaneHead(final WriterLaneId laneId) {
    final NavigableMap<OrderingKey, SortableEntry> laneEntries =
        historicalEntriesByLane.get(laneId);
    if (Objects.isNull(laneEntries) || laneEntries.isEmpty()) {
      return null;
    }
    final Map.Entry<OrderingKey, SortableEntry> firstEntry = laneEntries.firstEntry();
    return Objects.nonNull(firstEntry) ? firstEntry.getValue() : null;
  }

  private void bufferHistoricalEntry(final SortableEntry entry) {
    final WriterLaneId laneId = new WriterLaneId(entry.nodeId, entry.writerEpoch);
    final NavigableMap<OrderingKey, SortableEntry> laneEntries =
        historicalEntriesByLane.computeIfAbsent(laneId, ignored -> new TreeMap<>());
    if (Objects.isNull(laneEntries.put(entry.key, entry))) {
      historicalBufferedEntryCount.incrementAndGet();
    }
  }

  private void removeHistoricalEntry(final SortableEntry entry) {
    final WriterLaneId laneId = new WriterLaneId(entry.nodeId, entry.writerEpoch);
    final NavigableMap<OrderingKey, SortableEntry> laneEntries =
        historicalEntriesByLane.get(laneId);
    if (Objects.isNull(laneEntries)) {
      return;
    }
    if (Objects.nonNull(laneEntries.remove(entry.key))) {
      historicalBufferedEntryCount.decrementAndGet();
    }
    if (laneEntries.isEmpty()) {
      historicalEntriesByLane.remove(laneId);
    }
  }

  private void bufferRealtimeEntry(final PreparedEntry entry) {
    final WriterLaneId laneId = new WriterLaneId(entry.writerNodeId, entry.writerEpoch);
    realtimeEntriesByLane
        .computeIfAbsent(laneId, ignored -> new TreeMap<>())
        .put(entry.localSeq, entry);
  }

  private PreparedEntry peekRealtimeEntry(final WriterLaneId laneId) {
    final NavigableMap<Long, PreparedEntry> laneEntries = realtimeEntriesByLane.get(laneId);
    if (Objects.isNull(laneEntries) || laneEntries.isEmpty()) {
      return null;
    }
    final Map.Entry<Long, PreparedEntry> firstEntry = laneEntries.firstEntry();
    return Objects.nonNull(firstEntry) ? firstEntry.getValue() : null;
  }

  private void removeRealtimeEntry(final WriterLaneId laneId, final long localSeq) {
    final NavigableMap<Long, PreparedEntry> laneEntries = realtimeEntriesByLane.get(laneId);
    if (Objects.isNull(laneEntries)) {
      return;
    }
    laneEntries.remove(localSeq);
    if (laneEntries.isEmpty()) {
      realtimeEntriesByLane.remove(laneId);
    }
  }

  private PriorityQueue<LaneFrontier> buildRealtimeLaneFrontiers() {
    return buildLaneFrontiers(realtimeEntriesByLane, this::peekRealtimeEntry);
  }

  private SubscriptionEvent pollInternal(final String consumerId) {
    final long size = prefetchingQueue.size();
    if (size == 0) {
      LOGGER.debug(
          "ConsensusPrefetchingQueue {}: prefetching queue is empty for consumerId={}, "
              + "pendingEntriesSize={}, nextExpected={}, isClosed={}, threadAlive={}",
          this,
          consumerId,
          pendingEntries.size(),
          nextExpectedSearchIndex.get(),
          isClosed,
          prefetchThread.isAlive());
      return null;
    }

    LOGGER.debug(
        "ConsensusPrefetchingQueue {}: polling, queue size={}, consumerId={}",
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
              "ConsensusPrefetchingQueue {} poll committed event {} (broken invariant), remove it",
              this,
              event);
          continue;
        }

        if (!event.pollable()) {
          LOGGER.warn(
              "ConsensusPrefetchingQueue {} poll non-pollable event {} (broken invariant), nack it",
              this,
              event);
          event.nack();
          continue;
        }

        // Mark as polled before updating inFlightEvents
        event.recordLastPolledTimestamp();
        inFlightEvents.put(new Pair<>(consumerId, event.getCommitContext()), event);
        event.recordLastPolledConsumerId(consumerId);
        return event;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("ConsensusPrefetchingQueue {} interrupted while polling", this, e);
    }

    return null;
  }

  public SubscriptionEvent pollTablets(
      final String consumerId, final SubscriptionCommitContext commitContext, final int offset) {
    acquireReadLock();
    try {
      if (isClosed) {
        return null;
      }
      final SubscriptionEvent event = inFlightEvents.get(new Pair<>(consumerId, commitContext));
      if (Objects.isNull(event)) {
        if (isCommitContextOutdated(commitContext)) {
          return generateOutdatedErrorResponse();
        }
        return generateErrorResponse(
            String.format(
                "ConsensusPrefetchingQueue %s: no in-flight event for consumer %s, commit context %s",
                this, consumerId, commitContext));
      }
      return event;
    } finally {
      releaseReadLock();
    }
  }

  // ======================== Background Prefetch ========================

  public boolean executePrefetch() {
    acquireReadLock();
    try {
      if (isClosed) {
        return false;
      }
      // Recycle pollable events from inFlightEvents back to prefetchingQueue
      recycleInFlightEvents();
      return !prefetchingQueue.isEmpty();
    } finally {
      releaseReadLock();
    }
  }

  private static final long PENDING_DRAIN_TIMEOUT_MS = 10;

  private static final long WAL_WAIT_TIMEOUT_SECONDS = 2;

  private static final long PREFETCH_STATS_LOG_INTERVAL_MS = 5_000L;

  private void prefetchLoop() {
    LOGGER.info("ConsensusPrefetchingQueue {}: prefetch thread started", this);

    final DeliveryBatchState lingerBatch = new DeliveryBatchState(nextExpectedSearchIndex.get());
    long observedSeekGeneration = seekGeneration.get();
    long lastStatsLogTimeMs = System.currentTimeMillis();
    long lastPendingAcceptedEntries = pendingPathAcceptedEntries.get();
    long lastWalAcceptedEntries = walPathAcceptedEntries.get();

    try {
      while (!isClosed && !Thread.currentThread().isInterrupted()) {
        try {
          final long nowMs = System.currentTimeMillis();
          if (nowMs - lastStatsLogTimeMs >= PREFETCH_STATS_LOG_INTERVAL_MS) {
            final long currentPendingAcceptedEntries = pendingPathAcceptedEntries.get();
            final long currentWalAcceptedEntries = walPathAcceptedEntries.get();
            LOGGER.info(
                "ConsensusPrefetchingQueue {}: periodic stats, lag={}, pendingDelta={}, walDelta={}, "
                    + "pendingTotal={}, walTotal={}, pendingQueueSize={}, prefetchingQueueSize={}, "
                    + "inFlightEventsSize={}, historicalLaneEntryCount={}, realtimeLaneCount={}, "
                    + "isHistoricalCatchUpActive={}, isActive={}",
                this,
                getLag(),
                currentPendingAcceptedEntries - lastPendingAcceptedEntries,
                currentWalAcceptedEntries - lastWalAcceptedEntries,
                currentPendingAcceptedEntries,
                currentWalAcceptedEntries,
                pendingEntries.size(),
                prefetchingQueue.size(),
                inFlightEvents.size(),
                historicalBufferedEntryCount.get(),
                realtimeEntriesByLane.size(),
                isHistoricalCatchUpActive(),
                isActive);
            lastStatsLogTimeMs = nowMs;
            lastPendingAcceptedEntries = currentPendingAcceptedEntries;
            lastWalAcceptedEntries = currentWalAcceptedEntries;
          }

          final long currentSeekGeneration = seekGeneration.get();
          if (currentSeekGeneration != observedSeekGeneration) {
            lingerBatch.reset(nextExpectedSearchIndex.get());
            resetBatchWriterProgress();
            observedSeekGeneration = currentSeekGeneration;
          }

          // Dormant when not the preferred writer (leader); sleep to avoid busy-waiting
          if (!isActive) {
            Thread.sleep(200);
            continue;
          }

          // Back-pressure: wait if prefetchingQueue is full
          if (prefetchingQueue.size() >= MAX_PREFETCHING_QUEUE_SIZE) {
            Thread.sleep(50);
            continue;
          }

          // Historical catch-up: replay historical WAL through per-writer lanes before
          // switching back to the steady-state realtime/WAL path.
          if (isHistoricalCatchUpActive()) {
            handleHistoricalCatchUp(observedSeekGeneration);
            maybeInjectWatermark();
            continue;
          }

          // Phase B + C: existing logic (WAL catch-up + steady-state pendingEntries)
          final SubscriptionConfig config = SubscriptionConfig.getInstance();
          final int maxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
          final int batchMaxDelayMs = config.getSubscriptionConsensusBatchMaxDelayInMs();
          final int maxTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
          final long maxBatchBytes = config.getSubscriptionConsensusBatchMaxSizeInBytes();

          // Try to drain from pending entries (in-memory, fast path)
          final List<IndexedConsensusRequest> batch = new ArrayList<>();
          final IndexedConsensusRequest first =
              pendingEntries.poll(PENDING_DRAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (first != null) {
            batch.add(first);
            int drained = 0;
            IndexedConsensusRequest next;
            while (drained < maxWalEntries - 1 && (next = pendingEntries.poll()) != null) {
              batch.add(next);
              drained++;
            }
          }

          if (!batch.isEmpty()) {
            LOGGER.debug(
                "ConsensusPrefetchingQueue {}: drained {} entries from pendingEntries, "
                    + "first searchIndex={}, last searchIndex={}, nextExpected={}, "
                    + "prefetchingQueueSize={}",
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
              lingerBatch.reset(nextExpectedSearchIndex.get());
              resetBatchWriterProgress();
              observedSeekGeneration = seekGeneration.get();
              continue;
            }
          } else {
            // Pending queue was empty and no lingering tablets 闂?try catch-up from WAL
            final boolean realtimeAccepted =
                drainRealtimeLanes(lingerBatch, observedSeekGeneration, maxTablets, maxBatchBytes);
            if (!realtimeAccepted) {
              lingerBatch.reset(nextExpectedSearchIndex.get());
              resetBatchWriterProgress();
              observedSeekGeneration = seekGeneration.get();
              continue;
            }
            if (lingerBatch.isEmpty()) {
              tryCatchUpFromWAL(observedSeekGeneration);
              final boolean postCatchUpAccepted =
                  drainRealtimeLanes(
                      lingerBatch, observedSeekGeneration, maxTablets, maxBatchBytes);
              if (!postCatchUpAccepted) {
                lingerBatch.reset(nextExpectedSearchIndex.get());
                resetBatchWriterProgress();
                observedSeekGeneration = seekGeneration.get();
                continue;
              }
              maybeInjectWatermark();
            }
          }
          // If we have lingering tablets but pending was empty, fall through to time check below

          // Time-based flush: if tablets have been lingering longer than batchMaxDelayMs, flush now
          if (!lingerBatch.isEmpty()
              && lingerBatch.firstTabletTimeMs > 0
              && (System.currentTimeMillis() - lingerBatch.firstTabletTimeMs) >= batchMaxDelayMs) {
            if (seekGeneration.get() != observedSeekGeneration) {
              lingerBatch.reset(nextExpectedSearchIndex.get());
              resetBatchWriterProgress();
              observedSeekGeneration = seekGeneration.get();
              continue;
            }
            LOGGER.debug(
                "ConsensusPrefetchingQueue {}: time-based flush, {} tablets lingered for {}ms "
                    + "(threshold={}ms)",
                this,
                lingerBatch.tablets.size(),
                System.currentTimeMillis() - lingerBatch.firstTabletTimeMs,
                batchMaxDelayMs);
            flushBatch(lingerBatch, observedSeekGeneration, false);
          }

          // Emit watermark after processing data (if interval has elapsed)
          maybeInjectWatermark();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (final Throwable t) {
          LOGGER.error(
              "ConsensusPrefetchingQueue {}: CRITICAL error in prefetch loop "
                  + "(type={}, message={})",
              this,
              t.getClass().getName(),
              t.getMessage(),
              t);
          if (t instanceof VirtualMachineError) {
            LOGGER.error(
                "ConsensusPrefetchingQueue {}: caught VirtualMachineError, stopping thread", this);
            markClosed();
            break;
          }
          try {
            Thread.sleep(100);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }

      if (!lingerBatch.isEmpty()) {
        LOGGER.info(
            "ConsensusPrefetchingQueue {}: flushing {} lingering tablets on loop exit",
            this,
            lingerBatch.tablets.size());
        flushBatch(lingerBatch, observedSeekGeneration, false);
      }
    } catch (final Throwable fatal) {
      LOGGER.error(
          "ConsensusPrefetchingQueue {}: FATAL uncaught throwable escaped prefetch loop "
              + "(type={}, message={})",
          this,
          fatal.getClass().getName(),
          fatal.getMessage(),
          fatal);
    }
    LOGGER.info("ConsensusPrefetchingQueue {}: prefetch thread stopped", this);
  }

  /**
   * Accumulates tablets from pending entries into the linger buffer. Handles gap detection and
   * filling from WAL. Does NOT flush 闂?the caller is responsible for flush decisions.
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

    for (final IndexedConsensusRequest request : batch) {
      final long searchIndex = request.getSearchIndex();

      // Detect gap: if searchIndex > nextExpected, entries were dropped from pending queue.
      long expected = nextExpectedSearchIndex.get();
      if (shouldReanchorSearchIndexAfterHistoricalCatchUp(request, expected)) {
        reanchorSearchIndexAfterHistoricalCatchUp(request, "pending", expected);
        expected = nextExpectedSearchIndex.get();
      }
      if (searchIndex > expected) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: gap detected, expected={}, got={}. "
                + "Filling {} entries from WAL.",
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

      if (searchIndex < nextExpectedSearchIndex.get()) {
        skippedCount++;
        continue;
      }

      if (shouldSkipForRecoveryProgress(request)) {
        skippedCount++;
        nextExpectedSearchIndex.set(searchIndex + 1);
        continue;
      }

      final PreparedEntry preparedEntry = prepareEntry(request);
      if (Objects.nonNull(preparedEntry)) {
        if (!appendPreparedEntryViaRealtimeLane(
            lingerBatch, preparedEntry, expectedSeekGeneration, maxTablets, maxBatchBytes)) {
          return false;
        }
        markAcceptedFromPending();
        processedCount++;
      }
      nextExpectedSearchIndex.set(searchIndex + 1);
    }

    // Update WAL reader position to stay in sync
    syncSteadyStateWALPosition();

    LOGGER.debug(
        "ConsensusPrefetchingQueue {}: accumulate complete, batchSize={}, processed={}, "
            + "skipped={}, lingerTablets={}, nextExpected={}",
        this,
        batch.size(),
        processedCount,
        skippedCount,
        lingerBatch.tablets.size(),
        nextExpectedSearchIndex.get());

    return true;
  }

  /**
   * Fills a gap in the pending queue by reading entries from WAL. Called when gap is detected
   * between nextExpectedSearchIndex and an incoming entry's searchIndex.
   *
   * @return false if gap fill had to stop because the current batch became stale
   */
  private boolean fillGapFromWAL(
      final long fromIndex,
      final long toIndex,
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {
    // Re-position WAL reader to the gap start
    resetSteadyStateWALPosition(fromIndex);

    while (nextExpectedSearchIndex.get() < toIndex && steadyStateWalHasNext()) {
      try {
        final IndexedConsensusRequest walEntry = steadyStateWalNext();
        final long walIndex = walEntry.getSearchIndex();
        final long expected = nextExpectedSearchIndex.get();
        if (shouldReanchorSearchIndexAfterHistoricalCatchUp(walEntry, expected)) {
          reanchorSearchIndexAfterHistoricalCatchUp(walEntry, "wal-gap-fill", expected);
        }
        if (walIndex < nextExpectedSearchIndex.get()) {
          continue; // already processed
        }
        if (shouldSkipForRecoveryProgress(walEntry)) {
          nextExpectedSearchIndex.set(walIndex + 1);
          continue;
        }

        final PreparedEntry preparedEntry = prepareEntry(walEntry);
        if (Objects.nonNull(preparedEntry)) {
          if (!appendPreparedEntryViaRealtimeLane(
              batchState, preparedEntry, expectedSeekGeneration, maxTablets, maxBatchBytes)) {
            return false;
          }
          markAcceptedFromWal();
        }
        nextExpectedSearchIndex.set(walIndex + 1);
      } catch (final Exception e) {
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: error filling gap from WAL at index {}",
            this,
            nextExpectedSearchIndex.get(),
            e);
        return true;
      }
    }

    // If sealed WAL doesn't have the gap entries yet, preserve the wait semantics exposed by the
    // underlying steady-state cursor first, then roll the current writing WAL file and retry on
    // WALNode-backed readers.
    if (nextExpectedSearchIndex.get() < toIndex) {
      try {
        waitForSteadyStateWalNextReady(WAL_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        while (nextExpectedSearchIndex.get() < toIndex && steadyStateWalHasNext()) {
          final IndexedConsensusRequest walEntry = steadyStateWalNext();
          final long walIndex = walEntry.getSearchIndex();
          final long expected = nextExpectedSearchIndex.get();
          if (shouldReanchorSearchIndexAfterHistoricalCatchUp(walEntry, expected)) {
            reanchorSearchIndexAfterHistoricalCatchUp(
                walEntry, "wal-gap-fill-after-roll", expected);
          }
          if (walIndex < nextExpectedSearchIndex.get()) {
            continue;
          }
          if (shouldSkipForRecoveryProgress(walEntry)) {
            nextExpectedSearchIndex.set(walIndex + 1);
            continue;
          }
          final PreparedEntry preparedEntry = prepareEntry(walEntry);
          if (Objects.nonNull(preparedEntry)
              && !appendPreparedEntryViaRealtimeLane(
                  batchState, preparedEntry, expectedSeekGeneration, maxTablets, maxBatchBytes)) {
            return false;
          }
          nextExpectedSearchIndex.set(walIndex + 1);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final IOException e) {
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: error reading steady-state WAL gap fill at index {}",
            this,
            nextExpectedSearchIndex.get(),
            e);
      } catch (final TimeoutException e) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: timeout waiting for steady-state WAL gap fill [{}, {})",
            this,
            nextExpectedSearchIndex.get(),
            toIndex);
      }

      final long currentWALIndex = consensusReqReader.getCurrentSearchIndex();
      if (nextExpectedSearchIndex.get() <= currentWALIndex
          && consensusReqReader instanceof WALNode) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: gap fill incomplete (at {} vs WAL {}), "
                + "triggering WAL file roll",
            this,
            nextExpectedSearchIndex.get(),
            currentWALIndex);
        ((WALNode) consensusReqReader).rollWALFile();
        syncSteadyStateWALPosition();
        while (nextExpectedSearchIndex.get() < toIndex && steadyStateWalHasNext()) {
          try {
            final IndexedConsensusRequest walEntry = steadyStateWalNext();
            final long walIndex = walEntry.getSearchIndex();
            if (walIndex < nextExpectedSearchIndex.get()) {
              continue;
            }
            if (shouldSkipForRecoveryProgress(walEntry)) {
              nextExpectedSearchIndex.set(walIndex + 1);
              continue;
            }
            final PreparedEntry preparedEntry = prepareEntry(walEntry);
            if (Objects.nonNull(preparedEntry)
                && !appendPreparedEntryViaRealtimeLane(
                    batchState, preparedEntry, expectedSeekGeneration, maxTablets, maxBatchBytes)) {
              return false;
            }
            nextExpectedSearchIndex.set(walIndex + 1);
          } catch (final Exception e) {
            LOGGER.warn(
                "ConsensusPrefetchingQueue {}: error reading WAL after roll at index {}",
                this,
                nextExpectedSearchIndex.get(),
                e);
            return true;
          }
        }
      }
    }

    // If the gap still cannot be filled, WAL is corrupted/truncated
    if (nextExpectedSearchIndex.get() < toIndex) {
      final long skipped = toIndex - nextExpectedSearchIndex.get();
      walGapSkippedEntries.addAndGet(skipped);
      LOGGER.warn(
          "ConsensusPrefetchingQueue {}: WAL gap [{}, {}) cannot be filled - {} entries lost. "
              + "Total skipped entries so far: {}. "
              + "Possible causes: WAL retention policy reclaimed files, or WAL corruption/truncation.",
          this,
          nextExpectedSearchIndex.get(),
          toIndex,
          skipped,
          walGapSkippedEntries.get());
      nextExpectedSearchIndex.set(toIndex);
    }

    return true;
  }

  /**
   * Try catch-up from WAL when the pending queue was empty. This handles cold-start or scenarios
   * where the subscription started after data was already written.
   */
  private void tryCatchUpFromWAL(final long expectedSeekGeneration) {
    // Re-position WAL reader
    syncSteadyStateWALPosition();

    if (!steadyStateWalHasNext()) {
      // The WAL iterator excludes the current-writing WAL file for concurrency safety.
      // If entries exist in WAL but are all in the current file (e.g., after pending queue
      // overflow), we need to trigger a WAL file roll to make them readable.
      final long currentWALIndex = consensusReqReader.getCurrentSearchIndex();
      if (nextExpectedSearchIndex.get() <= currentWALIndex
          && consensusReqReader instanceof WALNode) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: subscription behind (at {} vs WAL {}), "
                + "triggering WAL file roll to make entries readable",
            this,
            nextExpectedSearchIndex.get(),
            currentWALIndex);
        ((WALNode) consensusReqReader).rollWALFile();
        syncSteadyStateWALPosition();
      }
      if (!steadyStateWalHasNext()) {
        // Data loss detection: if we expected earlier entries but WAL has advanced past them,
        // the retention policy has reclaimed WAL files before we consumed them.
        // Auto-seek to the current WAL position (similar to Kafka's auto.offset.reset=latest).
        if (nextExpectedSearchIndex.get() < currentWALIndex) {
          final long skipped = currentWALIndex - nextExpectedSearchIndex.get();
          LOGGER.warn(
              "ConsensusPrefetchingQueue {}: WAL data loss detected. Expected searchIndex={} "
                  + "but earliest available is {}. {} entries were reclaimed by WAL retention "
                  + "policy before consumption. Auto-seeking to current position.",
              this,
              nextExpectedSearchIndex.get(),
              currentWALIndex,
              skipped);
          walGapSkippedEntries.addAndGet(skipped);
          nextExpectedSearchIndex.set(currentWALIndex);
          syncSteadyStateWALPosition();
        }
        if (!steadyStateWalHasNext()) {
          return;
        }
      }
    }

    final SubscriptionConfig config = SubscriptionConfig.getInstance();
    final int maxTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
    final long maxBatchBytes = config.getSubscriptionConsensusBatchMaxSizeInBytes();
    final int maxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();

    final DeliveryBatchState batchState = new DeliveryBatchState(nextExpectedSearchIndex.get());
    int entriesRead = 0;

    while (entriesRead < maxWalEntries
        && steadyStateWalHasNext()
        && prefetchingQueue.size() < MAX_PREFETCHING_QUEUE_SIZE) {
      try {
        final IndexedConsensusRequest walEntry = steadyStateWalNext();
        final long walIndex = walEntry.getSearchIndex();
        entriesRead++;
        final long expected = nextExpectedSearchIndex.get();
        if (shouldReanchorSearchIndexAfterHistoricalCatchUp(walEntry, expected)) {
          reanchorSearchIndexAfterHistoricalCatchUp(walEntry, "wal-catch-up", expected);
        }

        if (walIndex < nextExpectedSearchIndex.get()) {
          continue;
        }
        if (shouldSkipForRecoveryProgress(walEntry)) {
          nextExpectedSearchIndex.set(walIndex + 1);
          continue;
        }

        final PreparedEntry preparedEntry = prepareEntry(walEntry);
        if (Objects.nonNull(preparedEntry)) {
          if (!appendPreparedEntryViaRealtimeLane(
              batchState, preparedEntry, expectedSeekGeneration, maxTablets, maxBatchBytes)) {
            return;
          }
          markAcceptedFromWal();
        }
        nextExpectedSearchIndex.set(walIndex + 1);
      } catch (final Exception e) {
        LOGGER.warn("ConsensusPrefetchingQueue {}: error reading WAL for catch-up", this, e);
        break;
      }
    }

    if (!batchState.isEmpty()) {
      flushBatch(batchState, expectedSeekGeneration, false);
    }

    if (entriesRead > 0) {
      LOGGER.debug(
          "ConsensusPrefetchingQueue {}: WAL catch-up read {} entries, "
              + "nextExpectedSearchIndex={}",
          this,
          entriesRead,
          nextExpectedSearchIndex.get());
    }
  }

  /**
   * Re-positions the WAL reader to the current nextExpectedSearchIndex. Called before reading from
   * WAL to ensure the iterator is in sync with tracking position.
   */
  private void syncSteadyStateWALPosition() {
    resetSteadyStateWALPosition(nextExpectedSearchIndex.get());
  }

  private static final class SteadyStateWalCursor {

    private final ProgressWALIterator walIterator;
    private final ConsensusReqReader.ReqIterator reqIterator;

    private SteadyStateWalCursor(final ProgressWALIterator walIterator) {
      this.walIterator = walIterator;
      this.reqIterator = null;
    }

    private SteadyStateWalCursor(final ConsensusReqReader.ReqIterator reqIterator) {
      this.walIterator = null;
      this.reqIterator = reqIterator;
    }

    private boolean hasNext() {
      return Objects.nonNull(walIterator)
          ? walIterator.hasNext()
          : Objects.nonNull(reqIterator) && reqIterator.hasNext();
    }

    private IndexedConsensusRequest next()
        throws IOException, InterruptedException, TimeoutException {
      if (Objects.nonNull(walIterator)) {
        return walIterator.next();
      }
      return reqIterator.next();
    }

    private void waitForNextReady(final long timeout, final TimeUnit unit)
        throws IOException, InterruptedException, TimeoutException {
      if (Objects.nonNull(reqIterator)) {
        reqIterator.waitForNextReady(timeout, unit);
      }
    }

    private void close() throws IOException {
      if (Objects.nonNull(walIterator)) {
        walIterator.close();
      }
    }
  }

  private void resetSteadyStateWALPosition(final long startSearchIndex) {
    if (consensusReqReader instanceof WALNode) {
      closeSteadyStateWalIterator();
      steadyStateWalCursor =
          new SteadyStateWalCursor(
              new ProgressWALIterator(
                  ((WALNode) consensusReqReader).getLogDirectory(), startSearchIndex));
      return;
    }

    steadyStateWalCursor =
        new SteadyStateWalCursor(consensusReqReader.getReqIterator(startSearchIndex));
  }

  private boolean steadyStateWalHasNext() {
    return Objects.nonNull(steadyStateWalCursor) && steadyStateWalCursor.hasNext();
  }

  private IndexedConsensusRequest steadyStateWalNext()
      throws IOException, InterruptedException, TimeoutException {
    return steadyStateWalCursor.next();
  }

  private void waitForSteadyStateWalNextReady(final long timeout, final TimeUnit unit)
      throws IOException, InterruptedException, TimeoutException {
    if (Objects.nonNull(steadyStateWalCursor)) {
      steadyStateWalCursor.waitForNextReady(timeout, unit);
    }
  }

  private void closeSteadyStateWalIterator() {
    if (steadyStateWalCursor != null) {
      try {
        steadyStateWalCursor.close();
      } catch (final IOException e) {
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: error closing steady-state WAL iterator", this, e);
      }
      steadyStateWalCursor = null;
    }
  }

  // ======================== Historical Catch-up ========================

  private void handleHistoricalCatchUp(final long expectedSeekGeneration)
      throws InterruptedException {
    // Discard pending entries 闁?their data is also in WAL, no loss
    pendingEntries.clear();

    if (historicalWALIterator == null) {
      // Fallback: no WALNode available, skip historical catch-up
      markHistoricalCatchUpComplete();
      return;
    }

    // Refresh file list to pick up newly sealed WAL files
    historicalWALIterator.refresh();

    final int batchSize =
        SubscriptionConfig.getInstance().getSubscriptionConsensusBatchMaxWalEntries();
    int readCount = 0;

    while (readCount < batchSize
        && historicalWALIterator.hasNext()
        && historicalBufferedEntryCount.get() < HISTORICAL_LANE_BUFFER_MAX_SIZE
        && prefetchingQueue.size() < MAX_PREFETCHING_QUEUE_SIZE) {
      try {
        final IndexedConsensusRequest walEntry = historicalWALIterator.next();
        if (shouldSkipForRecoveryProgress(walEntry)) {
          readCount++;
          continue;
        }
        final PreparedEntry preparedEntry = prepareEntry(walEntry);
        if (Objects.nonNull(preparedEntry)) {
          bufferPreparedEntryForOrdering(preparedEntry);
          markAcceptedFromWal();
        }
        readCount++;
      } catch (final Exception e) {
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: error reading WAL during historical catch-up", this, e);
        break;
      }
    }

    final boolean releasedAny = drainHistoricalLanes(expectedSeekGeneration);

    if (historicalBufferedEntryCount.get() == 0L && !historicalWALIterator.hasNext()) {
      markHistoricalCatchUpComplete();
      LOGGER.info(
          "ConsensusPrefetchingQueue {}: historical catch-up complete, transitioning to steady-state, runtimeVersion={}",
          this,
          runtimeVersion);
    }

    if (readCount == 0 && !releasedAny) {
      Thread.sleep(50);
    }
  }

  /**
   * Drains buffered historical lane heads in (physicalTime, nodeId, writerEpoch, localSeq) order,
   * creating subscription events. Only releases entries for which {@link
   * #canReleaseHistoricalEntry(SortableEntry)} returns true.
   *
   * @return true if at least one entry was released
   */
  private boolean drainHistoricalLanes(final long expectedSeekGeneration) {
    boolean released = false;
    final SubscriptionConfig config = SubscriptionConfig.getInstance();
    final int maxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    final int maxTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
    final long maxBatchBytes = config.getSubscriptionConsensusBatchMaxSizeInBytes();

    while (historicalBufferedEntryCount.get() > 0L
        && prefetchingQueue.size() < MAX_PREFETCHING_QUEUE_SIZE) {
      final DeliveryBatchState batchState = new DeliveryBatchState(nextExpectedSearchIndex.get());
      drainLaneEntries(
          batchState,
          this::buildHistoricalLaneFrontiers,
          this::getHistoricalLaneHead,
          this::canReleaseHistoricalEntry,
          (laneId, entry) -> removeHistoricalEntry(entry),
          maxWalEntries,
          maxTablets,
          maxBatchBytes,
          false);

      if (batchState.isEmpty()) {
        break;
      }

      if (!flushBatch(batchState, expectedSeekGeneration, true)) {
        break;
      }
      released = true;
    }
    return released;
  }

  /**
   * Determines whether a buffered historical entry can be safely released (dequeued and delivered).
   *
   * <p>The queue now treats per-writer lanes plus active-writer barriers as the primary release
   * mechanism. For historical catch-up we stay conservative in only two cases:
   *
   * <ol>
   *   <li>A competing historical lane/barrier is currently earlier than this entry
   *   <li>We have not yet observed any strictly later historical physical time and the historical
   *       WAL scan is still in progress
   * </ol>
   *
   * <p>Once a later physical time is buffered, or the historical WAL scan is exhausted, the current
   * earliest historical lane head can be released.
   */
  private boolean canReleaseHistoricalEntry(final SortableEntry entry) {
    if (!shouldUseConservativeHistoricalCatchUpRelease()) {
      return true;
    }
    if (isLaneBarrierBlockingRelease(entry)) {
      return false;
    }
    return hasBufferedLaterHistoricalPhysicalTime(entry) || isHistoricalWALExhausted();
  }

  private boolean shouldUseActiveWriterBarriers() {
    return !TopicConstant.ORDER_MODE_PER_WRITER_VALUE.equals(orderMode);
  }

  private boolean shouldUseConservativeHistoricalCatchUpRelease() {
    return !TopicConstant.ORDER_MODE_PER_WRITER_VALUE.equals(orderMode);
  }

  /**
   * Deserializes the IConsensusRequest entries within an IndexedConsensusRequest to produce an
   * InsertNode. WAL entries are typically stored as IoTConsensusRequest (serialized ByteBuffers),
   * and a single logical write may be split across multiple fragments (SearchNode). This method
   * handles both cases.
   *
   * <p>The deserialization follows the same pattern as {@code
   * DataRegionStateMachine.grabPlanNode()}.
   */
  private InsertNode deserializeToInsertNode(final IndexedConsensusRequest indexedRequest) {
    final List<SearchNode> searchNodes = new ArrayList<>();
    PlanNode nonSearchNode = null;

    for (final IConsensusRequest req : indexedRequest.getRequests()) {
      PlanNode planNode;
      try {
        if (req instanceof IoTConsensusRequest) {
          // WAL entries read from file are wrapped as IoTConsensusRequest (ByteBuffer)
          planNode = WALEntry.deserializeForConsensus(req.serializeToByteBuffer());
        } else if (req instanceof InsertNode) {
          // In-memory entries (not yet flushed to WAL file) may already be PlanNode
          planNode = (PlanNode) req;
        } else {
          // ByteBufferConsensusRequest or unknown
          planNode = PlanNodeType.deserialize(req.serializeToByteBuffer());
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: failed to deserialize IConsensusRequest "
                + "(type={}) in searchIndex={}: {}",
            this,
            req.getClass().getSimpleName(),
            indexedRequest.getSearchIndex(),
            e.getMessage(),
            e);
        continue;
      }

      if (planNode instanceof SearchNode) {
        final SearchNode searchNode = (SearchNode) planNode;
        searchNode.setSearchIndex(indexedRequest.getSearchIndex());
        if (indexedRequest.getSyncIndex() >= 0) {
          searchNode.setSyncIndex(indexedRequest.getSyncIndex());
        }
        if (indexedRequest.getPhysicalTime() > 0) {
          searchNode.setPhysicalTime(indexedRequest.getPhysicalTime());
        }
        if (indexedRequest.getNodeId() >= 0) {
          searchNode.setNodeId(indexedRequest.getNodeId());
        }
        if (indexedRequest.getWriterEpoch() > 0) {
          searchNode.setWriterEpoch(indexedRequest.getWriterEpoch());
        }
        searchNodes.add(searchNode);
      } else {
        nonSearchNode = planNode;
      }
    }

    // Merge split SearchNode fragments (same pattern as DataRegionStateMachine.grabPlanNode)
    if (!searchNodes.isEmpty()) {
      final PlanNode merged = searchNodes.get(0).merge(searchNodes);
      if (merged instanceof InsertNode) {
        final InsertNode mergedInsert = (InsertNode) merged;
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: deserialized merged InsertNode for searchIndex={}, "
                + "type={}, deviceId={}, searchNodeCount={}",
            this,
            indexedRequest.getSearchIndex(),
            mergedInsert.getType(),
            ConsensusLogToTabletConverter.safeDeviceIdForLog(mergedInsert),
            searchNodes.size());

        return mergedInsert;
      }
    }

    if (nonSearchNode != null) {
      LOGGER.debug(
          "ConsensusPrefetchingQueue {}: searchIndex={} contains non-InsertNode PlanNode: {}",
          this,
          indexedRequest.getSearchIndex(),
          nonSearchNode.getClass().getSimpleName());
    }

    return null;
  }

  private PreparedEntry prepareEntry(final IndexedConsensusRequest indexedRequest) {
    final InsertNode insertNode = deserializeToInsertNode(indexedRequest);
    if (Objects.isNull(insertNode)) {
      return null;
    }

    final long localSeq =
        indexedRequest.getProgressLocalSeq() >= 0
            ? indexedRequest.getProgressLocalSeq()
            : indexedRequest.getSearchIndex();
    final long searchIndex =
        indexedRequest.getSearchIndex() >= 0 ? indexedRequest.getSearchIndex() : localSeq;
    final long physicalTime =
        indexedRequest.getPhysicalTime() > 0
            ? indexedRequest.getPhysicalTime()
            : insertNode.getPhysicalTime();
    final int writerNodeId =
        indexedRequest.getNodeId() >= 0 ? indexedRequest.getNodeId() : insertNode.getNodeId();
    final long writerEpoch =
        indexedRequest.getWriterEpoch() > 0
            ? indexedRequest.getWriterEpoch()
            : insertNode.getWriterEpoch();

    trackWriterLane(writerNodeId, writerEpoch);
    recordTimestampSample(insertNode, searchIndex >= 0 ? searchIndex : localSeq);
    final long maxTs = extractMaxTime(insertNode);
    if (maxTs > maxObservedTimestamp) {
      maxObservedTimestamp = maxTs;
    }
    final List<Tablet> tablets = converter.convert(insertNode);
    if (tablets.isEmpty()) {
      return null;
    }

    return new PreparedEntry(
        tablets,
        searchIndex >= 0 ? searchIndex : localSeq,
        physicalTime,
        writerNodeId,
        writerEpoch,
        localSeq);
  }

  private static long estimateTabletSize(final Tablet tablet) {
    return PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
  }

  private void bufferPreparedEntryForOrdering(final PreparedEntry preparedEntry) {
    final OrderingKey key =
        new OrderingKey(
            preparedEntry.physicalTime,
            preparedEntry.writerNodeId,
            preparedEntry.writerEpoch,
            preparedEntry.localSeq);
    final SortableEntry entry =
        new SortableEntry(
            key,
            preparedEntry.tablets,
            preparedEntry.searchIndex,
            preparedEntry.physicalTime,
            preparedEntry.writerNodeId,
            preparedEntry.writerEpoch);
    bufferHistoricalEntry(entry);
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
    if (tablets.isEmpty()) {
      return true;
    }

    if (seekGeneration.get() != expectedSeekGeneration) {
      LOGGER.debug(
          "ConsensusPrefetchingQueue {}: skip stale event with searchIndex range [{}, {}], "
              + "expectedSeekGeneration={}, currentSeekGeneration={}",
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
    commitManager.recordMapping(brokerId, topicName, consensusGroupId, writerId, writerProgress);

    // nextOffset <= 0 means all tablets delivered in single batch
    // -tablets.size() indicates total count
    // Use Map<String, List<Tablet>> constructor with actual database name for table model;
    final TabletsPayload payload =
        new TabletsPayload(
            Collections.singletonMap(converter.getDatabaseName(), tablets), -tablets.size());

    final SubscriptionEvent event =
        new SubscriptionEvent(
            SubscriptionPollResponseType.TABLETS.getType(), payload, commitContext);

    prefetchingQueue.add(event);

    LOGGER.debug(
        "ConsensusPrefetchingQueue {}: ENQUEUED event with {} tablets, "
            + "searchIndex range [{}, {}], prefetchQueueSize={}",
        this,
        tablets.size(),
        startSearchIndex,
        endSearchIndex,
        prefetchingQueue.size());

    // After enqueuing the data event, control metadata is handled separately from user data.
    return true;
  }

  private SubscriptionCommitContext buildWriterCommitContext(final long localSeq) {
    final int effectiveNodeId =
        batchWriterNodeId >= 0
            ? batchWriterNodeId
            : IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    final WriterId writerId =
        new WriterId(consensusGroupId.toString(), effectiveNodeId, batchWriterEpoch);
    final WriterProgress writerProgress = new WriterProgress(batchPhysicalTime, localSeq);
    return new SubscriptionCommitContext(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        PipeDataNodeAgent.runtime().getRebootTimes(),
        topicName,
        brokerId,
        seekGeneration.get(),
        writerId,
        writerProgress);
  }

  private void updateBatchWriterProgress(
      final long physicalTime, final int writerNodeId, final long writerEpoch) {
    if (physicalTime > 0) {
      this.batchPhysicalTime = physicalTime;
    }
    if (writerNodeId >= 0) {
      this.batchWriterNodeId = writerNodeId;
    }
    if (writerEpoch > 0) {
      this.batchWriterEpoch = writerEpoch;
    }
  }

  private void resetBatchWriterProgress() {
    this.batchPhysicalTime = 0L;
    this.batchWriterNodeId = -1;
    this.batchWriterEpoch = 0L;
  }

  private long estimateTabletsBytes(final List<Tablet> tablets) {
    long estimatedBytes = 0L;
    for (final Tablet tablet : tablets) {
      estimatedBytes += estimateTabletSize(tablet);
    }
    return estimatedBytes;
  }

  private boolean appendPreparedEntryViaRealtimeLane(
      final DeliveryBatchState batchState,
      final PreparedEntry preparedEntry,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {
    bufferRealtimeEntry(preparedEntry);
    return drainRealtimeLanes(batchState, expectedSeekGeneration, maxTablets, maxBatchBytes);
  }

  private boolean canAppendLaneEntry(
      final DeliveryBatchState batchState,
      final LaneBufferedEntry entry,
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
        !batchState.isEmpty()
            && (batchState.writerNodeId != entry.getWriterNodeId()
                || batchState.writerEpoch != entry.getWriterEpoch());
    return !(wouldExceedEntryLimit
        || wouldExceedTabletLimit
        || wouldExceedByteLimit
        || writerChanged);
  }

  private boolean drainRealtimeLanes(
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final int maxTablets,
      final long maxBatchBytes) {
    return drainLaneEntries(
        batchState,
        this::buildRealtimeLaneFrontiers,
        this::peekRealtimeEntry,
        entry -> true,
        (laneId, entry) -> removeRealtimeEntry(laneId, entry.localSeq),
        Integer.MAX_VALUE,
        maxTablets,
        maxBatchBytes,
        true);
  }

  private <T extends LaneBufferedEntry> boolean drainLaneEntries(
      final DeliveryBatchState batchState,
      final Supplier<PriorityQueue<LaneFrontier>> frontierSupplier,
      final Function<WriterLaneId, T> headSupplier,
      final Predicate<T> releasePredicate,
      final BiConsumer<WriterLaneId, T> removeHeadAction,
      final int maxEntries,
      final int maxTablets,
      final long maxBatchBytes,
      final boolean trackLingerTime) {
    while (true) {
      final PriorityQueue<LaneFrontier> frontiers = frontierSupplier.get();
      if (frontiers.isEmpty()) {
        return true;
      }
      final LaneFrontier frontier = frontiers.peek();
      if (Objects.isNull(frontier) || frontier.isBarrier) {
        return true;
      }
      final T laneHead = headSupplier.apply(frontier.laneId);
      if (Objects.isNull(laneHead)) {
        return true;
      }
      if (!releasePredicate.test(laneHead)) {
        return true;
      }

      final long entryEstimatedBytes = estimateTabletsBytes(laneHead.getTablets());
      if (!canAppendLaneEntry(
          batchState, laneHead, entryEstimatedBytes, maxEntries, maxTablets, maxBatchBytes)) {
        return true;
      }

      removeHeadAction.accept(frontier.laneId, laneHead);
      batchState.append(laneHead, entryEstimatedBytes, trackLingerTime);
    }
  }

  private boolean flushBatch(
      final DeliveryBatchState batchState,
      final long expectedSeekGeneration,
      final boolean advanceHistoricalProgress) {
    updateBatchWriterProgress(
        batchState.physicalTime, batchState.writerNodeId, batchState.writerEpoch);
    if (!createAndEnqueueEvent(
        new ArrayList<>(batchState.tablets),
        batchState.startSearchIndex,
        batchState.endSearchIndex,
        batchState.lastLocalSeq,
        expectedSeekGeneration)) {
      return false;
    }
    resetBatchWriterProgress();
    if (advanceHistoricalProgress) {
      // Historical catch-up replays entries through historicalWALIterator instead of the normal
      // steady-state WAL/pendingEntries path. After releasing a batch, we must advance the
      // steady-state
      // read cursor as well, otherwise the normal path may re-read the same WAL range and enqueue
      // duplicate events for the same topic/region.
      nextExpectedSearchIndex.accumulateAndGet(batchState.endSearchIndex + 1, Math::max);
      lastReleasedPhysicalTime = batchState.physicalTime;
      lastReleasedLocalSeq = batchState.lastLocalSeq;
      lastHistoricalWriterNodeId = batchState.writerNodeId;
      lastHistoricalWriterEpoch = batchState.writerEpoch;
      searchIndexReanchorPendingAfterHistoricalCatchUp = true;
    }
    batchState.reset(nextExpectedSearchIndex.get());
    return true;
  }

  private boolean isHistoricalCatchUpActive() {
    return historicalBufferedEntryCount.get() > 0L
        || (Objects.nonNull(historicalWALIterator) && historicalWALIterator.hasNext());
  }

  private void markHistoricalCatchUpComplete() {
    // Historical catch-up completion is now driven by lane buffers and WAL exhaustion instead of
    // routing-epoch markers. Keep the last released progress only for status/reporting.
  }

  private boolean hasBufferedLaterHistoricalPhysicalTime(final SortableEntry entry) {
    for (final NavigableMap<OrderingKey, SortableEntry> laneEntries :
        historicalEntriesByLane.values()) {
      if (Objects.isNull(laneEntries) || laneEntries.isEmpty()) {
        continue;
      }
      final Map.Entry<OrderingKey, SortableEntry> lastEntry = laneEntries.lastEntry();
      if (Objects.nonNull(lastEntry) && lastEntry.getKey().physicalTime > entry.key.physicalTime) {
        return true;
      }
    }
    return false;
  }

  private boolean isHistoricalWALExhausted() {
    return Objects.isNull(historicalWALIterator) || !historicalWALIterator.hasNext();
  }

  // ======================== Commit (Ack/Nack) ========================

  private boolean canAcceptCommitContext(
      final SubscriptionCommitContext commitContext, final String action, final boolean silent) {
    if (isClosed) {
      return false;
    }
    if (!isActive) {
      if (silent) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: reject {} for inactive queue, commitContext={}, runtimeVersion={}",
            this,
            action,
            commitContext,
            runtimeVersion);
      } else {
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: reject {} for inactive queue, commitContext={}, runtimeVersion={}",
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
    final AtomicBoolean committedDirectly = new AtomicBoolean(false);
    inFlightEvents.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            final boolean directCommitted =
                commitManager.commitWithoutOutstanding(
                    brokerId, topicName, consensusGroupId, commitWriterId, commitWriterProgress);
            acked.set(directCommitted);
            committedDirectly.set(directCommitted);
            if (!acked.get()) {
              LOGGER.warn(
                  "ConsensusPrefetchingQueue {}: commit context {} does not exist for ack",
                  this,
                  commitContext);
            }
            return null;
          }

          if (ev.isCommitted()) {
            LOGGER.warn(
                "ConsensusPrefetchingQueue {}: event {} already committed", this, commitContext);
            ev.cleanUp(false);
            return null;
          }

          ev.ack();
          ev.recordCommittedTimestamp();
          acked.set(true);

          ev.cleanUp(false);
          return null;
        });

    if (acked.get() && !committedDirectly.get()) {
      commitManager.commit(
          brokerId, topicName, consensusGroupId, commitWriterId, commitWriterProgress);
    }

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
      final AtomicBoolean committedDirectly = new AtomicBoolean(false);
      inFlightEvents.compute(
          new Pair<>(consumerId, commitContext),
          (key, ev) -> {
            if (Objects.isNull(ev)) {
              final boolean directCommitted =
                  commitManager.commitWithoutOutstanding(
                      brokerId, topicName, consensusGroupId, commitWriterId, commitWriterProgress);
              acked.set(directCommitted);
              committedDirectly.set(directCommitted);
              return null;
            }
            if (ev.isCommitted()) {
              ev.cleanUp(false);
              return null;
            }
            ev.ack();
            ev.recordCommittedTimestamp();
            acked.set(true);
            ev.cleanUp(false);
            return null;
          });
      if (acked.get() && !committedDirectly.get()) {
        commitManager.commit(
            brokerId, topicName, consensusGroupId, commitWriterId, commitWriterProgress);
      }
      return acked.get();
    } finally {
      releaseReadLock();
    }
  }

  private WriterId extractCommitWriterId(final SubscriptionCommitContext commitContext) {
    final WriterId writerId = commitContext.getWriterId();
    return Objects.nonNull(writerId) ? writerId : new WriterId(consensusGroupId.toString(), -1, 0L);
  }

  private WriterProgress extractCommitWriterProgress(
      final SubscriptionCommitContext commitContext) {
    final WriterProgress writerProgress = commitContext.getWriterProgress();
    return Objects.nonNull(writerProgress)
        ? writerProgress
        : new WriterProgress(commitContext.getPhysicalTime(), commitContext.getLocalSeq());
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
          new Pair<>(consumerId, commitContext),
          (key, ev) -> {
            if (Objects.isNull(ev)) {
              return null;
            }
            ev.nack();
            nacked.set(true);
            if (ev.isPoisoned()) {
              LOGGER.error(
                  "ConsensusPrefetchingQueue {}: poison message detected (nackCount={}), "
                      + "force-acking event {} to prevent infinite re-delivery",
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
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            LOGGER.warn(
                "ConsensusPrefetchingQueue {}: commit context {} does not exist for nack",
                this,
                commitContext);
            return null;
          }

          ev.nack();
          nacked.set(true);
          if (ev.isPoisoned()) {
            LOGGER.error(
                "ConsensusPrefetchingQueue {}: poison message detected (nackCount={}), "
                    + "force-acking event {} to prevent infinite re-delivery",
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
    for (final Pair<String, SubscriptionCommitContext> key :
        new ArrayList<>(inFlightEvents.keySet())) {
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
                    "ConsensusPrefetchingQueue {}: poison message detected during recycle "
                        + "(nackCount={}), force-acking event {}",
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
                  "ConsensusPrefetchingQueue {}: recycled timed-out event {} back to prefetching queue",
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

      historicalEntriesByLane.clear();
      historicalBufferedEntryCount.set(0L);
      realtimeEntriesByLane.clear();
      writerLanes.clear();
      lastReleasedPhysicalTime = 0L;
      lastReleasedLocalSeq = -1L;
      lastHistoricalWriterNodeId = -1;
      lastHistoricalWriterEpoch = 0L;
      searchIndexReanchorPendingAfterHistoricalCatchUp = false;
      clearRecoveryWriterProgress();

      // Close historical WAL iterator
      if (historicalWALIterator != null) {
        try {
          historicalWALIterator.close();
        } catch (final IOException e) {
          LOGGER.warn("ConsensusPrefetchingQueue {}: error closing WAL iterator", this, e);
        }
        historicalWALIterator = null;
      }

      closeSteadyStateWalIterator();

      intervalMaxTimestampIndex.clear();
      currentIntervalStart = -1;
      currentIntervalMaxTimestamp = Long.MIN_VALUE;
    } finally {
      releaseWriteLock();
    }
  }

  // ======================== Seek ========================

  /**
   * Seeks the subscription to a specific WAL search index. Clears all pending, prefetched, and
   * in-flight events, resets the WAL reader, and invalidates all pre-seek commit contexts.
   *
   * <p>After seek, the consumer will receive data starting from {@code targetSearchIndex}. If the
   * target is beyond available WAL (reclaimed by retention), the consumer will start from the
   * earliest available position.
   */
  public void seekToSearchIndex(final long targetSearchIndex) {
    acquireWriteLock();
    try {
      if (isClosed) {
        return;
      }

      // 1. Invalidate all pre-seek commit contexts via fencing token
      seekGeneration.incrementAndGet();

      // 2. Clean up all queued and in-flight events
      prefetchingQueue.forEach(event -> event.cleanUp(true));
      prefetchingQueue.clear();
      inFlightEvents.values().forEach(event -> event.cleanUp(true));
      inFlightEvents.clear();

      // 3. Discard stale pending entries from in-memory queue
      pendingEntries.clear();

      // 3.5. Clear Phase A state 闂?seek resets ordering context
      historicalEntriesByLane.clear();
      historicalBufferedEntryCount.set(0L);
      realtimeEntriesByLane.clear();
      writerLanes.clear();
      lastReleasedPhysicalTime = 0;
      lastReleasedLocalSeq = -1;
      lastHistoricalWriterNodeId = -1;
      lastHistoricalWriterEpoch = 0L;
      searchIndexReanchorPendingAfterHistoricalCatchUp = false;
      clearRecoveryWriterProgress();

      // 3.7. Recreate the historical WAL iterator aligned with the new local searchIndex.
      if (historicalWALIterator != null) {
        try {
          historicalWALIterator.close();
        } catch (final IOException e) {
          LOGGER.warn(
              "ConsensusPrefetchingQueue {}: error closing WAL iterator during seek", this, e);
        }
      }
      if (consensusReqReader instanceof WALNode) {
        historicalWALIterator =
            new ProgressWALIterator(
                ((WALNode) consensusReqReader).getLogDirectory(), targetSearchIndex);
      }

      // 3.6. Keep timestamp interval index across seek operations.
      // This preserves historical timestamp->searchIndex hints so a later
      // seekToTimestamp() after seekToEnd/seekToBeginning does not only rely
      // on newly observed post-seek data.

      // 4. Reset WAL read position
      nextExpectedSearchIndex.set(targetSearchIndex);
      resetSteadyStateWALPosition(targetSearchIndex);

      // 5. Reset commit state in CommitManager. For searchIndex-based seek, keep the existing
      // Legacy search-index fallback; precise writer-progress seek uses dedicated paths below.
      commitManager.resetState(
          brokerId, topicName, consensusGroupId, null, new WriterProgress(0L, targetSearchIndex));

      // If prefetch was not yet initialized (seek before first poll), start it now
      if (!prefetchInitialized) {
        prefetchInitialized = true;
        prefetchThread.start();
      }

      LOGGER.info(
          "ConsensusPrefetchingQueue {}: seek to searchIndex={}, seekGeneration={}",
          this,
          targetSearchIndex,
          seekGeneration.get());
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Seeks to the earliest available WAL position. The actual position depends on WAL retention 闂?if
   * old files have been reclaimed, the earliest available position may be later than 0.
   */
  public void seekToBeginning() {
    // ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX is Long.MIN_VALUE;
    // getReqIterator will clamp to the earliest available file.
    seekToSearchIndex(0);
  }

  /**
   * Seeks to the current WAL write position. After this, only newly written data will be consumed.
   */
  public void seekToEnd() {
    seekToSearchIndex(consensusReqReader.getCurrentSearchIndex());
  }

  public void seekToRegionProgress(final RegionProgress regionProgress) {
    if (!(consensusReqReader instanceof WALNode)) {
      LOGGER.warn(
          "ConsensusPrefetchingQueue {}: seekToRegionProgress not supported (no WAL directory)",
          this);
      seekToBeginning();
      return;
    }
    final WALNode walNode = (WALNode) consensusReqReader;
    walNode.rollWALFile();

    final Pair<Long, RegionProgress> seekTarget =
        locateSeekTargetForRegionProgress(walNode.getLogDirectory(), regionProgress, false);
    if (seekTarget.left >= 0L) {
      LOGGER.info(
          "ConsensusPrefetchingQueue {}: seekToRegionProgress writerCount={} -> searchIndex={}",
          this,
          regionProgress.getWriterPositions().size(),
          seekTarget.left);
      seekToSearchIndexWithRegionProgress(seekTarget.left, seekTarget.right);
      return;
    }

    LOGGER.info(
        "ConsensusPrefetchingQueue {}: seekToRegionProgress writerCount={} -> no later entry, seek to end",
        this,
        regionProgress.getWriterPositions().size());
    seekToEnd();
  }

  public void seekAfterRegionProgress(final RegionProgress regionProgress) {
    if (!(consensusReqReader instanceof WALNode)) {
      LOGGER.warn(
          "ConsensusPrefetchingQueue {}: seekAfterRegionProgress not supported (no WAL directory)",
          this);
      seekToEnd();
      return;
    }
    final WALNode walNode = (WALNode) consensusReqReader;
    walNode.rollWALFile();

    final Pair<Long, RegionProgress> seekTarget =
        locateSeekTargetForRegionProgress(walNode.getLogDirectory(), regionProgress, true);
    if (seekTarget.left >= 0L) {
      LOGGER.info(
          "ConsensusPrefetchingQueue {}: seekAfterRegionProgress writerCount={} -> searchIndex={}",
          this,
          regionProgress.getWriterPositions().size(),
          seekTarget.left);
      seekToSearchIndexWithRegionProgress(seekTarget.left, seekTarget.right);
      return;
    }

    LOGGER.info(
        "ConsensusPrefetchingQueue {}: seekAfterRegionProgress writerCount={} -> no later entry, seek to end",
        this,
        regionProgress.getWriterPositions().size());
    seekToEnd();
  }

  private Pair<Long, RegionProgress> locateSeekTargetForRegionProgress(
      final File logDir, final RegionProgress regionProgress, final boolean seekAfter) {
    long earliestSearchIndex = Long.MAX_VALUE;
    boolean found = false;
    final Map<WriterId, WriterProgress> effectiveWriterProgress = new LinkedHashMap<>();

    for (final Map.Entry<WriterId, WriterProgress> entry :
        regionProgress.getWriterPositions().entrySet()) {
      final WriterId writerId = entry.getKey();
      final WriterProgress writerProgress = entry.getValue();
      if (Objects.isNull(writerId) || Objects.isNull(writerProgress)) {
        continue;
      }

      if (seekAfter) {
        final long candidate =
            WALFileUtils.findSearchIndexAfterWriterProgress(
                logDir,
                writerId.getNodeId(),
                writerId.getWriterEpoch(),
                writerProgress.getPhysicalTime(),
                writerProgress.getLocalSeq());
        effectiveWriterProgress.put(writerId, writerProgress);
        if (candidate >= 0L) {
          earliestSearchIndex = Math.min(earliestSearchIndex, candidate);
          found = true;
        }
        continue;
      }

      final long[] located =
          WALFileUtils.locateByWriterProgress(
              logDir,
              writerId.getNodeId(),
              writerId.getWriterEpoch(),
              writerProgress.getPhysicalTime(),
              writerProgress.getLocalSeq());
      if (Objects.nonNull(located)) {
        earliestSearchIndex = Math.min(earliestSearchIndex, located[0]);
        found = true;
        if (located[1] == 1L) {
          effectiveWriterProgress.put(
              writerId,
              new WriterProgress(
                  writerProgress.getPhysicalTime(),
                  writerProgress.getLocalSeq() > 0L
                      ? writerProgress.getLocalSeq() - 1L
                      : INVALID_COMMIT_ID));
        } else {
          effectiveWriterProgress.put(writerId, writerProgress);
        }
      } else {
        effectiveWriterProgress.put(writerId, writerProgress);
      }
    }

    return new Pair<>(
        found ? earliestSearchIndex : -1L, new RegionProgress(effectiveWriterProgress));
  }

  private void seekToSearchIndexWithRegionProgress(
      final long targetSearchIndex, final RegionProgress committedRegionProgress) {
    acquireWriteLock();
    try {
      if (isClosed) {
        return;
      }

      // 1. Invalidate all pre-seek commit contexts via fencing token
      seekGeneration.incrementAndGet();

      // 2. Clean up all queued and in-flight events
      prefetchingQueue.forEach(event -> event.cleanUp(true));
      prefetchingQueue.clear();
      inFlightEvents.values().forEach(event -> event.cleanUp(true));
      inFlightEvents.clear();

      // 3. Discard stale pending entries from in-memory queue
      pendingEntries.clear();

      // 3.5. Clear historical catch-up state - seek resets ordering context
      historicalEntriesByLane.clear();
      historicalBufferedEntryCount.set(0L);
      realtimeEntriesByLane.clear();
      writerLanes.clear();
      lastReleasedPhysicalTime = 0;
      lastReleasedLocalSeq = -1;
      lastHistoricalWriterNodeId = -1;
      lastHistoricalWriterEpoch = 0L;
      searchIndexReanchorPendingAfterHistoricalCatchUp = false;
      clearRecoveryWriterProgress();
      if (Objects.nonNull(committedRegionProgress)
          && !committedRegionProgress.getWriterPositions().isEmpty()) {
        installRecoveryWriterProgress(committedRegionProgress);
      }

      // 3.7. Recreate the historical WAL iterator aligned with the new local searchIndex.
      if (historicalWALIterator != null) {
        try {
          historicalWALIterator.close();
        } catch (final IOException e) {
          LOGGER.warn(
              "ConsensusPrefetchingQueue {}: error closing WAL iterator during seek", this, e);
        }
      }
      if (consensusReqReader instanceof WALNode) {
        historicalWALIterator =
            new ProgressWALIterator(
                ((WALNode) consensusReqReader).getLogDirectory(), targetSearchIndex);
      }

      // 4. Reset WAL read position
      nextExpectedSearchIndex.set(targetSearchIndex);
      resetSteadyStateWALPosition(targetSearchIndex);

      // 5. Reset commit state to the writer progress immediately before the first re-delivered
      // entry so seek/rebind resumes from the intended frontier.
      commitManager.resetState(brokerId, topicName, consensusGroupId, committedRegionProgress);

      if (!prefetchInitialized) {
        prefetchInitialized = true;
        prefetchThread.start();
      }

      LOGGER.info(
          "ConsensusPrefetchingQueue {}: seek to searchIndex={}, writerCount={}, seekGeneration={}",
          this,
          targetSearchIndex,
          Objects.nonNull(committedRegionProgress)
              ? committedRegionProgress.getWriterPositions().size()
              : 0,
          seekGeneration.get());
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Seeks to the earliest WAL entry whose data timestamp >= targetTimestamp. Uses the in-memory
   * interval-based index ({@link #intervalMaxTimestampIndex}) to find the first searchIndex
   * interval whose maxTimestamp >= targetTimestamp. This guarantees no data with timestamp >=
   * targetTimestamp is missed, even with out-of-order writes. If no interval matches, falls back to
   * seekToBeginning. If targetTimestamp exceeds all known intervals, seeks to end.
   */
  public void seekToTimestamp(final long targetTimestamp) {
    // Flush the current in-progress interval so it participates in the search
    flushCurrentInterval();

    long approxSearchIndex = 0; // fallback: seek to beginning
    if (!intervalMaxTimestampIndex.isEmpty()) {
      final Map.Entry<Long, Long> lastEntry = intervalMaxTimestampIndex.lastEntry();
      if (lastEntry != null && targetTimestamp > lastEntry.getValue()) {
        // targetTimestamp is beyond the max timestamp of all known intervals 闂?seek to end
        approxSearchIndex = consensusReqReader.getCurrentSearchIndex();
      } else {
        // Linear scan to find the first interval whose maxTimestamp >= targetTimestamp.
        // This guarantees no data with timestamp >= targetTimestamp is missed, even with
        // out-of-order writes. O(N) where N = number of intervals (typically < 10,000).
        for (final Map.Entry<Long, Long> entry : intervalMaxTimestampIndex.entrySet()) {
          if (entry.getValue() >= targetTimestamp) {
            approxSearchIndex = entry.getKey();
            break;
          }
        }
      }
    }
    LOGGER.info(
        "ConsensusPrefetchingQueue {}: seekToTimestamp={}, approxSearchIndex={} (from interval index, size={})",
        this,
        targetTimestamp,
        approxSearchIndex,
        intervalMaxTimestampIndex.size());
    seekToSearchIndex(approxSearchIndex);
  }

  /**
   * Records timestamp information for interval-based index. Called for every successfully
   * deserialized InsertNode during prefetch. Tracks the max data timestamp within each searchIndex
   * interval of size {@link #INTERVAL_SIZE}.
   */
  private void recordTimestampSample(final InsertNode insertNode, final long searchIndex) {
    final long maxTs = extractMaxTime(insertNode);
    if (maxTs == Long.MIN_VALUE) {
      return; // extraction failed
    }
    final long intervalStart = (searchIndex / INTERVAL_SIZE) * INTERVAL_SIZE;
    if (intervalStart != currentIntervalStart) {
      // Entering a new interval 闂?flush the previous one
      flushCurrentInterval();
      currentIntervalStart = intervalStart;
      currentIntervalMaxTimestamp = maxTs;
    } else {
      currentIntervalMaxTimestamp = Math.max(currentIntervalMaxTimestamp, maxTs);
    }
  }

  /** Persists the current in-progress interval into the index map. */
  private void flushCurrentInterval() {
    if (currentIntervalStart >= 0) {
      intervalMaxTimestampIndex.merge(currentIntervalStart, currentIntervalMaxTimestamp, Math::max);
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
   * has elapsed. Called from the prefetch loop after processing data and during idle periods.
   */
  private void maybeInjectWatermark() {
    if (maxObservedTimestamp == Long.MIN_VALUE) {
      return; // No data observed yet 闂?nothing to report
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
    // Watermarks are fire-and-forget (not in inFlightEvents), use INVALID_COMMIT_ID
    final int dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    final SubscriptionCommitContext watermarkCtx =
        new SubscriptionCommitContext(
            dataNodeId,
            PipeDataNodeAgent.runtime().getRebootTimes(),
            topicName,
            brokerId,
            INVALID_COMMIT_ID,
            seekGeneration.get(),
            consensusGroupId.toString(),
            runtimeVersion);
    final SubscriptionEvent watermarkEvent =
        new SubscriptionEvent(
            SubscriptionPollResponseType.WATERMARK.getType(),
            new WatermarkPayload(watermarkTimestamp, dataNodeId),
            watermarkCtx);
    prefetchingQueue.add(watermarkEvent);

    LOGGER.debug(
        "ConsensusPrefetchingQueue {}: injected WATERMARK, watermarkTimestamp={}",
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
    markClosed();
    // Deregister metrics
    ConsensusSubscriptionPrefetchingQueueMetrics.getInstance().deregister(getPrefetchingQueueId());
    // Stop background prefetch thread
    prefetchThread.interrupt();
    try {
      prefetchThread.join(5000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    try {
      // Unregister from IoTConsensusServerImpl (stop receiving in-memory data).
      serverImpl.unregisterSubscriptionQueue(pendingEntries);
    } catch (final Exception e) {
      LOGGER.warn("ConsensusPrefetchingQueue {}: error during unregister", this, e);
    } finally {
      try {
        cleanUp();
      } finally {
        // Persist progress before closing
        commitManager.persistAll();
      }
    }
  }

  private SubscriptionEvent generateErrorResponse(final String errorMessage) {
    return new SubscriptionEvent(
        SubscriptionPollResponseType.ERROR.getType(),
        new ErrorPayload(errorMessage, false),
        new SubscriptionCommitContext(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            PipeDataNodeAgent.runtime().getRebootTimes(),
            topicName,
            brokerId,
            INVALID_COMMIT_ID));
  }

  private SubscriptionEvent generateOutdatedErrorResponse() {
    return new SubscriptionEvent(
        SubscriptionPollResponseType.ERROR.getType(),
        ErrorPayload.OUTDATED_ERROR_PAYLOAD,
        new SubscriptionCommitContext(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            PipeDataNodeAgent.runtime().getRebootTimes(),
            topicName,
            brokerId,
            INVALID_COMMIT_ID));
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

  // ======================== Routing Epoch Control ========================

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
        "ConsensusPrefetchingQueue {}: isActive set to {} (region={})",
        this,
        active,
        consensusGroupId);
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
        "ConsensusPrefetchingQueue {}: runtimeActiveWriterNodeIds={}, effectiveActiveWriterNodeIds={} "
            + "(region={}, orderMode={}, preferredWriterNodeId={})",
        this,
        this.runtimeActiveWriterNodeIds,
        this.activeWriterNodeIds,
        consensusGroupId,
        orderMode,
        preferredWriterNodeId);
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
        "ConsensusPrefetchingQueue {}: preferredWriterNodeId set to {}, effectiveActiveWriterNodeIds={} "
            + "(region={}, orderMode={})",
        this,
        this.preferredWriterNodeId,
        this.activeWriterNodeIds,
        consensusGroupId,
        orderMode);
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
        "ConsensusPrefetchingQueue {}: orderMode set to {}, effectiveActiveWriterNodeIds={} "
            + "(region={}, preferredWriterNodeId={}, runtimeActiveWriterNodeIds={})",
        this,
        this.orderMode,
        this.activeWriterNodeIds,
        consensusGroupId,
        preferredWriterNodeId,
        runtimeActiveWriterNodeIds);
  }

  public String getOrderMode() {
    return orderMode;
  }

  private boolean isLaneRuntimeActive(final WriterLaneId laneId) {
    final Set<Integer> writerNodeIds = activeWriterNodeIds;
    return writerNodeIds.isEmpty() || writerNodeIds.contains(laneId.writerNodeId);
  }

  public void applyRuntimeState(final ConsensusRegionRuntimeState runtimeState) {
    Objects.requireNonNull(runtimeState, "runtimeState");
    this.runtimeVersion = runtimeState.getRuntimeVersion();
    runtimeVersionChangeCount.incrementAndGet();
    LOGGER.info(
        "ConsensusPrefetchingQueue {}: applied runtimeVersion {}",
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
        "ConsensusPrefetchingQueue {}: applied runtimeState={}, preferredWriterNodeId={}",
        this,
        runtimeState,
        runtimeState.getPreferredWriterNodeId());
  }

  public String getPrefetchingQueueId() {
    return brokerId + "_" + topicName;
  }

  public long getSubscriptionUncommittedEventCount() {
    return inFlightEvents.size();
  }

  public long getCurrentCommitId() {
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

  public String getBrokerId() {
    return brokerId;
  }

  public String getTopicName() {
    return topicName;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  /**
   * Returns the subscription lag for this queue: the difference between the current WAL write
   * position and the committed local sequence. A high lag indicates consumers are falling behind.
   */
  public long getLag() {
    final long currentWalIndex = consensusReqReader.getCurrentSearchIndex();
    final long committed =
        commitManager.getCommittedLocalSeq(brokerId, topicName, consensusGroupId);
    return Math.max(0, currentWalIndex - Math.max(committed, 0));
  }

  // ======================== Stringify ========================

  public Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("brokerId", brokerId);
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
    result.put("lag", String.valueOf(getLag()));
    result.put("isClosed", String.valueOf(isClosed));
    result.put("isActive", String.valueOf(isActive));
    result.put("orderMode", orderMode);
    result.put("preferredWriterNodeId", String.valueOf(preferredWriterNodeId));
    result.put("activeWriterCount", String.valueOf(activeWriterNodeIds.size()));
    result.put("runtimeActiveWriterCount", String.valueOf(runtimeActiveWriterNodeIds.size()));
    result.put("historicalLaneEntryCount", String.valueOf(historicalBufferedEntryCount.get()));
    result.put("lastReleasedPhysicalTime", String.valueOf(lastReleasedPhysicalTime));
    result.put("lastReleasedLocalSeq", String.valueOf(lastReleasedLocalSeq));
    result.put("lastHistoricalWriterNodeId", String.valueOf(lastHistoricalWriterNodeId));
    result.put("lastHistoricalWriterEpoch", String.valueOf(lastHistoricalWriterEpoch));
    result.put(
        "searchIndexReanchorPendingAfterHistoricalCatchUp",
        String.valueOf(searchIndexReanchorPendingAfterHistoricalCatchUp));
    result.put("recoveryWriterCount", String.valueOf(recoveryWriterProgressByWriter.size()));
    result.put("writerLaneCount", String.valueOf(writerLanes.size()));
    result.put("realtimeLaneCount", String.valueOf(realtimeEntriesByLane.size()));
    return result;
  }

  @Override
  public String toString() {
    return "ConsensusPrefetchingQueue" + coreReportMessage();
  }

  // ======================== Inner Classes ========================

  private interface LaneBufferedEntry {
    List<Tablet> getTablets();

    long getSearchIndex();

    long getPhysicalTime();

    int getWriterNodeId();

    long getWriterEpoch();

    long getLocalSeq();

    OrderingKey getOrderingKey();
  }

  private static final class DeliveryBatchState {

    private final List<Tablet> tablets = new ArrayList<>();
    private long startSearchIndex;
    private long endSearchIndex;
    private long estimatedBytes;
    private long firstTabletTimeMs;
    private long physicalTime;
    private long lastLocalSeq;
    private int writerNodeId;
    private long writerEpoch;
    private int entryCount;

    private DeliveryBatchState(final long startSearchIndex) {
      reset(startSearchIndex);
    }

    private boolean isEmpty() {
      return tablets.isEmpty();
    }

    private void append(
        final LaneBufferedEntry entry,
        final long entryEstimatedBytes,
        final boolean trackLingerTime) {
      if (tablets.isEmpty()) {
        startSearchIndex = entry.getSearchIndex();
        if (trackLingerTime) {
          firstTabletTimeMs = System.currentTimeMillis();
        }
        writerNodeId = entry.getWriterNodeId();
        writerEpoch = entry.getWriterEpoch();
      }
      tablets.addAll(entry.getTablets());
      endSearchIndex = entry.getSearchIndex();
      estimatedBytes += entryEstimatedBytes;
      physicalTime = entry.getPhysicalTime();
      lastLocalSeq = entry.getLocalSeq();
      writerNodeId = entry.getWriterNodeId();
      writerEpoch = entry.getWriterEpoch();
      entryCount++;
    }

    private void reset(final long nextStartSearchIndex) {
      tablets.clear();
      startSearchIndex = nextStartSearchIndex;
      endSearchIndex = nextStartSearchIndex;
      estimatedBytes = 0L;
      firstTabletTimeMs = 0L;
      physicalTime = 0L;
      lastLocalSeq = -1L;
      writerNodeId = -1;
      writerEpoch = 0L;
      entryCount = 0;
    }
  }

  private static final class WriterLaneId {
    private final int writerNodeId;
    private final long writerEpoch;

    private WriterLaneId(final int writerNodeId, final long writerEpoch) {
      this.writerNodeId = writerNodeId;
      this.writerEpoch = writerEpoch;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof WriterLaneId)) {
        return false;
      }
      final WriterLaneId that = (WriterLaneId) obj;
      return writerNodeId == that.writerNodeId && writerEpoch == that.writerEpoch;
    }

    @Override
    public int hashCode() {
      return Objects.hash(writerNodeId, writerEpoch);
    }
  }

  private static final class WriterLaneState {
    private long effectiveSafePt = 0L;
    private boolean closed = false;
  }

  private static final class PreparedEntry implements LaneBufferedEntry {
    private final List<Tablet> tablets;
    private final long searchIndex;
    private final long physicalTime;
    private final int writerNodeId;
    private final long writerEpoch;
    private final long localSeq;

    private PreparedEntry(
        final List<Tablet> tablets,
        final long searchIndex,
        final long physicalTime,
        final int writerNodeId,
        final long writerEpoch,
        final long localSeq) {
      this.tablets = tablets;
      this.searchIndex = searchIndex;
      this.physicalTime = physicalTime;
      this.writerNodeId = writerNodeId;
      this.writerEpoch = writerEpoch;
      this.localSeq = localSeq;
    }

    @Override
    public List<Tablet> getTablets() {
      return tablets;
    }

    @Override
    public long getSearchIndex() {
      return searchIndex;
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
    public long getWriterEpoch() {
      return writerEpoch;
    }

    @Override
    public long getLocalSeq() {
      return localSeq;
    }

    @Override
    public OrderingKey getOrderingKey() {
      return new OrderingKey(physicalTime, writerNodeId, writerEpoch, localSeq);
    }
  }

  private static final class LaneFrontier implements Comparable<LaneFrontier> {
    private final WriterLaneId laneId;
    private final OrderingKey orderingKey;
    private final boolean isBarrier;

    private LaneFrontier(
        final WriterLaneId laneId, final OrderingKey orderingKey, final boolean isBarrier) {
      this.laneId = laneId;
      this.orderingKey = orderingKey;
      this.isBarrier = isBarrier;
    }

    private static LaneFrontier forHead(final WriterLaneId laneId, final LaneBufferedEntry entry) {
      return new LaneFrontier(laneId, entry.getOrderingKey(), false);
    }

    private static LaneFrontier forBarrier(final WriterLaneId laneId, final long effectiveSafePt) {
      return new LaneFrontier(
          laneId,
          new OrderingKey(effectiveSafePt, Integer.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE),
          true);
    }

    @Override
    public int compareTo(final LaneFrontier other) {
      int cmp = orderingKey.compareTo(other.orderingKey);
      if (cmp != 0) {
        return cmp;
      }
      if (isBarrier != other.isBarrier) {
        return isBarrier ? -1 : 1;
      }
      cmp = Integer.compare(laneId.writerNodeId, other.laneId.writerNodeId);
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(laneId.writerEpoch, other.laneId.writerEpoch);
    }
  }

  /** Composite ordering key (physicalTime, nodeId, writerEpoch, localSeq) for lane ordering. */
  static final class OrderingKey implements Comparable<OrderingKey> {
    final long physicalTime;
    final int nodeId;
    final long writerEpoch;
    final long localSeq;

    OrderingKey(
        final long physicalTime, final int nodeId, final long writerEpoch, final long localSeq) {
      this.physicalTime = physicalTime;
      this.nodeId = nodeId;
      this.writerEpoch = writerEpoch;
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
      cmp = Long.compare(writerEpoch, o.writerEpoch);
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
          && writerEpoch == that.writerEpoch
          && localSeq == that.localSeq;
    }

    @Override
    public int hashCode() {
      return Objects.hash(physicalTime, nodeId, writerEpoch, localSeq);
    }

    @Override
    public String toString() {
      return "(" + physicalTime + "," + nodeId + "," + writerEpoch + "," + localSeq + ")";
    }
  }

  /** Buffered historical lane entry holding pre-converted tablets keyed by ordering position. */
  private static final class SortableEntry implements LaneBufferedEntry {
    final OrderingKey key;
    final List<Tablet> tablets;
    final long searchIndex;
    final long physicalTime;
    final int nodeId;
    final long writerEpoch;
    final long insertTimestamp;

    SortableEntry(
        final OrderingKey key,
        final List<Tablet> tablets,
        final long searchIndex,
        final long physicalTime,
        final int nodeId,
        final long writerEpoch) {
      this.key = key;
      this.tablets = tablets;
      this.searchIndex = searchIndex;
      this.physicalTime = physicalTime;
      this.nodeId = nodeId;
      this.writerEpoch = writerEpoch;
      this.insertTimestamp = System.currentTimeMillis();
    }

    @Override
    public List<Tablet> getTablets() {
      return tablets;
    }

    @Override
    public long getSearchIndex() {
      return searchIndex;
    }

    @Override
    public long getPhysicalTime() {
      return physicalTime;
    }

    @Override
    public int getWriterNodeId() {
      return nodeId;
    }

    @Override
    public long getWriterEpoch() {
      return writerEpoch;
    }

    @Override
    public long getLocalSeq() {
      return key.localSeq;
    }

    @Override
    public OrderingKey getOrderingKey() {
      return key;
    }
  }
}
