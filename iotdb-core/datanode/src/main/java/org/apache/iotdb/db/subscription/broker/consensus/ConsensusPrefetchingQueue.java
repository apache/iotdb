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
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.metric.ConsensusSubscriptionPrefetchingQueueMetrics;
import org.apache.iotdb.rpc.subscription.payload.poll.EpochChangePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.WatermarkPayload;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
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

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

/**
 * A prefetching queue that reads data from IoTConsensus using a hybrid approach:
 *
 * <ol>
 *   <li><b>In-memory pending queue</b>: Registered with {@link IoTConsensusServerImpl}, receives
 *       {@link IndexedConsensusRequest} in real-time from the write path (same mechanism as
 *       LogDispatcher). This avoids waiting for WAL flush to disk.
 *   <li><b>WAL fallback</b>: Uses {@link ConsensusReqReader.ReqIterator} to read from WAL files for
 *       gap-filling (pending queue overflow) or catch-up scenarios.
 * </ol>
 *
 * <p>WAL retention is size-based (mirrors Kafka's log retention policy): the WAL is preserved while
 * its total size is within the configured {@code subscriptionConsensusWalRetentionSizeInBytes}
 * limit. Once the limit is exceeded, WAL segments may be deleted regardless of consumer progress.
 * Consumers that fall too far behind may receive a gap-detection error and need to reset. This is
 * intentional — pinning the WAL indefinitely for slow consumers would risk unbounded disk growth,
 * consistent with how Kafka handles consumer lag.
 *
 * <p>A background prefetch thread continuously drains the pending queue, converts InsertNode
 * entries to Tablets via {@link ConsensusLogToTabletConverter}, and enqueues {@link
 * SubscriptionEvent} objects into the prefetchingQueue for consumer polling.
 *
 * <p>This design mirrors LogDispatcher's dual-path (pendingEntries + WAL reader) but targets
 * subscription delivery instead of replication.
 *
 * <p>Thread safety: Uses a fair {@link ReentrantReadWriteLock} to ensure mutual exclusion between
 * cleanup and other operations (poll, ack, nack), consistent with the existing prefetching queue
 * design.
 */
public class ConsensusPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusPrefetchingQueue.class);

  private final String brokerId; // consumer group id
  private final String topicName;
  private final ConsensusGroupId consensusGroupId;

  private final IoTConsensusServerImpl serverImpl;

  private final ConsensusReqReader consensusReqReader;

  private volatile ConsensusReqReader.ReqIterator reqIterator;

  /**
   * In-memory pending queue registered with {@link IoTConsensusServerImpl#write}. Receives
   * IndexedConsensusRequest in real-time without waiting for WAL flush. Capacity is bounded to
   * apply back-pressure; overflows are filled from WAL.
   */
  private final BlockingQueue<IndexedConsensusRequest> pendingEntries;

  private static final int PENDING_QUEUE_CAPACITY = 4096;

  private final ConsensusLogToTabletConverter converter;

  private final ConsensusSubscriptionCommitManager commitManager;

  /**
   * Seek generation counter (fencing token). Incremented on each seek operation. Any commit context
   * with a different seekGeneration is considered outdated. This replaces the old commitId-based
   * threshold mechanism, providing per-queue fencing without a shared generator.
   */
  private final AtomicLong seekGeneration;

  private final AtomicLong nextExpectedSearchIndex;

  private final PriorityBlockingQueue<SubscriptionEvent> prefetchingQueue;

  /**
   * Tracks in-flight events that have been polled but not yet committed. Key: (consumerId,
   * commitContext) -> event.
   */
  private final Map<Pair<String, SubscriptionCommitContext>, SubscriptionEvent> inFlightEvents;

  private static final int MAX_PREFETCHING_QUEUE_SIZE =
      SubscriptionConfig.getInstance().getSubscriptionConsensusPrefetchingQueueCapacity();

  /** Counter of WAL gap entries that could not be filled (data loss). */
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
   * timestamp→offset mappings, making it immune to out-of-order producer timestamps.
   */
  private final NavigableMap<Long, Long> intervalMaxTimestampIndex = new ConcurrentSkipListMap<>();

  private static final int INTERVAL_SIZE = 100;

  /** Tracks the current interval being built during prefetch. */
  private long currentIntervalStart = -1;

  private long currentIntervalMaxTimestamp = Long.MIN_VALUE;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private volatile boolean isClosed = false;

  // ======================== Epoch Ordering ========================

  /**
   * Epoch counter for this queue. Incremented when the preferred writer for this consensus group
   * changes. Attached to each message's {@link SubscriptionCommitContext} so the client-side {@code
   * EpochOrderingProcessor} can reorder across leader transitions.
   */
  private volatile long epoch = 0;

  /** Counter of epoch changes (setEpoch + injectEpochSentinel calls) for monitoring. */
  private final AtomicLong epochChangeCount = new AtomicLong(0);

  // ======================== Watermark ========================

  /** Maximum data timestamp observed across all InsertNodes processed by this queue. */
  private volatile long maxObservedTimestamp = Long.MIN_VALUE;

  /** Wall-clock time (ms) of last watermark injection. 0 means never injected. */
  private volatile long lastWatermarkEmitTimeMs = 0;

  private final Thread prefetchThread;

  public ConsensusPrefetchingQueue(
      final String brokerId,
      final String topicName,
      final ConsensusGroupId consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final long startSearchIndex) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.consensusGroupId = consensusGroupId;
    this.serverImpl = serverImpl;
    this.consensusReqReader = serverImpl.getConsensusReqReader();
    this.converter = converter;
    this.commitManager = commitManager;

    this.seekGeneration = new AtomicLong(0);
    this.nextExpectedSearchIndex = new AtomicLong(startSearchIndex);
    this.reqIterator = consensusReqReader.getReqIterator(startSearchIndex);

    this.prefetchingQueue = new PriorityBlockingQueue<>();
    this.inFlightEvents = new ConcurrentHashMap<>();

    // Create and register the in-memory pending queue with IoTConsensusServerImpl.
    this.pendingEntries = new ArrayBlockingQueue<>(PENDING_QUEUE_CAPACITY);
    serverImpl.registerSubscriptionQueue(pendingEntries);

    // Start background prefetch thread
    this.prefetchThread =
        new Thread(this::prefetchLoop, "ConsensusPrefetch-" + brokerId + "-" + topicName);
    this.prefetchThread.setDaemon(true);
    this.prefetchThread.start();

    LOGGER.info(
        "ConsensusPrefetchingQueue created: brokerId={}, topicName={}, consensusGroupId={}, "
            + "startSearchIndex={}",
        brokerId,
        topicName,
        consensusGroupId,
        startSearchIndex);

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
    acquireReadLock();
    try {
      return isClosed ? null : pollInternal(consumerId);
    } finally {
      releaseReadLock();
    }
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

        // Sentinel/metadata events (EPOCH_CHANGE, WATERMARK) are fire-and-forget:
        // skip inFlightEvents tracking so they are not recycled and re-delivered indefinitely.
        if (event.getCurrentResponse().getResponseType()
                == SubscriptionPollResponseType.EPOCH_CHANGE.getType()
            || event.getCurrentResponse().getResponseType()
                == SubscriptionPollResponseType.WATERMARK.getType()) {
          return event;
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

  /**
   * Background prefetch loop. Continuously drains from pendingEntries (in-memory, real-time),
   * detects gaps and fills from WAL reader, converts to Tablets, and enqueues SubscriptionEvents.
   *
   * <p>Batching strategy (linger): Tablets are accumulated across loop iterations until one of
   * three thresholds is met:
   *
   * <ul>
   *   <li>Tablet count exceeds {@code subscriptionConsensusBatchMaxTabletCount}
   *   <li>Estimated byte size exceeds {@code subscriptionConsensusBatchMaxSizeInBytes}
   *   <li>Time since first tablet in current batch exceeds {@code
   *       subscriptionConsensusBatchMaxDelayInMs}
   * </ul>
   */
  private void prefetchLoop() {
    LOGGER.info("ConsensusPrefetchingQueue {}: prefetch thread started", this);

    final List<Tablet> lingerTablets = new ArrayList<>();
    long lingerEstimatedBytes = 0;
    long lingerBatchStartSearchIndex = nextExpectedSearchIndex.get();
    long lingerBatchEndSearchIndex = lingerBatchStartSearchIndex;
    long lingerFirstTabletTimeMs = 0; // 0 means no tablets accumulated yet

    try {
      while (!isClosed && !Thread.currentThread().isInterrupted()) {
        try {
          // Back-pressure: wait if prefetchingQueue is full
          if (prefetchingQueue.size() >= MAX_PREFETCHING_QUEUE_SIZE) {
            Thread.sleep(50);
            continue;
          }

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

            // Accumulate tablets from pending entries into linger buffer
            final int tabletsBefore = lingerTablets.size();
            lingerBatchEndSearchIndex =
                accumulateFromPending(batch, lingerTablets, lingerBatchEndSearchIndex);

            // Update byte estimates for newly added tablets
            for (int i = tabletsBefore; i < lingerTablets.size(); i++) {
              lingerEstimatedBytes += estimateTabletSize(lingerTablets.get(i));
            }

            // Flush sub-batches that exceeded thresholds during accumulation
            while (lingerTablets.size() >= maxTablets || lingerEstimatedBytes >= maxBatchBytes) {
              final int flushCount = Math.min(lingerTablets.size(), maxTablets);
              final List<Tablet> toFlush = new ArrayList<>(lingerTablets.subList(0, flushCount));
              createAndEnqueueEvent(
                  toFlush, lingerBatchStartSearchIndex, lingerBatchEndSearchIndex);
              lingerTablets.subList(0, flushCount).clear();
              // Recalculate byte estimate for remaining tablets
              lingerEstimatedBytes = 0;
              for (final Tablet t : lingerTablets) {
                lingerEstimatedBytes += estimateTabletSize(t);
              }
              lingerBatchStartSearchIndex = nextExpectedSearchIndex.get();
              lingerFirstTabletTimeMs = lingerTablets.isEmpty() ? 0 : lingerFirstTabletTimeMs;
            }

            // Record first tablet time if we just started accumulating
            if (!lingerTablets.isEmpty() && lingerFirstTabletTimeMs == 0) {
              lingerFirstTabletTimeMs = System.currentTimeMillis();
            }
          } else if (lingerTablets.isEmpty()) {
            // Pending queue was empty and no lingering tablets — try catch-up from WAL
            tryCatchUpFromWAL();
            // Idle watermark: even without new data, periodically emit watermark
            maybeInjectWatermark();
          }
          // If we have lingering tablets but pending was empty, fall through to time check below

          // Time-based flush: if tablets have been lingering longer than batchMaxDelayMs, flush now
          if (!lingerTablets.isEmpty()
              && lingerFirstTabletTimeMs > 0
              && (System.currentTimeMillis() - lingerFirstTabletTimeMs) >= batchMaxDelayMs) {
            LOGGER.debug(
                "ConsensusPrefetchingQueue {}: time-based flush, {} tablets lingered for {}ms "
                    + "(threshold={}ms)",
                this,
                lingerTablets.size(),
                System.currentTimeMillis() - lingerFirstTabletTimeMs,
                batchMaxDelayMs);
            createAndEnqueueEvent(
                new ArrayList<>(lingerTablets),
                lingerBatchStartSearchIndex,
                lingerBatchEndSearchIndex);
            lingerTablets.clear();
            lingerEstimatedBytes = 0;
            lingerBatchStartSearchIndex = nextExpectedSearchIndex.get();
            lingerFirstTabletTimeMs = 0;
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

      if (!lingerTablets.isEmpty()) {
        LOGGER.info(
            "ConsensusPrefetchingQueue {}: flushing {} lingering tablets on loop exit",
            this,
            lingerTablets.size());
        createAndEnqueueEvent(
            lingerTablets, lingerBatchStartSearchIndex, lingerBatchEndSearchIndex);
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
   * filling from WAL. Does NOT flush — the caller is responsible for flush decisions.
   *
   * @return the updated batchEndSearchIndex
   */
  private long accumulateFromPending(
      final List<IndexedConsensusRequest> batch,
      final List<Tablet> lingerTablets,
      long batchEndSearchIndex) {

    int processedCount = 0;
    int skippedCount = 0;

    for (final IndexedConsensusRequest request : batch) {
      final long searchIndex = request.getSearchIndex();

      // Detect gap: if searchIndex > nextExpected, entries were dropped from pending queue.
      final long expected = nextExpectedSearchIndex.get();
      if (searchIndex > expected) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: gap detected, expected={}, got={}. "
                + "Filling {} entries from WAL.",
            this,
            expected,
            searchIndex,
            searchIndex - expected);
        final long gapMaxIndex = fillGapFromWAL(expected, searchIndex, lingerTablets);
        if (gapMaxIndex > batchEndSearchIndex) {
          batchEndSearchIndex = gapMaxIndex;
        }
      }

      if (searchIndex < nextExpectedSearchIndex.get()) {
        skippedCount++;
        continue;
      }

      // Process this entry
      final InsertNode insertNode = deserializeToInsertNode(request);
      if (insertNode != null) {
        recordTimestampSample(insertNode, searchIndex);
        // Track maximum data timestamp for watermark propagation
        final long maxTs = extractMaxTime(insertNode);
        if (maxTs > maxObservedTimestamp) {
          maxObservedTimestamp = maxTs;
        }
        final List<Tablet> tablets = converter.convert(insertNode);
        if (!tablets.isEmpty()) {
          lingerTablets.addAll(tablets);
          batchEndSearchIndex = searchIndex;
          processedCount++;
        }
      }
      nextExpectedSearchIndex.set(searchIndex + 1);
    }

    // Update WAL reader position to stay in sync
    syncReqIteratorPosition();

    LOGGER.debug(
        "ConsensusPrefetchingQueue {}: accumulate complete, batchSize={}, processed={}, "
            + "skipped={}, lingerTablets={}, nextExpected={}",
        this,
        batch.size(),
        processedCount,
        skippedCount,
        lingerTablets.size(),
        nextExpectedSearchIndex.get());

    return batchEndSearchIndex;
  }

  /**
   * Fills a gap in the pending queue by reading entries from WAL. Called when gap is detected
   * between nextExpectedSearchIndex and an incoming entry's searchIndex.
   *
   * @return the maximum searchIndex processed during gap filling, or -1 if no entries processed
   */
  private long fillGapFromWAL(
      final long fromIndex, final long toIndex, final List<Tablet> batchedTablets) {
    // Re-position WAL reader to the gap start
    reqIterator = consensusReqReader.getReqIterator(fromIndex);
    long maxProcessedIndex = -1;

    while (nextExpectedSearchIndex.get() < toIndex && reqIterator.hasNext()) {
      try {
        final IndexedConsensusRequest walEntry = reqIterator.next();
        final long walIndex = walEntry.getSearchIndex();
        if (walIndex < nextExpectedSearchIndex.get()) {
          continue; // already processed
        }

        final InsertNode insertNode = deserializeToInsertNode(walEntry);
        if (insertNode != null) {
          recordTimestampSample(insertNode, walIndex);
          final long maxTs = extractMaxTime(insertNode);
          if (maxTs > maxObservedTimestamp) {
            maxObservedTimestamp = maxTs;
          }
          final List<Tablet> tablets = converter.convert(insertNode);
          batchedTablets.addAll(tablets);
        }
        nextExpectedSearchIndex.set(walIndex + 1);
        if (walIndex > maxProcessedIndex) {
          maxProcessedIndex = walIndex;
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: error filling gap from WAL at index {}",
            this,
            nextExpectedSearchIndex.get(),
            e);
        break;
      }
    }

    // If WAL doesn't have the gap entries yet (still in memory buffer), wait briefly
    if (nextExpectedSearchIndex.get() < toIndex) {
      try {
        reqIterator.waitForNextReady(WAL_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        while (nextExpectedSearchIndex.get() < toIndex && reqIterator.hasNext()) {
          final IndexedConsensusRequest walEntry = reqIterator.next();
          final long walIndex = walEntry.getSearchIndex();
          if (walIndex < nextExpectedSearchIndex.get()) {
            continue;
          }
          final InsertNode insertNode = deserializeToInsertNode(walEntry);
          if (insertNode != null) {
            recordTimestampSample(insertNode, walIndex);
            final long maxTs = extractMaxTime(insertNode);
            if (maxTs > maxObservedTimestamp) {
              maxObservedTimestamp = maxTs;
            }
            final List<Tablet> tablets = converter.convert(insertNode);
            batchedTablets.addAll(tablets);
          }
          nextExpectedSearchIndex.set(walIndex + 1);
          if (walIndex > maxProcessedIndex) {
            maxProcessedIndex = walIndex;
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final TimeoutException e) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: timeout waiting for WAL gap fill [{}, {})",
            this,
            nextExpectedSearchIndex.get(),
            toIndex);
      }
    }

    // If entries are in the current-writing WAL file (excluded by PlanNodeIterator for
    // concurrency safety), trigger a WAL file roll to make them readable.
    if (nextExpectedSearchIndex.get() < toIndex && consensusReqReader instanceof WALNode) {
      final long currentWALIndex = consensusReqReader.getCurrentSearchIndex();
      if (nextExpectedSearchIndex.get() <= currentWALIndex) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: gap fill incomplete (at {} vs WAL {}), "
                + "triggering WAL file roll",
            this,
            nextExpectedSearchIndex.get(),
            currentWALIndex);
        ((WALNode) consensusReqReader).rollWALFile();
        syncReqIteratorPosition();
        // Retry reading after roll
        while (nextExpectedSearchIndex.get() < toIndex && reqIterator.hasNext()) {
          try {
            final IndexedConsensusRequest walEntry = reqIterator.next();
            final long walIndex = walEntry.getSearchIndex();
            if (walIndex < nextExpectedSearchIndex.get()) {
              continue;
            }
            final InsertNode insertNode = deserializeToInsertNode(walEntry);
            if (insertNode != null) {
              recordTimestampSample(insertNode, walIndex);
              final long maxTs = extractMaxTime(insertNode);
              if (maxTs > maxObservedTimestamp) {
                maxObservedTimestamp = maxTs;
              }
              final List<Tablet> tablets = converter.convert(insertNode);
              batchedTablets.addAll(tablets);
            }
            nextExpectedSearchIndex.set(walIndex + 1);
            if (walIndex > maxProcessedIndex) {
              maxProcessedIndex = walIndex;
            }
          } catch (final Exception e) {
            LOGGER.warn(
                "ConsensusPrefetchingQueue {}: error reading WAL after roll at index {}",
                this,
                nextExpectedSearchIndex.get(),
                e);
            break;
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

    return maxProcessedIndex;
  }

  /**
   * Try catch-up from WAL when the pending queue was empty. This handles cold-start or scenarios
   * where the subscription started after data was already written.
   */
  private void tryCatchUpFromWAL() {
    // Re-position WAL reader
    syncReqIteratorPosition();

    if (!reqIterator.hasNext()) {
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
        syncReqIteratorPosition();
      }
      if (!reqIterator.hasNext()) {
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
          syncReqIteratorPosition();
        }
        if (!reqIterator.hasNext()) {
          return;
        }
      }
    }

    final SubscriptionConfig config = SubscriptionConfig.getInstance();
    final int maxTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
    final long maxBatchBytes = config.getSubscriptionConsensusBatchMaxSizeInBytes();
    final int maxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();

    final List<Tablet> batchedTablets = new ArrayList<>();
    long batchStartSearchIndex = nextExpectedSearchIndex.get();
    long batchEndSearchIndex = batchStartSearchIndex;
    long estimatedBatchBytes = 0;
    int entriesRead = 0;

    while (entriesRead < maxWalEntries
        && reqIterator.hasNext()
        && prefetchingQueue.size() < MAX_PREFETCHING_QUEUE_SIZE) {
      try {
        final IndexedConsensusRequest walEntry = reqIterator.next();
        final long walIndex = walEntry.getSearchIndex();
        entriesRead++;

        if (walIndex < nextExpectedSearchIndex.get()) {
          continue;
        }

        final InsertNode insertNode = deserializeToInsertNode(walEntry);
        if (insertNode != null) {
          recordTimestampSample(insertNode, walIndex);
          final long maxTs = extractMaxTime(insertNode);
          if (maxTs > maxObservedTimestamp) {
            maxObservedTimestamp = maxTs;
          }
          final List<Tablet> tablets = converter.convert(insertNode);
          if (!tablets.isEmpty()) {
            batchedTablets.addAll(tablets);
            for (final Tablet t : tablets) {
              estimatedBatchBytes += estimateTabletSize(t);
            }
            batchEndSearchIndex = walIndex;
          }
        }
        nextExpectedSearchIndex.set(walIndex + 1);

        if (batchedTablets.size() >= maxTablets || estimatedBatchBytes >= maxBatchBytes) {
          createAndEnqueueEvent(
              new ArrayList<>(batchedTablets), batchStartSearchIndex, batchEndSearchIndex);
          batchedTablets.clear();
          estimatedBatchBytes = 0;
          // Reset start index for the next sub-batch
          batchStartSearchIndex = nextExpectedSearchIndex.get();
        }
      } catch (final Exception e) {
        LOGGER.warn("ConsensusPrefetchingQueue {}: error reading WAL for catch-up", this, e);
        break;
      }
    }

    if (!batchedTablets.isEmpty()) {
      createAndEnqueueEvent(batchedTablets, batchStartSearchIndex, batchEndSearchIndex);
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
  private void syncReqIteratorPosition() {
    reqIterator = consensusReqReader.getReqIterator(nextExpectedSearchIndex.get());
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
        ((SearchNode) planNode).setSearchIndex(indexedRequest.getSearchIndex());
        searchNodes.add((SearchNode) planNode);
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

  private static long estimateTabletSize(final Tablet tablet) {
    return PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
  }

  private void createAndEnqueueEvent(
      final List<Tablet> tablets, final long startSearchIndex, final long endSearchIndex) {
    if (tablets.isEmpty()) {
      return;
    }

    // endSearchIndex IS the event identity — no intermediate commitId mapping needed
    commitManager.recordMapping(brokerId, topicName, consensusGroupId, endSearchIndex);

    final SubscriptionCommitContext commitContext =
        new SubscriptionCommitContext(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            PipeDataNodeAgent.runtime().getRebootTimes(),
            topicName,
            brokerId,
            endSearchIndex,
            seekGeneration.get(),
            consensusGroupId.toString(),
            epoch);

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

    // After enqueuing the data event, no automatic sentinel injection in 方案B.
    // Sentinel injection is triggered externally by ConsensusSubscriptionSetupHandler.
  }

  /**
   * Injects an {@link SubscriptionPollResponseType#EPOCH_CHANGE} sentinel into the prefetching
   * queue. Called by the broker when this node loses preferred-writer status for the consensus
   * group. The sentinel signals the client that the ending epoch's data is complete.
   *
   * @param endingEpoch the epoch number that is ending
   */
  public void injectEpochSentinel(final long endingEpoch) {
    // Sentinels are fire-and-forget (not in inFlightEvents), use INVALID_COMMIT_ID
    final SubscriptionCommitContext sentinelCtx =
        new SubscriptionCommitContext(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            PipeDataNodeAgent.runtime().getRebootTimes(),
            topicName,
            brokerId,
            INVALID_COMMIT_ID,
            seekGeneration.get(),
            consensusGroupId.toString(),
            endingEpoch);
    final SubscriptionEvent sentinel =
        new SubscriptionEvent(
            SubscriptionPollResponseType.EPOCH_CHANGE.getType(),
            new EpochChangePayload(endingEpoch),
            sentinelCtx);
    prefetchingQueue.add(sentinel);
    epochChangeCount.incrementAndGet();

    LOGGER.info(
        "ConsensusPrefetchingQueue {}: injected EPOCH_CHANGE sentinel, endingEpoch={}",
        this,
        endingEpoch);
  }

  // ======================== Commit (Ack/Nack) ========================

  public boolean ack(final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      return !isClosed && ackInternal(consumerId, commitContext);
    } finally {
      releaseReadLock();
    }
  }

  private boolean ackInternal(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean acked = new AtomicBoolean(false);
    final long endSearchIndex = commitContext.getCommitId();
    inFlightEvents.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            LOGGER.warn(
                "ConsensusPrefetchingQueue {}: commit context {} does not exist for ack",
                this,
                commitContext);
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

    if (acked.get()) {
      commitManager.commit(brokerId, topicName, consensusGroupId, endSearchIndex);
    }

    return acked.get();
  }

  public boolean nack(final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      return !isClosed && nackInternal(consumerId, commitContext);
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
      if (isClosed) {
        return false;
      }
      final AtomicBoolean acked = new AtomicBoolean(false);
      final long endSearchIndex = commitContext.getCommitId();
      inFlightEvents.compute(
          new Pair<>(consumerId, commitContext),
          (key, ev) -> {
            if (Objects.isNull(ev)) {
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
      if (acked.get()) {
        commitManager.commit(brokerId, topicName, consensusGroupId, endSearchIndex);
      }
      return acked.get();
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Silent version of nack: returns false without logging if the commit context is not found. Used
   * in multi-region iteration where only one queue owns the event.
   */
  public boolean nackSilent(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      if (isClosed) {
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

      // 3.5. Keep timestamp interval index across seek operations.
      // This preserves historical timestamp->searchIndex hints so a later
      // seekToTimestamp() after seekToEnd/seekToBeginning does not only rely
      // on newly observed post-seek data.

      // 4. Reset WAL read position
      nextExpectedSearchIndex.set(targetSearchIndex);
      reqIterator = consensusReqReader.getReqIterator(targetSearchIndex);

      // 5. Reset commit state in CommitManager
      commitManager.resetState(brokerId, topicName, consensusGroupId, targetSearchIndex);

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
   * Seeks to the earliest available WAL position. The actual position depends on WAL retention — if
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
        // targetTimestamp is beyond the max timestamp of all known intervals — seek to end
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
      // Entering a new interval — flush the previous one
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
      return; // No data observed yet — nothing to report
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
   * Injects a {@link SubscriptionPollResponseType#WATERMARK} event into the prefetching queue.
   * Follows the same pattern as {@link #injectEpochSentinel(long)} — the committed mapping is
   * deliberately NOT recorded because watermark events are metadata, not user data.
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
            epoch);
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

  // ======================== Epoch Control ========================

  /**
   * Called on the <em>old</em> write-leader when routing changes away from this DataNode. Sets the
   * /** Sets the epoch counter. Called on the new write-leader when routing changes.
   */
  public void setEpoch(final long epoch) {
    this.epoch = epoch;
    epochChangeCount.incrementAndGet();
    LOGGER.info("ConsensusPrefetchingQueue {}: epoch set to {}", this, epoch);
  }

  public long getEpoch() {
    return epoch;
  }

  public long getWalGapSkippedEntries() {
    return walGapSkippedEntries.get();
  }

  public long getEpochChangeCount() {
    return epochChangeCount.get();
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
   * position and the committed search index. A high lag indicates consumers are falling behind.
   */
  public long getLag() {
    final long currentWalIndex = consensusReqReader.getCurrentSearchIndex();
    final long committed =
        commitManager.getCommittedSearchIndex(brokerId, topicName, consensusGroupId);
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
    result.put("seekGeneration", String.valueOf(seekGeneration.get()));
    result.put("walGapSkippedEntries", String.valueOf(walGapSkippedEntries.get()));
    result.put("lag", String.valueOf(getLag()));
    result.put("isClosed", String.valueOf(isClosed));
    return result;
  }

  @Override
  public String toString() {
    return "ConsensusPrefetchingQueue" + coreReportMessage();
  }
}
