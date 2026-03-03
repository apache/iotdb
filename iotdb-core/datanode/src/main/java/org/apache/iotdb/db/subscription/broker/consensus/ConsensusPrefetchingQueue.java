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

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

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
 *   <li><b>WAL pinning</b>: Supplies the earliest outstanding (uncommitted) search index to {@link
 *       IoTConsensusServerImpl}, preventing WAL deletion of entries not yet consumed by the
 *       subscription.
 * </ol>
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
  private final String consensusGroupId;

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
   * Cached LongSupplier instance for WAL pinning registration. Must be the SAME object reference
   * for both registerSubscriptionQueue and unregisterSubscriptionQueue, because
   * CopyOnWriteArrayList.remove() uses equals() which defaults to reference equality for lambdas.
   * Using this::method would create a new lambda instance each time, causing remove() to fail and
   * WAL to be pinned indefinitely.
   */
  private final LongSupplier walPinSupplier;

  /** Commit ID generator, monotonically increasing within this queue's lifetime. */
  private final AtomicLong commitIdGenerator;

  /** Records the initial commit ID for outdated event detection. */
  private final long initialCommitId;

  private final AtomicLong nextExpectedSearchIndex;

  private final PriorityBlockingQueue<SubscriptionEvent> prefetchingQueue;

  /**
   * Tracks in-flight events that have been polled but not yet committed. Key: (consumerId,
   * commitContext) -> event.
   */
  private final Map<Pair<String, SubscriptionCommitContext>, SubscriptionEvent> inFlightEvents;

  /**
   * Tracks outstanding (uncommitted) events for WAL pinning. Maps commitId to the startSearchIndex
   * of that event batch. The earliest entry's value is supplied to IoTConsensusServerImpl to pin
   * WAL files from deletion.
   */
  private final ConcurrentSkipListMap<Long, Long> outstandingCommitIdToStartIndex;

  private static final int MAX_TABLETS_PER_EVENT = 64;

  private static final int MAX_WAL_ENTRIES_PER_PREFETCH = 128;

  private static final int MAX_PREFETCHING_QUEUE_SIZE = 256;

  private static final long WAL_RETENTION_WARN_THRESHOLD = 100_000;

  /** Counter of WAL gap entries that could not be filled (data loss). */
  private final AtomicLong walGapSkippedEntries = new AtomicLong(0);

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private volatile boolean isClosed = false;

  /**
   * Background thread that drains pendingEntries and fills prefetchingQueue. TODO: manage thread
   * count
   */
  private final Thread prefetchThread;

  public ConsensusPrefetchingQueue(
      final String brokerId,
      final String topicName,
      final String consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final long startSearchIndex,
      final AtomicLong sharedCommitIdGenerator) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.consensusGroupId = consensusGroupId;
    this.serverImpl = serverImpl;
    this.consensusReqReader = serverImpl.getConsensusReqReader();
    this.converter = converter;
    this.commitManager = commitManager;

    this.commitIdGenerator = sharedCommitIdGenerator;
    this.initialCommitId = commitIdGenerator.get();
    this.nextExpectedSearchIndex = new AtomicLong(startSearchIndex);
    this.reqIterator = consensusReqReader.getReqIterator(startSearchIndex);

    this.prefetchingQueue = new PriorityBlockingQueue<>();
    this.inFlightEvents = new ConcurrentHashMap<>();
    this.outstandingCommitIdToStartIndex = new ConcurrentSkipListMap<>();

    // Create and register the in-memory pending queue with IoTConsensusServerImpl.
    // IMPORTANT: walPinSupplier is stored as a field (not a method reference) to ensure the
    // same object reference is used for both register and unregister.
    this.pendingEntries = new ArrayBlockingQueue<>(PENDING_QUEUE_CAPACITY);
    this.walPinSupplier = this::getEarliestOutstandingSearchIndex;
    serverImpl.registerSubscriptionQueue(pendingEntries, walPinSupplier);

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
  }

  /**
   * Returns the earliest outstanding (uncommitted) search index for WAL pinning. If there are no
   * outstanding events, returns the next expected search index (nothing to pin beyond what we've
   * already processed). Also monitors WAL retention gap for slow consumer detection.
   */
  private long getEarliestOutstandingSearchIndex() {
    final Map.Entry<Long, Long> first = outstandingCommitIdToStartIndex.firstEntry();
    if (first != null) {
      final long earliestIndex = first.getValue();
      // WAL retention health check: warn if outstanding gap grows too large
      final long currentIndex = nextExpectedSearchIndex.get();
      final long retentionGap = currentIndex - earliestIndex;
      if (retentionGap > WAL_RETENTION_WARN_THRESHOLD) {
        LOGGER.error(
            "ConsensusPrefetchingQueue {}: WAL retention gap is {} entries "
                + "(earliest outstanding={}, current={}). "
                + "A slow or stalled consumer is pinning WAL files and may cause disk exhaustion. "
                + "Consider committing events or increasing consumer throughput.",
            this,
            retentionGap,
            earliestIndex,
            currentIndex);
      }
      return earliestIndex;
    }
    return nextExpectedSearchIndex.get();
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
    // Recycle any uncommitted in-flight events for this consumer before serving new data.
    final int recycled = recycleInFlightEventsForConsumer(consumerId);
    if (recycled > 0) {
      LOGGER.debug(
          "ConsensusPrefetchingQueue {}: recycled {} uncommitted in-flight events for "
              + "consumer {} back to prefetching queue",
          this,
          recycled,
          consumerId);
    }

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

  private static final long PENDING_DRAIN_TIMEOUT_MS = 200;

  private static final long WAL_WAIT_TIMEOUT_SECONDS = 2;

  /**
   * Background prefetch loop. Continuously drains from pendingEntries (in-memory, real-time),
   * detects gaps and fills from WAL reader, converts to Tablets, and enqueues SubscriptionEvents.
   */
  private void prefetchLoop() {
    LOGGER.info("ConsensusPrefetchingQueue {}: prefetch thread started", this);
    try {
      while (!isClosed && !Thread.currentThread().isInterrupted()) {
        try {
          // Back-pressure: wait if prefetchingQueue is full
          if (prefetchingQueue.size() >= MAX_PREFETCHING_QUEUE_SIZE) {
            Thread.sleep(50);
            continue;
          }

          // Try to drain from pending entries (in-memory, fast path)
          final List<IndexedConsensusRequest> batch = new ArrayList<>();
          // Block briefly for first entry
          final IndexedConsensusRequest first =
              pendingEntries.poll(PENDING_DRAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (first != null) {
            batch.add(first);
            // Drain more non-blocking
            int drained = 0;
            IndexedConsensusRequest next;
            while (drained < MAX_WAL_ENTRIES_PER_PREFETCH - 1
                && (next = pendingEntries.poll()) != null) {
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
            processBatchFromPending(batch);
          } else {
            // Pending queue was empty - try catch-up from WAL for any gaps
            // (entries may have been dropped due to pending queue overflow)
            tryCatchUpFromWAL();
          }
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

  private void processBatchFromPending(final List<IndexedConsensusRequest> batch) {
    final List<Tablet> batchedTablets = new ArrayList<>();
    long batchStartSearchIndex = nextExpectedSearchIndex.get();
    long batchEndSearchIndex = batchStartSearchIndex;
    int processedCount = 0;
    int skippedCount = 0;
    int nullDeserCount = 0;
    int emptyConvertCount = 0;

    for (final IndexedConsensusRequest request : batch) {
      final long searchIndex = request.getSearchIndex();

      // Detect gap: if searchIndex > nextExpected, entries were dropped from pending queue.
      // Fill the gap from WAL.
      final long expected = nextExpectedSearchIndex.get();
      if (searchIndex > expected) {
        LOGGER.debug(
            "ConsensusPrefetchingQueue {}: gap detected, expected={}, got={}. "
                + "Filling {} entries from WAL.",
            this,
            expected,
            searchIndex,
            searchIndex - expected);
        final long gapMaxIndex = fillGapFromWAL(expected, searchIndex, batchedTablets);
        if (gapMaxIndex > batchEndSearchIndex) {
          batchEndSearchIndex = gapMaxIndex;
        }

        // If gap was not fully filled (e.g., WAL timeout), do NOT skip the gap.
        // Break and defer remaining entries to the next prefetch loop iteration.
        // WAL pin ensures the missing entries won't be deleted.
        if (nextExpectedSearchIndex.get() < searchIndex) {
          LOGGER.warn(
              "ConsensusPrefetchingQueue {}: gap [{}, {}) not fully filled (reached {}). "
                  + "Deferring remaining batch to next prefetch iteration.",
              this,
              expected,
              searchIndex,
              nextExpectedSearchIndex.get());
          break;
        }
      }

      if (searchIndex < nextExpectedSearchIndex.get()) {
        // Already processed (e.g., gap fill covered this entry), skip
        skippedCount++;
        continue;
      }

      // Process this entry
      final InsertNode insertNode = deserializeToInsertNode(request);
      if (insertNode != null) {
        final List<Tablet> tablets = converter.convert(insertNode);
        if (!tablets.isEmpty()) {
          batchedTablets.addAll(tablets);
          batchEndSearchIndex = searchIndex;
          processedCount++;
        } else {
          emptyConvertCount++;
          LOGGER.debug(
              "ConsensusPrefetchingQueue {}: converter returned empty tablets for "
                  + "searchIndex={}, insertNodeType={}, deviceId={}",
              this,
              searchIndex,
              insertNode.getType(),
              ConsensusLogToTabletConverter.safeDeviceIdForLog(insertNode));
        }
      } else {
        nullDeserCount++;
        LOGGER.warn(
            "ConsensusPrefetchingQueue {}: deserializeToInsertNode returned null for "
                + "searchIndex={}, requestType={}",
            this,
            searchIndex,
            request.getRequests().isEmpty()
                ? "EMPTY"
                : request.getRequests().get(0).getClass().getSimpleName());
      }
      nextExpectedSearchIndex.set(searchIndex + 1);

      // Flush batch if large enough
      if (batchedTablets.size() >= MAX_TABLETS_PER_EVENT) {
        createAndEnqueueEvent(
            new ArrayList<>(batchedTablets), batchStartSearchIndex, batchEndSearchIndex);
        batchedTablets.clear();
        // Reset start index for the next sub-batch so that
        // outstandingCommitIdToStartIndex records the correct WAL pin position
        batchStartSearchIndex = nextExpectedSearchIndex.get();
      }
    }

    // Update WAL reader position to stay in sync
    syncReqIteratorPosition();

    // Flush remaining tablets
    if (!batchedTablets.isEmpty()) {
      createAndEnqueueEvent(batchedTablets, batchStartSearchIndex, batchEndSearchIndex);
    }

    LOGGER.debug(
        "ConsensusPrefetchingQueue {}: batch processing complete, "
            + "batchSize={}, processed={}, skipped={}, nullDeser={}, emptyConvert={}, "
            + "tabletsCreated={}, nextExpected={}, prefetchQueueSize={}",
        this,
        batch.size(),
        processedCount,
        skippedCount,
        nullDeserCount,
        emptyConvertCount,
        batchedTablets.size(),
        nextExpectedSearchIndex.get(),
        prefetchingQueue.size());
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

    // If the gap still cannot be fully filled (WAL truncated/deleted), skip ahead to avoid
    // blocking consumption indefinitely. This results in data loss for the skipped range.
    if (nextExpectedSearchIndex.get() < toIndex) {
      final long skipped = toIndex - nextExpectedSearchIndex.get();
      walGapSkippedEntries.addAndGet(skipped);
      LOGGER.error(
          "ConsensusPrefetchingQueue {}: WAL gap [{}, {}) cannot be filled - {} entries lost. "
              + "Total skipped entries so far: {}. This indicates WAL truncation or deletion.",
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
        LOGGER.info(
            "ConsensusPrefetchingQueue {}: subscription behind (at {} vs WAL {}), "
                + "triggering WAL file roll to make entries readable",
            this,
            nextExpectedSearchIndex.get(),
            currentWALIndex);
        ((WALNode) consensusReqReader).rollWALFile();
        syncReqIteratorPosition();
      }
      if (!reqIterator.hasNext()) {
        return;
      }
    }

    final List<Tablet> batchedTablets = new ArrayList<>();
    long batchStartSearchIndex = nextExpectedSearchIndex.get();
    long batchEndSearchIndex = batchStartSearchIndex;
    int entriesRead = 0;

    while (entriesRead < MAX_WAL_ENTRIES_PER_PREFETCH
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
          final List<Tablet> tablets = converter.convert(insertNode);
          if (!tablets.isEmpty()) {
            batchedTablets.addAll(tablets);
            batchEndSearchIndex = walIndex;
          }
        }
        nextExpectedSearchIndex.set(walIndex + 1);

        if (batchedTablets.size() >= MAX_TABLETS_PER_EVENT) {
          createAndEnqueueEvent(
              new ArrayList<>(batchedTablets), batchStartSearchIndex, batchEndSearchIndex);
          batchedTablets.clear();
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

  private void createAndEnqueueEvent(
      final List<Tablet> tablets, final long startSearchIndex, final long endSearchIndex) {
    if (tablets.isEmpty()) {
      return;
    }

    final long commitId = commitIdGenerator.getAndIncrement();

    // Record the mapping from commitId to the end searchIndex
    // so that when the client commits, we know which WAL position has been consumed
    commitManager.recordCommitMapping(
        brokerId, topicName, consensusGroupId, commitId, endSearchIndex);

    // Track outstanding event for WAL pinning
    outstandingCommitIdToStartIndex.put(commitId, startSearchIndex);

    final SubscriptionCommitContext commitContext =
        new SubscriptionCommitContext(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            PipeDataNodeAgent.runtime().getRebootTimes(),
            topicName,
            brokerId,
            commitId);

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
            + "searchIndex range [{}, {}], commitId={}, prefetchQueueSize={}",
        this,
        tablets.size(),
        startSearchIndex,
        endSearchIndex,
        commitId,
        prefetchingQueue.size());
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
    final long commitId = commitContext.getCommitId();
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
      commitManager.commit(brokerId, topicName, consensusGroupId, commitId);
      outstandingCommitIdToStartIndex.remove(commitId);
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
      final long commitId = commitContext.getCommitId();
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
        commitManager.commit(brokerId, topicName, consensusGroupId, commitId);
        outstandingCommitIdToStartIndex.remove(commitId);
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

  /**
   * Maximum number of nack cycles before an in-flight event is kept in place rather than
   * re-enqueued. Prevents infinite re-delivery loops when a consumer repeatedly polls without
   * committing. Beyond this threshold, the event stays in inFlightEvents and will eventually be
   * recycled by the timeout-based {@link #recycleInFlightEvents()} when it becomes pollable.
   */
  private static final long MAX_CONSUMER_RECYCLE_NACK_COUNT = 10;

  /**
   * Recycles uncommitted in-flight events belonging to the given consumer back to the prefetching
   * queue. This provides at-least-once delivery: when a consumer polls again without committing,
   * the previously delivered events are nacked and re-queued for re-delivery.
   *
   * <p>Events that have been nacked more than {@link #MAX_CONSUMER_RECYCLE_NACK_COUNT} times are
   * left in-flight to avoid infinite re-delivery loops. They will be cleaned up by the periodic
   * timeout-based recycler instead.
   *
   * @return the number of events recycled
   */
  private int recycleInFlightEventsForConsumer(final String consumerId) {
    final AtomicInteger count = new AtomicInteger(0);
    for (final Pair<String, SubscriptionCommitContext> key :
        new ArrayList<>(inFlightEvents.keySet())) {
      if (!key.getLeft().equals(consumerId)) {
        continue;
      }
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
            // If the event has been nacked too many times, leave it and let the timeout recycler
            // handle it.
            if (ev.getNackCount() >= MAX_CONSUMER_RECYCLE_NACK_COUNT) {
              LOGGER.warn(
                  "ConsensusPrefetchingQueue {}: event {} for consumer {} exceeded max nack "
                      + "count ({}), skipping recycle to prevent infinite loop",
                  this,
                  ev,
                  consumerId,
                  MAX_CONSUMER_RECYCLE_NACK_COUNT);
              return ev; // keep in inFlightEvents
            }
            ev.nack();
            prefetchingQueue.add(ev);
            count.incrementAndGet();
            LOGGER.debug(
                "ConsensusPrefetchingQueue {}: recycled uncommitted event {} for consumer {} "
                    + "back to prefetching queue",
                this,
                ev,
                consumerId);
            return null;
          });
    }
    return count.get();
  }

  // ======================== Cleanup ========================

  public void cleanUp() {
    acquireWriteLock();
    try {
      prefetchingQueue.forEach(event -> event.cleanUp(true));
      prefetchingQueue.clear();

      inFlightEvents.values().forEach(event -> event.cleanUp(true));
      inFlightEvents.clear();

      outstandingCommitIdToStartIndex.clear();
    } finally {
      releaseWriteLock();
    }
  }

  public void close() {
    markClosed();
    // Stop background prefetch thread
    prefetchThread.interrupt();
    try {
      prefetchThread.join(5000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    try {
      // Unregister from IoTConsensusServerImpl (stop receiving in-memory data, unpin WAL).
      serverImpl.unregisterSubscriptionQueue(pendingEntries, walPinSupplier);
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
        || initialCommitId > commitContext.getCommitId();
  }

  // ======================== Status ========================

  public boolean isClosed() {
    return isClosed;
  }

  public void markClosed() {
    isClosed = true;
  }

  public String getPrefetchingQueueId() {
    return brokerId + "_" + topicName;
  }

  public long getSubscriptionUncommittedEventCount() {
    return inFlightEvents.size();
  }

  public long getCurrentCommitId() {
    return commitIdGenerator.get();
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

  public String getConsensusGroupId() {
    return consensusGroupId;
  }

  // ======================== Stringify ========================

  public Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("brokerId", brokerId);
    result.put("topicName", topicName);
    result.put("consensusGroupId", consensusGroupId);
    result.put("currentReadSearchIndex", String.valueOf(nextExpectedSearchIndex.get()));
    result.put("prefetchingQueueSize", String.valueOf(prefetchingQueue.size()));
    result.put("inFlightEventsSize", String.valueOf(inFlightEvents.size()));
    result.put("outstandingEventsSize", String.valueOf(outstandingCommitIdToStartIndex.size()));
    result.put("pendingEntriesSize", String.valueOf(pendingEntries.size()));
    result.put("commitIdGenerator", commitIdGenerator.toString());
    result.put("walGapSkippedEntries", String.valueOf(walGapSkippedEntries.get()));
    result.put("isClosed", String.valueOf(isClosed));
    return result;
  }

  @Override
  public String toString() {
    return "ConsensusPrefetchingQueue" + coreReportMessage();
  }
}
