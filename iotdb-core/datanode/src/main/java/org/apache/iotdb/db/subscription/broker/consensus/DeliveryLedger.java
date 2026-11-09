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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Owns materialized delivery indexes, in-flight transitions, payload accounting and replay records.
 *
 * <p>The queue keeps the ready queue, WAL materialization, lifecycle read/write lock and retention
 * refresh. The queue also keeps lifecycle fencing at each entry point. Per-key map computations and
 * event monitors retain their existing scopes; enqueue, commit and memory-release callbacks run
 * synchronously inside those scopes.
 */
final class DeliveryLedger {

  // Keep the existing log category and queue identity when moving delivery operations here.
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusPrefetchingQueue.class);

  private final ConsensusPrefetchingQueue owner;
  private final Consumer<SubscriptionEvent> enqueueEvent;
  private final LongConsumer releaseMemory;

  private final Map<InFlightEventKey, SubscriptionEvent> inFlightEvents = new ConcurrentHashMap<>();

  /** Tablet memory retained by queued and in-flight events, keyed by event identity. */
  private final Map<SubscriptionEvent, Long> retainedBytesByEvent = new ConcurrentHashMap<>();

  /** Materialized data events indexed across queued and in-flight lifecycle stages. */
  private final Map<SubscriptionCommitContext, SubscriptionEvent> outstandingEventsByCommitContext =
      new ConcurrentHashMap<>();

  /** Compact batch boundaries and source positions; no payload is retained. */
  private final Map<SubscriptionEvent, DeliveryBatchRecord> deliveryBatches =
      new ConcurrentHashMap<>();

  /**
   * Counts live batches at each earliest replay index. Long.MIN_VALUE protects batches containing
   * replicated entries without a local index. The first key pins WAL until the last owner is
   * cleaned.
   */
  private final ConcurrentSkipListMap<Long, Integer> deliveryStartIndices =
      new ConcurrentSkipListMap<>();

  DeliveryLedger(
      final ConsensusPrefetchingQueue owner,
      final Consumer<SubscriptionEvent> enqueueEvent,
      final LongConsumer releaseMemory) {
    this.owner = owner;
    this.enqueueEvent = enqueueEvent;
    this.releaseMemory = releaseMemory;
  }

  void register(
      final SubscriptionEvent event,
      final long retainedBytes,
      final DeliveryBatchRecord deliveryBatchRecord) {
    outstandingEventsByCommitContext.put(event.getCommitContext(), event);
    if (retainedBytes > 0L) {
      retainedBytesByEvent.put(event, retainedBytes);
    }
    if (Objects.nonNull(deliveryBatchRecord)) {
      deliveryBatches.put(event, deliveryBatchRecord);
      deliveryStartIndices.merge(deliveryBatchRecord.minimumSearchIndex, 1, Integer::sum);
    }
  }

  SubscriptionEvent getOutstanding(final SubscriptionCommitContext commitContext) {
    return outstandingEventsByCommitContext.get(commitContext);
  }

  boolean isOutstanding(
      final SubscriptionCommitContext commitContext, final SubscriptionEvent expectedEvent) {
    return outstandingEventsByCommitContext.get(commitContext) == expectedEvent;
  }

  DeliveryBatchRecord getDeliveryBatch(final SubscriptionEvent event) {
    return deliveryBatches.get(event);
  }

  void retainPayload(final SubscriptionEvent event, final long retainedBytes) {
    retainedBytesByEvent.put(event, retainedBytes);
  }

  /**
   * Releases only the payload owned by this response; replay records keep protecting WAL.
   *
   * @return whether the callback is current, so the caller can preserve its prefetch wakeup
   */
  boolean releaseDeliveredPayload(
      final SubscriptionEvent event,
      final SubscriptionPollResponse response,
      final BooleanSupplier lifecycleTransitionPending) {
    synchronized (event) {
      final DeliveryBatchRecord record = deliveryBatches.get(event);
      if (lifecycleTransitionPending.getAsBoolean()
          || Objects.isNull(record)
          || event.isCommitted()
          || event.getCurrentResponse() != response
          || outstandingEventsByCommitContext.get(event.getCommitContext()) != event) {
        return false;
      }
      // Delivery records protect the WAL needed for replay. Release the serialized payload
      // without a validation scan; if pending WAL is not readable yet, redelivery retries it
      // without advancing committed progress.
      if (event.detachCurrentPayload(response)) {
        final Long retainedBytes = retainedBytesByEvent.remove(event);
        if (Objects.nonNull(retainedBytes)) {
          releaseMemory.accept(retainedBytes);
        }
      }
    }
    return true;
  }

  /** Removes replay protection and releases event memory only for the remaining lifecycle owner. */
  void cleanUp(final SubscriptionEvent event, final boolean force) {
    synchronized (event) {
      final boolean indexed =
          outstandingEventsByCommitContext.remove(event.getCommitContext(), event);
      final Long retainedBytes = retainedBytesByEvent.remove(event);
      final DeliveryBatchRecord batchRecord = deliveryBatches.remove(event);
      if (Objects.nonNull(batchRecord)) {
        deliveryStartIndices.computeIfPresent(
            batchRecord.minimumSearchIndex, (index, count) -> count == 1 ? null : count - 1);
      }
      // A materialized data event can be observed by a regular ACK and a late ACK concurrently.
      // Only the thread that still owns either lifecycle index may clean and release it.
      if (!indexed
          && Objects.isNull(retainedBytes)
          && event.getCommitContext().hasWriterProgress()) {
        return;
      }
      try {
        event.cleanUp(force);
      } finally {
        if (Objects.nonNull(retainedBytes)) {
          releaseMemory.accept(retainedBytes);
        }
      }
    }
  }

  /** Clears residual indexes after queued/in-flight cleanup under the lifecycle write lock. */
  void discardRetainedEventMemory() {
    long retainedBytes = 0L;
    for (final Long eventRetainedBytes : retainedBytesByEvent.values()) {
      retainedBytes += eventRetainedBytes;
    }
    retainedBytesByEvent.clear();
    outstandingEventsByCommitContext.clear();
    deliveryBatches.clear();
    deliveryStartIndices.clear();
    releaseMemory.accept(retainedBytes);
  }

  void cleanUpInFlightEvents() {
    inFlightEvents.values().forEach(event -> cleanUp(event, true));
    inFlightEvents.clear();
  }

  Long firstDeliveryStartIndex() {
    final Map.Entry<Long, Integer> firstDelivery = deliveryStartIndices.firstEntry();
    return Objects.nonNull(firstDelivery) ? firstDelivery.getKey() : null;
  }

  int inFlightSize() {
    return inFlightEvents.size();
  }

  boolean hasInFlightEvents() {
    return !inFlightEvents.isEmpty();
  }

  SubscriptionEvent getInFlight(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    return inFlightEvents.get(new InFlightEventKey(consumerId, commitContext));
  }

  void claim(
      final String consumerId,
      final SubscriptionCommitContext commitContext,
      final SubscriptionEvent event) {
    inFlightEvents.put(new InFlightEventKey(consumerId, commitContext), event);
  }

  boolean refreshLease(final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean refreshed = new AtomicBoolean(false);
    inFlightEvents.compute(
        new InFlightEventKey(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            return null;
          }
          if (ev.isCommitted()) {
            cleanUp(ev, false);
            return null;
          }
          ev.recordLastPolledTimestamp();
          refreshed.set(true);
          return ev;
        });
    return refreshed.get();
  }

  boolean requeue(final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean requeued = new AtomicBoolean(false);
    inFlightEvents.compute(
        new InFlightEventKey(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            return null;
          }
          if (ev.isCommitted()) {
            cleanUp(ev, false);
            return null;
          }
          ev.resetLastPolledTimestamp();
          enqueueEvent.accept(ev);
          requeued.set(true);
          return null;
        });
    return requeued.get();
  }

  int requeueInFlightEvents(
      final String consumerId, final BiPredicate<String, SubscriptionCommitContext> requeue) {
    int requeuedCount = 0;
    for (final InFlightEventKey key : new ArrayList<>(inFlightEvents.keySet())) {
      // Each callback retains the queue's per-event lifecycle lock and context validation.
      if (Objects.equals(consumerId, key.consumerId)
          && requeue.test(consumerId, key.commitContext)) {
        requeuedCount++;
      }
    }
    return requeuedCount;
  }

  /**
   * Commits and cleans up inside the existing per-key computation. The caller refreshes WAL
   * retention after this returns, outside that computation.
   */
  InFlightAckResult ack(
      final String consumerId,
      final SubscriptionCommitContext commitContext,
      final boolean silent,
      final BooleanSupplier commitProgress) {
    final AtomicBoolean found = new AtomicBoolean(false);
    final AtomicBoolean acked = new AtomicBoolean(false);
    inFlightEvents.compute(
        new InFlightEventKey(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            return null;
          }
          found.set(true);

          if (ev.isCommitted()) {
            if (!silent) {
              LOGGER.warn(
                  DataNodePipeMessages
                      .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_EVENT_ALREADY_COMMITTED_AC34E829,
                  owner,
                  commitContext);
            }
            cleanUp(ev, false);
            return null;
          }

          final boolean committed = commitProgress.getAsBoolean();
          if (!committed) {
            if (!silent) {
              LOGGER.warn(
                  DataNodePipeMessages
                      .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_ADVANCE_COMMIT_FRONTIER_56E606C0,
                  owner,
                  commitContext);
            }
            return ev;
          }

          ev.ack();
          ev.recordCommittedTimestamp();
          acked.set(true);
          cleanUp(ev, false);
          return null;
        });
    return !found.get()
        ? InFlightAckResult.MISSING
        : acked.get() ? InFlightAckResult.ACKED : InFlightAckResult.REJECTED;
  }

  boolean nackSilent(final String consumerId, final SubscriptionCommitContext commitContext) {
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
                owner,
                ev.getNackCount(),
                ev);
            ev.ack();
            ev.recordCommittedTimestamp();
            cleanUp(ev, false);
            return null;
          }
          enqueueEvent.accept(ev);
          return null;
        });
    return nacked.get();
  }

  boolean nack(final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean nacked = new AtomicBoolean(false);
    inFlightEvents.compute(
        new InFlightEventKey(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            LOGGER.warn(
                DataNodePipeMessages
                    .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_05F6C6E0,
                owner,
                commitContext);
            return null;
          }

          ev.nack();
          nacked.set(true);
          if (ev.isPoisoned()) {
            LOGGER.error(
                DataNodePipeMessages
                    .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_NACKCOUNT_3A9255FB,
                owner,
                ev.getNackCount(),
                ev);
            ev.ack();
            ev.recordCommittedTimestamp();
            cleanUp(ev, false);
            return null;
          }
          enqueueEvent.accept(ev);
          return null;
        });

    return nacked.get();
  }

  void recycleInFlightEvents(final LongSupplier recycleIntervalMs) {
    for (final InFlightEventKey key : new ArrayList<>(inFlightEvents.keySet())) {
      inFlightEvents.compute(
          key,
          (k, ev) -> {
            if (Objects.isNull(ev)) {
              return null;
            }
            if (ev.isCommitted()) {
              cleanUp(ev, false);
              return null;
            }
            if (ev.pollable(recycleIntervalMs.getAsLong())) {
              ev.nack();
              if (ev.isPoisoned()) {
                LOGGER.error(
                    DataNodePipeMessages
                        .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_DURING_23159F02,
                    owner,
                    ev.getNackCount(),
                    ev);
                ev.ack();
                ev.recordCommittedTimestamp();
                cleanUp(ev, false);
                return null;
              }
              enqueueEvent.accept(ev);
              LOGGER.debug(
                  DataNodePipeMessages
                      .PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RECYCLED_TIMED_OUT_EVENT_BACK_5E58639C,
                  owner,
                  ev);
              return null;
            }
            return ev;
          });
    }
  }

  enum InFlightAckResult {
    ACKED,
    REJECTED,
    MISSING
  }

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

  static final class ReplayEntryPosition {
    final long searchIndex;
    final long physicalTime;
    final long localSeq;

    private ReplayEntryPosition(
        final long searchIndex, final long physicalTime, final long localSeq) {
      this.searchIndex = searchIndex;
      this.physicalTime = physicalTime;
      this.localSeq = localSeq;
    }
  }

  static final class DeliveryBatchRecord {
    // Delta + zigzag varints keep contiguous indices/sequences and timestamp deltas compact.
    // Positions are decoded only while materializing one memory-bounded batch for redelivery.
    private final byte[] positions;
    private final int entryCount;
    final long minimumSearchIndex;
    final int tabletCount;
    final long estimatedBytes;
    final String databaseName;
    final ConsensusLogToTabletConverter converter;

    DeliveryBatchRecord(
        final byte[] positions,
        final int entryCount,
        final long minimumSearchIndex,
        final int tabletCount,
        final long estimatedBytes,
        final String databaseName,
        final ConsensusLogToTabletConverter converter) {
      this.positions = positions;
      this.entryCount = entryCount;
      this.minimumSearchIndex = minimumSearchIndex;
      this.tabletCount = tabletCount;
      this.estimatedBytes = estimatedBytes;
      this.databaseName = databaseName;
      this.converter = converter;
    }

    List<ReplayEntryPosition> decodePositions() {
      final ByteBuffer buffer = ByteBuffer.wrap(positions);
      final List<ReplayEntryPosition> result = new ArrayList<>(entryCount);
      long searchIndex = 0L;
      long physicalTime = 0L;
      long localSeq = 0L;
      for (int index = 0; index < entryCount; index++) {
        searchIndex += readDelta(buffer);
        physicalTime += readDelta(buffer);
        localSeq += readDelta(buffer);
        result.add(new ReplayEntryPosition(searchIndex, physicalTime, localSeq));
      }
      return result;
    }

    static void writeDelta(final ByteArrayOutputStream stream, final long delta) {
      long encoded = (delta << 1) ^ (delta >> 63);
      while ((encoded & ~0x7FL) != 0L) {
        stream.write((int) (encoded & 0x7F) | 0x80);
        encoded >>>= 7;
      }
      stream.write((int) encoded);
    }

    private static long readDelta(final ByteBuffer buffer) {
      long encoded = 0L;
      int shift = 0;
      byte value;
      do {
        value = buffer.get();
        encoded |= (long) (value & 0x7F) << shift;
        shift += 7;
      } while (value < 0);
      return (encoded >>> 1) ^ -(encoded & 1L);
    }
  }
}
