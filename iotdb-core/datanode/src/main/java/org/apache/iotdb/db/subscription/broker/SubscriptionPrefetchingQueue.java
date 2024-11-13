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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeEventBatches;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionReceiverSubtask;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public abstract class SubscriptionPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPrefetchingQueue.class);

  private final String brokerId; // consumer group id
  private final String topicName;

  /** Contains events coming from the upstream pipeline, corresponding to the pipe sink stage. */
  private final SubscriptionBlockingPendingQueue inputPendingQueue;

  private final AtomicLong commitIdGenerator;

  /** A queue containing a series of prefetched pollable {@link SubscriptionEvent}. */
  protected final LinkedBlockingQueue<SubscriptionEvent> prefetchingQueue;

  /**
   * A map that tracks in-flight {@link SubscriptionEvent}, keyed by consumer id and commit context.
   */
  protected final Map<Pair<String, SubscriptionCommitContext>, SubscriptionEvent> inFlightEvents;

  protected final SubscriptionPipeEventBatches batches;

  /**
   * A ReentrantReadWriteLock to ensure thread-safe operations. This lock is used to guarantee
   * mutual exclusion between the {@link SubscriptionPrefetchingQueue#cleanUp} operation and other
   * operations such as {@link SubscriptionPrefetchingQueue#poll}, {@link
   * SubscriptionPrefetchingQueue#ack}, etc. However, it does not enforce mutual exclusion among the
   * other operations themselves.
   *
   * <p>Under the premise of obtaining this lock, to avoid inconsistencies, updates to the {@link
   * SubscriptionEvent} in both {@link SubscriptionPrefetchingQueue#prefetchingQueue} and {@link
   * SubscriptionPrefetchingQueue#inFlightEvents} MUST be performed within the {@link
   * ConcurrentHashMap#compute} method of inFlightEvents in the {@link
   * SubscriptionPrefetchingQueue}.
   *
   * <p>This lock is created with fairness set to true, which means threads acquire the lock in the
   * order they requested it, to avoid thread starvation.
   */
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private final SubscriptionPrefetchingQueueStates states;

  private volatile boolean isCompleted = false;
  private volatile boolean isClosed = false;

  public SubscriptionPrefetchingQueue(
      final String brokerId,
      final String topicName,
      final SubscriptionBlockingPendingQueue inputPendingQueue,
      final AtomicLong commitIdGenerator,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;
    this.commitIdGenerator = commitIdGenerator;

    this.prefetchingQueue = new LinkedBlockingQueue<>();
    this.inFlightEvents = new ConcurrentHashMap<>();
    this.batches = new SubscriptionPipeEventBatches(this, maxDelayInMs, maxBatchSizeInBytes);

    this.states = new SubscriptionPrefetchingQueueStates(this);
  }

  public void cleanUp() {
    acquireWriteLock();
    try {
      cleanUpInternal();
    } finally {
      releaseWriteLock();
    }
  }

  protected void cleanUpInternal() {
    // clean up events in batches
    batches.cleanUp();

    // clean up events in prefetchingQueue
    prefetchingQueue.forEach(SubscriptionEvent::cleanUp);
    prefetchingQueue.clear();

    // clean up events in inFlightEvents
    inFlightEvents.values().forEach(SubscriptionEvent::cleanUp);
    inFlightEvents.clear();

    // no need to clean up events in inputPendingQueue, see
    // org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask.close
  }

  /////////////////////////////////  lock  /////////////////////////////////

  protected void acquireReadLock() {
    lock.readLock().lock();
  }

  protected void releaseReadLock() {
    lock.readLock().unlock();
  }

  protected void acquireWriteLock() {
    lock.writeLock().lock();
  }

  protected void releaseWriteLock() {
    lock.writeLock().unlock();
  }

  /////////////////////////////// subtask ///////////////////////////////

  protected void executeReceiverSubtask(
      final SubscriptionReceiverSubtask subtask, final long timeoutMs) throws Exception {
    PipeSubtaskExecutorManager.getInstance()
        .getSubscriptionExecutor()
        .executeReceiverSubtask(subtask, timeoutMs);
  }

  /////////////////////////////// poll ///////////////////////////////

  public SubscriptionEvent poll(final String consumerId) {
    acquireReadLock();
    try {
      return isClosed() ? null : pollInternal(consumerId);
    } finally {
      releaseReadLock();
    }
  }

  public SubscriptionEvent pollInternal(final String consumerId) {
    states.markPollRequest();

    if (prefetchingQueue.isEmpty()) {
      states.markMissingPrefetch();
      try {
        executeReceiverSubtask(
            () -> {
              tryPrefetch();
              return null;
            },
            SubscriptionAgent.receiver().remainingMs());
      } catch (final Exception ignored) {
      }
    }

    if (prefetchingQueue.isEmpty()) {
      onEvent();
    }

    final long size = prefetchingQueue.size();
    long count = 0;

    SubscriptionEvent event;
    try {
      while (count++ < size // limit control
          && Objects.nonNull(
              event =
                  prefetchingQueue.poll(
                      SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs(),
                      TimeUnit.MILLISECONDS))) {
        if (event.isCommitted()) {
          LOGGER.warn(
              "Subscription: SubscriptionPrefetchingQueue {} poll committed event {} from prefetching queue (broken invariant), remove it",
              this,
              event);
          // no need to update inFlightEvents
          continue;
        }

        if (!event.pollable()) {
          LOGGER.warn(
              "Subscription: SubscriptionPrefetchingQueue {} poll non-pollable event {} from prefetching queue (broken invariant), nack and remove it",
              this,
              event);
          event.nack(); // now pollable
          // no need to update inFlightEvents and prefetchingQueue
          continue;
        }

        // This operation should be performed before updating inFlightEvents to prevent multiple
        // consumers from consuming the same event.
        event.recordLastPolledTimestamp(); // now non-pollable

        inFlightEvents.put(new Pair<>(consumerId, event.getCommitContext()), event);
        event.recordLastPolledConsumerId(consumerId);
        return event;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Subscription: SubscriptionPrefetchingQueue {} interrupted while polling events.",
          this,
          e);
    }

    return null;
  }

  /////////////////////////////// prefetch ///////////////////////////////

  public boolean executePrefetch() {
    acquireReadLock();
    try {
      if (isClosed()) {
        return false;
      }
      if (states.shouldPrefetch()) {
        tryPrefetch();
        remapInFlightEventsSnapshot(committedCleaner, pollableNacker, responsePrefetcher);
        return true;
      } else {
        remapInFlightEventsSnapshot(committedCleaner, pollableNacker);
        return false;
      }
    } finally {
      releaseReadLock();
    }
  }

  @SafeVarargs
  private final void remapInFlightEventsSnapshot(
      final RemappingFunction<SubscriptionEvent>... functions) {
    // Iterate on the snapshot of the key set, NOTE:
    // 1. Ignore entries added during iteration.
    // 2. For entries deleted by other threads during iteration, just check if the value is null.
    // 3. Since the compute call for the same key is atomic and will be executed serially, the
    // remapping operations are safe.
    for (final Pair<String, SubscriptionCommitContext> pair :
        ImmutableSet.copyOf(inFlightEvents.keySet())) {
      inFlightEvents.compute(pair, (key, ev) -> COMBINER(functions).remap(ev));
    }
  }

  protected void enqueueEventToPrefetchingQueue(final SubscriptionEvent event) {
    event.trySerializeCurrentResponse();
    prefetchingQueue.add(event);
  }

  /**
   * Prefetch at most one {@link SubscriptionEvent} from {@link
   * SubscriptionPrefetchingQueue#inputPendingQueue} to {@link
   * SubscriptionPrefetchingQueue#prefetchingQueue}.
   *
   * <p>It will continuously attempt to prefetch and generate a {@link SubscriptionEvent} until
   * {@link SubscriptionPrefetchingQueue#inputPendingQueue} is empty.
   */
  private void tryPrefetch() {
    while (!inputPendingQueue.isEmpty()) {
      final Event event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll());
      if (Objects.isNull(event)) {
        // The event will be null in two cases:
        // 1. The inputPendingQueue is empty.
        // 2. The tsfile event has been deduplicated.
        continue;
      }

      if (!(event instanceof EnrichedEvent)) {
        LOGGER.warn(
            "Subscription: SubscriptionPrefetchingQueue {} only support prefetch EnrichedEvent. Ignore {}.",
            this,
            event);
        continue;
      }

      if (event instanceof PipeTerminateEvent) {
        final PipeTerminateEvent terminateEvent = (PipeTerminateEvent) event;
        // add mark completed hook
        terminateEvent.addOnCommittedHook(
            () -> {
              markCompleted();
              return null;
            });
        // commit directly
        ((PipeTerminateEvent) event)
            .decreaseReferenceCount(SubscriptionPrefetchingQueue.class.getName(), true);
        LOGGER.info(
            "Subscription: SubscriptionPrefetchingQueue {} commit PipeTerminateEvent {}",
            this,
            terminateEvent);
        continue;
      }

      if (event instanceof TabletInsertionEvent) {
        if (onEvent((TabletInsertionEvent) event)) {
          return;
        }
        continue;
      }

      if (event instanceof PipeTsFileInsertionEvent) {
        if (onEvent((PipeTsFileInsertionEvent) event)) {
          return;
        }
        continue;
      }

      // TODO:
      //  - PipeHeartbeatEvent: ignored? (may affect pipe metrics)
      //  - UserDefinedEnrichedEvent: ignored?
      //  - Others: events related to meta sync, safe to ignore
      LOGGER.info(
          "Subscription: SubscriptionPrefetchingQueue {} ignore EnrichedEvent {} when prefetching.",
          this,
          event);
      if (onEvent()) {
        return;
      }
    }

    // At this moment, the inputPendingQueue is empty.
  }

  /**
   * @return {@code true} if there are subscription events prefetched.
   */
  protected abstract boolean onEvent(final TsFileInsertionEvent event);

  /**
   * @return {@code true} if there are subscription events prefetched.
   */
  protected boolean onEvent(final TabletInsertionEvent event) {
    return batches.onEvent((EnrichedEvent) event, this::enqueueEventToPrefetchingQueue);
  }

  /**
   * @return {@code true} if there are subscription events prefetched.
   */
  protected boolean onEvent() {
    return batches.onEvent(this::enqueueEventToPrefetchingQueue);
  }

  /////////////////////////////// commit ///////////////////////////////

  /**
   * @return {@code true} if ack successfully
   */
  public boolean ack(final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      return !isClosed() && ackInternal(consumerId, commitContext);
    } finally {
      releaseReadLock();
    }
  }

  /**
   * @return {@code true} if ack successfully
   */
  private boolean ackInternal(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean acked = new AtomicBoolean(false);
    inFlightEvents.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            LOGGER.warn(
                "Subscription: subscription commit context {} does not exist, it may have been committed or something unexpected happened, prefetching queue: {}",
                commitContext,
                this);
            return null;
          }

          if (ev.isCommitted()) {
            LOGGER.warn(
                "Subscription: subscription event {} is committed, subscription commit context {}, prefetching queue: {}",
                ev,
                commitContext,
                this);
            // clean up committed event
            ev.cleanUp();
            return null; // remove this entry
          }

          if (!ev.isCommittable()) {
            LOGGER.warn(
                "Subscription: subscription event {} is not committable, subscription commit context {}, prefetching queue: {}",
                ev,
                commitContext,
                this);
            return ev;
          }

          // check if a consumer acks event from another consumer group...
          final String consumerGroupId = commitContext.getConsumerGroupId();
          if (!Objects.equals(consumerGroupId, brokerId)) {
            LOGGER.warn(
                "inconsistent consumer group when acking event, current: {}, incoming: {}, consumer id: {}, event commit context: {}, prefetching queue: {}, commit it anyway...",
                brokerId,
                consumerGroupId,
                consumerId,
                commitContext,
                this);
          }

          ev.ack();
          ev.recordCommittedTimestamp(); // now committed
          acked.set(true);

          // clean up committed event
          ev.cleanUp();
          return null; // remove this entry
        });

    return acked.get();
  }

  /**
   * @return {@code true} if nack successfully
   */
  public boolean nack(final String consumerId, final SubscriptionCommitContext commitContext) {
    acquireReadLock();
    try {
      return !isClosed() && nackInternal(consumerId, commitContext);
    } finally {
      releaseReadLock();
    }
  }

  /**
   * @return {@code true} if nack successfully
   */
  public boolean nackInternal(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final AtomicBoolean nacked = new AtomicBoolean(false);
    inFlightEvents.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.isNull(ev)) {
            LOGGER.warn(
                "Subscription: subscription commit context [{}] does not exist, it may have been committed or something unexpected happened, prefetching queue: {}",
                commitContext,
                this);
            return null;
          }

          // check if a consumer nacks event from another consumer group...
          final String consumerGroupId = commitContext.getConsumerGroupId();
          if (!Objects.equals(consumerGroupId, brokerId)) {
            LOGGER.warn(
                "inconsistent consumer group when nacking event, current: {}, incoming: {}, consumer id: {}, event commit context: {}, prefetching queue: {}, commit it anyway...",
                brokerId,
                consumerGroupId,
                consumerId,
                commitContext,
                this);
          }

          ev.nack(); // now pollable
          nacked.set(true);

          // no need to update inFlightEvents and prefetchingQueue
          return ev;
        });

    return nacked.get();
  }

  public SubscriptionCommitContext generateSubscriptionCommitContext() {
    // Recording data node ID and reboot times to address potential stale commit IDs caused by
    // leader transfers or restarts.
    return new SubscriptionCommitContext(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        PipeDataNodeAgent.runtime().getRebootTimes(),
        topicName,
        brokerId,
        commitIdGenerator.getAndIncrement());
  }

  protected SubscriptionEvent generateSubscriptionPollErrorResponse(final String errorMessage) {
    // consider non-critical by default, meaning the client can retry
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

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getPrefetchingQueueId() {
    return generatePrefetchingQueueId(brokerId, topicName);
  }

  public static String generatePrefetchingQueueId(
      final String consumerGroupId, final String topicName) {
    return consumerGroupId + "_" + topicName;
  }

  public long getSubscriptionUncommittedEventCount() {
    return inFlightEvents.size();
  }

  public long getCurrentCommitId() {
    return commitIdGenerator.get();
  }

  public int getPipeEventCount() {
    return inputPendingQueue.size()
        + prefetchingQueue.stream()
            .map(SubscriptionEvent::getPipeEventCount)
            .reduce(Integer::sum)
            .orElse(0)
        + inFlightEvents.values().stream()
            .map(SubscriptionEvent::getPipeEventCount)
            .reduce(Integer::sum)
            .orElse(0);
  }

  public int getPrefetchedEventCount() {
    return prefetchingQueue.size();
  }

  /////////////////////////////// close & termination ///////////////////////////////

  public boolean isClosed() {
    return isClosed;
  }

  public void markClosed() {
    isClosed = true;
  }

  public boolean isCompleted() {
    return isCompleted;
  }

  public void markCompleted() {
    isCompleted = true;
  }

  /////////////////////////////// stringify ///////////////////////////////

  public Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("brokerId", brokerId);
    result.put("topicName", topicName);
    result.put("size of inputPendingQueue", String.valueOf(inputPendingQueue.size()));
    result.put("size of prefetchingQueue", String.valueOf(prefetchingQueue.size()));
    result.put("size of inFlightEvents", String.valueOf(inFlightEvents.size()));
    result.put("commitIdGenerator", commitIdGenerator.toString());
    result.put("states", states.toString());
    result.put("isCompleted", String.valueOf(isCompleted));
    result.put("isClosed", String.valueOf(isClosed));
    return result;
  }

  public Map<String, String> allReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("brokerId", brokerId);
    result.put("topicName", topicName);
    result.put("size of inputPendingQueue", String.valueOf(inputPendingQueue.size()));
    result.put("prefetchingQueue", prefetchingQueue.toString());
    result.put("inFlightEvents", inFlightEvents.toString());
    result.put("commitIdGenerator", commitIdGenerator.toString());
    result.put("states", states.toString());
    result.put("isCompleted", String.valueOf(isCompleted));
    result.put("isClosed", String.valueOf(isClosed));
    return result;
  }

  /////////////////////////////// remapping ///////////////////////////////

  @FunctionalInterface
  private interface RemappingFunction<V> {
    /* @Nullable */ V remap(final V v);
  }

  @SafeVarargs
  private static @NonNull RemappingFunction<SubscriptionEvent> COMBINER(
      final @NonNull RemappingFunction<SubscriptionEvent>... functions) {
    return (ev) -> {
      if (Objects.isNull(ev)) {
        return null;
      }
      for (final RemappingFunction<SubscriptionEvent> function : functions) {
        if (Objects.isNull(function.remap(ev))) {
          return null;
        }
      }
      return ev;
    };
  }

  private final RemappingFunction<SubscriptionEvent> committedCleaner =
      (ev) -> {
        if (ev.isCommitted()) {
          ev.cleanUp();
          return null; // remove this entry
        }
        return ev;
      };

  private final RemappingFunction<SubscriptionEvent> pollableNacker =
      (ev) -> {
        if (ev.eagerlyPollable()) {
          ev.nack(); // now pollable (the nack operation here is actually unnecessary)
          enqueueEventToPrefetchingQueue(ev);
          // no need to log warn for eagerly pollable event
          return null; // remove this entry
        } else if (ev.pollable()) {
          ev.nack(); // now pollable
          enqueueEventToPrefetchingQueue(ev);
          LOGGER.warn(
              "Subscription: SubscriptionPrefetchingQueue {} recycle event {} from in flight events, nack and enqueue it to prefetching queue",
              this,
              ev);
          return null; // remove this entry
        }
        return ev;
      };

  private final RemappingFunction<SubscriptionEvent> responsePrefetcher =
      (ev) -> {
        // prefetch and serialize the remaining responses
        try {
          ev.prefetchRemainingResponses();
          ev.trySerializeRemainingResponses();
        } catch (final Exception ignored) {
        }
        return ev;
      };
}
