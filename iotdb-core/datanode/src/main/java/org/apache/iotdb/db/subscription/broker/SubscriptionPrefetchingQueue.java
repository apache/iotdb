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

import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeEventBatches;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTsFileEventBatch;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionReceiverSubtask;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.session.subscription.util.PollTimer;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
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
  // record initial commit for outdated event detection
  private final long initialCommitId;

  /** A queue containing a series of prefetched pollable {@link SubscriptionEvent}. */
  protected final PriorityBlockingQueue<SubscriptionEvent> prefetchingQueue;

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

  private long lastStateReportTimestamp = System.currentTimeMillis();

  private volatile boolean isCompleted = false;
  private volatile boolean isClosed = false;

  // for prefetch v2
  // TODO: make it thread-local for higher throughput
  private volatile TsFileInsertionEvent currentTsFileInsertionEvent;
  private volatile RetryableEvent<TabletInsertionEvent> currentTabletInsertionEvent;
  private volatile SubscriptionTsFileToTabletIterator currentToTabletIterator;

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
    this.initialCommitId = commitIdGenerator.get();

    this.prefetchingQueue = new PriorityBlockingQueue<>();
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
    prefetchingQueue.forEach(event -> event.cleanUp(true));
    prefetchingQueue.clear();

    // clean up events in inFlightEvents
    inFlightEvents.values().forEach(event -> event.cleanUp(true));
    inFlightEvents.clear();

    // no need to clean up events in inputPendingQueue, see
    // org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask.close

    if (Objects.nonNull(currentToTabletIterator)) {
      currentToTabletIterator.cleanUp();
      currentToTabletIterator = null;
    }
    if (Objects.nonNull(currentTsFileInsertionEvent)) {
      ((EnrichedEvent) currentTsFileInsertionEvent).clearReferenceCount(this.getClass().getName());
      currentTsFileInsertionEvent = null;
    }
    if (Objects.nonNull(currentTabletInsertionEvent)) {
      ((EnrichedEvent) currentTabletInsertionEvent.innerEvent)
          .clearReferenceCount(this.getClass().getName());
      currentTabletInsertionEvent = null;
    }
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

  private SubscriptionEvent pollInternal(final String consumerId) {
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
      } catch (final Exception e) {
        LOGGER.warn("Exception {} occurred when {} execute receiver subtask", this, e, e);
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

  public SubscriptionEvent pollV2(final String consumerId, final PollTimer timer) {
    acquireReadLock();
    try {
      return isClosed() ? null : pollInternalV2(consumerId, timer);
    } finally {
      releaseReadLock();
    }
  }

  private SubscriptionEvent pollInternalV2(final String consumerId, final PollTimer timer) {
    states.markPollRequest();

    // do-while ensures at least one poll
    do {
      SubscriptionEvent event;
      try {
        if (prefetchingQueue.isEmpty()) {
          // TODO: concurrent polling of multiple prefetching queues
          Thread.sleep(100);
          onEvent();
        }

        final long size = prefetchingQueue.size();
        long count = 0;

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
      timer.update();
    } while (!timer.isExpired());

    return null;
  }

  /////////////////////////////// prefetch ///////////////////////////////

  public boolean executePrefetch() {
    acquireReadLock();
    try {
      if (isClosed()) {
        return false;
      }
      reportStateIfNeeded();
      // TODO: more refined behavior (prefetch/serialize/...) control
      if (states.shouldPrefetch()) {
        tryPrefetch();
        remapInFlightEventsSnapshot(
            committedCleaner, pollableNacker, responsePrefetcher, responseSerializer);
        return true;
      } else {
        peekOnce();
        remapInFlightEventsSnapshot(committedCleaner, pollableNacker);
        return false;
      }
    } finally {
      releaseReadLock();
    }
  }

  public boolean executePrefetchV2() {
    acquireReadLock();
    try {
      if (isClosed()) {
        return false;
      }
      reportStateIfNeeded();
      tryPrefetchV2();
      remapInFlightEventsSnapshot(committedCleaner, pollableNacker);
      // always return false
      return false;
    } finally {
      releaseReadLock();
    }
  }

  private void reportStateIfNeeded() {
    if (System.currentTimeMillis() - lastStateReportTimestamp
        > SubscriptionConfig.getInstance().getSubscriptionLogManagerBaseIntervalMs()
            * SubscriptionAgent.broker().getPrefetchingQueueCount()) {
      LOGGER.info("Subscription: SubscriptionPrefetchingQueue state {}", this);
      lastStateReportTimestamp = System.currentTimeMillis();
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

  public void prefetchEvent(final SubscriptionEvent thisEvent) {
    final SubscriptionEvent thatEvent = prefetchingQueue.peek();
    if (Objects.nonNull(thatEvent)) {
      if (thisEvent.compareTo(thatEvent) < 0) {
        // disorder causes:
        // 1. prefetch nacked event
        // 2. late cross-event of dataset payload
        states.markDisorderCause();
      }
    }

    prefetchingQueue.add(thisEvent);
  }

  private synchronized void peekOnce() {
    final Event peekedEvent = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.peek());
    if (Objects.isNull(peekedEvent)) {
      return;
    }

    if (!(peekedEvent instanceof PipeHeartbeatEvent)) {
      return;
    }

    final Event polledEvent = inputPendingQueue.waitedPoll();
    if (!Objects.equals(peekedEvent, polledEvent)) {
      LOGGER.warn(
          "Subscription: inconsistent heartbeat event when {} peeking (broken invariant), expected {}, actual {}, offer back",
          this,
          peekedEvent,
          polledEvent);
      inputPendingQueue.directOffer(polledEvent);
    } else {
      ((PipeHeartbeatEvent) peekedEvent)
          .decreaseReferenceCount(SubscriptionPrefetchingQueue.class.getName(), false);
    }
  }

  /**
   * Prefetch at most one {@link SubscriptionEvent} from {@link
   * SubscriptionPrefetchingQueue#inputPendingQueue} to {@link
   * SubscriptionPrefetchingQueue#prefetchingQueue}.
   *
   * <p>It will continuously attempt to prefetch and generate a {@link SubscriptionEvent} until
   * {@link SubscriptionPrefetchingQueue#inputPendingQueue} is empty.
   */
  private synchronized void tryPrefetch() {
    while (!inputPendingQueue.isEmpty() || Objects.nonNull(currentTabletInsertionEvent)) {
      if (Objects.nonNull(currentTabletInsertionEvent)) {
        final RetryableState state = onRetryableTabletInsertionEvent(currentTabletInsertionEvent);
        switch (state) {
          case PREFETCHED:
            return;
          case RETRY:
            continue;
          case NO_RETRY:
            break;
        }
      }

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
        terminateEvent.addOnCommittedHook(this::markCompleted);
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
        final RetryableState state =
            onRetryableTabletInsertionEvent(
                new RetryableEvent<>((TabletInsertionEvent) event, false, false));
        switch (state) {
          case PREFETCHED:
            return;
          case RETRY:
          case NO_RETRY:
            continue;
        }
      }

      if (event instanceof TsFileInsertionEvent) {
        if (onEvent((TsFileInsertionEvent) event)) {
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
      ((EnrichedEvent) event)
          .decreaseReferenceCount(SubscriptionPrefetchingQueue.class.getName(), false);
      if (onEvent()) {
        return;
      }
    }

    // At this moment, the inputPendingQueue is empty.
  }

  private synchronized void tryPrefetchV2() {
    if (!prefetchingQueue.isEmpty()) {
      return;
    }

    if (Objects.nonNull(currentTsFileInsertionEvent)) {
      constructToTabletIterator(currentTsFileInsertionEvent);
      return;
    }

    if (Objects.nonNull(currentTabletInsertionEvent)) {
      final RetryableState state = onRetryableTabletInsertionEvent(currentTabletInsertionEvent);
      switch (state) {
        case PREFETCHED:
        case RETRY:
          return;
        case NO_RETRY:
          break;
      }
    }

    if (Objects.nonNull(currentToTabletIterator)) {
      while (currentToTabletIterator.hasNext() || Objects.nonNull(currentTabletInsertionEvent)) {
        if (Objects.nonNull(currentTabletInsertionEvent)) {
          final RetryableState state = onRetryableTabletInsertionEvent(currentTabletInsertionEvent);
          switch (state) {
            case PREFETCHED:
            case RETRY:
              return;
            case NO_RETRY:
              break;
          }
        }
        final RetryableState state =
            onRetryableTabletInsertionEvent(
                new RetryableEvent<>(currentToTabletIterator.next(), true, true));
        switch (state) {
          case PREFETCHED:
          case RETRY:
            return;
          case NO_RETRY:
            continue;
        }
      }
      currentToTabletIterator.ack();
      currentToTabletIterator = null;
    }

    final Event event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll());
    if (Objects.isNull(event)) {
      // The event will be null in two cases:
      // 1. The inputPendingQueue is empty.
      // 2. The tsfile event has been deduplicated.
      return;
    }

    if (!(event instanceof EnrichedEvent)) {
      LOGGER.warn(
          "Subscription: SubscriptionPrefetchingQueue {} only support prefetch EnrichedEvent. Ignore {}.",
          this,
          event);
      return;
    }

    if (event instanceof PipeTerminateEvent) {
      final PipeTerminateEvent terminateEvent = (PipeTerminateEvent) event;
      // add mark completed hook
      terminateEvent.addOnCommittedHook(this::markCompleted);
      // commit directly
      ((PipeTerminateEvent) event)
          .decreaseReferenceCount(SubscriptionPrefetchingQueue.class.getName(), true);
      LOGGER.info(
          "Subscription: SubscriptionPrefetchingQueue {} commit PipeTerminateEvent {}",
          this,
          terminateEvent);
      return;
    }

    if (event instanceof TabletInsertionEvent) {
      onRetryableTabletInsertionEvent(
          new RetryableEvent<>((TabletInsertionEvent) event, false, false));
      return; // always return here
    }

    if (event instanceof TsFileInsertionEvent) {
      if (PipeEventCollector.canSkipParsing4TsFileEvent((PipeTsFileInsertionEvent) event)) {
        onEvent((TsFileInsertionEvent) event);
        return;
      }
      if (Objects.nonNull(currentToTabletIterator)) {
        LOGGER.warn(
            "Subscription: SubscriptionPrefetchingQueue {} prefetch TsFileInsertionEvent when ToTabletIterator is not null (broken invariant). Ignore {}.",
            this,
            event);
      } else {
        constructToTabletIterator((PipeTsFileInsertionEvent) event);
        return;
      }
    }

    // TODO:
    //  - PipeHeartbeatEvent: ignored? (may affect pipe metrics)
    //  - UserDefinedEnrichedEvent: ignored?
    //  - Others: events related to meta sync, safe to ignore
    LOGGER.info(
        "Subscription: SubscriptionPrefetchingQueue {} ignore EnrichedEvent {} when prefetching.",
        this,
        event);
    ((EnrichedEvent) event)
        .decreaseReferenceCount(SubscriptionPrefetchingQueue.class.getName(), false);

    onEvent();
  }

  private void constructToTabletIterator(final TsFileInsertionEvent event) {
    currentTsFileInsertionEvent = null;
    final Iterator<TabletInsertionEvent> tabletInsertionEventsIterator;
    try {
      tabletInsertionEventsIterator = event.toTabletInsertionEvents().iterator();
      currentToTabletIterator =
          new SubscriptionTsFileToTabletIterator(
              (PipeTsFileInsertionEvent) event, tabletInsertionEventsIterator);
    } catch (final PipeException e) {
      LOGGER.warn("Exception {} occurred when {} construct ToTabletIterator", this, e, e);
      currentTsFileInsertionEvent = event;
    }
  }

  private RetryableState onRetryableTabletInsertionEvent(
      final RetryableEvent<TabletInsertionEvent> retryableEvent) {
    currentTabletInsertionEvent = null;
    final EnrichedEvent event = (EnrichedEvent) retryableEvent.innerEvent;
    if (retryableEvent.shouldIncreaseReferenceCount) {
      if (!event.increaseReferenceCount(this.getClass().getName())) {
        LOGGER.warn(
            "Failed to increase reference count for {} when {} on retryable TabletInsertionEvent",
            event,
            this);
        currentTabletInsertionEvent = retryableEvent;
        return RetryableState.RETRY;
      }
      retryableEvent.shouldIncreaseReferenceCount = false;
    }
    if (retryableEvent.shouldEnrichWithCommitterKeyAndCommitId) {
      PipeEventCommitManager.getInstance()
          .enrichWithCommitterKeyAndCommitId(
              event,
              currentToTabletIterator.getCreationTime(),
              currentToTabletIterator.getRegionId());
      retryableEvent.shouldEnrichWithCommitterKeyAndCommitId = false;
    }
    try {
      return onEvent(retryableEvent.innerEvent)
          ? RetryableState.PREFETCHED
          : RetryableState.NO_RETRY;
    } catch (final Exception e) {
      LOGGER.warn(
          "Exception occurred when {} on retryable TabletInsertionEvent {}", this, event, e);
      currentTabletInsertionEvent = retryableEvent;
      return RetryableState.RETRY;
    }
  }

  /**
   * @return {@code true} if there are subscription events prefetched.
   */
  protected abstract boolean onEvent(final TsFileInsertionEvent event);

  /**
   * @return {@code true} if there are subscription events prefetched.
   * @throws Exception only occur when constructing {@link SubscriptionPipeTsFileEventBatch}.
   */
  protected boolean onEvent(final TabletInsertionEvent event) throws Exception {
    return batches.onEvent((EnrichedEvent) event, this::prefetchEvent);
  }

  /**
   * @return {@code true} if there are subscription events prefetched.
   */
  protected boolean onEvent() {
    return batches.onEvent(this::prefetchEvent);
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
            ev.cleanUp(false);
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
          ev.cleanUp(false);
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

  protected SubscriptionEvent generateSubscriptionPollOutdatedErrorResponse() {
    // consider non-critical by default, meaning the client can retry
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
  private static RemappingFunction<SubscriptionEvent> COMBINER(
      final RemappingFunction<SubscriptionEvent>... functions) {
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
          ev.cleanUp(false);
          return null; // remove this entry
        }
        return ev;
      };

  private final RemappingFunction<SubscriptionEvent> pollableNacker =
      (ev) -> {
        if (ev.eagerlyPollable()) {
          ev.nack(); // now pollable (the nack operation here is actually unnecessary)
          prefetchEvent(ev);
          // no need to log warn for eagerly pollable event
          return null; // remove this entry
        } else if (ev.pollable()) {
          ev.nack(); // now pollable
          prefetchEvent(ev);
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
        // prefetch the remaining responses
        try {
          ev.prefetchRemainingResponses();
        } catch (final Exception ignored) {
        }
        return ev;
      };

  private final RemappingFunction<SubscriptionEvent> responseSerializer =
      (ev) -> {
        // serialize the responses
        try {
          ev.trySerializeCurrentResponse();
          ev.trySerializeRemainingResponses();
        } catch (final Exception ignored) {
        }
        return ev;
      };

  /////////////////////////////// tsfile to tablet iteration ///////////////////////////////

  private static class SubscriptionTsFileToTabletIterator
      implements Iterator<TabletInsertionEvent> {

    private final PipeTsFileInsertionEvent tsFileInsertionEvent;
    private final Iterator<TabletInsertionEvent> tabletInsertionEventsIterator;

    private SubscriptionTsFileToTabletIterator(
        final PipeTsFileInsertionEvent tsFileInsertionEvent,
        final Iterator<TabletInsertionEvent> tabletInsertionEventsIterator) {
      this.tsFileInsertionEvent = tsFileInsertionEvent;
      this.tabletInsertionEventsIterator = tabletInsertionEventsIterator;
    }

    @Override
    public boolean hasNext() {
      return tabletInsertionEventsIterator.hasNext();
    }

    @Override
    public TabletInsertionEvent next() {
      return tabletInsertionEventsIterator.next();
    }

    public void ack() {
      try {
        tsFileInsertionEvent.close();
      } catch (final Exception ignored) {
      }
      // should not report here
      tsFileInsertionEvent.decreaseReferenceCount(this.getClass().getName(), false);
    }

    public void cleanUp() {
      try {
        tsFileInsertionEvent.close();
      } catch (final Exception ignored) {
      }
      tsFileInsertionEvent.clearReferenceCount(this.getClass().getName());
    }

    public long getCreationTime() {
      return tsFileInsertionEvent.getCreationTime();
    }

    public int getRegionId() {
      return tsFileInsertionEvent.getRegionId();
    }
  }

  /////////////////////////////// retryable Event ///////////////////////////////

  private static class RetryableEvent<E extends Event> {

    private final E innerEvent;
    private volatile boolean shouldIncreaseReferenceCount;
    private volatile boolean shouldEnrichWithCommitterKeyAndCommitId;

    private RetryableEvent(
        final E innerEvent,
        final boolean shouldIncreaseReferenceCount,
        final boolean shouldEnrichWithCommitterKeyAndCommitId) {
      this.innerEvent = innerEvent;
      this.shouldIncreaseReferenceCount = shouldIncreaseReferenceCount;
      this.shouldEnrichWithCommitterKeyAndCommitId = shouldEnrichWithCommitterKeyAndCommitId;
    }
  }

  private enum RetryableState {
    RETRY,
    NO_RETRY,
    PREFETCHED,
  }
}
