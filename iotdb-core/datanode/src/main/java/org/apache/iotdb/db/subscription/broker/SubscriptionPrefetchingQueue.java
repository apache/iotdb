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
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEmptyEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public abstract class SubscriptionPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPrefetchingQueue.class);

  protected final String brokerId; // consumer group id
  protected final String topicName;

  /** Contains events coming from the upstream pipeline, corresponding to the pipe sink stage. */
  protected final SubscriptionBlockingPendingQueue inputPendingQueue;

  /** A queue containing a series of prefetched pollable {@link SubscriptionEvent}. */
  protected final LinkedBlockingQueue<SubscriptionEvent> prefetchingQueue;

  /** A map recording all currently prefetched but uncommitted {@link SubscriptionEvent}. */
  protected final Map<SubscriptionCommitContext, SubscriptionEvent> uncommittedEvents;

  /**
   * A map that tracks in-flight {@link SubscriptionEvent}, keyed by consumer ID and commit context.
   */
  protected final Map<Pair<String, SubscriptionCommitContext>, SubscriptionEvent>
      inFlightSubscriptionEventMap;

  private final AtomicLong subscriptionCommitIdGenerator = new AtomicLong(0);

  private volatile boolean isCompleted = false;
  private volatile boolean isClosed = false;

  public SubscriptionPrefetchingQueue(
      final String brokerId,
      final String topicName,
      final SubscriptionBlockingPendingQueue inputPendingQueue) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;

    this.prefetchingQueue = new LinkedBlockingQueue<>();
    this.uncommittedEvents = new ConcurrentHashMap<>();

    this.inFlightSubscriptionEventMap = new ConcurrentHashMap<>();
  }

  public void cleanup() {
    // clean up uncommitted events
    uncommittedEvents.values().forEach(SubscriptionEvent::cleanup);
    uncommittedEvents.clear();

    // no need to clean up events in prefetchingQueue, since all events in prefetchingQueue are also
    // in uncommittedEvents
    prefetchingQueue.clear();

    // no need to clean up events in inFlightSubscriptionEventMap, since all events in
    // inFlightSubscriptionEventMap are also in uncommittedEvents
    inFlightSubscriptionEventMap.clear();

    // no need to clean up events in inputPendingQueue, see
    // org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask.close
  }

  /////////////////////////////// poll ///////////////////////////////

  public SubscriptionEvent poll(final String consumerId) {
    if (prefetchingQueue.isEmpty()) {
      tryPrefetch(true);
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
              "Subscription: SubscriptionPrefetchingQueue {} poll committed event {} from prefetching queue (broken invariant), clean up and remove it",
              this,
              event);
          event.cleanup();
          continue;
        }

        if (!event.pollable()) {
          LOGGER.warn(
              "Subscription: SubscriptionPrefetchingQueue {} poll non-pollable event {} from prefetching queue (broken invariant), nack and re-enqueue it",
              this,
              event);
          event.nack(); // now pollable
          prefetchingQueue.add(event);
          continue;
        }

        // This operation should be performed before updating inFlightSubscriptionEventMap to
        // prevent multiple consumers from consuming the same event.
        event.recordLastPolledTimestamp(); // now non-pollable

        // remove potential stale entry in inFlightSubscriptionEventMap for auto recycled event
        inFlightSubscriptionEventMap.remove(
            new Pair<>(event.getLastPolledConsumerId(), event.getCommitContext()));
        inFlightSubscriptionEventMap.put(new Pair<>(consumerId, event.getCommitContext()), event);
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

  public void executePrefetch() {
    tryPrefetch(false);
    prefetchAndSerializeInFlightEvents();
  }

  private void prefetchAndSerializeInFlightEvents() {
    // Iterate on the snapshot of the key set, NOTE:
    // 1. Ignore entries added during iteration.
    // 2. For entries deleted by other threads during iteration, just check if the value is null.
    for (final Pair<String, SubscriptionCommitContext> pair :
        ImmutableSet.copyOf(inFlightSubscriptionEventMap.keySet())) {
      inFlightSubscriptionEventMap.compute(
          pair,
          (key, ev) -> {
            if (Objects.isNull(ev)) {
              return null;
            }

            // clean up committed event
            if (ev.isCommitted()) {
              ev.cleanup();
              return null; // remove this entry
            }

            // nack pollable event
            if (ev.pollable()) {
              ev.nack(); // now pollable
              prefetchingQueue.add(ev);
              LOGGER.warn(
                  "Subscription: SubscriptionPrefetchingQueue {} recycle event {} from in flight events, nack and enqueue it to prefetching queue",
                  this,
                  ev);
              return null; // remove this entry
            }

            // prefetch and serialize remaining subscription events
            // NOTE: Since the compute call for the same key is atomic and will be executed
            // serially, the current prefetch and serialize operations are safe.
            try {
              ev.prefetchRemainingResponses();
              ev.trySerializeRemainingResponses();
            } catch (final Exception ignored) {
            }

            return ev;
          });
    }
  }

  /**
   * Prefetch at most one {@link SubscriptionEvent} from {@link
   * SubscriptionPrefetchingQueue#inputPendingQueue} to {@link
   * SubscriptionPrefetchingQueue#prefetchingQueue}.
   *
   * <p>It will continuously attempt to prefetch and generate a {@link SubscriptionEvent} until
   * {@link SubscriptionPrefetchingQueue#inputPendingQueue} is empty.
   *
   * @param onEventIfEmpty {@code true} if {@link SubscriptionPrefetchingQueue#onEvent()} is called
   *     when {@link SubscriptionPrefetchingQueue#inputPendingQueue} is empty, {@code false}
   *     otherwise
   */
  private void tryPrefetch(final boolean onEventIfEmpty) {
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
        LOGGER.info(
            "Subscription: SubscriptionPrefetchingQueue {} commit PipeTerminateEvent {}",
            this,
            event);
        // commit directly
        ((PipeTerminateEvent) event)
            .decreaseReferenceCount(SubscriptionPrefetchingQueue.class.getName(), true);
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
    if (onEventIfEmpty) {
      onEvent();
    }
  }

  /**
   * @return {@code true} if a new event has been prefetched.
   */
  protected abstract boolean onEvent(final TabletInsertionEvent event);

  /**
   * @return {@code true} if a new event has been prefetched.
   */
  protected abstract boolean onEvent(final PipeTsFileInsertionEvent event);

  /**
   * @return {@code true} if a new event has been prefetched.
   */
  protected abstract boolean onEvent();

  /////////////////////////////// commit ///////////////////////////////

  /**
   * @return {@code true} if ack successfully
   */
  public boolean ack(final String consumerId, final SubscriptionCommitContext commitContext) {
    final SubscriptionEvent event = uncommittedEvents.get(commitContext);
    if (Objects.isNull(event)) {
      LOGGER.warn(
          "Subscription: subscription commit context {} does not exist, it may have been committed or something unexpected happened, prefetching queue: {}",
          commitContext,
          this);
      return false;
    }

    if (event.isCommitted()) {
      event.cleanup();
      LOGGER.warn(
          "Subscription: subscription event {} is committed, subscription commit context {}, prefetching queue: {}",
          event,
          commitContext,
          this);
      return false;
    }

    if (!event.isCommittable()) {
      LOGGER.warn(
          "Subscription: subscription event {} is not committable, subscription commit context {}, prefetching queue: {}",
          event,
          commitContext,
          this);
      return false;
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

    event.ack();
    event.cleanup();
    event.recordCommittedTimestamp();

    inFlightSubscriptionEventMap.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.nonNull(ev) && Objects.equals(commitContext, ev.getCommitContext())) {
            return null; // remove this entry
          }
          return ev;
        });
    uncommittedEvents.remove(commitContext);

    return true;
  }

  /**
   * @return {@code true} if nack successfully
   */
  public boolean nack(final String consumerId, final SubscriptionCommitContext commitContext) {
    final SubscriptionEvent event = uncommittedEvents.get(commitContext);
    if (Objects.isNull(event)) {
      LOGGER.warn(
          "Subscription: subscription commit context [{}] does not exist, it may have been committed or something unexpected happened, prefetching queue: {}",
          commitContext,
          this);
      return false;
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

    event.nack(); // now pollable

    inFlightSubscriptionEventMap.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          if (Objects.nonNull(ev) && Objects.equals(commitContext, ev.getCommitContext())) {
            return null; // remove this entry
          }
          return ev;
        });
    prefetchingQueue.add(event);

    return true;
  }

  public SubscriptionCommitContext generateSubscriptionCommitContext() {
    // Recording data node ID and reboot times to address potential stale commit IDs caused by
    // leader transfers or restarts.
    return new SubscriptionCommitContext(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        PipeDataNodeAgent.runtime().getRebootTimes(),
        topicName,
        brokerId,
        subscriptionCommitIdGenerator.getAndIncrement());
  }

  private SubscriptionCommitContext generateInvalidSubscriptionCommitContext() {
    return new SubscriptionCommitContext(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        PipeDataNodeAgent.runtime().getRebootTimes(),
        topicName,
        brokerId,
        INVALID_COMMIT_ID);
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getPrefetchingQueueId() {
    return generatePrefetchingQueueId(brokerId, topicName);
  }

  public static String generatePrefetchingQueueId(
      final String consumerGroupId, final String topicName) {
    return consumerGroupId + "_" + topicName;
  }

  public long getUncommittedEventCount() {
    return uncommittedEvents.size();
  }

  public long getCurrentCommitId() {
    return subscriptionCommitIdGenerator.get();
  }

  /////////////////////////////// termination ///////////////////////////////

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

  public SubscriptionEvent generateSubscriptionPollTerminationResponse() {
    return new SubscriptionEvent(
        new SubscriptionPipeEmptyEvent(),
        Collections.singletonList(
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TERMINATION.getType(),
                new TerminationPayload(),
                generateInvalidSubscriptionCommitContext())));
  }

  public SubscriptionEvent generateSubscriptionPollErrorResponse(
      final String errorMessage, final boolean critical) {
    return new SubscriptionEvent(
        new SubscriptionPipeEmptyEvent(),
        Collections.singletonList(
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.ERROR.getType(),
                new ErrorPayload(errorMessage, critical),
                generateInvalidSubscriptionCommitContext())));
  }

  protected SubscriptionEvent generateSubscriptionPollErrorResponse(final String errorMessage) {
    // consider non-critical by default, meaning the client can retry
    return generateSubscriptionPollErrorResponse(errorMessage, false);
  }

  /////////////////////////////// stringify ///////////////////////////////

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("brokerId", brokerId);
    result.put("topicName", topicName);
    result.put("size of uncommittedEvents", String.valueOf(uncommittedEvents.size()));
    result.put("subscriptionCommitIdGenerator", subscriptionCommitIdGenerator.toString());
    result.put("isCompleted", String.valueOf(isCompleted));
    result.put("isClosed", String.valueOf(isClosed));
    return result;
  }

  protected Map<String, String> allReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("brokerId", brokerId);
    result.put("topicName", topicName);
    result.put("size of inputPendingQueue", String.valueOf(inputPendingQueue.size()));
    result.put("size of prefetchingQueue", String.valueOf(prefetchingQueue.size()));
    result.put("uncommittedEvents", uncommittedEvents.toString());
    result.put("subscriptionCommitIdGenerator", subscriptionCommitIdGenerator.toString());
    result.put("inFlightSubscriptionEventMap", inFlightSubscriptionEventMap.toString());
    result.put("isCompleted", String.valueOf(isCompleted));
    result.put("isClosed", String.valueOf(isClosed));
    return result;
  }
}
