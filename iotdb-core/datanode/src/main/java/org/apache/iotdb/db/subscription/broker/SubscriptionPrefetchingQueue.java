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
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeEventBatch;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEmptyEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public abstract class SubscriptionPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPrefetchingQueue.class);

  protected final String brokerId; // consumer group id
  protected final String topicName;
  protected final UnboundedBlockingPendingQueue<Event> inputPendingQueue;
  protected final LinkedBlockingQueue<SubscriptionEvent> prefetchingQueue;

  protected final Map<SubscriptionCommitContext, SubscriptionEvent> uncommittedEvents;
  private final AtomicLong subscriptionCommitIdGenerator = new AtomicLong(0);

  private volatile boolean isCompleted = false;
  private volatile boolean isClosed = false;

  protected final int maxDelayInMs = SubscriptionConfig.getInstance().getSubscriptionPrefetchBatchMaxDelayInMs();
  protected final long maxBatchSizeInBytes = SubscriptionConfig.getInstance().getSubscriptionPrefetchBatchMaxSizeInBytes();
  protected final AtomicReference<SubscriptionPipeEventBatch> currentBatchRef =
      new AtomicReference<>();

  public SubscriptionPrefetchingQueue(
      final String brokerId,
      final String topicName,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;

    this.prefetchingQueue = new LinkedBlockingQueue<>();
    this.uncommittedEvents = new ConcurrentHashMap<>();
  }

  public SubscriptionEvent poll(final String consumerId) {
    if (prefetchingQueue.isEmpty()) {
      prefetchOnce();
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
          event.cleanup();
          continue;
        }
        if (!event.pollable()) {
          // Re-enqueue the uncommitted event at the end of the queue.
          prefetchingQueue.add(event);
          continue;
        }
        event.recordLastPolledConsumerId(consumerId);
        event.recordLastPolledTimestamp();
        // Re-enqueue the uncommitted event at the end of the queue.
        // This operation should be performed after recordLastPolledTimestamp to prevent multiple
        // consumers from consuming the same event.
        prefetchingQueue.add(event);
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

  protected abstract boolean prefetchTabletInsertionEvent(final TabletInsertionEvent event);

  protected abstract boolean prefetchTsFileInsertionEvent(final PipeTsFileInsertionEvent event);

  protected abstract boolean prefetchEnrichedEvent();

  protected void prefetchOnce() {
    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
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
        if (prefetchTabletInsertionEvent((TabletInsertionEvent) event)) {
          break;
        }
      } else if (event instanceof PipeTsFileInsertionEvent) {
        if (prefetchTsFileInsertionEvent((PipeTsFileInsertionEvent) event)) {
          break;
        }
      } else {
        // TODO:
        //  - PipeHeartbeatEvent: ignored? (may affect pipe metrics)
        //  - UserDefinedEnrichedEvent: ignored?
        //  - Others: events related to meta sync, safe to ignore
        LOGGER.info(
            "Subscription: SubscriptionPrefetchingQueue {} ignore EnrichedEvent {} when prefetching.",
            this,
            event);
        if (prefetchEnrichedEvent()) {
          break;
        }
      }
    }
  }

  public abstract void executePrefetch();

  public void cleanup() {
    // clean up uncommitted events
    uncommittedEvents.values().forEach(SubscriptionEvent::cleanup);
    uncommittedEvents.clear();

    // clean up batch
    currentBatchRef.getAndUpdate(
        (batch) -> {
          if (Objects.nonNull(batch)) {
            batch.cleanup();
          }
          return null;
        });

    // no need to clean up events in inputPendingQueue, see
    // org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask.close
  }

  /////////////////////////////// commit ///////////////////////////////

  /**
   * @return {@code true} if ack successfully
   */
  public boolean ack(final SubscriptionCommitContext commitContext) {
    final SubscriptionEvent event = uncommittedEvents.get(commitContext);
    if (Objects.isNull(event)) {
      LOGGER.warn(
          "Subscription: subscription commit context {} does not exist, it may have been committed or something unexpected happened, prefetching queue: {}",
          commitContext,
          this);
      return false;
    }

    if (event.isCommitted()) {
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

    event.ack();
    event.cleanup();

    event.recordCommittedTimestamp();
    uncommittedEvents.remove(commitContext);
    return true;
  }

  /**
   * @return {@code true} if nack successfully
   */
  public boolean nack(final SubscriptionCommitContext commitContext) {
    final SubscriptionEvent event = uncommittedEvents.get(commitContext);
    if (Objects.isNull(event)) {
      LOGGER.warn(
          "Subscription: subscription commit context [{}] does not exist, it may have been committed or something unexpected happened, prefetching queue: {}",
          commitContext,
          this);
      return false;
    }
    event.nack();
    return true;
  }

  protected SubscriptionCommitContext generateSubscriptionCommitContext() {
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

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPrefetchingQueue{brokerId=" + brokerId + ", topicName=" + topicName + "}";
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
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.TERMINATION.getType(),
            new TerminationPayload(),
            generateInvalidSubscriptionCommitContext()));
  }

  public SubscriptionEvent generateSubscriptionPollErrorResponse(
      final String errorMessage, final boolean critical) {
    return new SubscriptionEvent(
        new SubscriptionPipeEmptyEvent(),
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.ERROR.getType(),
            new ErrorPayload(errorMessage, critical),
            generateInvalidSubscriptionCommitContext()));
  }
}
