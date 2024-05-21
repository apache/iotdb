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

import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEventBinaryCache;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public abstract class SubscriptionPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPrefetchingQueue.class);

  protected final String brokerId; // consumer group id
  protected final String topicName;
  protected final UnboundedBlockingPendingQueue<Event> inputPendingQueue;

  protected final Map<SubscriptionCommitContext, SubscriptionEvent> uncommittedEvents;
  private final AtomicLong subscriptionCommitIdGenerator = new AtomicLong(0);

  public SubscriptionPrefetchingQueue(
      final String brokerId,
      final String topicName,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;

    this.uncommittedEvents = new ConcurrentHashMap<>();
  }

  public abstract SubscriptionEvent poll(final String consumerId);

  public abstract void executePrefetch();

  /** clean up uncommitted events */
  public void cleanup() {
    for (final SubscriptionEvent event : uncommittedEvents.values()) {
      SubscriptionEventBinaryCache.getInstance().resetByteBuffer(event, true);
    }
  }

  /////////////////////////////// commit ///////////////////////////////

  /**
   * @return {@code true} if ack successfully
   */
  public boolean ack(final SubscriptionCommitContext commitContext) {
    final SubscriptionEvent event = uncommittedEvents.get(commitContext);
    if (Objects.isNull(event)) {
      LOGGER.warn(
          "Subscription: subscription commit context [{}] does not exist, it may have been committed or something unexpected happened, prefetching queue: {}",
          commitContext,
          this);
      return false;
    }
    event.decreaseReferenceCount();
    event.recordCommittedTimestamp();
    SubscriptionEventBinaryCache.getInstance().resetByteBuffer(event, true);
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
        PipeAgent.runtime().getRebootTimes(),
        topicName,
        brokerId,
        subscriptionCommitIdGenerator.getAndIncrement());
  }

  protected SubscriptionCommitContext generateInvalidSubscriptionCommitContext() {
    return new SubscriptionCommitContext(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        PipeAgent.runtime().getRebootTimes(),
        topicName,
        brokerId,
        -1);
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
}
