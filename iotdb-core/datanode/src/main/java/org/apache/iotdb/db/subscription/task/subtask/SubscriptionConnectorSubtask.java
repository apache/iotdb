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

package org.apache.iotdb.db.subscription.task.subtask;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.agent.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class SubscriptionConnectorSubtask extends PipeConnectorSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConnectorSubtask.class);

  private final String topicName;
  private final String consumerGroupId;

  public SubscriptionConnectorSubtask(
      final String taskID,
      final long creationTime,
      final String attributeSortedString,
      final int connectorIndex,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue,
      final PipeConnector outputPipeConnector,
      final String topicName,
      final String consumerGroupId) {
    super(
        taskID,
        creationTime,
        attributeSortedString,
        connectorIndex,
        inputPendingQueue,
        outputPipeConnector);
    this.topicName = topicName;
    this.consumerGroupId = consumerGroupId;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public UnboundedBlockingPendingQueue<Event> getInputPendingQueue() {
    return inputPendingQueue;
  }

  //////////////////////////// execution & callback ////////////////////////////

  @Override
  protected void registerCallbackHookAfterSubmit(final ListenableFuture<Boolean> future) {
    // TODO: timeout config
    final ListenableFuture<Boolean> nextFuture =
        Futures.withTimeout(future, Duration.ofSeconds(10), subtaskCallbackListeningExecutor);
    Futures.addCallback(nextFuture, this, subtaskCallbackListeningExecutor);
  }

  @Override
  public synchronized void onFailure(final Throwable throwable) {
    isSubmitted = false;

    // just resubmit
    submitSelf();
  }

  @Override
  protected boolean executeOnce() {
    if (isClosed.get()) {
      return false;
    }

    return SubscriptionAgent.broker().executePrefetch(consumerGroupId, topicName);
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  @Override
  public int getEventCount(final String pipeName) {
    // count the number of pipe events in sink queue and prefetching queue, note that can safely
    // ignore lastEvent
    return SubscriptionAgent.broker().getPipeEventCount(consumerGroupId, topicName);
  }
}
