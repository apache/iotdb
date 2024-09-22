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

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

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

  @Override
  protected boolean executeOnce() {
    if (isClosed.get()) {
      return false;
    }

    SubscriptionAgent.broker().executePrefetch(consumerGroupId, topicName);
    // always return true
    return true;
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

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  @Override
  public int getEventCount(final String pipeName) {
    final AtomicInteger count = new AtomicInteger(0);
    try {
      inputPendingQueue.forEach(
          event -> {
            if (event instanceof EnrichedEvent
                && pipeName.equals(((EnrichedEvent) event).getPipeName())) {
              count.incrementAndGet();
            }
          });
    } catch (final Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Exception occurred when counting event of pipe {}, root cause: {}",
            pipeName,
            ErrorHandlingUtils.getRootCause(e).getMessage(),
            e);
      }
    }
    // it's safe to ignore lastEvent and don't forget to count the pipe events in the prefetching
    // queue
    return count.get() + SubscriptionAgent.broker().getPipeEventCount(consumerGroupId, topicName);
  }
}
