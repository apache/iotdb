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

package org.apache.iotdb.db.pipe.subscription.task.subtask;

import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;

public class PipePullOnlyConnectorSubtask {

  private final String taskID;

  private final long creationTime;

  private BoundedBlockingPendingQueue<Event> inputPendingQueue;

  private final String topicName;

  private final String consumerGroupID;

  public PipePullOnlyConnectorSubtask(
      String taskID,
      long creationTime,
      BoundedBlockingPendingQueue<Event> inputPendingQueue,
      String topicName,
      String consumerGroupID) {
    this.taskID = taskID;
    this.creationTime = creationTime;
    this.inputPendingQueue = inputPendingQueue;
    this.topicName = topicName;
    this.consumerGroupID = consumerGroupID;
    // TODO: metrics
  }

  public String getTopicName() {
    return topicName;
  }

  public String getConsumerGroupID() {
    return consumerGroupID;
  }

  public BoundedBlockingPendingQueue<Event> getInputPendingQueue() {
    return inputPendingQueue;
  }
}
