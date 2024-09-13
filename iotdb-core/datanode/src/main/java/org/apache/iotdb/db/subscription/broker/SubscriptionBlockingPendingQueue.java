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
import org.apache.iotdb.pipe.api.event.Event;

public abstract class SubscriptionBlockingPendingQueue {

  protected final UnboundedBlockingPendingQueue<Event> inputPendingQueue;

  public SubscriptionBlockingPendingQueue(
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.inputPendingQueue = inputPendingQueue;
  }

  public abstract Event waitedPoll();

  public int size() {
    return inputPendingQueue.size();
  }

  public boolean isEmpty() {
    return inputPendingQueue.isEmpty();
  }
}
