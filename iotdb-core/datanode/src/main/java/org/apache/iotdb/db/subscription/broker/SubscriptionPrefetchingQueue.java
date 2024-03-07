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

import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.connection.EnrichedDeque;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

public class SubscriptionPrefetchingQueue {

  private String brokerID; // consumer group ID

  private String topicName;

  private EnrichedDeque<TabletInsertionEvent> prefetchingTabletInsertionEvents;

  private EnrichedDeque<TsFileInsertionEvent> prefetchingTsFileInsertionEvent;

  private BoundedBlockingPendingQueue<Event> inputPendingQueue;

  public SubscriptionPrefetchingQueue(
      String brokerID, String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.brokerID = brokerID;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;
  }
}
