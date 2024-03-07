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
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class SubscriptionBroker {

  private final String brokerID; // consumer group ID

  private final TreeMap<String, SubscriptionPrefetchingQueue> topicNameToPrefetchingQueue;

  public SubscriptionBroker(String brokerID) {
    this.brokerID = brokerID;
    this.topicNameToPrefetchingQueue = new TreeMap<>();
  }

  public Iterable<EnrichedTablets> poll(Set<String> topicNames) {
    List<EnrichedTablets> enrichedTabletsList = new ArrayList<>();
    topicNameToPrefetchingQueue.forEach(
        (topicName, prefetchingQueue) -> {
          if (topicNames.contains(topicName)) {
            enrichedTabletsList.add(prefetchingQueue.fetch());
          }
        });
    return enrichedTabletsList;
  }

  public void commit(List<Pair<String, Long>> committerKeyAndCommitIds) {
    topicNameToPrefetchingQueue
        .values()
        .forEach((prefetchingQueue) -> prefetchingQueue.commit(committerKeyAndCommitIds));
  }

  public void bindPrefetchingQueue(
      String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    SubscriptionPrefetchingQueue prefetchingQueue = topicNameToPrefetchingQueue.get(topicName);
    if (Objects.nonNull(prefetchingQueue)) {
      // TODO: handle error
      return;
    }
    topicNameToPrefetchingQueue.put(
        topicName, new SubscriptionPrefetchingQueue(brokerID, topicName, inputPendingQueue));
  }

  public void unbindPrefetchingQueue(String topicName) {
    SubscriptionPrefetchingQueue prefetchingQueue = topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      // TODO: handle error
      return;
    }
    // TODO: do something for events on-the-fly
    topicNameToPrefetchingQueue.remove(topicName);
  }
}
