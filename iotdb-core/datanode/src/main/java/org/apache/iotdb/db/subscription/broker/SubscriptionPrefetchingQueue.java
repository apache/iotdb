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

import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionPrefetchingQueue implements Runnable {

  private final String brokerID; // consumer group ID

  private final String topicName;

  private final BoundedBlockingPendingQueue<Event> inputPendingQueue;

  private final Map<String, EnrichedEvent> uncommittedEvents;

  private final ConcurrentIterableLinkedQueue<Pair<ByteBuffer, EnrichedTablets>> prefetchingQueue;

  public SubscriptionPrefetchingQueue(
      String brokerID, String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.brokerID = brokerID;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;
    this.uncommittedEvents = new ConcurrentHashMap<>();
    this.prefetchingQueue = new ConcurrentIterableLinkedQueue<>();
  }

  @Override
  public void run() {}

  public void executeOnce() {
    prefetchOnce(16);
    serialize();
  }

  public ByteBuffer fetch() {
    prefetchOnce(16);
    serialize();

    // fetch
    Pair<ByteBuffer, EnrichedTablets> enrichedTablets;
    try (ConcurrentIterableLinkedQueue<Pair<ByteBuffer, EnrichedTablets>>.DynamicIterator iter =
        prefetchingQueue.iterateFromEarliest()) {
      if (Objects.nonNull(enrichedTablets = iter.next(1000))) {
        if (Objects.isNull(enrichedTablets.left)) {
          try {
            prefetchingQueue.tryRemoveBefore(iter.getNextIndex());
            return PipeSubscribePollResp.serializeEnrichedTablets(enrichedTablets.right);
          } catch (IOException e) {
            // TODO: logger warn
            return null;
          }
        }
        prefetchingQueue.tryRemoveBefore(iter.getNextIndex());
        return enrichedTablets.left;
      }
    }

    return null;
  }

  public void commit(List<String> subscriptionCommitIds) {
    for (String subscriptionCommitId : subscriptionCommitIds) {
      EnrichedEvent enrichedEvent = uncommittedEvents.get(subscriptionCommitId);
      if (Objects.isNull(enrichedEvent)) {
        // TODO: logger warn
        continue;
      }
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
      uncommittedEvents.remove(subscriptionCommitId);
    }
  }

  private Pair<Tablet, String> convertToTablet(TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      Tablet tablet = ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
      String subscriptionId =
          ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent)
              .generateSubscriptionCommitId();
      return new Pair<>(tablet, subscriptionId);
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      Tablet tablet = ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
      String subscriptionId =
          ((PipeRawTabletInsertionEvent) tabletInsertionEvent).generateSubscriptionCommitId();
      return new Pair<>(tablet, subscriptionId);
    }
    // TODO: logger warn
    return new Pair<>(null, null);
  }

  // TODO: use org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.calculateTabletSizeInBytes
  private void prefetchOnce(long limit) {
    List<Tablet> tablets = new ArrayList<>();
    List<String> subscriptionCommitIds = new ArrayList<>();

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (event instanceof TabletInsertionEvent && event instanceof EnrichedEvent) {
        Pair<Tablet, String> tabletWithSubscriptionId =
            convertToTablet((TabletInsertionEvent) event);
        tablets.add(tabletWithSubscriptionId.left);
        subscriptionCommitIds.add(tabletWithSubscriptionId.right);
        uncommittedEvents.put(tabletWithSubscriptionId.right, (EnrichedEvent) event);
        if (tablets.size() >= limit) {
          break;
        }
      } else if (event instanceof PipeTsFileInsertionEvent) {
        for (TabletInsertionEvent tabletInsertionEvent :
            ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
          Pair<Tablet, String> tabletWithSubscriptionId = convertToTablet(tabletInsertionEvent);
          tablets.add(tabletWithSubscriptionId.left);
        }
        String subscriptionCommitId =
            ((PipeTsFileInsertionEvent) event).generateSubscriptionCommitId();
        subscriptionCommitIds.add(
            ((PipeTsFileInsertionEvent) event).generateSubscriptionCommitId());
        uncommittedEvents.put(subscriptionCommitId, (PipeTsFileInsertionEvent) event);
        if (tablets.size() >= limit) {
          break;
        }
      }
    }

    if (!tablets.isEmpty()) {
      prefetchingQueue.add(
          new Pair<>(null, new EnrichedTablets(topicName, tablets, subscriptionCommitIds)));
    }
  }

  private void serialize() {
    Pair<ByteBuffer, EnrichedTablets> enrichedTablets;
    try (ConcurrentIterableLinkedQueue<Pair<ByteBuffer, EnrichedTablets>>.DynamicIterator iter =
        prefetchingQueue.iterateFromEarliest()) {
      // TODO: memory control
      while (Objects.nonNull(enrichedTablets = iter.next(1000))) {
        if (Objects.isNull(enrichedTablets.left)) {
          try {
            enrichedTablets.left =
                PipeSubscribePollResp.serializeEnrichedTablets(enrichedTablets.right);
          } catch (IOException e) {
            // TODO: logger warn
          }
        }
      }
    }
  }
}
