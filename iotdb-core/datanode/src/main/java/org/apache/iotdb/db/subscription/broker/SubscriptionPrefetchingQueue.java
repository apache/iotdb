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

import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBroker.class);

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

  /////////////////////////////// provided for SubscriptionBroker ///////////////////////////////

  public ByteBuffer poll() {
    if (prefetchingQueue.isEmpty()) {
      prefetchOnce(SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching());
      // without serializeOnce here
    }

    Pair<ByteBuffer, EnrichedTablets> enrichedTablets;
    ByteBuffer byteBuffer;
    try (ConcurrentIterableLinkedQueue<Pair<ByteBuffer, EnrichedTablets>>.DynamicIterator iter =
        prefetchingQueue.iterateFromEarliest()) {
      if (Objects.nonNull(
          enrichedTablets =
              iter.next(SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs()))) {
        if (Objects.isNull(byteBuffer = enrichedTablets.left)) {
          try {
            byteBuffer = PipeSubscribePollResp.serializeEnrichedTablets(enrichedTablets.right);
          } catch (IOException e) {
            LOGGER.warn(
                "Subscription: something unexpected happened when serializing EnrichedTablets: {}",
                e.getMessage());
            return null;
          }
        }
        prefetchingQueue.tryRemoveBefore(iter.getNextIndex());
        return byteBuffer;
      }
    }

    return null;
  }

  public void commit(List<String> subscriptionCommitIds) {
    for (String subscriptionCommitId : subscriptionCommitIds) {
      EnrichedEvent enrichedEvent = uncommittedEvents.get(subscriptionCommitId);
      if (Objects.isNull(enrichedEvent)) {
        LOGGER.warn(
            "Subscription: broken invariants, subscription commit id [{}] does not exist, something unexpected happened",
            subscriptionCommitId);
        continue;
      }
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
      uncommittedEvents.remove(subscriptionCommitId);
    }
  }

  /////////////////////////////// prefetch ///////////////////////////////

  public void executePrefetch() {
    prefetchOnce(SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching());
    serializeOnce();
  }

  // TODO: use org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.calculateTabletSizeInBytes
  // for limit control
  private void prefetchOnce(long limit) {
    List<Tablet> tablets = new ArrayList<>();
    List<String> subscriptionCommitIds = new ArrayList<>();

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (!(event instanceof EnrichedEvent)) {
        // subscription agent should fetch committerKey and commitId of events
        LOGGER.warn("Subscription: Only support prefetch EnrichedEvent. Ignore {}.", event);
        continue;
      }

      if (event instanceof TabletInsertionEvent) {
        Pair<Tablet, String> tabletWithSubscriptionId =
            convertToTabletWithSubscriptionCommitId((TabletInsertionEvent) event);
        if (Objects.isNull(tabletWithSubscriptionId)) {
          continue;
        }
        tablets.add(tabletWithSubscriptionId.left);
        subscriptionCommitIds.add(tabletWithSubscriptionId.right);
        uncommittedEvents.put(tabletWithSubscriptionId.right, (EnrichedEvent) event);
        if (tablets.size() >= limit) {
          break;
        }
      } else if (event instanceof PipeTsFileInsertionEvent) {
        for (TabletInsertionEvent tabletInsertionEvent :
            ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
          Pair<Tablet, String> tabletWithSubscriptionId =
              convertToTabletWithSubscriptionCommitId(tabletInsertionEvent);
          if (Objects.isNull(tabletWithSubscriptionId)) {
            continue;
          }
          tablets.add(tabletWithSubscriptionId.left);
          // committerKey and commitId is missing in events generated by toTabletInsertionEvents
        }
        String subscriptionCommitId =
            ((PipeTsFileInsertionEvent) event).generateSubscriptionCommitId();
        subscriptionCommitIds.add(subscriptionCommitId);
        uncommittedEvents.put(subscriptionCommitId, (PipeTsFileInsertionEvent) event);
        if (tablets.size() >= limit) {
          break;
        }
      } else {
        // TODO:
        //  - PipeHeartbeatEvent: ignored? (may affect pipe metrics)
        //  - UserDefinedEnrichedEvent: ignored?
        //  - Others: events related to meta sync, safe to ignore
        LOGGER.warn("Subscription: Ignore EnrichedEvent {} when prefetching.", event);
      }
    }

    if (!tablets.isEmpty()) {
      prefetchingQueue.add(
          new Pair<>(null, new EnrichedTablets(topicName, tablets, subscriptionCommitIds)));
    }
  }

  private void serializeOnce() {
    Pair<ByteBuffer, EnrichedTablets> enrichedTablets;
    try (ConcurrentIterableLinkedQueue<Pair<ByteBuffer, EnrichedTablets>>.DynamicIterator iter =
        prefetchingQueue.iterateFromEarliest()) {
      // TODO: memory control
      while (Objects.nonNull(
          enrichedTablets =
              iter.next(
                  SubscriptionConfig.getInstance().getSubscriptionSerializeMaxBlockingTimeMs()))) {
        if (Objects.isNull(enrichedTablets.left)) {
          try {
            enrichedTablets.left =
                PipeSubscribePollResp.serializeEnrichedTablets(enrichedTablets.right);
          } catch (IOException e) {
            LOGGER.warn(
                "Subscription: something unexpected happened when serializing EnrichedTablets: {}",
                e.getMessage());
          }
        }
      }
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private Pair<Tablet, String> convertToTabletWithSubscriptionCommitId(
      TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      Tablet tablet = ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
      String subscriptionCommitId =
          ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent)
              .generateSubscriptionCommitId();
      return new Pair<>(tablet, subscriptionCommitId);
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      Tablet tablet = ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
      String subscriptionId =
          ((PipeRawTabletInsertionEvent) tabletInsertionEvent).generateSubscriptionCommitId();
      return new Pair<>(tablet, subscriptionId);
    }

    LOGGER.warn(
        "Subscription: Only support convert PipeInsertNodeTabletInsertionEvent or PipeRawTabletInsertionEvent to tablet. Ignore {}.",
        tabletInsertionEvent);
    return null;
  }
}
