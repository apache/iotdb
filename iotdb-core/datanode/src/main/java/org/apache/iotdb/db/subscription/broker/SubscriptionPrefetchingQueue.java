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
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionPrefetchingQueue {

  private final String brokerID; // consumer group ID

  private final String topicName;

  private final BoundedBlockingPendingQueue<Event> inputPendingQueue;

  private final Deque<TabletInsertionEvent> prefetchingTabletInsertionEvents;

  private final Deque<TsFileInsertionEvent> prefetchingTsFileInsertionEvent;

  private final Map<String, EnrichedEvent> uncommittedEvents;

  private final AtomicLong idGenerator = new AtomicLong(0);

  public SubscriptionPrefetchingQueue(
      String brokerID, String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.brokerID = brokerID;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;
    this.prefetchingTabletInsertionEvents = new LinkedList<>();
    this.prefetchingTsFileInsertionEvent = new LinkedList<>();
    this.uncommittedEvents = new HashMap<>();
  }

  public EnrichedTablets fetch() {
    prefetch(16);
    return toEnrichedTablets();
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
  private Pair<Long, EnrichedTablets> prefetchV2(long limit) {
    List<Tablet> tablets = new ArrayList<>();
    List<String> subscriptionCommitIds = new ArrayList<>();

    Event event;
    while ((event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll())) != null) {
      if (event instanceof TabletInsertionEvent) {
        Pair<Tablet, String> tabletWithSubscriptionId =
            convertToTablet((TabletInsertionEvent) event);
        tablets.add(tabletWithSubscriptionId.left);
        subscriptionCommitIds.add(tabletWithSubscriptionId.right);
        if (tablets.size() >= limit) {
          break;
        }
      } else if (event instanceof TsFileInsertionEvent) {
        if (event instanceof PipeTsFileInsertionEvent) {
          for (TabletInsertionEvent tabletInsertionEvent :
              ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
            Pair<Tablet, String> tabletWithSubscriptionId = convertToTablet(tabletInsertionEvent);
            tablets.add(tabletWithSubscriptionId.left);
          }
          subscriptionCommitIds.add(
              ((PipeTsFileInsertionEvent) event).generateSubscriptionCommitId());
        }
        if (tablets.size() >= limit) {
          break;
        }
      }
    }

    return new Pair<>(
        idGenerator.getAndIncrement(),
        new EnrichedTablets(topicName, tablets, subscriptionCommitIds));
  }

  /** @return true if prefetch @param maxSize events */
  public boolean prefetch(long maxSize) {
    int size = 0;
    Event event;
    while ((event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll())) != null) {
      if (event instanceof TabletInsertionEvent) {
        if (!(event instanceof PipeInsertNodeTabletInsertionEvent)
            && !(event instanceof PipeRawTabletInsertionEvent)) {
          // TODO: logger warn
          continue;
        }
        prefetchingTabletInsertionEvents.offer((TabletInsertionEvent) event);
        size++;
      } else if (event instanceof TsFileInsertionEvent) {
        if (!(event instanceof PipeTsFileInsertionEvent)) {
          // TODO: logger warn
          continue;
        }
        prefetchingTsFileInsertionEvent.offer((TsFileInsertionEvent) event);
        size++;
      }
      if (size == maxSize) {
        break;
      }
    }
    return size == maxSize;
  }

  private EnrichedTablets toEnrichedTablets() {
    final List<Tablet> tablets = new ArrayList<>();
    final List<String> subscriptionCommitIds = new ArrayList<>();

    while (!prefetchingTabletInsertionEvents.isEmpty()) {
      TabletInsertionEvent tabletInsertionEvent = prefetchingTabletInsertionEvents.poll();
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        PipeInsertNodeTabletInsertionEvent insertNodeTabletInsertionEvent =
            (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
        tablets.add(insertNodeTabletInsertionEvent.convertToTablet());
        String subscriptionCommitId = insertNodeTabletInsertionEvent.generateSubscriptionCommitId();
        subscriptionCommitIds.add(subscriptionCommitId);
        uncommittedEvents.put(subscriptionCommitId, insertNodeTabletInsertionEvent);
      } else { // PipeRawTabletInsertionEvent
        PipeRawTabletInsertionEvent rawTabletInsertionEvent =
            (PipeRawTabletInsertionEvent) tabletInsertionEvent;
        tablets.add(rawTabletInsertionEvent.convertToTablet());
        String subscriptionCommitId = rawTabletInsertionEvent.generateSubscriptionCommitId();
        subscriptionCommitIds.add(subscriptionCommitId);
        uncommittedEvents.put(subscriptionCommitId, rawTabletInsertionEvent);
      }
    }

    while (!prefetchingTsFileInsertionEvent.isEmpty()) {
      TsFileInsertionEvent tsFileInsertionEvent = prefetchingTsFileInsertionEvent.poll();
      for (TabletInsertionEvent tabletInsertionEvent :
          tsFileInsertionEvent.toTabletInsertionEvents()) {
        if (!(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
          // TODO: logger warn
          continue;
        }
        PipeRawTabletInsertionEvent rawTabletInsertionEvent =
            (PipeRawTabletInsertionEvent) tabletInsertionEvent;
        tablets.add(rawTabletInsertionEvent.convertToTablet());
      }
      String subscriptionCommitId =
          ((PipeTsFileInsertionEvent) tsFileInsertionEvent).generateSubscriptionCommitId();
      subscriptionCommitIds.add(subscriptionCommitId);
      uncommittedEvents.put(subscriptionCommitId, (PipeTsFileInsertionEvent) tsFileInsertionEvent);
    }

    return new EnrichedTablets(topicName, tablets, subscriptionCommitIds);
  }
}
