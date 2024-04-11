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
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.TsFileSubscriptionEvent;
import org.apache.iotdb.db.subscription.timer.SubscriptionPollTimer;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPrefetchingTsFileQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTsFileQueue.class);

  private final AtomicReference<TsFileSubscriptionEvent> eventRef;

  public SubscriptionPrefetchingTsFileQueue(
      final String brokerId,
      final String topicName,
      final BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.eventRef = new AtomicReference<>();
  }

  @Override
  public SubscriptionEvent poll(final SubscriptionPollTimer timer) {
    if (Objects.nonNull(eventRef.get())) {
      return null;
    }

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (!(event instanceof PipeTsFileInsertionEvent)) {
        LOGGER.warn(
            "Subscription: SubscriptionPrefetchingTsFileQueue only support poll PipeTsFileInsertionEvent. Ignore {}.",
            event);
        continue;
      }

      final PipeTsFileInsertionEvent tsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
      final String subscriptionCommitId = generateSubscriptionCommitContext();
      final TsFileSubscriptionEvent subscriptionEvent =
          new TsFileSubscriptionEvent(
              Collections.singletonList(tsFileInsertionEvent),
              subscriptionCommitId,
              topicName,
              tsFileInsertionEvent.getTsFile().getName());
      eventRef.set(subscriptionEvent);
      uncommittedEvents.put(subscriptionCommitId, subscriptionEvent);
      return subscriptionEvent;
    }

    return null;
  }

  @Override
  public void executePrefetch() {
    // do nothing now
  }
}
