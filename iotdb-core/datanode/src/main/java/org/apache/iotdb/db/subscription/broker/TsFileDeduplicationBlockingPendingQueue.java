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
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TsFileDeduplicationBlockingPendingQueue extends SubscriptionBlockingPendingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileDeduplicationBlockingPendingQueue.class);

  private final Cache<Integer, Integer> polledTsFiles;

  public TsFileDeduplicationBlockingPendingQueue(
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(inputPendingQueue);

    this.polledTsFiles =
        Caffeine.newBuilder()
            .expireAfterWrite(
                SubscriptionConfig.getInstance().getSubscriptionTsFileDeduplicationWindowSeconds(),
                TimeUnit.SECONDS)
            .build();
  }

  @Override
  public Event waitedPoll() {
    return filter(inputPendingQueue.waitedPoll());
  }

  private synchronized Event filter(final Event event) { // make it synchronized
    if (Objects.isNull(event)) {
      return null;
    }

    if (event instanceof PipeTsFileInsertionEvent) {
      final PipeTsFileInsertionEvent pipeTsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
      final int hashcode = pipeTsFileInsertionEvent.getTsFile().hashCode();
      if (Objects.nonNull(polledTsFiles.getIfPresent(hashcode))) {
        LOGGER.info(
            "Subscription: Detect duplicated PipeTsFileInsertionEvent {}, commit it directly",
            pipeTsFileInsertionEvent.coreReportMessage());
        // commit directly
        pipeTsFileInsertionEvent.decreaseReferenceCount(
            TsFileDeduplicationBlockingPendingQueue.class.getName(), true);
        return null;
      }
      polledTsFiles.put(hashcode, hashcode);
    }

    return event;
  }
}
