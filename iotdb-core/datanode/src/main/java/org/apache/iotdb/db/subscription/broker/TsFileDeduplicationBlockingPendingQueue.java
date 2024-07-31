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

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
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

  private final Cache<Integer, Boolean> hashCodeToIsGeneratedByHistoricalExtractor;

  public TsFileDeduplicationBlockingPendingQueue(
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(inputPendingQueue);

    this.hashCodeToIsGeneratedByHistoricalExtractor =
        Caffeine.newBuilder()
            .expireAfterAccess(
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

    if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) event;
      final EnrichedEvent sourceEvent = pipeRawTabletInsertionEvent.getSourceEvent();
      if (sourceEvent instanceof PipeTsFileInsertionEvent
          && isDuplicated((PipeTsFileInsertionEvent) sourceEvent)) {
        // commit directly
        pipeRawTabletInsertionEvent.decreaseReferenceCount(
            TsFileDeduplicationBlockingPendingQueue.class.getName(), true);
        return null;
      }
    }

    if (event instanceof PipeTsFileInsertionEvent) {
      final PipeTsFileInsertionEvent pipeTsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
      if (isDuplicated(pipeTsFileInsertionEvent)) {
        // commit directly
        pipeTsFileInsertionEvent.decreaseReferenceCount(
            TsFileDeduplicationBlockingPendingQueue.class.getName(), true);
        return null;
      }
    }

    return event;
  }

  private boolean isDuplicated(final PipeTsFileInsertionEvent event) {
    final int hashCode = event.getTsFile().hashCode();
    final boolean isGeneratedByHistoricalExtractor = event.isGeneratedByHistoricalExtractor();
    final Boolean existedIsGeneratedByHistoricalExtractor =
        hashCodeToIsGeneratedByHistoricalExtractor.getIfPresent(hashCode);
    if (Objects.isNull(existedIsGeneratedByHistoricalExtractor)) {
      hashCodeToIsGeneratedByHistoricalExtractor.put(hashCode, isGeneratedByHistoricalExtractor);
      return false;
    }
    // Multiple PipeRawTabletInsertionEvents parsed from the same PipeTsFileInsertionEvent (i.e.,
    // with the same isGeneratedByHistoricalExtractor field) are not considered duplicates.
    if (Objects.equals(existedIsGeneratedByHistoricalExtractor, isGeneratedByHistoricalExtractor)) {
      return false;
    }
    LOGGER.info(
        "Subscription: Detect duplicated PipeTsFileInsertionEvent {}, commit it directly",
        event.coreReportMessage());
    return true;
  }
}
