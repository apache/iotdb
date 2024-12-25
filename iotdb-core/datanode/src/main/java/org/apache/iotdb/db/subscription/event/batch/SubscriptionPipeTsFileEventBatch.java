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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTsFileBatchEvents;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionPipeTsFileEventBatch extends SubscriptionPipeEventBatch {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTsFileEventBatch.class);

  private final PipeTabletEventTsFileBatch batch;

  public SubscriptionPipeTsFileEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTsFileQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    super(regionId, prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);
    this.batch = new PipeTabletEventTsFileBatch(maxDelayInMs, maxBatchSizeInBytes);
  }

  @Override
  public synchronized void ack() {
    batch.decreaseEventsReferenceCount(this.getClass().getName(), true);
  }

  @Override
  public synchronized void cleanUp() {
    // close batch, it includes clearing the reference count of events
    batch.close();
    enrichedEvents.clear();
  }

  /////////////////////////////// utility ///////////////////////////////

  @Override
  protected void onTabletInsertionEvent(final TabletInsertionEvent event) {
    try {
      batch.onEvent(event);
    } catch (final Exception ignored) {
      // no exceptions will be thrown
    }
    ((EnrichedEvent) event)
        .decreaseReferenceCount(
            SubscriptionPipeTsFileEventBatch.class.getName(),
            false); // missing releaseLastEvent decreases reference count
  }

  @Override
  protected void onTsFileInsertionEvent(final TsFileInsertionEvent event) {
    LOGGER.warn(
        "SubscriptionPipeTsFileEventBatch {} ignore TsFileInsertionEvent {} when batching.",
        this,
        event);
  }

  @Override
  protected List<SubscriptionEvent> generateSubscriptionEvents() throws Exception {
    if (batch.isEmpty()) {
      return null;
    }

    final List<SubscriptionEvent> events = new ArrayList<>();
    final List<File> tsFiles = batch.sealTsFiles();
    final AtomicInteger referenceCount = new AtomicInteger(tsFiles.size());
    for (final File tsFile : tsFiles) {
      final SubscriptionCommitContext commitContext =
          prefetchingQueue.generateSubscriptionCommitContext();
      events.add(
          new SubscriptionEvent(
              new SubscriptionPipeTsFileBatchEvents(this, referenceCount), tsFile, commitContext));
    }
    return events;
  }

  @Override
  protected boolean shouldEmit() {
    return batch.shouldEmit();
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPipeTsFileEventBatch" + this.coreReportMessage();
  }

  @Override
  protected Map<String, String> coreReportMessage() {
    final Map<String, String> coreReportMessage = super.coreReportMessage();
    coreReportMessage.put("batch", batch.toString());
    return coreReportMessage;
  }
}
