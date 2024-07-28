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
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionPipeTsFileEventBatch extends SubscriptionPipeEventBatch {

  private final PipeTabletEventTsFileBatch batch;

  private boolean isSealed = false;

  public SubscriptionPipeTsFileEventBatch(
      final SubscriptionPrefetchingTsFileQueue prefetchingQueue,
      int maxDelayInMs,
      long maxBatchSizeInBytes) {
    super(prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);
    this.batch = new PipeTabletEventTsFileBatch(maxDelayInMs, maxBatchSizeInBytes);
  }

  @Override
  public synchronized List<SubscriptionEvent> onEvent(@Nullable final EnrichedEvent event)
      throws Exception {
    if (isSealed) {
      return Collections.emptyList();
    }
    if (Objects.nonNull(event) && event instanceof TabletInsertionEvent) {
      batch.onEvent((TabletInsertionEvent) event);
    }
    if (batch.shouldEmit()) {
      final List<SubscriptionEvent> events = generateSubscriptionEvents();
      isSealed = true;
      return events;
    }
    return Collections.emptyList();
  }

  @Override
  public synchronized void cleanUp() {
    // close batch, it includes clearing the reference count of events
    batch.close();
  }

  public synchronized void ack() {
    batch.decreaseEventsReferenceCount(this.getClass().getName(), true);
  }

  /////////////////////////////// utility ///////////////////////////////

  private List<SubscriptionEvent> generateSubscriptionEvents() throws Exception {
    final List<SubscriptionEvent> events = new ArrayList<>();
    final List<File> tsFiles = batch.sealTsFiles();
    final AtomicInteger referenceCount = new AtomicInteger(tsFiles.size());
    for (final File tsFile : tsFiles) {
      final SubscriptionCommitContext commitContext =
          prefetchingQueue.generateSubscriptionCommitContext();
      events.add(
          new SubscriptionEvent(
              new SubscriptionPipeTsFileBatchEvents(this, tsFile, referenceCount),
              new SubscriptionPollResponse(
                  SubscriptionPollResponseType.FILE_INIT.getType(),
                  new FileInitPayload(tsFile.getName()),
                  commitContext)));
    }
    return events;
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
