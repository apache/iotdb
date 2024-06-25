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

import java.util.stream.Collectors;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.write.record.Tablet;

import java.io.File;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionPipeTsFileEventBatch implements SubscriptionPipeEventBatch {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTsFileEventBatch.class);

  private final PipeTabletEventTsFileBatch batch;

  public SubscriptionPipeTsFileEventBatch(
      final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    this.batch = new PipeTabletEventTsFileBatch(maxDelayInMs, requestMaxBatchSizeInBytes);
  }

  @Override
  public List<File> sealTsFiles() throws Exception {
    return batch.sealTsFiles();
  }

  @Override
  public List<Tablet> sealTablets() {
    return Collections.emptyList();
  }

  @Override
  public boolean shouldEmit() {
    return batch.shouldEmit();
  }

  @Override
  public boolean onEvent(final EnrichedEvent event) throws Exception {
    if (event instanceof TabletInsertionEvent) {
      return batch.onEvent((TabletInsertionEvent) event);
    }

    LOGGER.warn(
        "SubscriptionPipeTsFileEventBatch {} only support convert TabletInsertionEvent to tsfile. Ignore {}.",
        this,
        event);
    return false;
  }

  @Override
  public void ack() {
    batch.decreaseEventsReferenceCount(this.getClass().getName(), true);
  }

  @Override
  public void cleanup() {
    // close batch, it includes clearing the reference count of events
    batch.close();
  }

  @Override
  public String toString() {
    return "SubscriptionPipeTsFileEventBatch{batch="
        + batch
        + "}";
  }
}
