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

import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import java.io.File;
import java.util.List;

public class SubscriptionPipeTsFileEventBatch {

  private final PipeTabletEventTsFileBatch batch;

  public SubscriptionPipeTsFileEventBatch(
      final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    this.batch = new PipeTabletEventTsFileBatch(maxDelayInMs, requestMaxBatchSizeInBytes);
  }

  public synchronized List<File> sealTsFiles() throws Exception {
    return batch.sealTsFiles();
  }

  public synchronized boolean shouldEmit() {
    return batch.shouldEmit();
  }

  public synchronized boolean onEvent(final TabletInsertionEvent event) throws Exception {
    return batch.onEvent(event);
  }

  public synchronized void ack() {
    batch.decreaseEventsReferenceCount(this.getClass().getName(), true);
  }

  public synchronized void cleanup() {
    // close batch, it includes clearing the reference count of events
    batch.close();
  }

  public String toString() {
    return "SubscriptionPipeTsFileEventBatch{batch=" + batch + "}";
  }
}
