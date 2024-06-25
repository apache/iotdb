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

package org.apache.iotdb.db.subscription.event.pipe;

import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTsFileEventBatch;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionPipeTsFileBatchEvents implements SubscriptionPipeEvents {

  private final SubscriptionPipeTsFileEventBatch batch;
  private final File tsFile;

  private final AtomicInteger referenceCount; // shared between the same batch
  private final AtomicBoolean isAcked = new AtomicBoolean(false);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public SubscriptionPipeTsFileBatchEvents(
      final SubscriptionPipeTsFileEventBatch batch,
      final File tsFile,
      final AtomicInteger referenceCount) {
    this.batch = batch;
    this.tsFile = tsFile;
    this.referenceCount = referenceCount;
  }

  @Override
  public File getTsFile() {
    return tsFile;
  }

  @Override
  public void ack() {
    if (!isAcked.get() && referenceCount.decrementAndGet() == 0) {
      isAcked.set(true);
      batch.ack();
    }
  }

  @Override
  public void cleanup() {
    if (!isClosed.get() && referenceCount.decrementAndGet() == 0) {
      isClosed.set(true);
      // close batch, it includes clearing the reference count of events
      batch.cleanup();
    }
  }

  @Override
  public String toString() {
    return "SubscriptionEvent{tsFile="
        + tsFile
        + ", referenceCount="
        + referenceCount
        + ", isAcked="
        + isAcked
        + ", isClosed="
        + isClosed
        + "}";
  }
}
