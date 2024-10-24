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

import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionPipeTsFileBatchEvents implements SubscriptionPipeEvents {

  private final SubscriptionPipeTsFileEventBatch batch;
  private final AtomicInteger referenceCount; // shared between the same batch
  private final int count; // snapshot the initial reference count, used for event count calculation

  public SubscriptionPipeTsFileBatchEvents(
      final SubscriptionPipeTsFileEventBatch batch, final AtomicInteger referenceCount) {
    this.batch = batch;
    this.referenceCount = referenceCount;
    this.count = Math.max(1, referenceCount.get());
  }

  @Override
  public void ack() {
    if (referenceCount.decrementAndGet() == 0) {
      batch.ack();
    }
  }

  @Override
  public void cleanUp() {
    if (referenceCount.decrementAndGet() == 0) {
      batch.cleanUp();
    }
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPipeTsFileBatchEvents{batch="
        + batch
        + ", referenceCount="
        + referenceCount
        + "}";
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  @Override
  public int getPipeEventCount() {
    // Since multiple events will share the same batch, equal division is performed here.
    // If it is not exact, round up to remain pessimistic.
    return (batch.getPipeEventCount() + count - 1) / count;
  }
}
