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

import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPipeEventBatchesConcurrencyTest {

  @Test
  public void concurrentPrefetchUpdatesDoNotBreakPollSnapshots() throws Exception {
    final SubscriptionPipeEventBatches batches = new SubscriptionPipeEventBatches(null, 1000, 1024);
    final Map<Integer, SubscriptionPipeEventBatch> regionIdToBatch = getRegionIdToBatch(batches);
    final SubscriptionPipeEventBatchSegmentLock segmentLock = getSegmentLock(batches);
    final SubscriptionPipeEventBatch batch = new EmptySubscriptionPipeEventBatch();
    for (int regionId = 0; regionId < 3; regionId++) {
      regionIdToBatch.put(regionId, batch);
    }

    final AtomicBoolean prefetchDone = new AtomicBoolean(false);
    final AtomicBoolean pollingStarted = new AtomicBoolean(false);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread prefetchThread =
        new Thread(
            () -> {
              try {
                while (!pollingStarted.get()) {
                  Thread.yield();
                }
                for (int regionId = 3; regionId < 20_000; regionId++) {
                  segmentLock.lock(regionId);
                  try {
                    regionIdToBatch.put(regionId, batch);
                  } finally {
                    segmentLock.unlock(regionId);
                  }

                  final int removedRegionId = regionId - 3;
                  segmentLock.lock(removedRegionId);
                  try {
                    regionIdToBatch.remove(removedRegionId);
                  } finally {
                    segmentLock.unlock(removedRegionId);
                  }
                }
              } catch (final Throwable t) {
                failure.compareAndSet(null, t);
              } finally {
                prefetchDone.set(true);
              }
            },
            "subscription-prefetch-map-updater");
    prefetchThread.start();

    try {
      batches.onEvent(event -> {});
      batches.emitAll(event -> {});
      pollingStarted.set(true);
      while (!prefetchDone.get() && failure.get() == null) {
        batches.onEvent(event -> {});
        batches.emitAll(event -> {});
      }
    } catch (final Throwable t) {
      failure.compareAndSet(null, t);
    } finally {
      prefetchThread.join();
    }

    Assert.assertNull(failure.get());
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, SubscriptionPipeEventBatch> getRegionIdToBatch(
      final SubscriptionPipeEventBatches batches) throws Exception {
    final Field field = SubscriptionPipeEventBatches.class.getDeclaredField("regionIdToBatch");
    field.setAccessible(true);
    return (Map<Integer, SubscriptionPipeEventBatch>) field.get(batches);
  }

  private SubscriptionPipeEventBatchSegmentLock getSegmentLock(
      final SubscriptionPipeEventBatches batches) throws Exception {
    final Field field = SubscriptionPipeEventBatches.class.getDeclaredField("segmentLock");
    field.setAccessible(true);
    return (SubscriptionPipeEventBatchSegmentLock) field.get(batches);
  }

  private static class EmptySubscriptionPipeEventBatch extends SubscriptionPipeEventBatch {

    private EmptySubscriptionPipeEventBatch() {
      super(0, (SubscriptionPrefetchingQueue) null, 0, 0);
    }

    @Override
    public void ack() {}

    @Override
    public void cleanUp(final boolean force) {}

    @Override
    protected void onTabletInsertionEvent(final TabletInsertionEvent event) {}

    @Override
    protected void onTsFileInsertionEvent(final TsFileInsertionEvent event) {}

    @Override
    protected boolean shouldEmit() {
      return false;
    }

    @Override
    protected List<SubscriptionEvent> generateSubscriptionEvents() {
      return Collections.emptyList();
    }
  }
}
