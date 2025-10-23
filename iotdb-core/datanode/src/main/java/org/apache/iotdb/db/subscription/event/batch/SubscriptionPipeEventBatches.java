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
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SubscriptionPipeEventBatches {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPipeEventBatches.class);

  protected final SubscriptionPrefetchingQueue prefetchingQueue;
  protected final int maxDelayInMs;
  protected final long maxBatchSizeInBytes;

  private final Map<Integer, SubscriptionPipeEventBatch> regionIdToBatch;
  private final SubscriptionPipeEventBatchSegmentLock segmentLock;

  public SubscriptionPipeEventBatches(
      final SubscriptionPrefetchingQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;

    this.regionIdToBatch = new HashMap<>();
    this.segmentLock = new SubscriptionPipeEventBatchSegmentLock();
  }

  /**
   * @return {@code true} if there are subscription events consumed.
   */
  public boolean onEvent(final Consumer<SubscriptionEvent> consumer) {
    final AtomicBoolean hasNew = new AtomicBoolean(false);
    for (final int regionId : ImmutableList.copyOf(regionIdToBatch.keySet())) {
      try {
        segmentLock.lock(regionId);
        final SubscriptionPipeEventBatch batch = regionIdToBatch.get(regionId);
        if (Objects.isNull(batch)) {
          continue;
        }
        try {
          if (batch.onEvent(consumer)) {
            hasNew.set(true);
          }
        } catch (final Exception e) {
          LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
        }
        if (hasNew.get()) {
          regionIdToBatch.remove(regionId);
          break;
        }
      } finally {
        segmentLock.unlock(regionId);
      }
    }

    return hasNew.get();
  }

  /**
   * @return {@code true} if there are subscription events consumed.
   */
  public boolean onEvent(final EnrichedEvent event, final Consumer<SubscriptionEvent> consumer)
      throws Exception {
    final int regionId = event.getCommitterKey().getRegionId();

    final AtomicBoolean hasNew = new AtomicBoolean(false);
    try {
      segmentLock.lock(regionId);
      SubscriptionPipeEventBatch batch = regionIdToBatch.get(regionId);
      if (Objects.isNull(batch)) {
        try {
          batch =
              prefetchingQueue instanceof SubscriptionPrefetchingTabletQueue
                  ? new SubscriptionPipeTabletEventBatch(
                      regionId,
                      (SubscriptionPrefetchingTabletQueue) prefetchingQueue,
                      maxDelayInMs,
                      maxBatchSizeInBytes)
                  : new SubscriptionPipeTsFileEventBatch(
                      regionId,
                      (SubscriptionPrefetchingTsFileQueue) prefetchingQueue,
                      maxDelayInMs,
                      maxBatchSizeInBytes);
        } catch (final Exception e) {
          LOGGER.warn("Exception occurred when construct new batch", e);
          throw e; // rethrow exception for retry
        }
      }

      try {
        if (batch.onEvent(event, consumer)) {
          hasNew.set(true);
        }
      } catch (final Exception e) {
        LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
      }

      if (hasNew.get()) {
        regionIdToBatch.remove(regionId);
      } else {
        regionIdToBatch.put(regionId, batch);
      }

    } finally {
      segmentLock.unlock(regionId);
    }

    return hasNew.get();
  }

  public void cleanUp() {
    regionIdToBatch.values().forEach(batch -> batch.cleanUp(true));
    regionIdToBatch.clear();
  }
}
