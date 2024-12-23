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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SubscriptionPipeEventBatches {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPipeEventBatches.class);

  protected final SubscriptionPrefetchingQueue prefetchingQueue;
  protected final int maxDelayInMs;
  protected final long maxBatchSizeInBytes;

  private final Map<Integer, SubscriptionPipeEventBatch> regionIdToBatch;

  public SubscriptionPipeEventBatches(
      final SubscriptionPrefetchingQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;

    this.regionIdToBatch = new ConcurrentHashMap<>();
  }

  /**
   * @return {@code true} if there are subscription events consumed.
   */
  public boolean onEvent(final Consumer<SubscriptionEvent> consumer) {
    final AtomicBoolean hasNew = new AtomicBoolean(false);
    for (final int regionId : ImmutableList.copyOf(regionIdToBatch.keySet())) {
      regionIdToBatch.compute(
          regionId,
          (key, batch) -> {
            if (Objects.isNull(batch)) {
              return null;
            }

            try {
              if (batch.onEvent(consumer)) {
                hasNew.set(true);
                return null; // remove this entry
              }
              // Seal this batch next time.
            } catch (final Exception e) {
              LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
              // Seal this batch next time.
            }

            return batch;
          });

      if (hasNew.get()) {
        break;
      }
    }

    return hasNew.get();
  }

  /**
   * @return {@code true} if there are subscription events consumed.
   */
  public boolean onEvent(
      final @NonNull EnrichedEvent event, final Consumer<SubscriptionEvent> consumer) {
    final int regionId = event.getCommitterKey().getRegionId();

    final AtomicBoolean hasNew = new AtomicBoolean(false);
    regionIdToBatch.compute(
        regionId,
        (key, batch) -> {
          if (Objects.isNull(batch)) {
            batch =
                prefetchingQueue instanceof SubscriptionPrefetchingTabletQueue
                    ? new SubscriptionPipeTabletEventBatch(
                        key,
                        (SubscriptionPrefetchingTabletQueue) prefetchingQueue,
                        maxDelayInMs,
                        maxBatchSizeInBytes)
                    : new SubscriptionPipeTsFileEventBatch(
                        key,
                        (SubscriptionPrefetchingTsFileQueue) prefetchingQueue,
                        maxDelayInMs,
                        maxBatchSizeInBytes);
          }

          try {
            if (batch.onEvent(event, consumer)) {
              hasNew.set(true);
              return null; // remove this entry
            }
            // Seal this batch next time.
          } catch (final Exception e) {
            LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
            // Seal this batch next time.
          }

          return batch;
        });

    return hasNew.get();
  }

  public void cleanUp() {
    regionIdToBatch.values().forEach(SubscriptionPipeEventBatch::cleanUp);
    regionIdToBatch.clear();
  }
}
