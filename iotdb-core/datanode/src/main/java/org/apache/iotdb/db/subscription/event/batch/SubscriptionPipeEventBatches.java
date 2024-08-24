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
import org.apache.iotdb.commons.pipe.progress.PipeEventCommitManager;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class SubscriptionPipeEventBatches {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPipeEventBatches.class);

  protected final SubscriptionPrefetchingQueue prefetchingQueue;
  protected final int maxDelayInMs;
  protected final long maxBatchSizeInBytes;

  private final LoadingCache<Integer, Pair<SubscriptionPipeEventBatch, ReentrantLock>>
      batchWithLocks;

  public SubscriptionPipeEventBatches(
      final SubscriptionPrefetchingQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;

    this.batchWithLocks =
        Caffeine.newBuilder()
            // TODO: config
            .expireAfterAccess(60L, TimeUnit.SECONDS)
            .build(
                regionId ->
                    new Pair<>(
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
                                maxBatchSizeInBytes),
                        new ReentrantLock(true)));
  }

  public List<SubscriptionEvent> onEvent() {
    final List<SubscriptionEvent> events = new ArrayList<>();

    for (final Pair<SubscriptionPipeEventBatch, ReentrantLock> batchWithLock :
        ImmutableSet.copyOf(batchWithLocks.asMap().values())) {
      final SubscriptionPipeEventBatch batch = batchWithLock.getLeft();
      final int regionId = batch.getRegionId();
      final ReentrantLock lock = batchWithLock.getRight();

      try {
        lock.lock();
        if (batch.isSealed()) {
          continue;
        }

        try {
          final List<SubscriptionEvent> evs = batch.onEvent();
          if (!evs.isEmpty()) {
            events.addAll(evs);
            batchWithLocks.invalidate(regionId);
          }
        } catch (final Exception e) {
          LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
          continue; // try to seal again
        }

        if (!events.isEmpty()) {
          break;
        }
      } finally {
        lock.unlock();
      }
    }

    return events;
  }

  public List<SubscriptionEvent> onEvent(@NonNull final EnrichedEvent event) {
    final int regionId =
        PipeEventCommitManager.parseRegionIdFromCommitterKey(event.getCommitterKey());
    final List<SubscriptionEvent> events = new ArrayList<>();

    while (true) {
      final Pair<SubscriptionPipeEventBatch, ReentrantLock> batchWithLock =
          Objects.requireNonNull(batchWithLocks.get(regionId));
      final SubscriptionPipeEventBatch batch = batchWithLock.getLeft();
      final ReentrantLock lock = batchWithLock.getRight();

      try {
        lock.lock();
        if (batch.isSealed()) {
          continue;
        }

        try {
          final List<SubscriptionEvent> evs = batch.onEvent();
          if (!evs.isEmpty()) {
            events.addAll(evs);
            batchWithLocks.invalidate(regionId);
          }
        } catch (final Exception e) {
          LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
          continue; // try to seal again
        }

        if (!events.isEmpty()) {
          break;
        }

        // It can be guaranteed that the batch has not been called to generateSubscriptionEvents at
        // this time.
        try {
          final List<SubscriptionEvent> evs = batch.onEvent(event);
          if (!evs.isEmpty()) {
            events.addAll(evs);
            batchWithLocks.invalidate(regionId);
          }
          break;
        } catch (final Exception e) {
          LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
          break; // can be guaranteed that the event is calculated into the batch, seal it next time
        }
      } finally {
        lock.unlock();
      }
    }

    return events;
  }

  public void cleanUp() {
    batchWithLocks.asMap().values().forEach((pair) -> pair.getLeft().cleanUp());
    batchWithLocks.invalidateAll();
  }
}
