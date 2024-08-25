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

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class SubscriptionPipeEventBatches {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPipeEventBatches.class);

  protected final SubscriptionPrefetchingQueue prefetchingQueue;
  protected final int maxDelayInMs;
  protected final long maxBatchSizeInBytes;

  private final Map<Integer, Pair<SubscriptionPipeEventBatch, ReentrantLock>> batchWithLocks;

  public SubscriptionPipeEventBatches(
      final SubscriptionPrefetchingQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;

    this.batchWithLocks = new ConcurrentHashMap<>();
  }

  public List<SubscriptionEvent> onEvent() {
    final List<SubscriptionEvent> events = new ArrayList<>();

    for (final Pair<SubscriptionPipeEventBatch, ReentrantLock> batchWithLock :
        ImmutableSet.copyOf(batchWithLocks.values())) {
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
            batchWithLocks.remove(regionId);
          }
          if (!evs.isEmpty()) {
            break;
          }
        } catch (final Exception e) {
          LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
          // seal it next time
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
          batchWithLocks.computeIfAbsent(
              regionId,
              (id) ->
                  new Pair<>(
                      prefetchingQueue instanceof SubscriptionPrefetchingTabletQueue
                          ? new SubscriptionPipeTabletEventBatch(
                              id,
                              (SubscriptionPrefetchingTabletQueue) prefetchingQueue,
                              maxDelayInMs,
                              maxBatchSizeInBytes)
                          : new SubscriptionPipeTsFileEventBatch(
                              id,
                              (SubscriptionPrefetchingTsFileQueue) prefetchingQueue,
                              maxDelayInMs,
                              maxBatchSizeInBytes),
                      new ReentrantLock(true)));
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
            batchWithLocks.remove(regionId);
            continue; // it is necessary to calculate the event into a batch, try next batch
          }
        } catch (final Exception e) {
          LOGGER.warn("Exception occurred when sealing events from batch {}", batch, e);
          continue; // try to seal again
        }

        // It can be guaranteed that the batch has not been called to generateSubscriptionEvents at
        // this time.
        try {
          final List<SubscriptionEvent> evs = batch.onEvent(event);
          if (!evs.isEmpty()) {
            events.addAll(evs);
            batchWithLocks.remove(regionId);
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
    ImmutableSet.copyOf(batchWithLocks.entrySet())
        .forEach(
            entry -> {
              final int regionId = entry.getKey();
              final Pair<SubscriptionPipeEventBatch, ReentrantLock> batchWithLock =
                  entry.getValue();
              final SubscriptionPipeEventBatch batch = batchWithLock.getLeft();
              final ReentrantLock lock = batchWithLock.getRight();
              lock.lock();
              try {
                batch.cleanUp();
              } finally {
                lock.unlock();
                batchWithLocks.remove(regionId);
              }
            });
  }
}
