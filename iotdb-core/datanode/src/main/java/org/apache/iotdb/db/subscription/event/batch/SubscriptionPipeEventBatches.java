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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class SubscriptionPipeEventBatches {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPipeEventBatches.class);

  // TODO: config
  private static final int SEGMENT_LOCK_COUNT = 4;

  protected final SubscriptionPrefetchingQueue prefetchingQueue;
  protected final int maxDelayInMs;
  protected final long maxBatchSizeInBytes;

  private final SubscriptionPipeEventBatch[] batches;
  private final ReentrantLock[] segmentLocks;

  private final AtomicLong pseudoCommitIdGenerator = new AtomicLong(0);

  public SubscriptionPipeEventBatches(
      final SubscriptionPrefetchingQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;

    this.batches = new SubscriptionPipeEventBatch[SEGMENT_LOCK_COUNT];
    for (int i = 0; i < SEGMENT_LOCK_COUNT; i++) {
      reconstructBatch(i);
    }

    this.segmentLocks = new ReentrantLock[SEGMENT_LOCK_COUNT];
    for (int i = 0; i < SEGMENT_LOCK_COUNT; i++) {
      this.segmentLocks[i] = new ReentrantLock(true);
    }
  }

  private void reconstructBatch(final int index) {
    if (Objects.nonNull(batches[index]) && !batches[index].isSealed()) {
      LOGGER.warn("Construct batch for non-sealed batch {}", batches[index]);
    }

    if (prefetchingQueue instanceof SubscriptionPrefetchingTabletQueue) {
      batches[index] =
          new SubscriptionPipeTabletEventBatch(
              (SubscriptionPrefetchingTabletQueue) prefetchingQueue,
              maxDelayInMs,
              maxBatchSizeInBytes);
    } else {
      batches[index] =
          new SubscriptionPipeTsFileEventBatch(
              (SubscriptionPrefetchingTsFileQueue) prefetchingQueue,
              maxDelayInMs,
              maxBatchSizeInBytes);
    }
  }

  public List<SubscriptionEvent> onEvent(@Nullable final EnrichedEvent event) {
    final long commitId =
        Objects.isNull(event) ? pseudoCommitIdGenerator.getAndIncrement() : event.getCommitId();
    final int index = (int) (commitId % SEGMENT_LOCK_COUNT);

    final List<SubscriptionEvent> events = new ArrayList<>();
    while (true) {
      segmentLocks[index].lock();
      try {
        final List<SubscriptionEvent> evs = batches[index].onEvent();
        if (!evs.isEmpty()) {
          events.addAll(evs);
          reconstructBatch(index);
        }
      } catch (final Exception e) {
        LOGGER.warn("Exception occurred when sealing events from batch {}", batches[index], e);
        continue;
      } finally {
        segmentLocks[index].unlock();
      }

      if (Objects.isNull(event)) {
        break;
      }

      try {
        final List<SubscriptionEvent> evs = batches[index].onEvent(event);
        if (!evs.isEmpty()) {
          events.addAll(evs);
          reconstructBatch(index);
        }
        break;
      } catch (final Exception e) {
        LOGGER.warn("Exception occurred when sealing events from batch {}", batches[index], e);
        break;
      } finally {
        segmentLocks[index].unlock();
      }
    }

    return events;
  }

  public void cleanUp() {
    Arrays.stream(batches).forEach(SubscriptionPipeEventBatch::cleanUp);
  }
}
