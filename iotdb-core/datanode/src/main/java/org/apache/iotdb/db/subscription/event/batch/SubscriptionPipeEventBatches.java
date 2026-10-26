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
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

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
          LOGGER.warn(DataNodeMiscMessages.EXCEPTION_SEALING_EVENTS, batch, e);
          throw propagate(e);
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
          batch = createBatch(regionId);
        } catch (final Exception e) {
          LOGGER.warn(DataNodeMiscMessages.EXCEPTION_CONSTRUCT_NEW_BATCH, e);
          throw e; // rethrow exception for retry
        }
      }

      if (!batch.isCompatibleWithCurrentTopicConfig() && batch.getPipeEventCount() > 0) {
        if (!batch.emit(consumer)) {
          throw new SubscriptionException(
              DataNodeMiscMessages.EXCEPTION_FAILED_TO_SEAL_SUBSCRIPTION_EVENT_BATCH_1FB7E92C);
        }
        hasNew.set(true);
        regionIdToBatch.remove(regionId);
        batch = createBatch(regionId);
      }

      final boolean emittedCurrentBatch;
      try {
        emittedCurrentBatch = batch.onEvent(event, consumer);
        if (emittedCurrentBatch) {
          hasNew.set(true);
        }
      } catch (final Exception e) {
        LOGGER.warn(DataNodeMiscMessages.EXCEPTION_SEALING_EVENTS, batch, e);
        throw e;
      }

      if (emittedCurrentBatch) {
        regionIdToBatch.remove(regionId);
      } else {
        regionIdToBatch.put(regionId, batch);
      }

    } finally {
      segmentLock.unlock(regionId);
    }

    return hasNew.get();
  }

  public boolean emitAll(final Consumer<SubscriptionEvent> consumer) throws Exception {
    final AtomicBoolean hasNew = new AtomicBoolean(false);
    Exception exception = null;
    for (final int regionId : ImmutableList.copyOf(regionIdToBatch.keySet())) {
      try {
        segmentLock.lock(regionId);
        final SubscriptionPipeEventBatch batch = regionIdToBatch.get(regionId);
        if (Objects.isNull(batch)) {
          continue;
        }
        if (batch.emit(consumer)) {
          hasNew.set(true);
          regionIdToBatch.remove(regionId);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodeMiscMessages.EXCEPTION_SEALING_EVENTS, regionIdToBatch.get(regionId), e);
        exception = e;
      } finally {
        segmentLock.unlock(regionId);
      }
    }

    if (Objects.nonNull(exception)) {
      throw exception;
    }
    return hasNew.get();
  }

  public void cleanUp() {
    regionIdToBatch.values().forEach(batch -> batch.cleanUp(true));
    regionIdToBatch.clear();
  }

  private SubscriptionPipeEventBatch createBatch(final int regionId) {
    return prefetchingQueue instanceof SubscriptionPrefetchingTabletQueue
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
  }

  private static RuntimeException propagate(final Exception exception) {
    return exception instanceof RuntimeException
        ? (RuntimeException) exception
        : new SubscriptionException(
            DataNodeMiscMessages.EXCEPTION_FAILED_TO_SEAL_SUBSCRIPTION_EVENT_BATCH_1FB7E92C,
            exception);
  }
}
