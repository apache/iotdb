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

package org.apache.iotdb.consensus.iot.subscription;

import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.i18n.IoTConsensusMessages;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class SubscriptionQueueRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionQueueRegistry.class);

  private static final long QUEUE_FULL_LOG_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

  private final String consensusGroupId;
  private final Map<BlockingQueue<IndexedConsensusRequest>, SubscriptionQueueRegistration> queues =
      new ConcurrentHashMap<>();
  private final AtomicLong droppedEntries = new AtomicLong();
  private final AtomicLong lastDropLogTimeMs = new AtomicLong();

  public SubscriptionQueueRegistry(final String consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
  }

  /**
   * Registers a queue without a committed-progress constraint.
   *
   * <p>This overload keeps callers using the original queue-registration API source-compatible.
   * {@link Long#MAX_VALUE} is the neutral value for the retention calculator and therefore does not
   * add an extra WAL-retention constraint.
   */
  public synchronized void register(
      final BlockingQueue<IndexedConsensusRequest> queue,
      final SubscriptionWalRetentionPolicy retentionPolicy) {
    register(queue, retentionPolicy, () -> Long.MAX_VALUE);
  }

  public synchronized void register(
      final BlockingQueue<IndexedConsensusRequest> queue,
      final SubscriptionWalRetentionPolicy retentionPolicy,
      final LongSupplier committedRetainedMinVersionIdSupplier) {
    queues.put(
        queue,
        new SubscriptionQueueRegistration(retentionPolicy, committedRetainedMinVersionIdSupplier));
  }

  // Shares the monitor with offer() so unregister() is a real stop-receiving barrier.
  public synchronized void unregister(final BlockingQueue<IndexedConsensusRequest> queue) {
    queues.remove(queue);
  }

  public synchronized boolean isEmpty() {
    return queues.isEmpty();
  }

  public synchronized int size() {
    return queues.size();
  }

  public synchronized Collection<SubscriptionWalRetentionPolicy> getRetentionPolicies() {
    final Collection<SubscriptionWalRetentionPolicy> retentionPolicies = new ArrayList<>();
    for (final SubscriptionQueueRegistration registration : queues.values()) {
      retentionPolicies.add(registration.retentionPolicy);
    }
    return retentionPolicies;
  }

  public Collection<Long> getCommittedRetainedMinVersionIds() {
    final Collection<LongSupplier> suppliers = new ArrayList<>();
    synchronized (this) {
      for (final SubscriptionQueueRegistration registration : queues.values()) {
        suppliers.add(registration.committedRetainedMinVersionIdSupplier);
      }
    }

    final Collection<Long> committedRetainedMinVersionIds = new ArrayList<>();
    for (final LongSupplier supplier : suppliers) {
      committedRetainedMinVersionIds.add(supplier.getAsLong());
    }
    return committedRetainedMinVersionIds;
  }

  public synchronized boolean offer(final IndexedConsensusRequest indexedConsensusRequest) {
    final int queueCount = queues.size();
    if (queueCount <= 0) {
      return false;
    }

    // Subscription queues reserve request memory, including the serialized buffers.
    indexedConsensusRequest.buildSerializedRequests();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          IoTConsensusMessages
              .LOG_WRITE_OFFERING_ARG_SUBSCRIPTION_QUEUE_S_GROUP_ARG_SEARCHINDEX_ARG_A8489EDF,
          queueCount,
          consensusGroupId,
          indexedConsensusRequest.getSearchIndex(),
          indexedConsensusRequest.getRequests().isEmpty()
              ? "EMPTY"
              : indexedConsensusRequest.getRequests().get(0).getClass().getSimpleName());
    }

    boolean offeredToAnyQueue = false;
    for (final BlockingQueue<IndexedConsensusRequest> queue : queues.keySet()) {
      final boolean offered = queue.offer(indexedConsensusRequest);
      offeredToAnyQueue |= offered;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            IoTConsensusMessages.LOG_OFFER_RESULT_ARG_QUEUESIZE_ARG_QUEUEREMAINING_ARG_7ADC84C2,
            offered,
            queue.size(),
            queue.remainingCapacity());
      }
      if (!offered) {
        final long droppedCount = droppedEntries.incrementAndGet();
        final long now = System.currentTimeMillis();
        final long lastLogTime = lastDropLogTimeMs.get();
        if (now - lastLogTime >= QUEUE_FULL_LOG_INTERVAL_MS
            && lastDropLogTimeMs.compareAndSet(lastLogTime, now)) {
          LOGGER.warn(
              IoTConsensusMessages
                      .LOG_SUBSCRIPTION_QUEUE_FULL_DROPPED_ARG_ENTRY_S_LAST_ARG_MS_2AD8AB3D
                  + IoTConsensusMessages
                      .LOG_SEARCHINDEX_ARG_QUEUESIZE_ARG_QUEUEREMAINING_ARG_2EA619ED,
              droppedEntries.getAndSet(0),
              QUEUE_FULL_LOG_INTERVAL_MS,
              indexedConsensusRequest.getSearchIndex(),
              queue.size(),
              queue.remainingCapacity());
        } else if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              IoTConsensusMessages
                  .LOG_SUBSCRIPTION_QUEUE_FULL_DROPPED_ENTRY_SEARCHINDEX_ARG_DROPPEDCOUNT_ARG_61F126B8,
              indexedConsensusRequest.getSearchIndex(),
              droppedCount);
        }
      }
    }
    return offeredToAnyQueue;
  }

  private static final class SubscriptionQueueRegistration {

    private final SubscriptionWalRetentionPolicy retentionPolicy;
    private final LongSupplier committedRetainedMinVersionIdSupplier;

    private SubscriptionQueueRegistration(
        final SubscriptionWalRetentionPolicy retentionPolicy,
        final LongSupplier committedRetainedMinVersionIdSupplier) {
      this.retentionPolicy = retentionPolicy;
      this.committedRetainedMinVersionIdSupplier = committedRetainedMinVersionIdSupplier;
    }
  }
}
