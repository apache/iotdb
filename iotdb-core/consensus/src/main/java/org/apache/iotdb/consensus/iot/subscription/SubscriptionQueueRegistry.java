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
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionQueueRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionQueueRegistry.class);

  private static final long QUEUE_FULL_LOG_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

  private final String consensusGroupId;
  private final Map<BlockingQueue<IndexedConsensusRequest>, SubscriptionWalRetentionPolicy> queues =
      new ConcurrentHashMap<>();
  private final AtomicLong droppedEntries = new AtomicLong();
  private final AtomicLong lastDropLogTimeMs = new AtomicLong();

  public SubscriptionQueueRegistry(final String consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
  }

  public void register(
      final BlockingQueue<IndexedConsensusRequest> queue,
      final SubscriptionWalRetentionPolicy retentionPolicy) {
    queues.put(queue, retentionPolicy);
  }

  public void unregister(final BlockingQueue<IndexedConsensusRequest> queue) {
    queues.remove(queue);
  }

  public boolean isEmpty() {
    return queues.isEmpty();
  }

  public int size() {
    return queues.size();
  }

  public Collection<SubscriptionWalRetentionPolicy> getRetentionPolicies() {
    return queues.values();
  }

  public void offer(final IndexedConsensusRequest indexedConsensusRequest) {
    final int queueCount = queues.size();
    if (queueCount <= 0) {
      return;
    }

    LOGGER.debug(
        "write() offering to {} subscription queue(s), group={}, searchIndex={}, requestType={}",
        queueCount,
        consensusGroupId,
        indexedConsensusRequest.getSearchIndex(),
        indexedConsensusRequest.getRequests().isEmpty()
            ? "EMPTY"
            : indexedConsensusRequest.getRequests().get(0).getClass().getSimpleName());

    for (final BlockingQueue<IndexedConsensusRequest> queue : queues.keySet()) {
      final boolean offered = queue.offer(indexedConsensusRequest);
      LOGGER.debug(
          "offer result={}, queueSize={}, queueRemaining={}",
          offered,
          queue.size(),
          queue.remainingCapacity());
      if (!offered) {
        final long droppedCount = droppedEntries.incrementAndGet();
        final long now = System.currentTimeMillis();
        final long lastLogTime = lastDropLogTimeMs.get();
        if (now - lastLogTime >= QUEUE_FULL_LOG_INTERVAL_MS
            && lastDropLogTimeMs.compareAndSet(lastLogTime, now)) {
          LOGGER.warn(
              "Subscription queue full, dropped {} entry(s) in the last {} ms, latest "
                  + "searchIndex={}, queueSize={}, queueRemaining={}",
              droppedEntries.getAndSet(0),
              QUEUE_FULL_LOG_INTERVAL_MS,
              indexedConsensusRequest.getSearchIndex(),
              queue.size(),
              queue.remainingCapacity());
        } else {
          LOGGER.debug(
              "Subscription queue full, dropped entry searchIndex={}, droppedCount={}",
              indexedConsensusRequest.getSearchIndex(),
              droppedCount);
        }
      }
    }
  }
}
