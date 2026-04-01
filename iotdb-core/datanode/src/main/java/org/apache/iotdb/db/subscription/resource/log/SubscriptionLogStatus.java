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

package org.apache.iotdb.db.subscription.resource.log;

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class SubscriptionLogStatus {

  private static final long BASE_INTERVAL_IN_MS =
      SubscriptionConfig.getInstance().getSubscriptionLogManagerBaseIntervalMs();

  private final Logger logger;
  private final Cache<Pair<String, String>, AtomicLong> lastReportTimestamps;

  public SubscriptionLogStatus(final Class<?> logClass) {
    this.logger = LoggerFactory.getLogger(logClass);
    this.lastReportTimestamps =
        Caffeine.newBuilder()
            .expireAfterAccess(
                SubscriptionConfig.getInstance().getSubscriptionLogManagerWindowSeconds(),
                TimeUnit.SECONDS)
            .build();
  }

  public Optional<Logger> schedule(final String consumerGroupId, final String topicName) {
    final Pair<String, String> key = new Pair<>(consumerGroupId, topicName);
    final long now = System.currentTimeMillis();
    // Calculate the allowed logging interval based on the current prefetching queue count
    final int count = SubscriptionAgent.broker().getPrefetchingQueueCount();
    final long allowedInterval = BASE_INTERVAL_IN_MS * count;
    // If the key does not exist, initialize an AtomicLong set to one interval before now
    final AtomicLong lastTime =
        Objects.requireNonNull(
            lastReportTimestamps.get(
                key,
                k ->
                    new AtomicLong(
                        now
                            // introduce randomness
                            - BASE_INTERVAL_IN_MS
                                * ThreadLocalRandom.current().nextLong(Math.max(count, 1)))));
    final long last = lastTime.get();
    if (now - last >= allowedInterval) {
      // Use compareAndSet to ensure that only one thread updates at a time,
      // so that only one log entry is printed per allowed interval
      if (lastTime.compareAndSet(last, now)) {
        return Optional.of(logger);
      }
    }
    return Optional.empty();
  }
}
