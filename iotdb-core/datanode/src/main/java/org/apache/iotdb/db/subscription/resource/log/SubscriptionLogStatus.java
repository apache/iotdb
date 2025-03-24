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

import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class SubscriptionLogStatus {

  private final Logger logger;
  // Use AtomicLong to ensure thread-safe updates for the lastReportTimestamps
  private final Map<Pair<String, String>, AtomicLong> lastReportTimestamps;
  // Base interval, e.g., 1 second
  private static final long BASE_INTERVAL_IN_MS = 1_000;

  public SubscriptionLogStatus(final Class<?> logClass) {
    this.logger = LoggerFactory.getLogger(logClass);
    this.lastReportTimestamps = new ConcurrentHashMap<>();
  }

  /**
   * Determines whether logging is allowed and updates the last log time for the key. The
   * allowedInterval is calculated based on the number of threads (pre-fetch queue count). For
   * example, if there are 10 threads, each thread is allowed to log once in 10 seconds.
   *
   * @param consumerGroupId Consumer group identifier
   * @param topicName Topic identifier
   * @return An Optional containing the logger if logging is allowed; otherwise, an empty Optional.
   */
  public Optional<Logger> schedule(final String consumerGroupId, final String topicName) {
    final Pair<String, String> key = new Pair<>(consumerGroupId, topicName);
    final long now = System.currentTimeMillis();
    // Calculate the allowed logging interval based on the current thread count
    final int threadCount = SubscriptionAgent.broker().getPrefetchingQueueCount();
    final long allowedInterval = BASE_INTERVAL_IN_MS * threadCount;
    // If the key does not exist, initialize an AtomicLong with a time set to one interval before
    // now
    lastReportTimestamps.putIfAbsent(key, new AtomicLong(now - allowedInterval));
    final AtomicLong lastTime = lastReportTimestamps.get(key);
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
