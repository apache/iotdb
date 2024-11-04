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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.metrics.core.utils.IoTDBMovingAverage;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SubscriptionPrefetchingQueueStates} manages the state of a {@link
 * SubscriptionPrefetchingQueue}. It determines whether prefetching should occur based on memory
 * availability, poll request rates, and missing prefetch rates.
 */
public class SubscriptionPrefetchingQueueStates {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingQueueStates.class);

  private static final double EPSILON = 1e-6;

  // TODO: config
  private static final double PREFETCH_MEMORY_THRESHOLD = 0.6;
  private static final double MISSING_RATE_THRESHOLD = 0.9;
  private static final int PREFETCHED_EVENT_COUNT_CONTROL_PARAMETER = 100;

  private final SubscriptionPrefetchingQueue prefetchingQueue;

  private volatile long lastPollRequestTimestamp;
  private final Meter pollRequestMeter;
  private final Meter missingPrefechMeter;

  public SubscriptionPrefetchingQueueStates(final SubscriptionPrefetchingQueue prefetchingQueue) {
    this.prefetchingQueue = prefetchingQueue;

    this.lastPollRequestTimestamp = -1;
    this.pollRequestMeter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
    this.missingPrefechMeter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
  }

  public void markPollRequest() {
    lastPollRequestTimestamp = System.currentTimeMillis();
    pollRequestMeter.mark();
  }

  public void markMissingPrefetch() {
    missingPrefechMeter.mark();
  }

  public boolean shouldPrefetch() {
    if (!isMemoryEnough()) {
      return false;
    }

    if (missingRate() > MISSING_RATE_THRESHOLD) {
      return true;
    }

    if (hasTooManyPrefetchedEvents()) {
      return false;
    }

    // the delta between the prefetch timestamp and the timestamp of the last poll request > poll
    // request frequency
    return (System.currentTimeMillis() - lastPollRequestTimestamp) * pollRate() > 1000;
  }

  private boolean isMemoryEnough() {
    return PipeDataNodeResourceManager.memory().getTotalMemorySizeInBytes()
            * PREFETCH_MEMORY_THRESHOLD
        > PipeDataNodeResourceManager.memory().getUsedMemorySizeInBytes();
  }

  private double pollRate() {
    return pollRequestMeter.getOneMinuteRate();
  }

  private double missingRate() {
    if (isApproximatelyZero(pollRate())) {
      return 0.0;
    }
    return missingPrefechMeter.getOneMinuteRate() / pollRate();
  }

  private boolean hasTooManyPrefetchedEvents() {
    // The number of prefetched events in the current prefetching queue > floor(t / number of
    // prefetching queues), where t is an adjustable parameter.
    return prefetchingQueue.getPrefetchedEventCount()
            * SubscriptionAgent.broker().getPrefetchingQueueCount()
        > PREFETCHED_EVENT_COUNT_CONTROL_PARAMETER;
  }

  private static boolean isApproximatelyZero(final double value) {
    return Math.abs(value) < EPSILON;
  }

  @Override
  public String toString() {
    return "PollPrefetchStates{lastPollRequestTimestamp="
        + lastPollRequestTimestamp
        + ", pollRate="
        + pollRate()
        + ", missingRate="
        + missingRate()
        + "}";
  }
}
