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

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.metrics.core.utils.IoTDBMovingAverage;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * The {@link SubscriptionPrefetchingQueueStates} manages the state of a {@link
 * SubscriptionPrefetchingQueue}. It determines whether prefetching should occur based on memory
 * availability, poll request rates, and missing prefetch rates.
 */
public class SubscriptionPrefetchingQueueStates {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingQueueStates.class);

  private static final double EPSILON = 1e-6;

  private static final double PREFETCH_MEMORY_THRESHOLD =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchMemoryThreshold();
  private static final double PREFETCH_MISSING_RATE_THRESHOLD =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchMissingRateThreshold();
  private static final int PREFETCH_EVENT_LOCAL_COUNT_THRESHOLD =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchEventLocalCountThreshold();
  private static final int PREFETCH_EVENT_GLOBAL_COUNT_THRESHOLD =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchEventGlobalCountThreshold();

  private final SubscriptionPrefetchingQueue prefetchingQueue;

  private volatile long lastPollRequestTimestamp;
  private final Meter pollRequestMeter;
  private final Meter missingPrefechMeter;
  private final Counter disorderCauseCounter; // TODO: use meter

  public SubscriptionPrefetchingQueueStates(final SubscriptionPrefetchingQueue prefetchingQueue) {
    this.prefetchingQueue = prefetchingQueue;

    this.lastPollRequestTimestamp = -1;
    this.pollRequestMeter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
    this.missingPrefechMeter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
    this.disorderCauseCounter = new Counter();
  }

  public void markPollRequest() {
    lastPollRequestTimestamp = System.currentTimeMillis();
    pollRequestMeter.mark();
  }

  public void markMissingPrefetch() {
    missingPrefechMeter.mark();
  }

  public void markDisorderCause() {
    disorderCauseCounter.inc();
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

  public boolean shouldPrefetch() {
    // 1. reject conditions
    // 1.1. option
    if (!SubscriptionConfig.getInstance().getSubscriptionPrefetchEnabled()) {
      return false;
    }

    // 1.2. memory usage
    if (!isMemoryEnough()) {
      return false;
    }

    // 1.3. local event count
    if (hasTooManyPrefetchedLocalEvent()) {
      return false;
    }

    // 1.4. global event count
    if (hasTooManyPrefetchedGlobalEvent()) {
      return false;
    }

    // 1.5. disorder history
    if (hasDisorderCause()) {
      return false;
    }

    // 2. permissive conditions
    // 2.1. missing rate
    if (isMissingRateTooHigh()) {
      return true;
    }

    // 2.2. prefetch statistics
    // the delta between the prefetch timestamp and the timestamp of the last poll request > poll
    // request frequency
    return (System.currentTimeMillis() - lastPollRequestTimestamp) * pollRate() > 1000;
  }

  private boolean isMemoryEnough() {
    return PipeDataNodeResourceManager.memory().getTotalMemorySizeInBytes()
            * PREFETCH_MEMORY_THRESHOLD
        > PipeDataNodeResourceManager.memory().getUsedMemorySizeInBytes();
  }

  private boolean hasTooManyPrefetchedLocalEvent() {
    return prefetchingQueue.getPrefetchedEventCount() > PREFETCH_EVENT_LOCAL_COUNT_THRESHOLD;
  }

  private boolean hasTooManyPrefetchedGlobalEvent() {
    // The number of prefetched events in the current prefetching queue > floor(t / number of
    // prefetching queues), where t is an adjustable parameter.
    return prefetchingQueue.getPrefetchedEventCount()
            * SubscriptionAgent.broker().getPrefetchingQueueCount()
        > PREFETCH_EVENT_GLOBAL_COUNT_THRESHOLD;
  }

  private boolean isMissingRateTooHigh() {
    return missingRate() > PREFETCH_MISSING_RATE_THRESHOLD;
  }

  private boolean hasDisorderCause() {
    return disorderCauseCounter.getCount() > 0;
  }

  private static boolean isApproximatelyZero(final double value) {
    return Math.abs(value) < EPSILON;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("lastPollRequestTimestamp", lastPollRequestTimestamp)
        .add("pollRate", pollRate())
        .add("missingRate", missingRate())
        .add("disorderCause", disorderCauseCounter.getCount())
        .toString();
  }
}
