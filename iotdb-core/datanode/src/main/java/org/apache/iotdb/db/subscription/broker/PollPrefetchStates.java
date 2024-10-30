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

public class PollPrefetchStates {

  private static final Logger LOGGER = LoggerFactory.getLogger(PollPrefetchStates.class);

  // TODO: config
  private static final double PREFETCH_MEMORY_THRESHOLD = 0.6;
  private static final double MISSING_RATE_THRESHOLD = 0.8;
  private static final int PREFETCHED_EVENT_COUNT_CONTROL_PARAMETER = 100;

  private final SubscriptionPrefetchingQueue prefetchingQueue;

  private volatile long lastPollRequestTimestamp;
  private final Meter pollRequestMeter;
  private final Meter missingMeter;

  public PollPrefetchStates(final SubscriptionPrefetchingQueue prefetchingQueue) {
    this.prefetchingQueue = prefetchingQueue;

    this.lastPollRequestTimestamp = -1;
    this.pollRequestMeter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
    this.missingMeter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
  }

  public void markPollRequest() {
    lastPollRequestTimestamp = System.currentTimeMillis();
    pollRequestMeter.mark();
  }

  public void markMissing() {
    missingMeter.mark();
  }

  public boolean shouldPrefetch() {
    if (!isResourceEnough()) {
      return false;
    }

    if (missRate() > MISSING_RATE_THRESHOLD) {
      return true;
    }

    if (hasTooManyPrefetchedEvents()) {
      return false;
    }

    if (lastPollRequestTimestamp == -1) {
      return true;
    }

    final long delta = System.currentTimeMillis() - lastPollRequestTimestamp;
    final double rate = pollRate();
    return delta * rate > 1000 * Math.log(SubscriptionAgent.broker().getPrefetchingQueueCount());
  }

  private boolean isResourceEnough() {
    return PipeDataNodeResourceManager.memory().getTotalMemorySizeInBytes()
            * PREFETCH_MEMORY_THRESHOLD
        > PipeDataNodeResourceManager.memory().getUsedMemorySizeInBytes();
  }

  private double missRate() {
    return missingMeter.getOneMinuteRate() / pollRequestMeter.getOneMinuteRate();
  }

  private double pollRate() {
    return pollRequestMeter.getOneMinuteRate();
  }

  private boolean hasTooManyPrefetchedEvents() {
    return prefetchingQueue.getPrefetchedEventCount()
            * SubscriptionAgent.broker().getPrefetchingQueueCount()
        > PREFETCHED_EVENT_COUNT_CONTROL_PARAMETER;
  }

  @Override
  public String toString() {
    return "PollPrefetchStates{lastPollRequestTimestamp="
        + lastPollRequestTimestamp
        + ", pollRate="
        + pollRate()
        + ", missRate="
        + missRate()
        + "}";
  }
}
