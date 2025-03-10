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

package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.cache.detector.FixedDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.PhiAccrualDetector;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractLoadCache caches the recent MAXIMUM_WINDOW_SIZE heartbeat samples and calculates the
 * current statistics based on the latest heartbeat mple.
 */
public abstract class AbstractLoadCache {

  // Max heartbeat cache samples store size
  private static final int MAXIMUM_WINDOW_SIZE = 100;

  // Caching the recent MAXIMUM_WINDOW_SIZE heartbeat sample
  protected final List<AbstractHeartbeatSample> slidingWindow;
  // The current statistics calculated by the latest heartbeat sample
  protected final AtomicReference<AbstractStatistics> currentStatistics;

  protected final IFailureDetector failureDetector;

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  protected AbstractLoadCache() {
    this.currentStatistics = new AtomicReference<>();
    this.slidingWindow = Collections.synchronizedList(new LinkedList<>());
    switch (CONF.getFailureDetector()) {
      case IFailureDetector.PHI_ACCRUAL_DETECTOR:
        this.failureDetector =
            new PhiAccrualDetector(
                CONF.getFailureDetectorPhiThreshold(),
                CONF.getFailureDetectorPhiAcceptablePauseInMs() * 1000_000L,
                CONF.getHeartbeatIntervalInMs() * 200_000L,
                60,
                new FixedDetector(CONF.getFailureDetectorFixedThresholdInMs() * 1000_000L));
        break;
      case IFailureDetector.FIXED_DETECTOR:
      default:
        this.failureDetector =
            new FixedDetector(CONF.getFailureDetectorFixedThresholdInMs() * 1000_000L);
    }
  }

  /**
   * Cache the latest heartbeat sample.
   *
   * @param newHeartbeatSample The latest heartbeat sample.
   */
  public void cacheHeartbeatSample(AbstractHeartbeatSample newHeartbeatSample) {
    synchronized (slidingWindow) {
      // Only sequential heartbeats are accepted.
      // And un-sequential heartbeats will be discarded.
      if (getLastSample() == null
          || getLastSample().getSampleLogicalTimestamp()
              <= newHeartbeatSample.getSampleLogicalTimestamp()) {
        slidingWindow.add(newHeartbeatSample);
      }

      if (slidingWindow.size() > MAXIMUM_WINDOW_SIZE) {
        slidingWindow.remove(0);
      }
    }
  }

  /**
   * Get the latest heartbeat sample that cached in the slidingWindow.
   *
   * @return The latest heartbeat sample.
   */
  public AbstractHeartbeatSample getLastSample() {
    return slidingWindow.isEmpty() ? null : slidingWindow.get(slidingWindow.size() - 1);
  }

  /**
   * Update currentStatistics based on the latest heartbeat sample that cached in the slidingWindow.
   */
  public abstract void updateCurrentStatistics(boolean forceUpdate);

  public AbstractStatistics getCurrentStatistics() {
    return currentStatistics.get();
  }
}
