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

package org.apache.iotdb.confignode.manager.load.cache.region;

import org.apache.iotdb.commons.cluster.RegionStatus;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache.HEARTBEAT_TIMEOUT_TIME_IN_NS;
import static org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache.MAXIMUM_WINDOW_SIZE;

public class RegionCache {

  private final List<RegionHeartbeatSample> slidingWindow;
  private final AtomicReference<RegionStatistics> currentStatistics;

  public RegionCache() {
    this.slidingWindow = Collections.synchronizedList(new LinkedList<>());
    this.currentStatistics =
        new AtomicReference<>(RegionStatistics.generateDefaultRegionStatistics());
  }

  public void cacheHeartbeatSample(RegionHeartbeatSample newHeartbeatSample) {
    synchronized (slidingWindow) {
      // Only sequential HeartbeatSamples are accepted.
      // And un-sequential HeartbeatSamples will be discarded.
      if (getLastSample() == null
          || getLastSample().getSampleNanoTimestamp()
              < newHeartbeatSample.getSampleNanoTimestamp()) {
        slidingWindow.add(newHeartbeatSample);
      }

      if (slidingWindow.size() > MAXIMUM_WINDOW_SIZE) {
        slidingWindow.remove(0);
      }
    }
  }

  public void updateCurrentStatistics() {
    RegionHeartbeatSample lastSample;
    synchronized (slidingWindow) {
      lastSample = getLastSample();
    }

    RegionStatus status;
    long currentNanoTime = System.nanoTime();
    if (lastSample == null) {
      status = RegionStatus.Unknown;
    } else if (currentNanoTime - lastSample.getSampleNanoTimestamp()
        > HEARTBEAT_TIMEOUT_TIME_IN_NS) {
      // TODO: Optimize Unknown judge logic
      status = RegionStatus.Unknown;
    } else {
      status = lastSample.getStatus();
    }
    this.currentStatistics.set(new RegionStatistics(currentNanoTime, status));
  }

  public RegionStatistics getCurrentStatistics() {
    return currentStatistics.get();
  }

  private RegionHeartbeatSample getLastSample() {
    return slidingWindow.isEmpty() ? null : slidingWindow.get(slidingWindow.size() - 1);
  }
}
