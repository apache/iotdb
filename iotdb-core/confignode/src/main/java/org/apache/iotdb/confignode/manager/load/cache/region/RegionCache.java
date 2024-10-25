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
import org.apache.iotdb.confignode.manager.load.cache.AbstractLoadCache;

/**
 * RegionCache caches the RegionHeartbeatSamples of a Region. Update and cache the current
 * statistics of the Region based on the latest RegionHeartbeatSample.
 */
public class RegionCache extends AbstractLoadCache {

  public RegionCache() {
    super();
    this.currentStatistics.set(RegionStatistics.generateDefaultRegionStatistics());
  }

  @Override
  public synchronized void updateCurrentStatistics(boolean forceUpdate) {
    RegionHeartbeatSample lastSample;
    synchronized (slidingWindow) {
      lastSample = (RegionHeartbeatSample) getLastSample();
    }

    RegionStatus status;
    long currentNanoTime = System.nanoTime();
    if (lastSample == null) {
      status = RegionStatus.Unknown;
    } else if (currentNanoTime - lastSample.getSampleLogicalTimestamp()
        > HEARTBEAT_TIMEOUT_TIME_IN_NS) {
      // TODO: Optimize Unknown judge logic
      status = RegionStatus.Unknown;
    } else {
      status = lastSample.getStatus();
    }
    this.currentStatistics.set(new RegionStatistics(currentNanoTime, status));
  }

  public RegionStatistics getCurrentStatistics() {
    return (RegionStatistics) currentStatistics.get();
  }

  public synchronized void cacheHeartbeatSample(
      RegionHeartbeatSample newHeartbeatSample, boolean overwrite) {
    if (overwrite || getLastSample() == null) {
      super.cacheHeartbeatSample(newHeartbeatSample);
      return;
    }
    RegionStatus lastStatus = ((RegionHeartbeatSample) getLastSample()).getStatus();
    if (lastStatus.equals(RegionStatus.Adding) || lastStatus.equals(RegionStatus.Removing)) {
      RegionHeartbeatSample fakeHeartbeatSample =
          new RegionHeartbeatSample(newHeartbeatSample.getSampleLogicalTimestamp(), lastStatus);
      super.cacheHeartbeatSample(fakeHeartbeatSample);
    } else {
      super.cacheHeartbeatSample(newHeartbeatSample);
    }
  }
}
