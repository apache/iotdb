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
package org.apache.iotdb.confignode.manager.partition;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.persistence.partition.statistics.RegionStatistics;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.apache.iotdb.confignode.manager.node.BaseNodeCache.HEARTBEAT_TIMEOUT_TIME;
import static org.apache.iotdb.confignode.manager.node.BaseNodeCache.MAXIMUM_WINDOW_SIZE;

public class RegionCache {

  private final List<RegionHeartbeatSample> slidingWindow;

  public RegionCache() {
    this.slidingWindow = Collections.synchronizedList(new LinkedList<>());
  }

  public void cacheHeartbeatSample(RegionHeartbeatSample newHeartbeatSample) {
    synchronized (slidingWindow) {
      // Only sequential HeartbeatSamples are accepted.
      // And un-sequential HeartbeatSamples will be discarded.
      if (slidingWindow.size() == 0
          || getLastSample().getSendTimestamp() < newHeartbeatSample.getSendTimestamp()) {
        slidingWindow.add(newHeartbeatSample);
      }

      if (slidingWindow.size() > MAXIMUM_WINDOW_SIZE) {
        slidingWindow.remove(0);
      }
    }
  }

  public RegionStatistics getRegionStatistics() {
    RegionHeartbeatSample lastSample;
    synchronized (slidingWindow) {
      lastSample = getLastSample();
    }

    // TODO: Optimize judge logic
    RegionStatus status;
    if (System.currentTimeMillis() - lastSample.getSendTimestamp() > HEARTBEAT_TIMEOUT_TIME) {
      status = RegionStatus.Unknown;
    } else {
      status = lastSample.getStatus();
    }

    return new RegionStatistics(lastSample.getSendTimestamp(), lastSample.isLeader(), status);
  }

  private RegionHeartbeatSample getLastSample() {
    return slidingWindow.get(slidingWindow.size() - 1);
  }
}
