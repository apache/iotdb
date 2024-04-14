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

package org.apache.iotdb.confignode.manager.load.cache.consensus;

import org.apache.iotdb.confignode.manager.load.cache.AbstractLoadCache;

/**
 * ConsensusCache caches the ConsensusHeartbeatSamples of a consensus group. Update and cache the
 * current statistics of the consensus group based on the latest ConsensusHeartbeatSample.
 */
public class ConsensusCache extends AbstractLoadCache {

  public static final int UN_READY_LEADER_ID = -1;

  public ConsensusCache() {
    super();
    this.currentStatistics.set(ConsensusStatistics.generateDefaultConsensusStatistics());
  }

  @Override
  public void updateCurrentStatistics() {
    ConsensusHeartbeatSample lastSample;
    synchronized (slidingWindow) {
      lastSample = (ConsensusHeartbeatSample) getLastSample();
    }
    if (lastSample != null && lastSample.getLeaderId() != UN_READY_LEADER_ID) {
      currentStatistics.set(new ConsensusStatistics(System.nanoTime(), lastSample.getLeaderId()));
    }
  }

  public ConsensusStatistics getCurrentStatistics() {
    return (ConsensusStatistics) currentStatistics.get();
  }

  public boolean isLeaderUnSelected() {
    return UN_READY_LEADER_ID == ((ConsensusStatistics) currentStatistics.get()).getLeaderId();
  }

  public int getLeaderId() {
    return ((ConsensusStatistics) currentStatistics.get()).getLeaderId();
  }
}
