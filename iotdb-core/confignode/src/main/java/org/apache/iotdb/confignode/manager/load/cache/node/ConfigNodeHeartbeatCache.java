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

package org.apache.iotdb.confignode.manager.load.cache.node;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

/** Heartbeat cache for cluster ConfigNodes. */
public class ConfigNodeHeartbeatCache extends BaseNodeCache {

  /** Only get CURRENT_NODE_ID here due to initialization order. */
  public static final int CURRENT_NODE_ID =
      ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();

  public static final NodeStatistics CURRENT_NODE_STATISTICS =
      new NodeStatistics(0, NodeStatus.Running, null, 0);

  /** Constructor for create ConfigNodeHeartbeatCache with default NodeStatistics. */
  public ConfigNodeHeartbeatCache(int configNodeId) {
    super(configNodeId);
  }

  /** Constructor only for ConfigNode-leader. */
  public ConfigNodeHeartbeatCache(int configNodeId, NodeStatistics statistics) {
    super(configNodeId);
    this.currentStatistics.set(statistics);
  }

  @Override
  public synchronized void updateCurrentStatistics(boolean forceUpdate) {
    // Skip itself and the Removing status can not be updated
    if (nodeId == CURRENT_NODE_ID || NodeStatus.Removing.equals(getNodeStatus())) {
      return;
    }

    NodeHeartbeatSample lastSample;
    synchronized (slidingWindow) {
      lastSample = (NodeHeartbeatSample) getLastSample();
    }
    long lastSendTime = lastSample == null ? 0 : lastSample.getSampleLogicalTimestamp();

    // Update Node status
    NodeStatus status;
    long currentNanoTime = System.nanoTime();
    if (lastSample == null) {
      status = NodeStatus.Unknown;
    } else if (currentNanoTime - lastSendTime > HEARTBEAT_TIMEOUT_TIME_IN_NS) {
      // TODO: Optimize Unknown judge logic
      status = NodeStatus.Unknown;
    } else {
      status = lastSample.getStatus();
    }

    /* Update loadScore */
    // Only consider Running ConfigNode as available currently
    // TODO: Construct load score module
    long loadScore = NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE;

    currentStatistics.set(new NodeStatistics(currentNanoTime, status, null, loadScore));
  }
}
