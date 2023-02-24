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
package org.apache.iotdb.confignode.manager.node.heartbeat;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

public class ConfigNodeHeartbeatCache extends BaseNodeCache {

  /** Only get CURRENT_NODE_ID here due to initialization order */
  public static final int CURRENT_NODE_ID =
      ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();

  public static final NodeStatistics CURRENT_NODE_STATISTICS =
      new NodeStatistics(0, NodeStatus.Running, null);

  private final int configNodeId;

  /** Constructor for create ConfigNodeHeartbeatCache with default NodeStatistics */
  public ConfigNodeHeartbeatCache(int configNodeId) {
    super();
    this.configNodeId = configNodeId;
  }

  /** Constructor only for ConfigNode-leader */
  public ConfigNodeHeartbeatCache(int configNodeId, NodeStatistics statistics) {
    super();
    this.configNodeId = configNodeId;
    this.previousStatistics = statistics;
    this.currentStatistics = statistics;
  }

  @Override
  protected void updateCurrentStatistics() {
    // Skip itself
    if (configNodeId == CURRENT_NODE_ID) {
      return;
    }

    NodeHeartbeatSample lastSample = null;
    synchronized (slidingWindow) {
      if (!slidingWindow.isEmpty()) {
        lastSample = slidingWindow.getLast();
      }
    }
    long lastSendTime = lastSample == null ? 0 : lastSample.getSendTimestamp();

    // Update Node status
    NodeStatus status = null;
    // TODO: Optimize judge logic
    if (System.currentTimeMillis() - lastSendTime > HEARTBEAT_TIMEOUT_TIME) {
      status = NodeStatus.Unknown;
    } else if (lastSample != null) {
      status = lastSample.getStatus();
    }

    /* Update loadScore */
    // Only consider Running ConfigNode as available currently
    // TODO: Construct load score module
    long loadScore = NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE;

    NodeStatistics newStatistics = new NodeStatistics(loadScore, status, null);
    if (!currentStatistics.equals(newStatistics)) {
      // Update the current NodeStatistics if necessary
      currentStatistics = newStatistics;
    }
  }
}
