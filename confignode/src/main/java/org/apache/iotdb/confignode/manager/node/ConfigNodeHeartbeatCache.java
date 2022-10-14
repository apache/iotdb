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
package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.persistence.node.NodeStatistics;

public class ConfigNodeHeartbeatCache extends BaseNodeCache {

  private static final NodeStatistics CURRENT_NODE_STATISTICS = new NodeStatistics(
          0, NodeStatus.Running, null
  );

  private final TConfigNodeLocation configNodeLocation;

  public ConfigNodeHeartbeatCache(TConfigNodeLocation configNodeLocation) {
    super();
    this.configNodeLocation = configNodeLocation;
  }

  @Override
  public NodeStatistics updateNodeStatistics() {
    // Skip itself
    if (configNodeLocation.getInternalEndPoint().equals(NodeManager.CURRENT_NODE)) {
      // For first time update
      return CURRENT_NODE_STATISTICS.equals(statistics) ? null : (statistics = CURRENT_NODE_STATISTICS);
    }

    long lastSendTime = 0;
    synchronized (slidingWindow) {
      if (slidingWindow.size() > 0) {
        lastSendTime = slidingWindow.getLast().getSendTimestamp();
      }
    }

    // Update Node status
    NodeStatus status;
    // TODO: Optimize judge logic
    if (System.currentTimeMillis() - lastSendTime > HEARTBEAT_TIMEOUT_TIME) {
      status = NodeStatus.Unknown;
    } else {
      status = NodeStatus.Running;
    }

    // Update loadScore
    long loadScore = NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE;

    NodeStatistics newStatistics = new NodeStatistics(loadScore, status, null);
    return newStatistics.equals(statistics) ? null : (statistics = newStatistics);
  }
}
