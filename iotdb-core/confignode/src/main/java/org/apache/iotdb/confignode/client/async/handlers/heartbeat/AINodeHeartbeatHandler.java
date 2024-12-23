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

package org.apache.iotdb.confignode.client.async.handlers.heartbeat;

import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatResp;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;

import org.apache.thrift.async.AsyncMethodCallback;

public class AINodeHeartbeatHandler implements AsyncMethodCallback<TAIHeartbeatResp> {

  private final int nodeId;

  private final LoadManager loadManager;

  public AINodeHeartbeatHandler(int nodeId, LoadManager loadManager) {
    this.nodeId = nodeId;
    this.loadManager = loadManager;
  }

  @Override
  public void onComplete(TAIHeartbeatResp aiHeartbeatResp) {
    loadManager
        .getLoadCache()
        .cacheAINodeHeartbeatSample(nodeId, new NodeHeartbeatSample(aiHeartbeatResp));
  }

  @Override
  public void onError(Exception e) {
    if (ThriftClient.isConnectionBroken(e)) {
      loadManager.forceUpdateNodeCache(
          NodeType.DataNode, nodeId, new NodeHeartbeatSample(NodeStatus.Unknown));
    }
    loadManager.getLoadCache().resetHeartbeatProcessing(nodeId);
  }
}
