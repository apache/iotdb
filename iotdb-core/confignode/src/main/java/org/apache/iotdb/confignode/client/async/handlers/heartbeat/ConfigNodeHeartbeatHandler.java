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

import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatResp;

import org.apache.thrift.async.AsyncMethodCallback;

public class ConfigNodeHeartbeatHandler implements AsyncMethodCallback<TConfigNodeHeartbeatResp> {

  private final IManager configManager;
  private final int nodeId;
  private final LoadCache loadCache;

  public ConfigNodeHeartbeatHandler(IManager configManager, int nodeId, LoadCache loadCache) {
    this.configManager = configManager;
    this.nodeId = nodeId;
    this.loadCache = loadCache;
  }

  @Override
  public void onComplete(TConfigNodeHeartbeatResp resp) {
    loadCache.cacheConfigNodeHeartbeatSample(nodeId, resp);
  }

  @Override
  public void onError(Exception e) {
    if (ThriftClient.isConnectionBroken(e)) {
      loadCache.forceUpdateNodeCache(
          NodeType.ConfigNode,
          nodeId,
          NodeHeartbeatSample.generateDefaultSample(NodeStatus.Unknown));
    }
  }
}
