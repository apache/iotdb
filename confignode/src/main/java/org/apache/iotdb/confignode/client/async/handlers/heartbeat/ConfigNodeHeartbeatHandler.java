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

import org.apache.iotdb.confignode.manager.load.heartbeat.HeartbeatSampleCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.NodeHeartbeatSample;

import org.apache.thrift.async.AsyncMethodCallback;

public class ConfigNodeHeartbeatHandler implements AsyncMethodCallback<Long> {

  private final int nodeId;
  private final HeartbeatSampleCache cache;

  public ConfigNodeHeartbeatHandler(int nodeId, HeartbeatSampleCache cache) {
    this.nodeId = nodeId;
    this.cache = cache;
  }

  @Override
  public void onComplete(Long timestamp) {
    long receiveTime = System.currentTimeMillis();
    cache.cacheConfigNodeHeartbeatSample(nodeId, new NodeHeartbeatSample(timestamp, receiveTime));
  }

  @Override
  public void onError(Exception e) {
    // Do nothing
  }
}
