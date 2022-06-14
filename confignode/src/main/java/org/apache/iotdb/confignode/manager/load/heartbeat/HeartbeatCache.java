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
package org.apache.iotdb.confignode.manager.load.heartbeat;

import org.apache.iotdb.commons.cluster.NodeStatus;

import java.util.HashMap;
import java.util.Map;

/** HeartbeatCache caches and maintains all the heartbeat data */
public class HeartbeatCache implements IHeartbeatStatistic {

  // Cached heartbeat samples
  private final HeartbeatWindow window;

  // For guiding queries, the higher the score the higher the load
  private volatile float loadScore;
  // For showing cluster
  private volatile NodeStatus status;

  public HeartbeatCache() {
    this.window = new HeartbeatWindow();

    this.loadScore = 0;
    this.status = NodeStatus.Running;
  }

  @Override
  public void cacheHeartBeat(HeartbeatPackage newHeartbeat) {
    window.addHeartbeat(newHeartbeat);
  }

  @Override
  public void updateLoadStatistic() {
    long lastSendTime = window.getLastSendTime();
    // TODO: Optimize
    loadScore = -lastSendTime;
    if (System.currentTimeMillis() - lastSendTime > 20_000) {
      status = NodeStatus.Unknown;
    } else {
      status = NodeStatus.Running;
    }
  }

  @Override
  public float getLoadScore() {
    return loadScore;
  }

  @Override
  public NodeStatus getNodeStatus() {
    return status;
  }
}
