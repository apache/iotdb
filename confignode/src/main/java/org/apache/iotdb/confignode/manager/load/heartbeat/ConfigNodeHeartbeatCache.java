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

import java.util.LinkedList;

public class ConfigNodeHeartbeatCache implements IHeartbeatStatistic {

  // Cache heartbeat samples
  private static final int maximumWindowSize = 100;
  private final LinkedList<HeartbeatPackage> slidingWindow;

  // For showing cluster
  private volatile NodeStatus status;

  public ConfigNodeHeartbeatCache() {
    this.slidingWindow = new LinkedList<>();

    this.status = NodeStatus.Running;
  }

  @Override
  public void cacheHeartBeat(HeartbeatPackage newHeartbeat) {
    synchronized (slidingWindow) {
      // Only sequential heartbeats are accepted.
      // And un-sequential heartbeats will be discarded.
      if (slidingWindow.size() == 0
          || slidingWindow.getLast().getSendTimestamp() < newHeartbeat.getSendTimestamp()) {
        slidingWindow.add(newHeartbeat);
      }

      while (slidingWindow.size() > maximumWindowSize) {
        slidingWindow.removeFirst();
      }
    }
  }

  @Override
  public void updateLoadStatistic() {
    long lastSendTime = 0;
    synchronized (slidingWindow) {
      if (slidingWindow.size() > 0) {
        lastSendTime = slidingWindow.getLast().getSendTimestamp();
      }
    }

    // TODO: Optimize
    if (System.currentTimeMillis() - lastSendTime > 20_000) {
      status = NodeStatus.Unknown;
    } else {
      status = NodeStatus.Running;
    }
  }

  @Override
  public float getLoadScore() {
    // Return a copy of loadScore
    switch (status) {
      case Running:
        return 0;
      case Unknown:
      default:
        // The Unknown Node will get the highest loadScore
        return Float.MAX_VALUE;
    }
  }

  @Override
  public NodeStatus getNodeStatus() {
    // Return a copy of status
    switch (status) {
      case Running:
        return NodeStatus.Running;
      case Unknown:
      default:
        return NodeStatus.Unknown;
    }
  }
}
