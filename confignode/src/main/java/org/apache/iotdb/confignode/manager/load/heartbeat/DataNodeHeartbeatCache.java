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

/** DataNodeHeartbeatCache caches and maintains all the heartbeat data */
public class DataNodeHeartbeatCache implements INodeCache {

  // TODO: This class might be split into DataNodeCache and ConfigNodeCache

  // Cache heartbeat samples
  private static final int maximumWindowSize = 100;
  private final LinkedList<NodeHeartbeatSample> slidingWindow;

  // For guiding queries, the higher the score the higher the load
  private volatile float loadScore;
  // For showing cluster
  private volatile NodeStatus status;

  public DataNodeHeartbeatCache() {
    this.slidingWindow = new LinkedList<>();

    this.loadScore = 0;
    this.status = NodeStatus.Running;
  }

  @Override
  public void cacheHeartbeatSample(NodeHeartbeatSample newHeartbeatSample) {
    synchronized (slidingWindow) {
      // Only sequential HeartbeatSamples are accepted.
      // And un-sequential HeartbeatSamples will be discarded.
      if (slidingWindow.size() == 0
          || slidingWindow.getLast().getSendTimestamp() < newHeartbeatSample.getSendTimestamp()) {
        slidingWindow.add(newHeartbeatSample);
      }

      if (slidingWindow.size() > maximumWindowSize) {
        slidingWindow.removeFirst();
      }
    }
  }

  @Override
  public boolean updateLoadStatistic() {
    long lastSendTime = 0;
    synchronized (slidingWindow) {
      if (slidingWindow.size() > 0) {
        lastSendTime = slidingWindow.getLast().getSendTimestamp();
      }
    }

    NodeStatus originStatus;
    switch (status) {
      case Running:
        originStatus = NodeStatus.Running;
        break;
      case Unknown:
      default:
        originStatus = NodeStatus.Unknown;
    }

    // TODO: Optimize judge logic
    if (System.currentTimeMillis() - lastSendTime > 20_000) {
      status = NodeStatus.Unknown;
    } else {
      status = NodeStatus.Running;
    }
    return !status.getStatus().equals(originStatus.getStatus());
  }

  @Override
  public float getLoadScore() {
    // Return a copy of loadScore
    switch (status) {
      case Running:
        return loadScore;
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
