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

import org.apache.iotdb.commons.cluster.NodeStatus;

import java.util.LinkedList;

/** All the statistic interfaces that provided by HeartbeatCache */
public abstract class BaseNodeCache {

  /** When the response time of heartbeat is more than 20s, the node is considered as down */
  public static final int HEARTBEAT_TIMEOUT_TIME = 20_000;

  /** Max heartbeat cache samples store size */
  public static final int MAXIMUM_WINDOW_SIZE = 100;

  /** SlidingWindow stores the heartbeat sample data */
  final LinkedList<NodeHeartbeatSample> slidingWindow = new LinkedList<>();

  /** The current status of the Node */
  volatile NodeStatus status = NodeStatus.Unknown;
  /** The reason why lead to the current NodeStatus (for showing cluster) */
  volatile String statusReason;

  /**
   * Cache the newest HeartbeatSample
   *
   * @param newHeartbeatSample The newest HeartbeatSample
   */
  public abstract void cacheHeartbeatSample(NodeHeartbeatSample newHeartbeatSample);

  /**
   * Invoking periodically to update Nodes' current running status
   *
   * @return True if the specific Node's status changed, false otherwise
   */
  public abstract boolean updateNodeStatus();

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  public abstract long getLoadScore();

  /** @return The current status of the Node */
  public NodeStatus getNodeStatus() {
    // Return a copy of status
    return NodeStatus.parse(status.getStatus());
  }

  public String getNodeStatusWithReason() {
    if (statusReason == null) {
      return status.getStatus();
    } else {
      return status.getStatus() + "(" + statusReason + ")";
    }
  }

  public void setRemoving() {
    this.status = NodeStatus.Removing;
  }
}
