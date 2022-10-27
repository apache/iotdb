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
import org.apache.iotdb.confignode.persistence.node.NodeStatistics;

import java.util.LinkedList;

/** All the statistic interfaces that provided by HeartbeatCache */
public abstract class BaseNodeCache {

  /** When the response time of heartbeat is more than 20s, the node is considered as down */
  public static final int HEARTBEAT_TIMEOUT_TIME = 20_000;

  /** Max heartbeat cache samples store size */
  public static final int MAXIMUM_WINDOW_SIZE = 100;

  /** SlidingWindow stores the heartbeat sample data */
  protected final LinkedList<NodeHeartbeatSample> slidingWindow = new LinkedList<>();

  protected volatile NodeStatistics statistics;

  /** Constructor for NodeCache with default NodeStatistics */
  protected BaseNodeCache() {
    this.statistics = NodeStatistics.generateDefaultNodeStatistics();
  }

  /**
   * Cache the newest HeartbeatSample
   *
   * @param newHeartbeatSample The newest HeartbeatSample
   */
  public void cacheHeartbeatSample(NodeHeartbeatSample newHeartbeatSample) {
    synchronized (slidingWindow) {
      // Only sequential heartbeats are accepted.
      // And un-sequential heartbeats will be discarded.
      if (slidingWindow.size() == 0
          || slidingWindow.getLast().getSendTimestamp() < newHeartbeatSample.getSendTimestamp()) {
        slidingWindow.add(newHeartbeatSample);
      }

      if (slidingWindow.size() > MAXIMUM_WINDOW_SIZE) {
        slidingWindow.removeFirst();
      }
    }
  }

  /** Invoking periodically to update Node's latest NodeStatistics */
  public abstract void updateNodeStatistics();

  /**
   * Actively append a custom NodeHeartbeatSample to force a change in the NodeStatistics.
   *
   * <p>For example, this interface can be invoked in Node removing process to forcibly change the
   * corresponding Node's status to Removing without waiting for heartbeat sampling
   *
   * @param newHeartbeatSample A custom NodeHeartbeatSample that will lead to needed NodeStatistics
   */
  public void forceUpdate(NodeHeartbeatSample newHeartbeatSample) {
    cacheHeartbeatSample(newHeartbeatSample);
    updateNodeStatistics();
  }

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  public long getLoadScore() {
    return statistics.getLoadScore();
  }

  /** @return The current status of the Node */
  public NodeStatus getNodeStatus() {
    // Return a copy of status
    return NodeStatus.parse(statistics.getStatus().getStatus());
  }

  /** @return The reason why lead to current NodeStatus */
  public String getNodeStatusWithReason() {
    if (statistics.getStatusReason() == null) {
      return statistics.getStatus().getStatus();
    } else {
      return statistics.getStatus().getStatus() + "(" + statistics.getStatusReason() + ")";
    }
  }

  public NodeStatistics getStatistics() {
    return statistics;
  }
}
