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

import java.util.LinkedList;

/** All the statistic interfaces that provided by HeartbeatCache */
public abstract class BaseNodeCache {

  // When the response time of heartbeat is more than 20s, the Node is considered as down
  public static final int HEARTBEAT_TIMEOUT_TIME = 20_000;

  // Max heartbeat cache samples store size
  public static final int MAXIMUM_WINDOW_SIZE = 100;

  // SlidingWindow stores the heartbeat sample data
  protected final LinkedList<NodeHeartbeatSample> slidingWindow = new LinkedList<>();

  // The previous NodeStatistics, used for comparing with
  // the current NodeStatistics to initiate notification when they are different
  protected volatile NodeStatistics previousStatistics;
  // The current NodeStatistics, used for providing statistics to other services
  protected volatile NodeStatistics currentStatistics;

  /** Constructor for NodeCache with default NodeStatistics */
  protected BaseNodeCache() {
    this.previousStatistics = NodeStatistics.generateDefaultNodeStatistics();
    this.currentStatistics = NodeStatistics.generateDefaultNodeStatistics();
  }

  /**
   * Cache the newest NodeHeartbeatSample
   *
   * @param newHeartbeatSample The newest NodeHeartbeatSample
   */
  public void cacheHeartbeatSample(NodeHeartbeatSample newHeartbeatSample) {
    synchronized (slidingWindow) {
      // Only sequential heartbeats are accepted.
      // And un-sequential heartbeats will be discarded.
      if (slidingWindow.isEmpty()
          || slidingWindow.getLast().getSendTimestamp() < newHeartbeatSample.getSendTimestamp()) {
        slidingWindow.add(newHeartbeatSample);
      }

      if (slidingWindow.size() > MAXIMUM_WINDOW_SIZE) {
        slidingWindow.removeFirst();
      }
    }
  }

  /**
   * Invoking periodically in the Cluster-LoadStatistics-Service to update currentStatistics and
   * compare with the previousStatistics, in order to detect whether the Node's statistics has
   * changed
   *
   * @return True if the currentStatistics has changed recently(compare with the
   *     previousStatistics), false otherwise
   */
  public boolean periodicUpdate() {
    updateCurrentStatistics();
    if (!currentStatistics.equals(previousStatistics)) {
      previousStatistics = currentStatistics.deepCopy();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Actively append a custom NodeHeartbeatSample to force a change in the NodeStatistics.
   *
   * <p>For example, this interface can be invoked in Node removing process to forcibly change the
   * corresponding Node's status to Removing without waiting for heartbeat sampling
   *
   * <p>Notice: The ConfigNode-leader doesn't know the specified Node's statistics has changed even
   * if this interface is invoked, since the ConfigNode-leader only detect cluster Nodes' statistics
   * by periodicUpdate interface. However, other service can still read the update of
   * currentStatistics by invoking getters below.
   *
   * @param newHeartbeatSample A custom NodeHeartbeatSample that will lead to needed NodeStatistics
   */
  public void forceUpdate(NodeHeartbeatSample newHeartbeatSample) {
    cacheHeartbeatSample(newHeartbeatSample);
    updateCurrentStatistics();
  }

  /**
   * Update currentStatistics based on recent NodeHeartbeatSamples that cached in the slidingWindow
   */
  protected abstract void updateCurrentStatistics();

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  public long getLoadScore() {
    return currentStatistics.getLoadScore();
  }

  /** @return The current status of the Node */
  public NodeStatus getNodeStatus() {
    // Return a copy of status
    return NodeStatus.parse(currentStatistics.getStatus().getStatus());
  }

  /** @return The reason why lead to current NodeStatus */
  public String getNodeStatusWithReason() {
    if (currentStatistics.getStatusReason() == null) {
      return currentStatistics.getStatus().getStatus();
    } else {
      return currentStatistics.getStatus().getStatus()
          + "("
          + currentStatistics.getStatusReason()
          + ")";
    }
  }

  public NodeStatistics getStatistics() {
    return currentStatistics;
  }

  public NodeStatistics getPreviousStatistics() {
    return previousStatistics;
  }
}
