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

package org.apache.iotdb.confignode.manager.load.cache.node;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.AbstractLoadCache;

/**
 * NodeCache caches the NodeHeartbeatSamples of a Node. Update and cache the current statistics of
 * the Node based on the latest NodeHeartbeatSample.
 */
public abstract class BaseNodeCache extends AbstractLoadCache {

  protected final int nodeId;

  /** Constructor for NodeCache with default NodeStatistics. */
  protected BaseNodeCache(int nodeId) {
    super();
    this.nodeId = nodeId;
    this.currentStatistics.set(NodeStatistics.generateDefaultNodeStatistics());
  }

  public int getNodeId() {
    return nodeId;
  }

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  public long getLoadScore() {
    return ((NodeStatistics) currentStatistics.get()).getLoadScore();
  }

  /**
   * @return The current status of the Node.
   */
  public NodeStatus getNodeStatus() {
    return ((NodeStatistics) currentStatistics.get()).getStatus();
  }

  /**
   * @return The reason why lead to current NodeStatus.
   */
  public String getNodeStatusWithReason() {
    NodeStatistics statistics = (NodeStatistics) this.currentStatistics.get();
    return statistics.getStatusReason() == null
        ? statistics.getStatus().getStatus()
        : statistics.getStatus().getStatus() + "(" + statistics.getStatusReason() + ")";
  }
}
