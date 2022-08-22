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

/** All the statistic interfaces that provided by HeartbeatCache */
public abstract class BaseNodeCache {

  /** When the response time of heartbeat is more than 20s, the node is considered as down */
  static final int HEARTBEAT_TIMEOUT_TIME = 20_000;

  /** Max heartbeat cache samples store size */
  static final int MAXIMUM_WINDOW_SIZE = 100;

  /** SlidingWindow stores the heartbeat sample data */
  final LinkedList<NodeHeartbeatSample> slidingWindow = new LinkedList<>();

  /** Node status, using for `show cluster` command */
  volatile NodeStatus status = NodeStatus.Unknown;

  /** Represent that if this node is in removing status */
  volatile boolean removing;

  /**
   * Cache the newest HeartbeatSample
   *
   * @param newHeartbeatSample The newest HeartbeatSample
   */
  public abstract void cacheHeartbeatSample(NodeHeartbeatSample newHeartbeatSample);

  /**
   * Invoking periodically to update Nodes' load statistics
   *
   * @return true if some load statistic changed
   */
  public abstract boolean updateLoadStatistic();

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  public abstract long getLoadScore();

  /** @return The latest status of a node for showing cluster */
  public abstract NodeStatus getNodeStatus();

  public void setRemoving(boolean removing) {
    this.removing = removing;
  }

  public boolean isRemoving() {
    return this.removing;
  }
}
