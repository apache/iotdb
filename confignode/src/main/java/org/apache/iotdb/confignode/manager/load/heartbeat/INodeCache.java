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

/** All the statistic interfaces that provided by HeartbeatCache */
public interface INodeCache {

  /**
   * Cache the newest HeartbeatSample
   *
   * @param newHeartbeatSample The newest HeartbeatSample
   */
  void cacheHeartbeatSample(NodeHeartbeatSample newHeartbeatSample);

  /**
   * Invoking periodically to update Nodes' load statistics
   *
   * @return true if some load statistic changed
   */
  boolean updateLoadStatistic();

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  long getLoadScore();

  /** @return The latest status of a node for showing cluster */
  NodeStatus getNodeStatus();
}
