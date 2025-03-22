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

package org.apache.iotdb.confignode.manager.load.subscriber;

import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;

import org.apache.tsfile.utils.Pair;

import java.util.Collections;
import java.util.Map;

/** NodeStatisticsChangeEvent represents the change of Node statistics. */
public class NodeStatisticsChangeEvent {

  // Map<NodeId, Pair<old NodeStatistics, new NodeStatistics>>
  private final Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap;

  public NodeStatisticsChangeEvent(
      Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap) {
    this.differentNodeStatisticsMap = differentNodeStatisticsMap;
  }

  public Map<Integer, Pair<NodeStatistics, NodeStatistics>> getDifferentNodeStatisticsMap() {
    return Collections.unmodifiableMap(differentNodeStatisticsMap);
  }
}
