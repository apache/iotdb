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

package org.apache.iotdb.cluster.query.manage;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.monitor.NodeStatus;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * QueryCoordinator records the spec and load of each node, deciding the order of replicas that
 * should be queried
 */
public class QueryCoordinator {

  // a status is considered stale if it is older than one minute and should be updated
  private static final QueryCoordinator INSTANCE = new QueryCoordinator();
  private static final NodeStatusManager STATUS_MANAGER = NodeStatusManager.getINSTANCE();

  private final Comparator<Node> nodeComparator = Comparator.comparing(this::getNodeStatus);

  private QueryCoordinator() {
    // singleton class
  }

  public static QueryCoordinator getINSTANCE() {
    return INSTANCE;
  }

  /**
   * Reorder the given nodes based on their status, the nodes that are more suitable (have low delay
   * or load) are placed first. This won't change the order of the original list.
   *
   * @param nodes
   * @return
   */
  public List<Node> reorderNodes(List<Node> nodes) {
    List<Node> reordered = new ArrayList<>(nodes);
    reordered.sort(nodeComparator);
    return reordered;
  }

  public NodeStatus getNodeStatus(Node node) {
    return STATUS_MANAGER.getNodeStatus(node, true);
  }
}
