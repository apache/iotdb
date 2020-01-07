/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query.manage;

import static org.apache.iotdb.cluster.server.RaftServer.CONNECTION_TIME_OUT_MS;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.MetaClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QueryCoordinator records the spec and load of each node, deciding the order of replicas that
 * should be queried
 */
public class QueryCoordinator {

  private static final Logger logger = LoggerFactory.getLogger(QueryCoordinator.class);

  // a status is considered stale if it is older than one minute and should be updated
  private static final long NODE_STATUS_UPDATE_INTERVAL_MS = 60 * 1000L;
  private static final QueryCoordinator INSTANCE = new QueryCoordinator();

  private final Comparator<Node> nodeComparator = Comparator.comparing(this::getNodeStatus);

  private MetaGroupMember metaGroupMember;
  private Map<Node, NodeStatus> nodeStatusMap = new ConcurrentHashMap<>();


  private QueryCoordinator() {
    // singleton class
  }

  public static QueryCoordinator getINSTANCE() {
    return INSTANCE;
  }

  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  /**
   * Reorder the given nodes based on their status, the nodes that are more suitable (have low
   * delay or load) are placed first. This won't change the order of the origin list.
   * @param nodes
   * @return
   */
  public List<Node> reorderNodes(List<Node> nodes) {
    List<Node> reordered = new ArrayList<>(nodes);
    reordered.sort(nodeComparator);
    return reordered;
  }

  private NodeStatus getNodeStatus(Node node) {
    // avoid duplicated computing of concurrent queries
    NodeStatus nodeStatus = nodeStatusMap.computeIfAbsent(node, n -> new NodeStatus());
    long currTime = System.currentTimeMillis();
    if (currTime - nodeStatus.getLastUpdateTime() > NODE_STATUS_UPDATE_INTERVAL_MS
        || nodeStatus.getStatus() == null) {
      MetaClient metaClient = (MetaClient) metaGroupMember.connectNode(node);
      AtomicReference<TNodeStatus> resultRef = new AtomicReference<>();
      GenericHandler<TNodeStatus> handler = new GenericHandler<>(node, resultRef);

      try {
        long startTime = System.nanoTime();
        synchronized (resultRef) {
          metaClient.queryNodeStatus(handler);
          resultRef.wait(CONNECTION_TIME_OUT_MS);
        }
        TNodeStatus status = resultRef.get();
        if (status != null) {
          long responseTime = System.nanoTime() - startTime;
          nodeStatus.setStatus(status);
          nodeStatus.setLastUpdateTime(System.currentTimeMillis());
          nodeStatus.setLastResponseLatency(responseTime);
        } else {
          nodeStatus.setLastResponseLatency(Long.MAX_VALUE);
        }
        logger.info("NodeStatus of {} is updated, status: {}, response time: {}", node,
            nodeStatus.getStatus(), nodeStatus.getLastResponseLatency());
      } catch (TException | InterruptedException e) {
        logger.error("Cannot query the node status of {}", node, e);
      }
    }
    return nodeStatus;
  }
}
