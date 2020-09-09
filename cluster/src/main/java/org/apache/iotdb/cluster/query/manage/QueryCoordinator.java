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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.utils.TestOnly;
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
    if (node.equals(metaGroupMember.getThisNode())) {
      return nodeStatus;
    }

    long currTime = System.currentTimeMillis();
    if (currTime - nodeStatus.getLastUpdateTime() > NODE_STATUS_UPDATE_INTERVAL_MS
        || nodeStatus.getStatus() == null) {

      try {
        long startTime;
        long responseTime;
        TNodeStatus status;
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncMetaClient asyncMetaClient = (AsyncMetaClient) metaGroupMember.getAsyncClient(node);
          startTime = System.nanoTime();
          status = SyncClientAdaptor.queryNodeStatus(asyncMetaClient);
          responseTime = System.nanoTime() - startTime;
        } else {
          SyncMetaClient syncMetaClient = (SyncMetaClient) metaGroupMember.getSyncClient(node);
          startTime = System.nanoTime();
          status = syncMetaClient.queryNodeStatus();
          responseTime = System.nanoTime() - startTime;
          metaGroupMember.putBackSyncClient(syncMetaClient);
        }

        if (status != null) {
          nodeStatus.setStatus(status);
          nodeStatus.setLastUpdateTime(System.currentTimeMillis());
          nodeStatus.setLastResponseLatency(responseTime);
        } else {
          nodeStatus.setLastResponseLatency(Long.MAX_VALUE);
        }
        logger.warn("NodeStatus of {} is updated, status: {}, response time: {}", node,
            nodeStatus.getStatus(), nodeStatus.getLastResponseLatency());
      } catch (TException e) {
        logger.error("Cannot query the node status of {}", node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Cannot query the node status of {}", node, e);
      }
    }
    return nodeStatus;
  }

  public long getLastResponseLatency(Node node) {
    NodeStatus nodeStatus = getNodeStatus(node);
    return nodeStatus.getLastResponseLatency();
  }

  @TestOnly
  public void clear() {
    nodeStatusMap.clear();
  }
}
