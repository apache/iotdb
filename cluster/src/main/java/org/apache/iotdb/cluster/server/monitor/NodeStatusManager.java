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

package org.apache.iotdb.cluster.server.monitor;

import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NodeStatusManager manages the status (network latency, workload, connectivity) of each node in
 * the whole cluster. The status is updated on demand, so it may not be up-to-date if not forced to
 * update.
 */
public class NodeStatusManager {

  private static final Logger logger = LoggerFactory.getLogger(NodeStatusManager.class);
  // a status is considered stale if it is older than one minute and should be updated
  private static final long NODE_STATUS_UPDATE_INTERVAL_MS = 60 * 1000L;
  private static final NodeStatusManager INSTANCE = new NodeStatusManager();

  private MetaGroupMember metaGroupMember;
  private Map<Node, NodeStatus> nodeStatusMap = new ConcurrentHashMap<>();

  public static NodeStatusManager getINSTANCE() {
    return INSTANCE;
  }

  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  private TNodeStatus getNodeStatusWithAsyncServer(Node node) {
    TNodeStatus status;
    AsyncMetaClient asyncMetaClient = (AsyncMetaClient) metaGroupMember.getAsyncClient(node);
    if (asyncMetaClient == null) {
      return null;
    }
    try {
      status = SyncClientAdaptor.queryNodeStatus(asyncMetaClient);
    } catch (TException e) {
      if (e.getCause() instanceof ConnectException) {
        logger.warn("Cannot query the node status of {}: {}", node, e.getCause());
      } else {
        logger.error("query node status failed {}", node, e);
      }
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Cannot query the node status of {}", node, e);
      return null;
    }
    return status;
  }

  private TNodeStatus getNodeStatusWithSyncServer(Node node) {
    TNodeStatus status;
    SyncMetaClient syncMetaClient = (SyncMetaClient) metaGroupMember.getSyncClient(node);
    if (syncMetaClient == null) {
      logger.error("Cannot query the node status of {} for no available client", node);
      return null;
    }
    try {
      status = syncMetaClient.queryNodeStatus();
    } catch (TException e) {
      syncMetaClient.getInputProtocol().getTransport().close();
      logger.error("Cannot query the node status of {}", node, e);
      return null;
    } finally {
      ClientUtils.putBackSyncClient(syncMetaClient);
    }
    return status;
  }

  /**
   * Get the status of the given node. If tryUpdate == true and the current status is older than
   * NODE_STATUS_UPDATE_INTERVAL_MS, it will be updated.
   *
   * @param node
   * @param tryUpdate when set to true, the manager will try to update the status of the node if it
   *     is old enough, otherwise, it will just return the last recorded status.
   * @return
   */
  public NodeStatus getNodeStatus(Node node, boolean tryUpdate) {
    // avoid duplicated computing of concurrent queries
    NodeStatus nodeStatus = nodeStatusMap.computeIfAbsent(node, n -> new NodeStatus());
    if (metaGroupMember == null || node.equals(metaGroupMember.getThisNode())) {
      return nodeStatus;
    }

    if (tryUpdate) {
      tryUpdateNodeStatus(node, nodeStatus);
    }
    return nodeStatus;
  }

  private void tryUpdateNodeStatus(Node node, NodeStatus nodeStatus) {
    long currTime = System.currentTimeMillis();
    if (nodeStatus.getStatus() != null
        && currTime - nodeStatus.getLastUpdateTime() <= NODE_STATUS_UPDATE_INTERVAL_MS) {
      return;
    }

    long startTime = System.nanoTime();
    TNodeStatus status;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      status = getNodeStatusWithAsyncServer(node);
    } else {
      status = getNodeStatusWithSyncServer(node);
    }
    long responseTime = System.nanoTime() - startTime;

    if (status != null) {
      nodeStatus.setStatus(status);
      nodeStatus.setLastUpdateTime(System.currentTimeMillis());
      nodeStatus.setLastResponseLatency(responseTime);
    } else {
      nodeStatus.setLastResponseLatency(Long.MAX_VALUE);
    }
    logger.info(
        "NodeStatus of {} is updated, status: {}, response time: {}",
        node,
        nodeStatus.getStatus(),
        nodeStatus.getLastResponseLatency());
  }

  public long getLastResponseLatency(Node node) {
    NodeStatus nodeStatus = getNodeStatus(node, true);
    return nodeStatus.getLastResponseLatency();
  }

  @TestOnly
  public void clear() {
    nodeStatusMap.clear();
  }

  public void activate(Node node) {
    getNodeStatus(node, false).activate();
  }

  public void deactivate(Node node) {
    getNodeStatus(node, false).deactivate();
  }

  /**
   * @param node
   * @return whether the node is CURRENTLY available, this method will not try to update its status
   *     to avoid deadlock
   */
  public boolean isActivated(Node node) {
    return getNodeStatus(node, false).isActivated();
  }
}
