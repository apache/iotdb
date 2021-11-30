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

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.SessionManager;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterSessionManager extends SessionManager {

  protected ClusterSessionManager() {
    // singleton
  }

  private static final Logger logger = LoggerFactory.getLogger(ClusterSessionManager.class);

  /**
   * The Coordinator of the local node. Through this node ClientServer queries data and meta from
   * the cluster and performs data manipulations to the cluster.
   */
  private Coordinator coordinator;

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  /**
   * queryId -> queryContext map. When a query ends either normally or accidentally, the resources
   * used by the query can be found in the context and then released.
   */
  private Map<Long, RemoteQueryContext> queryContextMap = new ConcurrentHashMap<>();

  public void putContext(long queryId, RemoteQueryContext context) {
    queryContextMap.put(queryId, context);
  }

  public void releaseQueryResource(long queryId) throws StorageEngineException {
    super.releaseQueryResource(queryId);
    this.releaseRemoteQueryResource(queryId);
  }

  /** Release remote resources used by a query. */
  public void releaseRemoteQueryResource(long queryId) {
    // release resources remotely
    RemoteQueryContext context = queryContextMap.remove(queryId);
    if (context != null) {
      // release the resources in every queried node
      for (Entry<RaftNode, Set<Node>> headerEntry : context.getQueriedNodesMap().entrySet()) {
        RaftNode header = headerEntry.getKey();
        Set<Node> queriedNodes = headerEntry.getValue();
        for (Node queriedNode : queriedNodes) {
          releaseQueryResourceForOneNode(queryId, header, queriedNode);
        }
      }
    }
  }

  protected void releaseQueryResourceForOneNode(long queryId, RaftNode header, Node queriedNode) {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      GenericHandler<Void> handler = new GenericHandler<>(queriedNode, new AtomicReference<>());
      try {
        AsyncDataClient client =
            ClusterIoTDB.getInstance()
                .getAsyncDataClient(queriedNode, ClusterConstant.getReadOperationTimeoutMS());
        client.endQuery(header, coordinator.getThisNode(), queryId, handler);
      } catch (IOException | TException e) {
        logger.error("Cannot end query {} in {}", queryId, queriedNode);
      }
    } else {
      SyncDataClient syncDataClient = null;
      try {
        syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(queriedNode, ClusterConstant.getReadOperationTimeoutMS());
        syncDataClient.endQuery(header, coordinator.getThisNode(), queryId);
      } catch (IOException | TException e) {
        // the connection may be broken, close it to avoid it being reused
        if (syncDataClient != null) {
          syncDataClient.close();
        }
        logger.error("Cannot end query {} in {}", queryId, queriedNode);
      } finally {
        if (syncDataClient != null) {
          syncDataClient.returnSelf();
        }
      }
    }
  }

  public static ClusterSessionManager getInstance() {
    return ClusterSessionManagerHolder.INSTANCE;
  }

  private static class ClusterSessionManagerHolder {

    private ClusterSessionManagerHolder() {}

    private static final ClusterSessionManager INSTANCE = new ClusterSessionManager();
  }
}
