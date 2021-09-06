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

package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.query.ClusterPlanExecutor;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ClusterTSServiceImpl is the cluster version of TSServiceImpl, which is responsible for the
 * processing of the user requests (sqls and session api). It inherits the basic procedures from
 * TSServiceImpl, but redirect the queries of data and metadata to a MetaGroupMember of the local
 * node.
 */
public class ClusterTSServiceImpl extends TSServiceImpl {

  private static final Logger logger = LoggerFactory.getLogger(ClusterTSServiceImpl.class);
  /**
   * The Coordinator of the local node. Through this node queries data and meta from the cluster and
   * performs data manipulations to the cluster.
   */
  private Coordinator coordinator;

  /**
   * queryId -> queryContext map. When a query ends either normally or accidentally, the resources
   * used by the query can be found in the context and then released.
   */
  private Map<Long, RemoteQueryContext> queryContextMap = new ConcurrentHashMap<>();

  public ClusterTSServiceImpl() throws QueryProcessException {}

  public void setExecutor(MetaGroupMember metaGroupMember) throws QueryProcessException {
    executor = new ClusterPlanExecutor(metaGroupMember);
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  /**
   * Redirect the plan to the local Coordinator so that it will be processed cluster-wide.
   *
   * @param plan
   * @return
   */
  @Override
  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      plan.checkIntegrity();
      if (!(plan instanceof SetSystemModePlan)
          && !(plan instanceof FlushPlan)
          && IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
        throw new QueryProcessException(
            "Current system mode is read-only, does not support non-query operation");
      }
    } catch (QueryProcessException e) {
      logger.warn("Illegal plan detected： {}", plan);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }

    return coordinator.executeNonQueryPlan(plan);
  }

  /**
   * Generate and cache a QueryContext using "queryId". In the distributed version, the QueryContext
   * is a RemoteQueryContext.
   *
   * @param queryId
   * @return a RemoteQueryContext using queryId
   */
  @Override
  protected QueryContext genQueryContext(long queryId, boolean debug) {
    RemoteQueryContext context = new RemoteQueryContext(queryId, debug);
    queryContextMap.put(queryId, context);
    return context;
  }

  /**
   * Release the local and remote resources used by a query.
   *
   * @param queryId
   * @throws StorageEngineException
   */
  @Override
  protected void releaseQueryResource(long queryId) throws StorageEngineException {
    // release resources locally
    super.releaseQueryResource(queryId);
    // release resources remotely
    RemoteQueryContext context = queryContextMap.remove(queryId);
    if (context != null) {
      // release the resources in every queried node
      for (Entry<RaftNode, Set<Node>> headerEntry : context.getQueriedNodesMap().entrySet()) {
        RaftNode header = headerEntry.getKey();
        Set<Node> queriedNodes = headerEntry.getValue();

        for (Node queriedNode : queriedNodes) {
          GenericHandler<Void> handler = new GenericHandler<>(queriedNode, new AtomicReference<>());
          try {
            if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
              AsyncDataClient client =
                  coordinator.getAsyncDataClient(
                      queriedNode, ClusterConstant.getReadOperationTimeoutMS());
              client.endQuery(header, coordinator.getThisNode(), queryId, handler);
            } else {
              try (SyncDataClient syncDataClient =
                  coordinator.getSyncDataClient(
                      queriedNode, ClusterConstant.getReadOperationTimeoutMS())) {
                try {
                  syncDataClient.endQuery(header, coordinator.getThisNode(), queryId);
                } catch (TException e) {
                  // the connection may be broken, close it to avoid it being reused
                  syncDataClient.getInputProtocol().getTransport().close();
                  throw e;
                }
              }
            }
          } catch (IOException | TException e) {
            logger.error("Cannot end query {} in {}", queryId, queriedNode);
          }
        }
      }
    }
  }
}
