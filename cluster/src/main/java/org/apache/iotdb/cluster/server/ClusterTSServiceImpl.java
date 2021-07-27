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
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.query.ClusterPlanExecutor;
import org.apache.iotdb.cluster.query.ClusterPlanner;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ClusterTSServiceImpl is the cluster version of TSServiceImpl, which is responsible for the processing of
 * the user requests (sqls and session api). It inherits the basic procedures from TSServiceImpl,
 * but redirect the queries of data and metadata to a MetaGroupMember of the local node.
 */
public class ClusterTSServiceImpl extends TSServiceImpl {

  private static final Logger logger = LoggerFactory.getLogger(ClusterTSServiceImpl.class);
  /**
   * The Coordinator of the local node. Through this node queries data and meta from
   * the cluster and performs data manipulations to the cluster.
   */
  private Coordinator coordinator;


//  /**
//   * Using the poolServer, ClusterTSServiceImpl will listen to a socket to accept thrift requests like an
//   * HttpServer.
//   */
//  private TServer poolServer;
//
//  /** The socket poolServer will listen to. Async service requires nonblocking socket */
//  private TServerTransport serverTransport;

  /**
   * queryId -> queryContext map. When a query ends either normally or accidentally, the resources
   * used by the query can be found in the context and then released.
   */
  private Map<Long, RemoteQueryContext> queryContextMap = new ConcurrentHashMap<>();

  public ClusterTSServiceImpl(MetaGroupMember metaGroupMember) throws QueryProcessException {
    super();
    this.processor = new ClusterPlanner();
  }

  public void setExecutor(MetaGroupMember metaGroupMember) throws QueryProcessException {
    this.executor = new ClusterPlanExecutor(metaGroupMember);
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
    } catch (QueryProcessException e) {
      logger.warn("Illegal plan detected： {}", plan);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }

    return coordinator.executeNonQueryPlan(plan);
  }

  /**
   * EventHandler handles the preprocess and postprocess of the thrift requests, but it currently
   * only release resources when a client disconnects.
   */
  class EventHandler implements TServerEventHandler {

    @Override
    public void preServe() {
      // do nothing
    }

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
      return null;
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
      ClusterTSServiceImpl.this.handleClientExit();
    }

    @Override
    public void processContext(
        ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
      // do nothing
    }
  }

  /**
   * Get the data types of each path in “paths”. If "aggregations" is not null, then it should be
   * corresponding to "paths" one to one and the data type will be the type of the aggregation over
   * the corresponding path.
   *
   * @param paths full timeseries paths
   * @param aggregations if not null, it should be the same size as "paths"
   * @return the data types of "paths" (using the aggregations)
   * @throws MetadataException
   */
  @Override
  protected List<TSDataType> getSeriesTypesByPaths(
      List<PartialPath> paths, List<String> aggregations) throws MetadataException {
    return ((CMManager) IoTDB.metaManager).getSeriesTypesByPath(paths, aggregations).left;
  }

  /**
   * Get the data types of each path in “paths”. If "aggregation" is not null, all "paths" will use
   * this aggregation.
   *
   * @param paths full timeseries paths
   * @param aggregation if not null, it means "paths" all use this aggregation
   * @return the data types of "paths" (using the aggregation)
   * @throws MetadataException
   */
  protected List<TSDataType> getSeriesTypesByString(List<PartialPath> paths, String aggregation)
      throws MetadataException {
    return ((CMManager) IoTDB.metaManager).getSeriesTypesByPaths(paths, aggregation).left;
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
                      queriedNode, RaftServer.getReadOperationTimeoutMS());
              client.endQuery(header, coordinator.getThisNode(), queryId, handler);
            } else {
              try (SyncDataClient syncDataClient =
                  coordinator.getSyncDataClient(
                      queriedNode, RaftServer.getReadOperationTimeoutMS())) {
                syncDataClient.endQuery(header, coordinator.getThisNode(), queryId);
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
