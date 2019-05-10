/**
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
package org.apache.iotdb.cluster.entity;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.cluster.concurrent.pool.QPTaskManager;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.data.DataPartitionHolder;
import org.apache.iotdb.cluster.entity.metadata.MetadataHolder;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.rpc.raft.impl.RaftNodeAsClientManager;
import org.apache.iotdb.cluster.rpc.raft.processor.QueryMetricAsyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.nonquery.DataGroupNonQueryAsyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.nonquery.MetaGroupNonQueryAsyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querydata.CloseSeriesReaderSyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querydata.InitSeriesReaderSyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querydata.QuerySeriesDataByTimestampSyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querydata.QuerySeriesDataSyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querymetadata.QueryMetadataAsyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querymetadata.QueryMetadataInStringAsyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querymetadata.QueryPathsAsyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querymetadata.QuerySeriesTypeAsyncProcessor;
import org.apache.iotdb.cluster.rpc.raft.processor.querymetadata.QueryTimeSeriesAsyncProcessor;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.RegisterManager;
import org.apache.iotdb.cluster.service.ClusterMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each server represents a node in the physical world.
 */
public class Server {

  private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

  private static final ClusterConfig CLUSTER_CONF = ClusterDescriptor.getInstance().getConfig();

  private static final RaftNodeAsClientManager CLIENT_MANAGER = RaftNodeAsClientManager
      .getInstance();

  /**
   * Metadata Group Holder
   */
  private MetadataHolder metadataHolder;

  /**
   * Data Group Holder Map String: group id
   */
  private Map<String, DataPartitionHolder> dataPartitionHolderMap;

  /**
   * PeerId of this physical node
   */
  private PeerId serverId;

  /**
   * IoTDB stand-alone instance
   */
  private IoTDB iotdb;

  private RegisterManager registerManager = new RegisterManager();

  public static void main(String[] args) throws ProcessorException, InterruptedException {
    Server server = Server.getInstance();
    server.start();
  }

  public void start() throws ProcessorException, InterruptedException {
    /** Stand-alone version of IoTDB, be careful to replace the internal JDBC Server with a cluster version **/
    iotdb = new IoTDB();
    iotdb.active();
    CLIENT_MANAGER.init();

    /** Init raft groups **/
    PeerId[] peerIds = RaftUtils.convertStringArrayToPeerIdArray(CLUSTER_CONF.getNodes());
    serverId = new PeerId(CLUSTER_CONF.getIp(), CLUSTER_CONF.getPort());
    RpcServer rpcServer = new RpcServer(serverId.getPort());
    RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);

    registerNonQueryProcessor(rpcServer);
    registerQueryMetadataProcessor(rpcServer);
    registerQueryDataProcessor(rpcServer);
    registerQueryMetricProcessor(rpcServer);

    metadataHolder = new MetadataRaftHolder(peerIds, serverId, rpcServer, true);
    metadataHolder.init();
    metadataHolder.start();

    LOGGER.info("Metadata group has started.");

    dataPartitionHolderMap = new HashMap<>();
    Router router = Router.getInstance();
    PhysicalNode[][] groups = router.getGroupsNodes(serverId.getIp(), serverId.getPort());

    for (int i = 0; i < groups.length; i++) {
      PhysicalNode[] group = groups[i];
      String groupId = router.getGroupID(group);
      DataPartitionHolder dataPartitionHolder = new DataPartitionRaftHolder(groupId,
          RaftUtils.getPeerIdArrayFrom(group), serverId, rpcServer, false);
      dataPartitionHolder.init();
      dataPartitionHolder.start();
      dataPartitionHolderMap.put(groupId, dataPartitionHolder);
      LOGGER.info("{} group has started", groupId);
      Router.getInstance().showPhysicalNodes(groupId);
    }

    try {
      LOGGER.info("Register Cluster Monitor to JMX service.");
      registerManager.register(ClusterMonitor.INSTANCE);
    } catch (StartupException e) {
      stop();
      return;
    }
  }

  private void registerNonQueryProcessor(RpcServer rpcServer) {
    rpcServer.registerUserProcessor(new DataGroupNonQueryAsyncProcessor());
    rpcServer.registerUserProcessor(new MetaGroupNonQueryAsyncProcessor());
  }

  private void registerQueryMetadataProcessor(RpcServer rpcServer) {
    rpcServer.registerUserProcessor(new QueryTimeSeriesAsyncProcessor());
    rpcServer.registerUserProcessor(new QueryMetadataInStringAsyncProcessor());
    rpcServer.registerUserProcessor(new QueryMetadataAsyncProcessor());
    rpcServer.registerUserProcessor(new QuerySeriesTypeAsyncProcessor());
    rpcServer.registerUserProcessor(new QueryPathsAsyncProcessor());
  }

  private void registerQueryDataProcessor(RpcServer rpcServer) {
    rpcServer.registerUserProcessor(new InitSeriesReaderSyncProcessor());
    rpcServer.registerUserProcessor(new QuerySeriesDataSyncProcessor());
    rpcServer.registerUserProcessor(new QuerySeriesDataByTimestampSyncProcessor());
    rpcServer.registerUserProcessor(new CloseSeriesReaderSyncProcessor());
  }

  private void registerQueryMetricProcessor(RpcServer rpcServer) {
    rpcServer.registerUserProcessor(new QueryMetricAsyncProcessor());
  }

  public void stop() throws ProcessorException, InterruptedException {
    QPTaskManager.getInstance().close(true, ClusterConstant.CLOSE_QP_SUB_TASK_BLOCK_TIMEOUT);
    iotdb.deactivate();
    CLIENT_MANAGER.shutdown();
    metadataHolder.stop();
    for (DataPartitionHolder dataPartitionHolder : dataPartitionHolderMap.values()) {
      dataPartitionHolder.stop();
    }

    registerManager.deregisterAll();
  }

  public PeerId getServerId() {
    return serverId;
  }

  public MetadataHolder getMetadataHolder() {
    return metadataHolder;
  }

  public Map<String, DataPartitionHolder> getDataPartitionHolderMap() {
    return dataPartitionHolderMap;
  }

  public DataPartitionHolder getDataPartitionHolder(String groupId) {
    return dataPartitionHolderMap.get(groupId);
  }

  public static final Server getInstance() {
    return ServerHolder.INSTANCE;
  }

  private static class ServerHolder {

    private static final Server INSTANCE = new Server();

    private ServerHolder() {

    }
  }
}
