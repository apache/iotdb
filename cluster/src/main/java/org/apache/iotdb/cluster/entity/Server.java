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
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.data.DataPartitionHolder;
import org.apache.iotdb.cluster.entity.metadata.MetadataHolder;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.service.IoTDB;

public class Server {

  private static final ClusterConfig ClusterConf = ClusterDescriptor.getInstance().getConfig();

  private MetadataHolder metadataHolder;
  private Map<String, DataPartitionHolder> dataPartitionHolderMap;

  public void start() throws AuthException {
    // Stand-alone version of IoTDB, be careful to replace the internal JDBC Server with a cluster version
    IoTDB iotdb = new IoTDB();
    iotdb.active();

    PeerId[] peerIds = RaftUtils.convertStringArrayToPeerIdArray(ClusterConf.getNodes());
    PeerId serverId = new PeerId(ClusterConf.getIp(), ClusterConf.getPort());
    RpcServer rpcServer = new RpcServer(serverId.getPort());
    RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);
    // TODO processor
    rpcServer.registerUserProcessor(NonQueryAsyncProcessor);
    metadataHolder = new MetadataRaftHolder(peerIds, serverId, rpcServer);
    metadataHolder.init();
    metadataHolder.start();

    dataPartitionHolderMap = new HashMap<>();
    Router router = Router.getInstance();
    PhysicalNode[][] groups = router.generateGroups(serverId.getIp(), serverId.getPort());

    for(int i = 0; i < groups.length; i++) {
      PhysicalNode[] group = groups[i];
      String groupId = router.getGroupID(group);
      DataPartitionHolder dataPartitionHolder = new DataPartitionRaftHolder(groupId, RaftUtils.convertPhysicalNodeArrayToPeerIdArray(group), serverId, rpcServer);
      dataPartitionHolder.init();
      dataPartitionHolder.start();
      dataPartitionHolderMap.put(groupId, dataPartitionHolder);
    }

  }
}
