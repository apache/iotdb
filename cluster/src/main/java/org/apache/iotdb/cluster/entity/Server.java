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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.Utils;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.data.DataPartitionHolder;
import org.apache.iotdb.cluster.entity.metadata.MetadataHolder;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftNode;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.utils.Pair;

public class Server {

  private static final ClusterConfig ClusterConf = ClusterDescriptor.getInstance().getConfig();

  private MetadataHolder metadataHolder;
  private Map<Integer, DataPartitionHolder> dataPartitionHolderMap;

  public void start() throws AuthException {
    // Stand-alone version of IoTDB, be careful to replace the internal JDBC Server with a cluster version
    IoTDB iotdb = new IoTDB();
    iotdb.active();

    List<RaftNode> nodeList = Utils.convertNodesToRaftNodeList(ClusterConf.getNodes());
    metadataHolder = new MetadataRaftHolder(nodeList);
    metadataHolder.init();
    metadataHolder.start();

    dataPartitionHolderMap = new HashMap<>();
    int index = Utils.getIndexOfIpFromRaftNodeList(ClusterConf.getIp(), nodeList);
    List<Pair<Integer, List<RaftNode>>> groupNodeList = getDataPartitonNodeList(index, nodeList);
    for(int i = 0; i < groupNodeList.size(); i++) {
      Pair<Integer, List<RaftNode>> pair = groupNodeList.get(i);
      DataPartitionHolder dataPartitionHolder = new DataPartitionRaftHolder(pair.left, pair.right);
      dataPartitionHolder.init();
      dataPartitionHolder.start();
      dataPartitionHolderMap.put(pair.left, dataPartitionHolder);
    }
  }

  // A node belongs to multiple data groups and calculates the members of the i-th data group.
  private List<Pair<Integer, List<RaftNode>>> getDataPartitonNodeList(int i, List<RaftNode> nodeList) {
    return Collections.emptyList();
  }
}
