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

package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.AbstractEnv;
import org.apache.iotdb.it.env.cluster.DataNodeWrapper;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.MppJVMConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRestartIT {

  private static final String ratisConsensusProtocolClass =
      "org.apache.iotdb.consensus.ratis.RatisConsensus";
  private static final int testConfigNodeNum = 2;
  private static final int testDataNodeNum = 2;
  private static final int testReplicationFactor = 2;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ratisConsensusProtocolClass)
        .setSchemaRegionConsensusProtocolClass(ratisConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(ratisConsensusProtocolClass)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor);

    // Init 2C2D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void clusterRestartTest() throws InterruptedException {
    // Shutdown all cluster nodes
    for (int i = 0; i < testConfigNodeNum; i++) {
      EnvFactory.getEnv().shutdownConfigNode(i);
    }
    for (int i = 0; i < testDataNodeNum; i++) {
      EnvFactory.getEnv().shutdownDataNode(i);
    }

    // Sleep 1s before restart
    TimeUnit.SECONDS.sleep(1);

    // Restart all cluster nodes
    for (int i = 0; i < testConfigNodeNum; i++) {
      EnvFactory.getEnv().startConfigNode(i);
    }
    for (int i = 0; i < testDataNodeNum; i++) {
      EnvFactory.getEnv().startDataNode(i);
    }

    ((AbstractEnv) EnvFactory.getEnv()).testWorking();
  }

  @Test
  public void clusterRestartAfterUpdateDataNodeTest()
      throws InterruptedException, ClientManagerException, IOException, TException {
    // Shutdown all DataNodes
    for (int i = 0; i < testDataNodeNum; i++) {
      EnvFactory.getEnv().shutdownDataNode(i);
    }
    TimeUnit.SECONDS.sleep(1);

    List<DataNodeWrapper> dataNodeWrapperList = EnvFactory.getEnv().getDataNodeWrapperList();
    for (int i = 0; i < testDataNodeNum; i++) {
      // Modify DataNode clientRpcEndPoint
      int[] portList = EnvUtils.searchAvailablePorts();
      dataNodeWrapperList.get(i).setPort(portList[0]);
      // Update DataNode files' names
      dataNodeWrapperList.get(i).renameFile();
    }

    // Restart DataNodes
    for (int i = 0; i < testDataNodeNum; i++) {
      dataNodeWrapperList
          .get(i)
          .changeConfig(
              (MppBaseConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig(),
              (MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig(),
              (MppJVMConfig) EnvFactory.getEnv().getConfig().getDataNodeJVMConfig());
      EnvFactory.getEnv().startDataNode(i);
    }

    // Check DataNode status
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Arrays.asList(
                EnvFactory.getEnv().getDataNodeWrapper(0),
                EnvFactory.getEnv().getDataNodeWrapper(1)),
            Arrays.asList(NodeStatus.Running, NodeStatus.Running));

    // Check DataNode EndPoint
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TShowClusterResp showClusterResp = client.showCluster();
      ConfigNodeTestUtils.checkNodeConfig(
          showClusterResp.getConfigNodeList(),
          showClusterResp.getDataNodeList(),
          EnvFactory.getEnv().getConfigNodeWrapperList(),
          dataNodeWrapperList);
    }
  }

  // TODO: Add persistence tests in the future
}
