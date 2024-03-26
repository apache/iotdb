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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRestartIT {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBClusterRestartIT.class);

  private static final int testReplicationFactor = 2;

  private static final int testConfigNodeNum = 3, testDataNodeNum = 2;

  private static final long testTimePartitionInterval = 604800000;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor);

    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void clusterRestartTest() {
    // Shutdown all cluster nodes
    logger.info("Shutting down all ConfigNodes and DataNodes...");
    EnvFactory.getEnv().shutdownAllConfigNodes();
    EnvFactory.getEnv().shutdownAllDataNodes();

    // Restart all cluster nodes
    logger.info("Restarting all ConfigNodes...");
    EnvFactory.getEnv().startAllConfigNodes();
    logger.info("Restarting all DataNodes...");
    EnvFactory.getEnv().startAllDataNodes();
    ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusWithoutUnknown();
  }

  @Test
  public void clusterRestartAfterUpdateDataNodeTest()
      throws InterruptedException, ClientManagerException, IOException, TException {
    // Create default Database
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status = client.setDatabase(new TDatabaseSchema("root.database"));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
    // Create some DataPartitions to extend 2 DataRegionGroups
    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
        ConfigNodeTestUtils.constructPartitionSlotsMap(
            "root.database", 0, 10, 0, 10, testTimePartitionInterval);
    TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
    TDataPartitionTableResp dataPartitionTableResp = null;
    for (int retry = 0; retry < 5; retry++) {
      // Build new Client since it's unstable in Win8 environment
      try (SyncConfigNodeIServiceClient configNodeClient =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        dataPartitionTableResp = configNodeClient.getOrCreateDataPartitionTable(dataPartitionReq);
        if (dataPartitionTableResp != null) {
          break;
        }
      } catch (Exception e) {
        // Retry sometimes in order to avoid request timeout
        logger.error(e.getMessage());
        TimeUnit.SECONDS.sleep(1);
      }
    }
    Assert.assertNotNull(dataPartitionTableResp);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), dataPartitionTableResp.getStatus().getCode());
    Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
    ConfigNodeTestUtils.checkDataPartitionTable(
        "root.database",
        0,
        10,
        0,
        10,
        testTimePartitionInterval,
        dataPartitionTableResp.getDataPartitionTable());

    // Shutdown all DataNodes
    EnvFactory.getEnv().shutdownAllDataNodes();
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
      // Check update in NodeInfo
      TShowClusterResp showClusterResp = client.showCluster();
      ConfigNodeTestUtils.checkNodeConfig(
          showClusterResp.getConfigNodeList(),
          showClusterResp.getDataNodeList(),
          EnvFactory.getEnv().getConfigNodeWrapperList(),
          dataNodeWrapperList);

      // Check update in PartitionInfo
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                AtomicBoolean matched = new AtomicBoolean(false);
                dataNodeWrapperList.forEach(
                    dataNodeWrapper -> {
                      if (regionInfo.getClientRpcPort() == dataNodeWrapper.getPort()) {
                        matched.set(true);
                      }
                    });
                Assert.assertTrue(matched.get());
              });
    }
  }

  // TODO: Add persistence tests in the future

  @Test
  public void clusterRestartWithoutSeedConfigNode() {
    // shutdown all ConfigNodes and DataNodes
    for (int i = testConfigNodeNum - 1; i >= 0; i--) {
      EnvFactory.getEnv().shutdownConfigNode(i);
    }
    for (int i = testDataNodeNum - 1; i >= 0; i--) {
      EnvFactory.getEnv().shutdownDataNode(i);
    }
    logger.info("Shutdown all ConfigNodes and DataNodes");
    // restart without seed ConfigNode, the cluster should still work
    for (int i = 1; i < testConfigNodeNum; i++) {
      EnvFactory.getEnv().startConfigNode(i);
    }
    EnvFactory.getEnv().startAllDataNodes();
    logger.info("Restarted");
    ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusOneUnknownOtherRunning();
    logger.info("Working without Seed-ConfigNode");
  }
}
