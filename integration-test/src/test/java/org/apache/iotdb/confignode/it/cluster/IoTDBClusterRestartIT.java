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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
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
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRestartIT {

  private static final String ratisConsensusProtocolClass =
      "org.apache.iotdb.consensus.ratis.RatisConsensus";
  private static final int testConfigNodeNum = 2;
  private static final int testDataNodeNum = 2;
  private static final int testReplicationFactor = 2;

  private static final String database = "root.db";
  private static final long testTimePartitionInterval = 604800000;

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
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval);

    // Init 2C2D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void clusterRestartTest()
      throws InterruptedException, ClientManagerException, IOException, TException,
          IllegalPathException {

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Insert some data into cluster

      TSStatus status = client.setDatabase(new TDatabaseSchema(database));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      final String d0 = database + ".d0.s";
      TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq();
      TSchemaPartitionTableResp schemaPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;
      ByteBuffer buffer = ConfigNodeTestUtils.generatePatternTreeBuffer(new String[] {d0});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(1, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      Assert.assertEquals(1, schemaPartitionTable.get(database).size());

      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              database, 0, 1, 0, 1, testTimePartitionInterval);
      TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      ConfigNodeTestUtils.checkDataPartitionTable(
          database,
          0,
          1,
          0,
          1,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());
    }

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

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // The cluster is still writable after restart
      final String d1 = database + ".d1.s";
      TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq();
      TSchemaPartitionTableResp schemaPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;
      ByteBuffer buffer = ConfigNodeTestUtils.generatePatternTreeBuffer(new String[] {d1});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(1, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      Assert.assertEquals(1, schemaPartitionTable.get(database).size());

      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              database, 1, 2, 1, 2, testTimePartitionInterval);
      TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      ConfigNodeTestUtils.checkDataPartitionTable(
          database,
          1,
          2,
          1,
          2,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());
    }
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
