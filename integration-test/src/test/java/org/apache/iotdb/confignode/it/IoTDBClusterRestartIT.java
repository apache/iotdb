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
package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.AbstractEnv;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.checkNodeConfig;
import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;
import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.getClusterNodeInfos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRestartIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClusterRestartIT.class);

  private static final String ratisConsensusProtocolClass =
      "org.apache.iotdb.consensus.ratis.RatisConsensus";
  private static final int testConfigNodeNum = 2;
  private static final int testDataNodeNum = 2;
  private static final int testReplicationFactor = 2;
  private static final long testTimePartitionInterval = 604800000;
  protected static String originalConfigNodeConsensusProtocolClass;
  protected static String originalSchemaRegionConsensusProtocolClass;
  protected static String originalDataRegionConsensusProtocolClass;
  protected static int originSchemaReplicationFactor;
  protected static int originalDataReplicationFactor;
  protected static long originalTimePartitionInterval;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(ratisConsensusProtocolClass);
    ConfigFactory.getConfig().setSchemaRegionConsensusProtocolClass(ratisConsensusProtocolClass);
    ConfigFactory.getConfig().setDataRegionConsensusProtocolClass(ratisConsensusProtocolClass);
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);

    originSchemaReplicationFactor = ConfigFactory.getConfig().getSchemaReplicationFactor();
    originalDataReplicationFactor = ConfigFactory.getConfig().getDataReplicationFactor();
    ConfigFactory.getConfig().setSchemaReplicationFactor(testReplicationFactor);
    ConfigFactory.getConfig().setDataReplicationFactor(testReplicationFactor);

    originalTimePartitionInterval = ConfigFactory.getConfig().getTimePartitionInterval();
    ConfigFactory.getConfig().setTimePartitionInterval(testTimePartitionInterval);
    // Init 2C2D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setSchemaRegionConsensusProtocolClass(originalSchemaRegionConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);
    ConfigFactory.getConfig().setTimePartitionInterval(originalTimePartitionInterval);
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
  @Ignore
  public void clusterRestartAfterUpdateDataNodeTest() throws InterruptedException {
    TShowClusterResp clusterNodes;
    final String sg0 = "root.sg0";

    final String d00 = sg0 + ".d0.s";
    final String d01 = sg0 + ".d1.s";
    // Shutdown all data nodes
    for (int i = 0; i < testDataNodeNum; i++) {
      EnvFactory.getEnv().shutdownDataNode(i);
    }

    // Sleep 1s before restart
    TimeUnit.SECONDS.sleep(1);

    // Modify data node config
    List<DataNodeWrapper> dataNodeWrapperList = EnvFactory.getEnv().getDataNodeWrapperList();
    List<ConfigNodeWrapper> configNodeWrappersList = EnvFactory.getEnv().getConfigNodeWrapperList();
    for (int i = 0; i < testDataNodeNum; i++) {
      int[] portList = EnvUtils.searchAvailablePorts();
      dataNodeWrapperList.get(i).setPort(portList[0]);
      dataNodeWrapperList.get(i).setInternalPort(portList[1]);
      dataNodeWrapperList.get(i).setMppDataExchangePort(portList[2]);

      // update data node files'names
      dataNodeWrapperList.get(i).renameFile();
    }

    for (int i = 0; i < testDataNodeNum; i++) {
      dataNodeWrapperList.get(i).changeConfig(ConfigFactory.getConfig().getEngineProperties());
      EnvFactory.getEnv().startDataNode(i);
    }

    ((AbstractEnv) EnvFactory.getEnv()).testWorking();

    // check nodeInfo in cluster
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // check the number and status of nodes
      clusterNodes = getClusterNodeInfos(client, testConfigNodeNum, testDataNodeNum);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), clusterNodes.getStatus().getCode());

      // check the configuration of nodes
      List<TConfigNodeLocation> configNodeLocationList = clusterNodes.getConfigNodeList();
      List<TDataNodeLocation> dataNodeLocationList = clusterNodes.getDataNodeList();
      checkNodeConfig(
          configNodeLocationList,
          dataNodeLocationList,
          configNodeWrappersList,
          dataNodeWrapperList);

      // check whether the cluster is working by testing GetAndCreateSchemaPartition
      TSStatus status;
      ByteBuffer buffer;
      TSchemaPartitionReq schemaPartitionReq;
      TSchemaPartitionTableResp schemaPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;

      // Set StorageGroups
      status = client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg0)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Test getSchemaPartition, the result should be empty
      buffer = generatePatternTreeBuffer(new String[] {d00, d01});
      schemaPartitionReq = new TSchemaPartitionReq(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(0, schemaPartitionTableResp.getSchemaPartitionTableSize());

      // Test getOrCreateSchemaPartition, ConfigNode should create SchemaPartitions and return
      buffer = generatePatternTreeBuffer(new String[] {d00, d01});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(1, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      Assert.assertTrue(schemaPartitionTable.containsKey(sg0));
      Assert.assertEquals(2, schemaPartitionTable.get(sg0).size());
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      fail(e.getMessage());
    }
  }

  // TODO: Add persistence tests in the future
}
