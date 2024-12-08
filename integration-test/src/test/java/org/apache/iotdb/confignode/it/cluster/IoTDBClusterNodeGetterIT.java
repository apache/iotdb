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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterNodeGetterIT {

  private static final int testConfigNodeNum = 2;
  private static final int testDataNodeNum = 2;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(testConsensusProtocolClass);

    // Init 2C2D environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void showClusterAndNodesTest() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      List<ConfigNodeWrapper> configNodeWrappers = EnvFactory.getEnv().getConfigNodeWrapperList();
      List<DataNodeWrapper> dataNodeWrappers = EnvFactory.getEnv().getDataNodeWrapperList();

      /* Test showCluster */
      TShowClusterResp showClusterResp = client.showCluster();
      // Check ConfigNodeLocation
      List<TConfigNodeLocation> configNodeLocations = showClusterResp.getConfigNodeList();
      for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
        boolean found = false;
        for (ConfigNodeWrapper configNodeWrapper : configNodeWrappers) {
          if (configNodeWrapper.getIp().equals(configNodeLocation.getInternalEndPoint().getIp())
              && configNodeWrapper.getPort() == configNodeLocation.getInternalEndPoint().getPort()
              && configNodeWrapper.getConsensusPort()
                  == configNodeLocation.getConsensusEndPoint().getPort()) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }
      // Check DataNodeLocation
      List<TDataNodeLocation> dataNodeLocations = showClusterResp.getDataNodeList();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        boolean found = false;
        for (DataNodeWrapper dataNodeWrapper : dataNodeWrappers) {
          if (dataNodeWrapper.getIp().equals(dataNodeLocation.getInternalEndPoint().getIp())
              && dataNodeWrapper.getPort() == dataNodeLocation.getClientRpcEndPoint().getPort()
              && dataNodeWrapper.getInternalPort()
                  == dataNodeLocation.getInternalEndPoint().getPort()
              && dataNodeWrapper.getMppDataExchangePort()
                  == dataNodeLocation.getMPPDataExchangeEndPoint().getPort()
              && dataNodeWrapper.getSchemaRegionConsensusPort()
                  == dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort()
              && dataNodeWrapper.getDataRegionConsensusPort()
                  == dataNodeLocation.getDataRegionConsensusEndPoint().getPort()) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }

      /* Tests showClusterParameters */
      TShowVariablesResp showVariablesResp = client.showVariables();
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showVariablesResp.getStatus().getCode());
      TClusterParameters clusterParameters = showVariablesResp.getClusterParameters();
      TClusterParameters expectedParameters = ConfigNodeTestUtils.generateClusterParameters();
      Assert.assertEquals(
          testConsensusProtocolClass, clusterParameters.getConfigNodeConsensusProtocolClass());
      Assert.assertEquals(
          testConsensusProtocolClass, clusterParameters.getDataRegionConsensusProtocolClass());
      Assert.assertEquals(
          testConsensusProtocolClass, clusterParameters.getSchemaRegionConsensusProtocolClass());
      Assert.assertEquals(
          expectedParameters.getSeriesPartitionSlotNum(),
          clusterParameters.getSeriesPartitionSlotNum());
      Assert.assertEquals(
          expectedParameters.getSeriesPartitionExecutorClass(),
          clusterParameters.getSeriesPartitionExecutorClass());
      Assert.assertEquals(
          expectedParameters.getTimePartitionInterval(),
          clusterParameters.getTimePartitionInterval());
      Assert.assertEquals(
          expectedParameters.getDataReplicationFactor(),
          clusterParameters.getDataReplicationFactor());
      Assert.assertEquals(
          expectedParameters.getSchemaReplicationFactor(),
          clusterParameters.getSchemaReplicationFactor());
      Assert.assertEquals(
          expectedParameters.getDataRegionPerDataNode(),
          clusterParameters.getDataRegionPerDataNode());
      Assert.assertEquals(
          expectedParameters.getSchemaRegionPerDataNode(),
          clusterParameters.getSchemaRegionPerDataNode());
      Assert.assertEquals(
          expectedParameters.getDiskSpaceWarningThreshold(),
          clusterParameters.getDiskSpaceWarningThreshold(),
          0.001);
      Assert.assertEquals(
          expectedParameters.getReadConsistencyLevel(),
          clusterParameters.getReadConsistencyLevel());

      /* Test showConfigNodes */
      TShowConfigNodesResp showConfigNodesResp = client.showConfigNodes();
      // Check ConfigNodeInfo
      List<TConfigNodeInfo> configNodesInfo = showConfigNodesResp.getConfigNodesInfoList();
      for (TConfigNodeInfo configNodeInfo : configNodesInfo) {
        boolean found = false;
        for (ConfigNodeWrapper configNodeWrapper : configNodeWrappers) {
          if (configNodeWrapper.getIp().equals(configNodeInfo.getInternalAddress())
              && configNodeWrapper.getPort() == configNodeInfo.getInternalPort()) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }

      /* Test showDataNodes */
      TShowDataNodesResp showDataNodesResp = client.showDataNodes();
      // Check DataNodeInfo
      List<TDataNodeInfo> dataNodesInfo = showDataNodesResp.getDataNodesInfoList();
      for (TDataNodeInfo dataNodeInfo : dataNodesInfo) {
        boolean found = false;
        for (DataNodeWrapper dataNodeWrapper : dataNodeWrappers) {
          if (dataNodeWrapper.getIp().equals(dataNodeInfo.getRpcAddresss())
              && dataNodeWrapper.getPort() == dataNodeInfo.getRpcPort()) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }
    }
  }

  @Test
  public void removeAndStopConfigNodeTest() throws Exception {
    TShowClusterResp showClusterResp;
    TSStatus status;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Test remove ConfigNode
      showClusterResp = client.showCluster();
      TConfigNodeLocation removedConfigNodeLocation = showClusterResp.getConfigNodeList().get(1);
      status = client.removeConfigNode(removedConfigNodeLocation);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Waiting for RemoveConfigNodeProcedure
      boolean isRemoved = false;
      for (int retry = 0; retry < 10; retry++) {
        showClusterResp = client.showCluster();
        if (showClusterResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && showClusterResp.getConfigNodeListSize() == testConfigNodeNum - 1) {
          isRemoved = true;
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      if (!isRemoved) {
        fail("Remove ConfigNode failed");
      }

      // Test stop ConfigNode
      status = client.stopConfigNode(removedConfigNodeLocation);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  @Test
  public void queryAndRemoveDataNodeTest() throws Exception {

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Pick a registered DataNode
      TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
      TShowDataNodesResp showDataNodesResp = client.showDataNodes();
      TDataNodeInfo dataNodeInfo = showDataNodesResp.getDataNodesInfoList().get(0);

      dataNodeLocation.setDataNodeId(dataNodeInfo.getDataNodeId());
      dataNodeLocation.setClientRpcEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort()));
      dataNodeLocation.setMPPDataExchangeEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 1));
      dataNodeLocation.setInternalEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 2));
      dataNodeLocation.setDataRegionConsensusEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 3));
      dataNodeLocation.setSchemaRegionConsensusEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 4));

      /* Test query one DataNodeInfo */
      TDataNodeConfigurationResp dataNodeConfigurationResp =
          client.getDataNodeConfiguration(dataNodeLocation.getDataNodeId());
      Map<Integer, TDataNodeConfiguration> configurationMap =
          dataNodeConfigurationResp.getDataNodeConfigurationMap();
      TDataNodeLocation dnLocation =
          dataNodeConfigurationResp
              .getDataNodeConfigurationMap()
              .get(dataNodeLocation.getDataNodeId())
              .getLocation();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataNodeConfigurationResp.getStatus().getCode());
      assertEquals(1, configurationMap.size());
      assertEquals(dataNodeLocation, dnLocation);

      /* Test query all DataNodeConfiguration */
      dataNodeConfigurationResp = client.getDataNodeConfiguration(-1);
      configurationMap = dataNodeConfigurationResp.getDataNodeConfigurationMap();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataNodeConfigurationResp.getStatus().getCode());
      assertEquals(testDataNodeNum, configurationMap.size());

      /* Test remove DataNode */
      // Remove DataNode
      dataNodeLocation =
          dataNodeConfigurationResp
              .getDataNodeConfigurationMap()
              .get(
                  showDataNodesResp.getDataNodesInfoList().get(testDataNodeNum - 1).getDataNodeId())
              .getLocation();
      TDataNodeRemoveReq dataNodeRemoveReq =
          new TDataNodeRemoveReq(Collections.singletonList(dataNodeLocation));
      TDataNodeRemoveResp dataNodeRemoveResp = client.removeDataNode(dataNodeRemoveReq);
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), dataNodeRemoveResp.getStatus().getCode());

      // Waiting for RemoveDataNodeProcedure
      for (int retry = 0; retry < 10; retry++) {
        showDataNodesResp = client.showDataNodes();
        if (showDataNodesResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && showDataNodesResp.getDataNodesInfoListSize() == testDataNodeNum - 1) {
          return;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      fail("Remove DataNode failed");
    }
  }
}
