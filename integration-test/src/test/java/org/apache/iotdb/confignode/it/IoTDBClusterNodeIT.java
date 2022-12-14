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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.it.env.EnvFactory;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterNodeIT {

  private static final int testConfigNodeNum = 2;
  private static final int testDataNodeNum = 2;

  protected static String originalConfigNodeConsensusProtocolClass;
  protected static String originalSchemaRegionConsensusProtocolClass;
  protected static String originalDataRegionConsensusProtocolClass;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(testConsensusProtocolClass);
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    ConfigFactory.getConfig().setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass);
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    ConfigFactory.getConfig().setDataRegionConsensusProtocolClass(testConsensusProtocolClass);

    // Init 2C2D environment
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
  }

  @Test
  public void showClusterAndNodesTest() throws IOException, InterruptedException, TException {
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
  public void removeAndStopConfigNodeTest() throws TException, IOException, InterruptedException {
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
  public void queryAndRemoveDataNodeTest() throws TException, IOException, InterruptedException {

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      /* Test success re-register */
      TDataNodeRegisterReq dataNodeRegisterReq = new TDataNodeRegisterReq();
      TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
      TDataNodeLocation dataNodeLocation = new TDataNodeLocation();

      // Pick a registered DataNode
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

      // Re-register DataNode
      dataNodeConfiguration.setLocation(dataNodeLocation);
      dataNodeConfiguration.setResource(new TNodeResource(8, 1024 * 1024));
      dataNodeRegisterReq.setDataNodeConfiguration(dataNodeConfiguration);
      TDataNodeRegisterResp dataNodeRegisterResp = client.registerDataNode(dataNodeRegisterReq);
      //      assertEquals(
      //          TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode(),
      //          dataNodeRegisterResp.getStatus().getCode());

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
