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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.DataNodeWrapper;
import org.apache.iotdb.it.env.cluster.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.MppCommonConfig;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterNodeErrorStartUpIT {

  private static final int testConfigNodeNum = 3;
  private static final int testDataNodeNum = 1;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  private static final String TEST_CLUSTER_NAME = "defaultCluster";
  private static final String ERROR_CLUSTER_NAME = "errorCluster";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass);

    // Init 3C1D environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testIllegalNodeRegistration()
      throws ClientManagerException, IOException, InterruptedException, TException {
    ConfigNodeWrapper configNodeWrapper = EnvFactory.getEnv().generateRandomConfigNodeWrapper();
    DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().generateRandomDataNodeWrapper();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      /* Register with error cluster name */
      TConfigNodeRegisterReq configNodeRegisterReq =
          ConfigNodeTestUtils.generateTConfigNodeRegisterReq(ERROR_CLUSTER_NAME, configNodeWrapper);
      configNodeRegisterReq
          .getClusterParameters()
          .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass);
      TConfigNodeRegisterResp configNodeRegisterResp =
          client.registerConfigNode(configNodeRegisterReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          configNodeRegisterResp.getStatus().getCode());
      Assert.assertTrue(
          configNodeRegisterResp.getStatus().getMessage().contains("cluster are inconsistent"));

      TDataNodeRegisterReq dataNodeRegisterReq =
          ConfigNodeTestUtils.generateTDataNodeRegisterReq(ERROR_CLUSTER_NAME, dataNodeWrapper);
      TDataNodeRegisterResp dataNodeRegisterResp = client.registerDataNode(dataNodeRegisterReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          dataNodeRegisterResp.getStatus().getCode());
      Assert.assertTrue(
          dataNodeRegisterResp.getStatus().getMessage().contains("cluster are inconsistent"));
    }
  }

  @Test
  public void testConflictNodeRegistration()
      throws ClientManagerException, InterruptedException, TException, IOException {
    /* Test ConfigNode conflict register */

    // Construct a ConfigNodeWrapper that conflicts in consensus port with an existed one.
    ConfigNodeWrapper conflictConfigNodeWrapper =
        EnvFactory.getEnv().generateRandomConfigNodeWrapper();
    conflictConfigNodeWrapper.setConsensusPort(
        EnvFactory.getEnv().getConfigNodeWrapper(1).getConsensusPort());
    conflictConfigNodeWrapper.changeConfig(
        (MppBaseConfig) EnvFactory.getEnv().getConfig().getConfigNodeConfig(),
        (MppCommonConfig) EnvFactory.getEnv().getConfig().getConfigNodeCommonConfig(),
        null);

    // The registration request should be rejected since there exists conflict port
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TConfigNodeRegisterReq req =
          ConfigNodeTestUtils.generateTConfigNodeRegisterReq(
              TEST_CLUSTER_NAME, conflictConfigNodeWrapper);
      req.getClusterParameters().setConfigNodeConsensusProtocolClass(testConsensusProtocolClass);
      TConfigNodeRegisterResp resp = client.registerConfigNode(req);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(), resp.getStatus().getCode());
    }

    // The confignode-system.properties file should be empty before register
    File systemProperties = new File(conflictConfigNodeWrapper.getSystemPropertiesPath());
    Assert.assertFalse(systemProperties.exists());

    // The confignode-system.properties file should remain empty since the registration will fail
    EnvFactory.getEnv().registerNewConfigNode(conflictConfigNodeWrapper, false);
    Assert.assertFalse(systemProperties.exists());

    /* Construct a DataNodeWrapper that conflicts with an existed one. */

    // Construct a DataNodeWrapper that conflicts in internal port with an existed one.
    DataNodeWrapper conflictDataNodeWrapper = EnvFactory.getEnv().generateRandomDataNodeWrapper();
    conflictDataNodeWrapper.setInternalPort(
        EnvFactory.getEnv().getDataNodeWrapper(0).getInternalPort());
    conflictDataNodeWrapper.changeConfig(
        (MppBaseConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig(),
        (MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig(),
        null);

    // The registration request should be rejected since there exists conflict port
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TDataNodeRegisterReq req =
          ConfigNodeTestUtils.generateTDataNodeRegisterReq(
              TEST_CLUSTER_NAME, conflictDataNodeWrapper);
      TDataNodeRegisterResp resp = client.registerDataNode(req);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(), resp.getStatus().getCode());
    }

    // The system.properties file should be empty before register
    systemProperties = new File(conflictDataNodeWrapper.getSystemPropertiesPath());
    Assert.assertFalse(systemProperties.exists());

    // The system.properties file should remain empty since the registration will fail
    EnvFactory.getEnv().registerNewDataNode(conflictDataNodeWrapper, false);
    Assert.assertFalse(systemProperties.exists());
  }

  @Test
  public void testIllegalNodeRestart()
      throws ClientManagerException, IOException, InterruptedException, TException {
    ConfigNodeWrapper registeredConfigNodeWrapper = EnvFactory.getEnv().getConfigNodeWrapper(1);
    DataNodeWrapper registeredDataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      /* Restart with error cluster name */

      TConfigNodeRestartReq configNodeRestartReq =
          ConfigNodeTestUtils.generateTConfigNodeRestartReq(
              ERROR_CLUSTER_NAME, 1, registeredConfigNodeWrapper);
      TSStatus configNodeRestartStatus = client.restartConfigNode(configNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(), configNodeRestartStatus.getCode());
      Assert.assertTrue(configNodeRestartStatus.getMessage().contains("cluster are inconsistent"));

      TDataNodeRestartReq dataNodeRestartReq =
          ConfigNodeTestUtils.generateTDataNodeRestartReq(
              ERROR_CLUSTER_NAME, 2, registeredDataNodeWrapper);
      TDataNodeRestartResp dataNodeRestartResp = client.restartDataNode(dataNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          dataNodeRestartResp.getStatus().getCode());
      Assert.assertTrue(
          dataNodeRestartResp.getStatus().getMessage().contains("cluster are inconsistent"));

      /* Restart with error NodeId */

      configNodeRestartReq =
          ConfigNodeTestUtils.generateTConfigNodeRestartReq(
              TEST_CLUSTER_NAME, 100, registeredConfigNodeWrapper);
      configNodeRestartStatus = client.restartConfigNode(configNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(), configNodeRestartStatus.getCode());
      Assert.assertTrue(configNodeRestartStatus.getMessage().contains("whose nodeId="));

      dataNodeRestartReq =
          ConfigNodeTestUtils.generateTDataNodeRestartReq(
              TEST_CLUSTER_NAME, 200, registeredDataNodeWrapper);
      dataNodeRestartResp = client.restartDataNode(dataNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          dataNodeRestartResp.getStatus().getCode());
      Assert.assertTrue(dataNodeRestartResp.getStatus().getMessage().contains("whose nodeId="));

      // Shutdown and check
      EnvFactory.getEnv().shutdownConfigNode(1);
      EnvFactory.getEnv().shutdownDataNode(0);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Arrays.asList(
                  EnvFactory.getEnv().getConfigNodeWrapper(1),
                  EnvFactory.getEnv().getDataNodeWrapper(0)),
              Arrays.asList(NodeStatus.Unknown, NodeStatus.Unknown));

      /* Restart and updatePeer */
      // TODO: Delete this IT after enable modify internal TEndPoints
      int registeredConfigNodeId = -1;
      TShowClusterResp showClusterResp = client.showCluster();
      for (TConfigNodeLocation configNodeLocation : showClusterResp.getConfigNodeList()) {
        if (configNodeLocation.getConsensusEndPoint().getPort()
            == registeredConfigNodeWrapper.getConsensusPort()) {
          registeredConfigNodeId = configNodeLocation.getConfigNodeId();
          break;
        }
      }
      Assert.assertNotEquals(-1, registeredConfigNodeId);
      int originPort = registeredConfigNodeWrapper.getConsensusPort();
      registeredConfigNodeWrapper.setConsensusPort(-12345);
      configNodeRestartReq =
          ConfigNodeTestUtils.generateTConfigNodeRestartReq(
              TEST_CLUSTER_NAME, registeredConfigNodeId, registeredConfigNodeWrapper);
      configNodeRestartStatus = client.restartConfigNode(configNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(), configNodeRestartStatus.getCode());
      Assert.assertTrue(configNodeRestartStatus.getMessage().contains("the internal TEndPoints"));
      registeredConfigNodeWrapper.setConsensusPort(originPort);

      int registeredDataNodeId = -1;
      showClusterResp = client.showCluster();
      for (TDataNodeLocation dataNodeLocation : showClusterResp.getDataNodeList()) {
        if (dataNodeLocation.getInternalEndPoint().getPort()
            == registeredDataNodeWrapper.getInternalPort()) {
          registeredDataNodeId = dataNodeLocation.getDataNodeId();
          break;
        }
      }
      Assert.assertNotEquals(-1, registeredDataNodeId);
      originPort = registeredDataNodeWrapper.getInternalPort();
      registeredDataNodeWrapper.setInternalPort(-12345);
      dataNodeRestartReq =
          ConfigNodeTestUtils.generateTDataNodeRestartReq(
              TEST_CLUSTER_NAME, registeredDataNodeId, registeredDataNodeWrapper);
      dataNodeRestartResp = client.restartDataNode(dataNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          dataNodeRestartResp.getStatus().getCode());
      Assert.assertTrue(
          dataNodeRestartResp.getStatus().getMessage().contains("the internal TEndPoints"));
      registeredDataNodeWrapper.setInternalPort(originPort);

      // Restart and check
      EnvFactory.getEnv().startConfigNode(1);
      EnvFactory.getEnv().startDataNode(0);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Arrays.asList(
                  EnvFactory.getEnv().getConfigNodeWrapper(1),
                  EnvFactory.getEnv().getDataNodeWrapper(0)),
              Arrays.asList(NodeStatus.Running, NodeStatus.Running));
    }
  }
}
