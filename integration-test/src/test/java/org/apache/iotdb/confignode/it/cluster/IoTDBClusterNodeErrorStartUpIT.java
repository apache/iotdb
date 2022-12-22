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
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseConfig;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterNodeErrorStartUpIT {

  private static final BaseConfig CONF = ConfigFactory.getConfig();

  private static final int testConfigNodeNum = 3;
  private static final int testDataNodeNum = 1;

  protected static String originalConfigNodeConsensusProtocolClass;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  private static final String TEST_CLUSTER_NAME = "defaultCluster";
  private static final String ERROR_CLUSTER_NAME = "errorCluster";

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass = CONF.getConfigNodeConsesusProtocolClass();
    CONF.setConfigNodeConsesusProtocolClass(testConsensusProtocolClass);

    // Init 3C1D environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();
    CONF.setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
  }

  @Test
  public void testConflictNodeRegistration() throws IOException, InterruptedException, TException {
    /* Test ConfigNode conflict register */

    // Construct a ConfigNodeWrapper that conflicts in consensus port with an existed one.
    ConfigNodeWrapper conflictConfigNodeWrapper =
        EnvFactory.getEnv().generateRandomConfigNodeWrapper();
    conflictConfigNodeWrapper.setConsensusPort(
        EnvFactory.getEnv().getConfigNodeWrapper(1).getConsensusPort());
    conflictConfigNodeWrapper.changeConfig(ConfigFactory.getConfig().getConfignodeProperties());

    // The registration request should be rejected since there exists conflict port
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TConfigNodeRegisterReq req =
          ConfigNodeTestUtils.generateTConfigNodeRegisterReq(conflictConfigNodeWrapper);
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
    conflictDataNodeWrapper.changeConfig(ConfigFactory.getConfig().getEngineProperties());

    // The registration request should be rejected since there exists conflict port
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TDataNodeRegisterReq req =
          ConfigNodeTestUtils.generateTDataNodeRegisterReq(conflictDataNodeWrapper);
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
  public void testIllegalNodeRestart() throws IOException, InterruptedException, TException {
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

      /* Restart an alive Node */

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
      configNodeRestartReq =
          ConfigNodeTestUtils.generateTConfigNodeRestartReq(
              TEST_CLUSTER_NAME, registeredConfigNodeId, registeredConfigNodeWrapper);
      configNodeRestartStatus = client.restartConfigNode(configNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(), configNodeRestartStatus.getCode());
      Assert.assertTrue(
          configNodeRestartStatus
              .getMessage()
              .contains("exists an alive Node with the same nodeId"));

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
      dataNodeRestartReq =
          ConfigNodeTestUtils.generateTDataNodeRestartReq(
              TEST_CLUSTER_NAME, registeredDataNodeId, registeredDataNodeWrapper);
      dataNodeRestartResp = client.restartDataNode(dataNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          dataNodeRestartResp.getStatus().getCode());
      Assert.assertTrue(
          dataNodeRestartResp
              .getStatus()
              .getMessage()
              .contains("exists an alive Node with the same nodeId"));

      // Shutdown and check
      EnvFactory.getEnv().shutdownConfigNode(1);
      EnvFactory.getEnv().shutdownDataNode(0);
      while (true) {
        AtomicInteger unknownCnt = new AtomicInteger(0);
        showClusterResp = client.showCluster();
        showClusterResp
            .getNodeStatus()
            .forEach(
                (nodeId, status) -> {
                  if (NodeStatus.Unknown.equals(NodeStatus.parse(status))) {
                    unknownCnt.getAndIncrement();
                  }
                });

        if (unknownCnt.get() == 2) {
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }

      /* Restart and updatePeer */
      // TODO: @Itami-sho, enable this test and delete it
      int originPort = registeredConfigNodeWrapper.getConsensusPort();
      registeredConfigNodeWrapper.setConsensusPort(-12345);
      configNodeRestartReq =
          ConfigNodeTestUtils.generateTConfigNodeRestartReq(
              TEST_CLUSTER_NAME, registeredConfigNodeId, registeredConfigNodeWrapper);
      configNodeRestartStatus = client.restartConfigNode(configNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(), configNodeRestartStatus.getCode());
      Assert.assertTrue(configNodeRestartStatus.getMessage().contains("have been changed"));
      registeredConfigNodeWrapper.setConsensusPort(originPort);

      originPort = registeredDataNodeWrapper.getInternalPort();
      registeredDataNodeWrapper.setInternalPort(-12345);
      dataNodeRestartReq =
          ConfigNodeTestUtils.generateTDataNodeRestartReq(
              TEST_CLUSTER_NAME, registeredDataNodeId, registeredDataNodeWrapper);
      dataNodeRestartResp = client.restartDataNode(dataNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          dataNodeRestartResp.getStatus().getCode());
      Assert.assertTrue(dataNodeRestartResp.getStatus().getMessage().contains("have been changed"));
      registeredDataNodeWrapper.setInternalPort(originPort);

      // Restart and check
      EnvFactory.getEnv().startConfigNode(1);
      EnvFactory.getEnv().startDataNode(0);
      while (true) {
        AtomicInteger runningCnt = new AtomicInteger(0);
        showClusterResp = client.showCluster();
        showClusterResp
            .getNodeStatus()
            .forEach(
                (nodeId, status) -> {
                  if (NodeStatus.Running.equals(NodeStatus.parse(status))) {
                    runningCnt.getAndIncrement();
                  }
                });

        if (runningCnt.get() == 3) {
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }
    }
  }
}
