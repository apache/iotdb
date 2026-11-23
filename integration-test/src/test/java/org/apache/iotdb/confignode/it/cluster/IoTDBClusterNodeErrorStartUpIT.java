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
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterNodeErrorStartUpIT {

  private static final int testConfigNodeNum = 3;
  private static final int testDataNodeNum = 1;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  private static final String TEST_CLUSTER_NAME = "defaultCluster";
  private static final String ERROR_CLUSTER_NAME = "errorCluster";

  private static final int START_TIMEOUT_SECONDS = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass);

    // Init 3C1D environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void resetCluster() throws IOException {
    // Generated wrappers are tracked even when registration fails. Remove them before checking
    // readiness so later tests see only the original cluster and use its reachable DataNode.
    try {
      removeExtraNodes(EnvFactory.getEnv().getConfigNodeWrapperList(), testConfigNodeNum);
    } finally {
      removeExtraNodes(EnvFactory.getEnv().getDataNodeWrapperList(), testDataNodeNum);
    }
    // An assertion can interrupt the restart scenario while original nodes are stopped.
    for (ConfigNodeWrapper node : EnvFactory.getEnv().getConfigNodeWrapperList()) {
      if (!node.isAlive()) {
        node.start();
      }
    }
    for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
      if (!node.isAlive()) {
        node.start();
      }
    }
    ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusWithoutUnknown();
  }

  private static void removeExtraNodes(List<? extends AbstractNodeWrapper> nodes, int originalSize)
      throws IOException {
    while (nodes.size() > originalSize) {
      AbstractNodeWrapper node = nodes.get(nodes.size() - 1);
      node.stopForcibly();
      node.destroyDir();
      Files.deleteIfExists(Paths.get(EnvUtils.getLockFilePath(node.getPort())));
      // Keep a wrapper tracked until cleanup succeeds so @AfterClass can retry on failure.
      nodes.remove(nodes.size() - 1);
    }
  }

  /** Reject ConfigNode and DataNode registration requests with an inconsistent cluster name. */
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

  /** Reject ports that conflict with registered nodes without persisting their node identities. */
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

  /**
   * Reject an incorrect cluster name, node ID and internal endpoint, then restart the original
   * nodes and verify that both return to Running.
   */
  @Test
  public void testIllegalNodeRestart()
      throws ClientManagerException, IOException, InterruptedException, TException {
    ConfigNodeWrapper registeredConfigNodeWrapper = EnvFactory.getEnv().getConfigNodeWrapper(1);
    DataNodeWrapper registeredDataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      /* Restart with error cluster name */
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
      dataNodeRestartReq =
          ConfigNodeTestUtils.generateTDataNodeRestartReq(
              TEST_CLUSTER_NAME, 200, registeredDataNodeWrapper);
      dataNodeRestartResp = client.restartDataNode(dataNodeRestartReq);
      Assert.assertEquals(
          TSStatusCode.REJECT_NODE_START.getStatusCode(),
          dataNodeRestartResp.getStatus().getCode());
      Assert.assertTrue(dataNodeRestartResp.getStatus().getMessage().contains("whose nodeId="));

      // Local stop runs shutdown hooks on Unix, but terminates the process directly on Windows.
      // A ConfigNode that was the leader at shutdown time can also remain Unknown if its report
      // cannot reach another leader.
      EnvFactory.getEnv().shutdownConfigNode(1);
      EnvFactory.getEnv().shutdownDataNode(0);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Arrays.asList(EnvFactory.getEnv().getDataNodeWrapper(0)),
              Arrays.asList(EnvUtils.getNodeStatusAfterLocalStop()));
      Awaitility.await()
          .pollDelay(0, MILLISECONDS)
          .pollInterval(100, MILLISECONDS)
          .atMost(30, SECONDS)
          .until(
              () -> {
                TShowClusterResp cluster = client.showCluster();
                for (TConfigNodeLocation configNodeLocation : cluster.getConfigNodeList()) {
                  if (configNodeLocation.getConsensusEndPoint().getPort()
                      == registeredConfigNodeWrapper.getConsensusPort()) {
                    String nodeStatus =
                        cluster.getNodeStatus().get(configNodeLocation.getConfigNodeId());
                    return NodeStatus.Stopped.getStatus().equals(nodeStatus)
                        || NodeStatus.Unknown.getStatus().equals(nodeStatus);
                  }
                }
                return false;
              });

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
      try {
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
      } finally {
        registeredDataNodeWrapper.setInternalPort(originPort);
      }

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

  /**
   * Duplicate ports within a node must terminate startup without adding either node to the cluster.
   */
  @Test
  public void testIllegalNodeStartUp()
      throws IOException, ClientManagerException, InterruptedException, TException {
    ConfigNodeWrapper portConflictConfigNodeWrapper =
        EnvFactory.getEnv().generateRandomConfigNodeWrapper();
    DataNodeWrapper portConflictDataNodeWrapper =
        EnvFactory.getEnv().generateRandomDataNodeWrapper();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TShowClusterResp showClusterResp = client.showCluster();
      int beforeStartConfigNodes = showClusterResp.getConfigNodeListSize();
      int beforeStartDataNodes = showClusterResp.getDataNodeListSize();
      // set ConfigNode port repeat
      portConflictConfigNodeWrapper.setConsensusPort(portConflictConfigNodeWrapper.getPort());
      portConflictConfigNodeWrapper.changeConfig(
          (MppBaseConfig) EnvFactory.getEnv().getConfig().getConfigNodeConfig(),
          (MppCommonConfig) EnvFactory.getEnv().getConfig().getConfigNodeCommonConfig(),
          null);
      portConflictConfigNodeWrapper.start();
      awaitRejectedStartup(
          portConflictConfigNodeWrapper, client, beforeStartConfigNodes, beforeStartDataNodes);

      // set datanode port repeat
      portConflictDataNodeWrapper.setMppDataExchangePort(
          portConflictDataNodeWrapper.getDataRegionConsensusPort());
      portConflictDataNodeWrapper.changeConfig(
          (MppBaseConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig(),
          (MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig(),
          null);
      portConflictDataNodeWrapper.start();
      awaitRejectedStartup(
          portConflictDataNodeWrapper, client, beforeStartConfigNodes, beforeStartDataNodes);
    }
  }

  private static void awaitRejectedStartup(
      AbstractNodeWrapper node,
      SyncConfigNodeIServiceClient client,
      int expectedConfigNodes,
      int expectedDataNodes) {
    // Check membership until the invalid process exits. It cannot register after exiting, so the
    // final membership check replaces the remaining fixed observation window.
    Awaitility.await()
        .pollDelay(0, MILLISECONDS)
        .pollInterval(100, MILLISECONDS)
        .atMost(START_TIMEOUT_SECONDS, SECONDS)
        .until(
            () -> {
              boolean exited = !node.isAlive();
              TShowClusterResp cluster = client.showCluster();
              Assert.assertEquals(expectedConfigNodes, cluster.getConfigNodeListSize());
              Assert.assertEquals(expectedDataNodes, cluster.getDataNodeListSize());
              return exited;
            });
  }
}
