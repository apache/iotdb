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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ClusterManager;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class ClusterNodeStartUtilsTest {

  private static final String SEED_VERSION = "2.0.11";
  private static final String MISMATCHED_VERSION = "2.0.10";
  private static final String DIFFERENT_PRODUCT_EDITION =
      "IOTDB".equals(IoTDBConstant.PRODUCT_EDITION) ? "TIMECHODB" : "IOTDB";

  private ConfigManager configManager;
  private NodeManager nodeManager;

  @Before
  public void setUp() {
    configManager = Mockito.mock(ConfigManager.class);
    nodeManager = Mockito.mock(NodeManager.class);
    Mockito.when(configManager.getNodeManager()).thenReturn(nodeManager);
    Mockito.when(nodeManager.getNodeVersionInfo())
        .thenReturn(
            Collections.singletonMap(
                0,
                new TNodeVersionInfo(SEED_VERSION, "seed-build-info")
                    .setProductEdition(IoTDBConstant.PRODUCT_EDITION)));
  }

  @Test
  public void testAcceptDifferentReleaseVersionWithSameProductEdition() {
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ClusterNodeStartUtils.confirmProductEdition(
                NodeType.DataNode,
                new TNodeVersionInfo(MISMATCHED_VERSION, "different-build-info")
                    .setProductEdition(IoTDBConstant.PRODUCT_EDITION),
                configManager)
            .getCode());
  }

  @Test
  public void testAcceptMissingReleaseVersionIfProductEditionMatches() {
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ClusterNodeStartUtils.confirmProductEdition(
                NodeType.DataNode,
                new TNodeVersionInfo().setProductEdition(IoTDBConstant.PRODUCT_EDITION),
                configManager)
            .getCode());
  }

  @Test
  public void testRejectDifferentProductEditionOnDataNodeRestart() {
    Mockito.when(configManager.getNodeManager().getNodeVersionInfo())
        .thenReturn(
            Map.of(
                0,
                new TNodeVersionInfo(SEED_VERSION, "seed-build-info")
                    .setProductEdition(IoTDBConstant.PRODUCT_EDITION),
                1,
                new TNodeVersionInfo(SEED_VERSION, "node-build-info")
                    .setProductEdition(IoTDBConstant.PRODUCT_EDITION)));

    TSStatus status =
        ClusterNodeStartUtils.confirmProductEditionOnRestart(
            NodeType.DataNode,
            1,
            new TNodeVersionInfo(SEED_VERSION, "node-build-info")
                .setProductEdition(DIFFERENT_PRODUCT_EDITION),
            configManager);

    assertProductEditionMismatch(status);

    Assert.assertEquals(
        TSStatusCode.REJECT_NODE_START.getStatusCode(),
        ClusterNodeStartUtils.confirmProductEditionOnRestart(
                NodeType.ConfigNode,
                1,
                new TNodeVersionInfo(SEED_VERSION, "node-build-info")
                    .setProductEdition(DIFFERENT_PRODUCT_EDITION),
                configManager)
            .getCode());
  }

  @Test
  public void testUseCurrentConfigNodeEditionWhenSeedVersionInfoIsMissing() {
    Mockito.when(configManager.getNodeManager().getNodeVersionInfo())
        .thenReturn(Collections.emptyMap());

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ClusterNodeStartUtils.confirmProductEdition(
                NodeType.DataNode,
                new TNodeVersionInfo("any-version", "build-info")
                    .setProductEdition(IoTDBConstant.PRODUCT_EDITION),
                configManager)
            .getCode());
  }

  @Test
  public void testIgnoreMismatchedReleaseVersion() {
    final TNodeVersionInfo versionInfo =
        new TNodeVersionInfo(MISMATCHED_VERSION, "build-info")
            .setProductEdition(IoTDBConstant.PRODUCT_EDITION);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ClusterNodeStartUtils.confirmProductEdition(NodeType.DataNode, versionInfo, configManager)
            .getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ClusterNodeStartUtils.confirmProductEdition(NodeType.ConfigNode, versionInfo, configManager)
            .getCode());
  }

  @Test
  public void testRejectDifferentProductEditionForConfigNodeAndDataNode() {
    final String clusterName = ConfigNodeDescriptor.getInstance().getConf().getClusterName();
    final TNodeVersionInfo differentEditionVersionInfo =
        new TNodeVersionInfo(SEED_VERSION, "build-info")
            .setProductEdition(DIFFERENT_PRODUCT_EDITION);

    assertProductEditionMismatch(
        ClusterNodeStartUtils.confirmDataNodeRegistration(
            new TDataNodeRegisterReq()
                .setClusterName(clusterName)
                .setVersionInfo(differentEditionVersionInfo),
            configManager));
    assertProductEditionMismatch(
        ClusterNodeStartUtils.confirmConfigNodeRegistration(
            new TConfigNodeRegisterReq()
                .setClusterParameters(new TClusterParameters().setClusterName(clusterName))
                .setVersionInfo(differentEditionVersionInfo),
            configManager));
  }

  @Test
  public void testIgnoreProductEditionForAINodeRegistration() {
    ClusterManager clusterManager = Mockito.mock(ClusterManager.class);
    Mockito.when(configManager.getClusterManager()).thenReturn(clusterManager);
    Mockito.when(clusterManager.getClusterIdWithRetry(Mockito.anyLong())).thenReturn("cluster-id");
    Mockito.when(nodeManager.getRegisteredAINodes()).thenReturn(Collections.emptyList());
    Mockito.when(nodeManager.registerAINodeActivationCheck(Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    TAINodeRegisterReq req =
        new TAINodeRegisterReq()
            .setClusterName(ConfigNodeDescriptor.getInstance().getConf().getClusterName())
            .setAiNodeConfiguration(
                new TAINodeConfiguration(
                    new TAINodeLocation(-1, new TEndPoint("127.0.0.1", 10810)),
                    new TNodeResource(1, 1024)))
            .setVersionInfo(
                new TNodeVersionInfo(MISMATCHED_VERSION, "build-info")
                    .setProductEdition(DIFFERENT_PRODUCT_EDITION));

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ClusterNodeStartUtils.confirmAINodeRegistration(req, configManager).getCode());
  }

  @Test
  public void testAcceptRegistrationWithoutProductEditionForCompatibility() {
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ClusterNodeStartUtils.confirmProductEdition(
                NodeType.DataNode, new TNodeVersionInfo(SEED_VERSION, "build-info"), configManager)
            .getCode());
  }

  private static void assertProductEditionMismatch(TSStatus status) {
    Assert.assertEquals(TSStatusCode.REJECT_NODE_START.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains(DIFFERENT_PRODUCT_EDITION));
    Assert.assertTrue(status.getMessage().contains(IoTDBConstant.PRODUCT_EDITION));
  }
}
