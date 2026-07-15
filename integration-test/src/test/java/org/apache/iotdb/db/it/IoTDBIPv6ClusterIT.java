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

package org.apache.iotdb.db.it;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.IPv6TestUtils;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.it.utils.IPv6TestUtils.IPV6_LOOPBACK_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBIPv6ClusterIT {

  private static String previousTestNodeAddress;

  @BeforeClass
  public static void setUp() {
    IPv6TestUtils.assumeIPv6LoopbackAvailable();
    previousTestNodeAddress = IPv6TestUtils.setTestNodeAddressToIPv6Loopback();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
    IPv6TestUtils.restoreTestNodeAddress(previousTestNodeAddress);
  }

  @Test
  public void clusterCanCommunicateThroughIPv6Loopback() throws Exception {
    for (ConfigNodeWrapper configNode : EnvFactory.getEnv().getConfigNodeWrapperList()) {
      assertEquals(IPV6_LOOPBACK_ADDRESS, configNode.getIp());
      assertTrue(configNode.getIpAndPortString().startsWith("[::1]:"));
    }
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      assertEquals(IPV6_LOOPBACK_ADDRESS, dataNode.getIp());
      assertTrue(dataNode.getIpAndPortString().startsWith("[::1]:"));
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TShowClusterResp showClusterResp = client.showCluster();
      final TSStatus status = showClusterResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      assertEquals(1, showClusterResp.getConfigNodeListSize());
      assertEquals(3, showClusterResp.getDataNodeListSize());
      showClusterResp
          .getConfigNodeList()
          .forEach(
              configNode ->
                  assertEquals(IPV6_LOOPBACK_ADDRESS, configNode.getInternalEndPoint().getIp()));
      showClusterResp.getDataNodeList().forEach(this::assertDataNodeLocationUsesIPv6);
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ipv6_cluster");
      statement.execute("CREATE TIMESERIES root.ipv6_cluster.d1.s1 INT64");
      statement.execute("INSERT INTO root.ipv6_cluster.d1(time, s1) VALUES (1, 300)");
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.ipv6_cluster.d1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertEquals(300L, resultSet.getLong(2));
        assertFalse(resultSet.next());
      }
    }
  }

  private void assertDataNodeLocationUsesIPv6(final TDataNodeLocation dataNodeLocation) {
    assertEquals(IPV6_LOOPBACK_ADDRESS, dataNodeLocation.getClientRpcEndPoint().getIp());
    assertEquals(IPV6_LOOPBACK_ADDRESS, dataNodeLocation.getInternalEndPoint().getIp());
    assertEquals(IPV6_LOOPBACK_ADDRESS, dataNodeLocation.getMPPDataExchangeEndPoint().getIp());
    assertEquals(
        IPV6_LOOPBACK_ADDRESS, dataNodeLocation.getSchemaRegionConsensusEndPoint().getIp());
    assertEquals(IPV6_LOOPBACK_ADDRESS, dataNodeLocation.getDataRegionConsensusEndPoint().getIp());
  }
}
