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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.IPv6TestUtils;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.it.utils.IPv6TestUtils.IPV6_LOOPBACK_ADDRESS;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeIPv6IT extends AbstractPipeDualTreeModelAutoIT {

  private static String previousTestNodeAddress;

  @BeforeClass
  public static void setUpIPv6() {
    IPv6TestUtils.assumeIPv6LoopbackAvailable();
    previousTestNodeAddress = IPv6TestUtils.setTestNodeAddressToIPv6Loopback();
  }

  @AfterClass
  public static void tearDownIPv6() {
    IPv6TestUtils.restoreTestNodeAddress(previousTestNodeAddress);
  }

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 1);
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
  }

  @Test
  public void testSyncAndAsyncSinksThroughIPv6Loopback() throws Exception {
    assertDataNodesUseIPv6(senderEnv.getDataNodeWrapperList());
    assertDataNodesUseIPv6(receiverEnv.getDataNodeWrapperList());

    final String receiverNodeUrl = receiverEnv.getDataNodeWrapper(0).getIpAndPortString();
    Assert.assertTrue(receiverNodeUrl.startsWith("[::1]:"));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      createAndStartPipe(
          client,
          "ipv6_async_pipe",
          "root.ipv6_async.**",
          BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName(),
          receiverNodeUrl);
      createAndStartPipe(
          client,
          "ipv6_sync_pipe",
          "root.ipv6_sync.**",
          BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName(),
          receiverNodeUrl);
    }

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "insert into root.ipv6_async.d1(time, s1) values (1, 1)",
            "insert into root.ipv6_sync.d1(time, s1) values (1, 1)",
            "flush"),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(s1) from root.ipv6_async.d1",
        "count(root.ipv6_async.d1.s1),",
        Collections.singleton("1,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(s1) from root.ipv6_sync.d1",
        "count(root.ipv6_sync.d1.s1),",
        Collections.singleton("1,"));
  }

  private static void createAndStartPipe(
      final SyncConfigNodeIServiceClient client,
      final String pipeName,
      final String sourcePath,
      final String sinkName,
      final String receiverNodeUrl)
      throws Exception {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.path", sourcePath);

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put("sink", sinkName);
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.node-urls", receiverNodeUrl);

    final TSStatus status =
        client.createPipe(
            new TCreatePipeReq(pipeName, sinkAttributes).setExtractorAttributes(sourceAttributes));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe(pipeName).getCode());
  }

  private static void assertDataNodesUseIPv6(final Iterable<DataNodeWrapper> dataNodes) {
    for (final DataNodeWrapper dataNode : dataNodes) {
      Assert.assertEquals(IPV6_LOOPBACK_ADDRESS, dataNode.getIp());
      Assert.assertTrue(dataNode.getIpAndPortString().startsWith("[::1]:"));
    }
  }
}
