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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
/** Test pipe's basic functionalities under multiple cluster and consensus protocol settings. */
public class IoTDBPipeProtocolIT {

  private BaseEnv senderEnv;
  private BaseEnv receiverEnv;

  @Before
  public void setUp() throws Exception {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
  }

  @After
  public void tearDown() {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }

  private void innerSetUp(
      String configNodeConsensus,
      String schemaRegionConsensus,
      String dataRegionConsensus,
      int configNodesNum,
      int dataNodesNum,
      int schemaRegionReplicationFactor,
      int dataRegionReplicationFactor) {
    schemaRegionReplicationFactor = Math.min(schemaRegionReplicationFactor, dataNodesNum);
    dataRegionReplicationFactor = Math.min(dataRegionReplicationFactor, dataNodesNum);
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(configNodeConsensus)
        .setSchemaRegionConsensusProtocolClass(schemaRegionConsensus)
        .setDataRegionConsensusProtocolClass(dataRegionConsensus)
        .setSchemaReplicationFactor(schemaRegionReplicationFactor)
        .setDataReplicationFactor(dataRegionReplicationFactor);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(configNodeConsensus)
        .setSchemaRegionConsensusProtocolClass(schemaRegionConsensus)
        .setDataRegionConsensusProtocolClass(dataRegionConsensus)
        .setSchemaReplicationFactor(schemaRegionReplicationFactor)
        .setDataReplicationFactor(dataRegionReplicationFactor);

    senderEnv.initClusterEnvironment(configNodesNum, dataNodesNum);
    receiverEnv.initClusterEnvironment(configNodesNum, dataNodesNum);
  }

  @Test
  public void test1C1DWithRatisRatisIot() throws Exception {
    innerSetUp(
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.IOT_CONSENSUS,
        1,
        1,
        1,
        1);
    doTest();
  }

  @Test
  public void test1C1DWithSimpleSimpleIot() throws Exception {
    innerSetUp(
        ConsensusFactory.SIMPLE_CONSENSUS,
        ConsensusFactory.SIMPLE_CONSENSUS,
        ConsensusFactory.IOT_CONSENSUS,
        1,
        1,
        1,
        1);
    doTest();
  }

  @Test
  public void test1C1DWithRatisRatisSimple() throws Exception {
    innerSetUp(
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.SIMPLE_CONSENSUS,
        1,
        1,
        1,
        1);
    doTest();
  }

  @Test
  public void test3C3DWith3SchemaRegionFactor3DataRegionFactor() throws Exception {
    innerSetUp(
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.IOT_CONSENSUS,
        3,
        3,
        3,
        3);
    doTest();
  }

  @Test
  public void test3C3DWith3SchemaRegionFactor2DataRegionFactor() throws Exception {
    innerSetUp(
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.RATIS_CONSENSUS,
        ConsensusFactory.IOT_CONSENSUS,
        3,
        3,
        3,
        2);
    doTest();
  }

  @Test
  public void testPipeOnBothSenderAndReceiver() throws Exception {
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1);

    senderEnv.initClusterEnvironment(3, 3);
    receiverEnv.initClusterEnvironment(1, 1);

    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());
    }

    DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    String senderIp = senderDataNode.getIp();
    int senderPort = senderDataNode.getPort();
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          receiverEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderIp);
      connectorAttributes.put("connector.port", Integer.toString(senderPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataOnEnv(
          senderEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  private void doTest() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (3, 3)");

      Thread.sleep(5000);
      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  @Test
  public void testSyncConnectorUseNodeUrls() throws Exception {
    doTestUseNodeUrls(BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName());
  }

  @Test
  public void testAsyncConnectorUseNodeUrls() throws Exception {
    doTestUseNodeUrls(BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName());
  }

  @Test
  public void testAirGapConnectorUseNodeUrls() throws Exception {
    doTestUseNodeUrls(BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName());
  }

  private void doTestUseNodeUrls(String connectorName) throws Exception {
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeAirGapReceiverEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeAirGapReceiverEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);

    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 3);

    StringBuilder nodeUrlsBuilder = new StringBuilder();
    for (DataNodeWrapper wrapper : receiverEnv.getDataNodeWrapperList()) {
      if (connectorName.equals(BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName())) {
        // use default port for convenience
        nodeUrlsBuilder
            .append(wrapper.getIp())
            .append(":")
            .append(wrapper.getPipeAirGapReceiverPort())
            .append(",");
      } else {
        nodeUrlsBuilder.append(wrapper.getIpAndPortString()).append(",");
      }
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", connectorName);
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.node-urls", nodeUrlsBuilder.toString());

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      System.out.println(status.getMessage());
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2, 2)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }
}
