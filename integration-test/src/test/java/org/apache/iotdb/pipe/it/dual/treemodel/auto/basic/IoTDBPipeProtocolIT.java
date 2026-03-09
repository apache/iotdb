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
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Test pipe's basic functionalities under multiple cluster and consensus protocol settings. */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeProtocolIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
  }

  private void innerSetUp(
      final String configNodeConsensus,
      final String schemaRegionConsensus,
      final String dataRegionConsensus,
      final int configNodesNum,
      final int dataNodesNum,
      int schemaRegionReplicationFactor,
      int dataRegionReplicationFactor) {
    schemaRegionReplicationFactor = Math.min(schemaRegionReplicationFactor, dataNodesNum);
    dataRegionReplicationFactor = Math.min(dataRegionReplicationFactor, dataNodesNum);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(dataRegionConsensus)
        .setSchemaReplicationFactor(schemaRegionReplicationFactor)
        .setDataReplicationFactor(dataRegionReplicationFactor)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(configNodeConsensus)
        .setSchemaRegionConsensusProtocolClass(schemaRegionConsensus)
        .setDataRegionConsensusProtocolClass(dataRegionConsensus)
        .setSchemaReplicationFactor(schemaRegionReplicationFactor)
        .setDataReplicationFactor(dataRegionReplicationFactor)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

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

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment(3, 3);
    receiverEnv.initClusterEnvironment(1, 1);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQuery(senderEnv, "insert into root.db.d1(time, s1) values (1, 1)", null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());
    }

    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQuery(
          receiverEnv, "insert into root.db.d1(time, s1) values (2, 2)", null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", senderIp);
      sinkAttributes.put("sink.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          senderEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  private void doTest() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQuery(senderEnv, "insert into root.db.d1(time, s1) values (1, 1)", null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      Thread.sleep(5000);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
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

  private void doTestUseNodeUrls(String sinkName) throws Exception {
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeAirGapReceiverEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
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

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 3);

    final StringBuilder nodeUrlsBuilder = new StringBuilder();
    for (final DataNodeWrapper wrapper : receiverEnv.getDataNodeWrapperList()) {
      if (sinkName.equals(BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName())) {
        // Use default port for convenience
        nodeUrlsBuilder
            .append(wrapper.getIp())
            .append(":")
            .append(wrapper.getPipeAirGapReceiverPort())
            .append(",");
      } else {
        nodeUrlsBuilder.append(wrapper.getIpAndPortString()).append(",");
      }
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Test mods transfer
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (1, 1)",
              "insert into root.db.d1(time, s1) values (3, 1)",
              "flush",
              "delete from root.db.d1.s1 where time > 2"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sinkAttributes.put("sink", sinkName);
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.node-urls", nodeUrlsBuilder.toString());

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.mods.enable", "true");
      sourceAttributes.put("user", "root");

      // Test forced-log mode, in open releases this might be "file"
      sourceAttributes.put("source.realtime.mode", "forced-log");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(s1) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));

      // Test metadata
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create timeseries root.db.d1.s2 with datatype=BOOLEAN,encoding=PLAIN",
              "create database root.test1"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton("4,"));

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("3,"));

      // Test file mode
      sourceAttributes.put("source.inclusion", "data");
      sourceAttributes.replace("source.realtime.mode", "file");

      status =
          client.createPipe(
              new TCreatePipeReq("p2", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (3, 3)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(s1) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("3,"));
    }
  }
}
