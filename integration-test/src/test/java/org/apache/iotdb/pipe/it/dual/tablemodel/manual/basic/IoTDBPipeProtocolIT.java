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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/** Test pipe's basic functionalities under multiple cluster and consensus protocol settings. */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeProtocolIT extends AbstractPipeTableModelDualManualIT {

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
        .setDnConnectionTimeoutMs(600000)
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
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

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
        .setDataReplicationFactor(2)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    senderEnv.initClusterEnvironment(3, 3);
    receiverEnv.initClusterEnvironment(1, 1);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("mode.streaming", "true");
      extractorAttributes.put("mode.snapshot", "false");
      extractorAttributes.put("mode.strict", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TableModelUtils.assertCountData("test", "test", 100, receiverEnv, handleFailure);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());
    }

    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(receiverEnv, "test1", "test1");
      TableModelUtils.insertData("test1", "test1", 0, 100, receiverEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", "test.*");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("mode.streaming", "true");
      extractorAttributes.put("mode.snapshot", "false");
      extractorAttributes.put("mode.strict", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderIp);
      connectorAttributes.put("connector.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TableModelUtils.assertCountData("test1", "test1", 100, senderEnv, handleFailure);
    }
  }

  private void doTest() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", "test.*");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("mode.streaming", "true");
      extractorAttributes.put("mode.snapshot", "false");
      extractorAttributes.put("mode.strict", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TableModelUtils.assertData("test", "test", 0, 100, receiverEnv, handleFailure);

      TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

      TableModelUtils.assertData("test", "test", 0, 200, receiverEnv, handleFailure);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TableModelUtils.insertData("test", "test", 200, 300, senderEnv);

      TableModelUtils.assertData("test", "test", 0, 200, receiverEnv, handleFailure);
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
        .setDataReplicationFactor(1)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeAirGapReceiverEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 3);

    final StringBuilder nodeUrlsBuilder = new StringBuilder();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    for (final DataNodeWrapper wrapper : receiverEnv.getDataNodeWrapperList()) {
      if (connectorName.equals(BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName())) {
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

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", connectorName);
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.batch.max-delay-seconds", "1");
      connectorAttributes.put("connector.batch.size-bytes", "2048");
      connectorAttributes.put("connector.node-urls", nodeUrlsBuilder.toString());

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", "test.*");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("mode.snapshot", "false");
      extractorAttributes.put("mode.strict", "true");
      extractorAttributes.put("user", "root");

      // Test forced-log mode, in open releases this might be "file"
      extractorAttributes.put("realtime.mode", "forced-log");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

      TableModelUtils.assertData("test", "test", 0, 200, receiverEnv, handleFailure);

      TableModelUtils.insertData("test", "test", 200, 300, senderEnv);

      TableModelUtils.assertData("test", "test", 0, 300, receiverEnv, handleFailure);

      extractorAttributes.replace("realtime.mode", "file");

      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TableModelUtils.insertData("test", "test", 300, 400, senderEnv);

      TableModelUtils.assertData("test", "test", 0, 400, receiverEnv, handleFailure);
    }
  }
}
