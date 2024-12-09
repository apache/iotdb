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

package org.apache.iotdb.pipe.it.autocreate;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeAutoConflictIT extends AbstractPipeDualAutoIT {
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @Test
  public void testDoubleLivingAutoConflict() throws Exception {
    // Double living is two clusters each with a pipe connecting to the other.
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    for (int i = 0; i < 100; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "all");
      extractorAttributes.put("source.inclusion.exclusion", "");
      // Add this property to avoid making self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

      connectorAttributes.put("sink", "iotdb-thrift-sink");
      connectorAttributes.put("sink.batch.enable", "false");
      connectorAttributes.put("sink.ip", receiverIp);
      connectorAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    for (int i = 100; i < 200; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    for (int i = 200; i < 300; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "all");
      extractorAttributes.put("source.inclusion.exclusion", "");
      // Add this property to avoid to make self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

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
    }
    for (int i = 300; i < 400; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    for (int i = 400; i < 500; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }
    for (int i = 500; i < 600; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    for (int i = 400; i < 600; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.**", "Time,root.db.d1.s1,", expectedResSet);
  }

  @Test
  public void testDoubleLivingAutoConflictTemplate() throws Exception {
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "all");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

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
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "all");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

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
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Arrays.asList(
            // Auto extend s1
            "create schema template t1 (s2 INT64 encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)",
            "create database root.db",
            "set device template t1 to root.db"))) {
      return;
    }

    for (int i = 0; i < 200; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }

    for (int i = 200; i < 400; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }

    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select s1 from root.db.d1", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select s1 from root.db.d1", "Time,root.db.d1.s1,", expectedResSet);
  }

  @Test
  public void testAutoManualCreateRace() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, "create timeSeries root.ln.wf01.wt01.status with datatype=BOOLEAN")) {
        return;
      }

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv,
          "create timeSeries root.ln.wf01.wt01.status with datatype=BOOLEAN tags (tag3=v3) attributes (attr4=v4)")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeSeries",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          Collections.singleton(
              "root.ln.wf01.wt01.status,null,root.ln,BOOLEAN,RLE,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,"));
    }
  }
}
