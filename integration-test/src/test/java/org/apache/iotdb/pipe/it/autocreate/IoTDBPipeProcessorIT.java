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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeProcessorIT extends AbstractPipeDualAutoIT {
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
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @Test
  public void testTumblingTimeSamplingProcessor() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Test empty tsFile parsing
      // Assert that an empty tsFile will not be parsed by the processor then block
      // the subsequent data processing
      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (0, 1)", "delete from root.**"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.realtime.mode", "log");

      processorAttributes.put("processor", "tumbling-time-sampling-processor");
      processorAttributes.put("processor.tumbling-time.interval-seconds", "20");
      processorAttributes.put("processor.down-sampling.split-file", "true");

      connectorAttributes.put("sink", "iotdb-thrift-sink");
      connectorAttributes.put("sink.batch.enable", "false");
      connectorAttributes.put("sink.ip", receiverIp);
      connectorAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (0, 1)",
              "insert into root.vehicle.d0(time, s1) values (10000, 2)",
              "insert into root.vehicle.d0(time, s1) values (19999, 3)",
              "insert into root.vehicle.d0(time, s1) values (20000, 4)",
              "insert into root.vehicle.d0(time, s1) values (20001, 5)",
              "insert into root.vehicle.d0(time, s1) values (45000, 6)",
              "flush"))) {
        return;
      }

      final Set<String> expectedResSet = new HashSet<>();

      expectedResSet.add("0,1.0,");
      expectedResSet.add("20000,4.0,");
      expectedResSet.add("45000,6.0,");

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.vehicle.d0.s1,", expectedResSet);
    }
  }

  @Test
  public void testChangingValueProcessor() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Test empty tsFile parsing
      // Assert that an empty tsFile will not be parsed by the processor then block
      // the subsequent data processing
      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (0, 1)", "delete from root.**"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.realtime.mode", "log");

      processorAttributes.put("processor", "changing-value-sampling-processor");
      processorAttributes.put("processor.changing-value.compression-deviation", "10");
      processorAttributes.put("processor.changing-value.min-time-interval", "10000");
      processorAttributes.put("processor.changing-value.max-time-interval", "20000");

      connectorAttributes.put("sink", "iotdb-thrift-sink");
      connectorAttributes.put("sink.batch.enable", "false");
      connectorAttributes.put("sink.ip", receiverIp);
      connectorAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (0, 10)",
              "insert into root.vehicle.d0(time, s1) values (9999, 20)",
              "insert into root.vehicle.d0(time, s1) values (10000, 30)",
              "insert into root.vehicle.d0(time, s1) values (19000, 40)",
              "insert into root.vehicle.d0(time, s1) values (20000, 50)",
              "insert into root.vehicle.d0(time, s1) values (29001, 60)",
              "insert into root.vehicle.d0(time, s1) values (50000, 70)",
              "insert into root.vehicle.d0(time, s1) values (60000, 71)",
              "flush"))) {
        return;
      }

      final Set<String> expectedResSet = new HashSet<>();

      expectedResSet.add("0,10.0,");
      expectedResSet.add("10000,30.0,");
      expectedResSet.add("20000,50.0,");
      expectedResSet.add("50000,70.0,");

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.vehicle.d0.s1,", expectedResSet);
    }
  }

  @Test
  public void testChangingPointProcessor() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Test empty tsFile parsing
      // Assert that an empty tsFile will not be parsed by the processor then block
      // the subsequent data processing
      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (0, 1)", "delete from root.**"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.realtime.mode", "log");

      processorAttributes.put("processor", "changing-point-sampling-processor");
      processorAttributes.put("processor.changing-point.compression-deviation", "10");
      processorAttributes.put("processor.changing-point.arrival-time.min-interval", "10000");
      processorAttributes.put("processor.changing-point.arrival-time.max-interval", "30000");
      processorAttributes.put("processor.changing-point.event-time.min-interval", "10000");
      processorAttributes.put("processor.changing-point.event-time.max-interval", "30000");

      connectorAttributes.put("sink", "iotdb-thrift-sink");
      connectorAttributes.put("sink.batch.enable", "false");
      connectorAttributes.put("sink.ip", receiverIp);
      connectorAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (0, 10)",
              "insert into root.vehicle.d0(time, s1) values (100000, 20)",
              "insert into root.vehicle.d0(time, s1) values (110000, 30)"))) {
        return;
      }

      Thread.sleep(10000);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.vehicle.d0(time, s1) values (100000, 40)",
              "insert into root.vehicle.d0(time, s1) values (400000, 50)",
              "insert into root.vehicle.d0(time, s1) values (500000, 60)"))) {
        return;
      }

      Thread.sleep(10000);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList("insert into root.vehicle.d0(time, s1) values (100000, 41)", "flush"))) {
        return;
      }

      final Set<String> expectedResSet = new HashSet<>();

      expectedResSet.add("0,10.0,");
      expectedResSet.add("100000,40.0,");

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from root.**", "Time,root.vehicle.d0.s1,", expectedResSet);
    }
  }
}
