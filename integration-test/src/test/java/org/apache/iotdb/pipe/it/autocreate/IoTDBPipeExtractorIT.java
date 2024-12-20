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
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeExtractorIT extends AbstractPipeDualAutoIT {

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
        // Disable sender compaction for tsfile determination in loose range test
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
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
  public void testExtractorValidParameter() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    // ---------------------------------------------------------- //
    // Scenario 1: when 'extractor.history.enable' is set to true //
    // ---------------------------------------------------------- //

    // Scenario 1.1: test when 'extractor.history.start-time' and 'extractor.history.end-time' are
    // not set
    final String p1_1 =
        String.format(
            "create pipe p1_1"
                + " with extractor ("
                + "'extractor.history.enable'='true')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p1_1);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Scenario 1.2: test when only 'extractor.history.start-time' is set
    final String p1_2 =
        String.format(
            "create pipe p1_2"
                + " with extractor ("
                + "'extractor.history.enable'='true',"
                + "'extractor.history.start-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p1_2);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Scenario 1.3: test when only 'extractor.history.end-time' is set
    final String p1_3 =
        String.format(
            "create pipe p1_3"
                + " with extractor ("
                + "'extractor.history.enable'='true',"
                + "'extractor.history.end-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p1_3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Scenario 1.4: test when 'extractor.history.start-time' equals 'extractor.history.end-time'
    final String p1_4 =
        String.format(
            "create pipe p1_4"
                + " with extractor ("
                + "'extractor.history.enable'='true',"
                + "'extractor.history.start-time'='2000.01.01T08:00:00',"
                + "'extractor.history.end-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p1_4);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Scenario 1.5: test when 'extractor.history.end-time' is future time
    final String p1_5 =
        String.format(
            "create pipe p1_5"
                + " with extractor ("
                + "'extractor.history.enable'='true',"
                + "'extractor.history.start-time'='2000.01.01T08:00:00',"
                + "'extractor.history.end-time'='2100.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p1_5);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    assertPipeCount(5);

    // ---------------------------------------------------------------------- //
    // Scenario 2: when 'source.start-time' or 'source.end-time' is specified //
    // ---------------------------------------------------------------------- //

    // Scenario 2.1: test when only 'source.start-time' is set
    final String p2_1 =
        String.format(
            "create pipe p2_1"
                + " with source ("
                + "'source.start-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p2_1);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Scenario 2.2: test when only 'source.end-time' is set
    final String p2_2 =
        String.format(
            "create pipe p2_2"
                + " with source ("
                + "'source.end-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p2_2);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Scenario 2.3: test when 'source.start-time' equals 'source.end-time'
    final String p2_3 =
        String.format(
            "create pipe p2_3"
                + " with source ("
                + "'source.start-time'='2000.01.01T08:00:00',"
                + "'source.end-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p2_3);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Scenario 2.4: test when only when 'source.start-time' is less than 'source.end-time'
    final String p2_4 =
        String.format(
            "create pipe p2_4"
                + " with source ("
                + "'source.start-time'='2000.01.01T08:00:00',"
                + "'source.end-time'='2100.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p2_4);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    assertPipeCount(9);

    // ---------------------------------------------------------------------- //
    // Scenario 3: when 'source.start-time' or 'source.end-time' is timestamp //
    // ---------------------------------------------------------------------- //

    // Scenario 3.1: test when 'source.start-time' is timestamp string
    final String p3_1 =
        String.format(
            "create pipe p3_1"
                + " with source ("
                + "'source.start-time'='1000',"
                + "'source.end-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p3_1);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    assertPipeCount(10);
  }

  @Test
  public void testExtractorInvalidParameter() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    // Scenario 1: invalid 'extractor.history.start-time'
    final String formatString =
        String.format(
            "create pipe p1"
                + " with extractor ("
                + "'extractor.history.enable'='true',"
                + "'extractor.history.start-time'=%s)"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            "%s", receiverIp, receiverPort);

    final List<String> invalidStartTimes =
        Arrays.asList("''", "null", "'null'", "'-1000-01-01T00:00:00'", "'2000-01-01T00:00:0'");
    for (final String invalidStartTime : invalidStartTimes) {
      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(String.format(formatString, invalidStartTime));
        fail();
      } catch (final SQLException ignored) {
      }
    }
    assertPipeCount(0);

    // Scenario 2: can not set 'extractor.history.enable' and 'extractor.realtime.enable' both to
    // false
    final String p2 =
        String.format(
            "create pipe p2"
                + " with extractor ("
                + "'extractor.history.enable'='false',"
                + "'extractor.realtime.enable'='false')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p2);
      fail();
    } catch (final SQLException ignored) {
    }
    assertPipeCount(0);

    // Scenario 3: test when 'extractor.history.start-time' is greater than
    // 'extractor.history.end-time'
    final String p3 =
        String.format(
            "create pipe p3"
                + " with extractor ("
                + "'extractor.history.enable'='true',"
                + "'extractor.history.start-time'='2001.01.01T08:00:00',"
                + "'extractor.history.end-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p3);
      fail();
    } catch (final SQLException ignored) {
    }
    assertPipeCount(0);

    // Scenario 4: test when 'source.start-time' is greater than 'source.end-time'
    final String p4 =
        String.format(
            "create pipe p4"
                + " with source ("
                + "'source.start-time'='2001.01.01T08:00:00',"
                + "'source.end-time'='2000.01.01T08:00:00')"
                + " with connector ("
                + "'connector'='iotdb-thrift-connector',"
                + "'connector.ip'='%s',"
                + "'connector.port'='%s',"
                + "'connector.batch.enable'='false')",
            receiverIp, receiverPort);
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(p4);
      fail();
    } catch (final SQLException ignored) {
    }
    assertPipeCount(0);
  }

  @Test
  public void testExtractorPatternMatch() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.nonAligned.1TS (time, s_float) values (now(), 0.5)",
              "insert into root.nonAligned.100TS (time, s_float) values (now(), 0.5)",
              "insert into root.nonAligned.1000TS (time, s_float) values (now(), 0.5)",
              "insert into root.nonAligned.`1(TS)` (time, s_float) values (now(), 0.5)",
              "insert into root.nonAligned.6TS.`6` ("
                  + "time, `s_float(1)`, `s_int(1)`, `s_double(1)`, `s_long(1)`, `s_text(1)`, `s_bool(1)`) "
                  + "values (now(), 0.5, 1, 1.5, 2, \"text1\", true)",
              "insert into root.aligned.1TS (time, s_float) aligned values (now(), 0.5)",
              "insert into root.aligned.100TS (time, s_float) aligned values (now(), 0.5)",
              "insert into root.aligned.1000TS (time, s_float) aligned values (now(), 0.5)",
              "insert into root.aligned.`1(TS)` (time, s_float) aligned values (now(), 0.5)",
              "insert into root.aligned.6TS.`6` ("
                  + "time, `s_float(1)`, `s_int(1)`, `s_double(1)`, `s_long(1)`, `s_text(1)`, `s_bool(1)`) "
                  + "aligned values (now(), 0.5, 1, 1.5, 2, \"text1\", true)"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", null);
      extractorAttributes.put("extractor.inclusion", "data.insert");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final List<String> patterns =
          Arrays.asList(
              "root.db.nonExistPath", // match nothing
              "root.nonAligned.1TS.s_float",
              "root.nonAligned.6TS.`6`.`s_text(1)`",
              "root.aligned.1TS",
              "root.aligned.6TS.`6`.`s_int(1)`",
              "root.aligned.`1(TS)`",
              "root.nonAligned.`1(TS)`",
              "root.aligned.6TS.`6`", // match device root.aligned.6TS.`6`
              "root.nonAligned.6TS.`6`", // match device root.nonAligned.6TS.`6`
              "root.nonAligned.`1`", // match nothing
              "root.nonAligned.1", // match device root.nonAligned.1TS, 100TS and 100TS
              "root.aligned.1" // match device root.aligned.1TS, 100TS and 100TS
              );

      final List<Integer> expectedTimeseriesCount =
          Arrays.asList(0, 1, 2, 3, 4, 5, 6, 11, 16, 16, 18, 20);

      for (int i = 0; i < patterns.size(); ++i) {
        extractorAttributes.replace("extractor.pattern", patterns.get(i));
        final TSStatus status =
            client.createPipe(
                new TCreatePipeReq("p" + i, connectorAttributes)
                    .setExtractorAttributes(extractorAttributes)
                    .setProcessorAttributes(processorAttributes));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p" + i).getCode());
        // We add flush here because the pipe may be created on the new IoT leader
        // and the old leader's data may come as an unclosed historical tsfile
        // and is skipped flush when the pipe starts. In this case, the "waitForTsFileClose()"
        // may not return until a flush is executed, namely the data transfer relies
        // on a flush operation.
        if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
          return;
        }
        assertTimeseriesCountOnReceiver(receiverEnv, expectedTimeseriesCount.get(i));
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show devices",
          "Device,IsAligned,Template,TTL(ms),",
          new HashSet<>(
              Arrays.asList(
                  "root.nonAligned.1TS,false,null,INF,",
                  "root.nonAligned.100TS,false,null,INF,",
                  "root.nonAligned.1000TS,false,null,INF,",
                  "root.nonAligned.`1(TS)`,false,null,INF,",
                  "root.nonAligned.6TS.`6`,false,null,INF,",
                  "root.aligned.1TS,true,null,INF,",
                  "root.aligned.100TS,true,null,INF,",
                  "root.aligned.1000TS,true,null,INF,",
                  "root.aligned.`1(TS)`,true,null,INF,",
                  "root.aligned.6TS.`6`,true,null,INF,")));
    }
  }

  @Test
  public void testMatchingMultipleDatabases() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", "root.db1");
      extractorAttributes.put("extractor.inclusion", "data.insert");

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
      assertTimeseriesCountOnReceiver(receiverEnv, 0);

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db1.d1 (time, at1) values (1, 10)",
              "insert into root.db2.d1 (time, at1) values (1, 20)",
              "flush"))) {
        return;
      }

      extractorAttributes.replace("extractor.pattern", "root.db2");
      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());
      assertTimeseriesCountOnReceiver(receiverEnv, 2);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p1").getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("p2").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db1.d1 (time, at1) values (2, 11)",
              "insert into root.db2.d1 (time, at1) values (2, 21)",
              "flush"))) {
        return;
      }

      extractorAttributes.remove("extractor.pattern"); // no pattern, will match all databases
      status =
          client.createPipe(
              new TCreatePipeReq("p3", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p3").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db1.d1.at1),count(root.db2.d1.at1),",
          Collections.singleton("2,2,"));
    }
  }

  @Test
  public void testHistoryAndRealtime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1) values (1, 10)",
              "insert into root.db.d2 (time, at1) values (1, 20)",
              "insert into root.db.d3 (time, at1) values (1, 30)",
              "insert into root.db.d4 (time, at1) values (1, 40)",
              "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.pattern", "root.db.d2");
      extractorAttributes.put("extractor.history.enable", "false");
      extractorAttributes.put("extractor.realtime.enable", "true");
      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      extractorAttributes.replace("extractor.pattern", "root.db.d3");
      extractorAttributes.replace("extractor.history.enable", "true");
      extractorAttributes.replace("extractor.realtime.enable", "false");
      status =
          client.createPipe(
              new TCreatePipeReq("p3", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p3").getCode());

      extractorAttributes.replace("extractor.pattern", "root.db.d4");
      extractorAttributes.replace("extractor.history.enable", "true");
      extractorAttributes.replace("extractor.realtime.enable", "true");
      status =
          client.createPipe(
              new TCreatePipeReq("p4", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p4").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1) values (2, 11)",
              "insert into root.db.d2 (time, at1) values (2, 21)",
              "insert into root.db.d3 (time, at1) values (2, 31)",
              "insert into root.db.d4 (time, at1) values (2, 41), (3, 51)"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.** where time <= 1",
          "count(root.db.d4.at1),count(root.db.d2.at1),count(root.db.d3.at1),",
          Collections.singleton("1,0,1,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.** where time >= 2",
          "count(root.db.d4.at1),count(root.db.d2.at1),count(root.db.d3.at1),",
          Collections.singleton("2,1,0,"));
    }
  }

  @Test
  public void testHistoryStartTimeAndEndTimeWorkingWithOrWithoutPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)"
                  + " values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)",
              "insert into root.db.d2 (time, at1)"
                  + " values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)",
              "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", "root.db.d1");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.history.enable", "true");
      // 1970-01-01T08:00:02+08:00
      extractorAttributes.put("extractor.history.start-time", "2000");
      extractorAttributes.put("extractor.history.end-time", "1970-01-01T08:00:04+08:00");

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

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),",
          Collections.singleton("3,"));

      extractorAttributes.remove("extractor.pattern");
      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d2.at1),",
          Collections.singleton("3,3,"));
    }
  }

  @Test
  public void testExtractorTimeRangeMatch() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // insert history data
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)"
                  + " values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)",
              "insert into root.db.d2 (time, at1)"
                  + " values (6000, 6), (7000, 7), (8000, 8), (9000, 9), (10000, 10)",
              "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("source.inclusion", "data");
      extractorAttributes.put("source.start-time", "1970-01-01T08:00:02+08:00");
      extractorAttributes.put("source.end-time", "1970-01-01T08:00:04+08:00");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),",
          Collections.singleton("3,"));

      // Insert realtime data that overlapped with time range
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d3 (time, at1)"
                  + " values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)",
              "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d3.at1),",
          Collections.singleton("3,3,"));

      // Insert realtime data that does not overlap with time range
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d4 (time, at1)"
                  + " values (6000, 6), (7000, 7), (8000, 8), (9000, 9), (10000, 10)",
              "flush"))) {
        return;
      }

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d3.at1),",
          Collections.singleton("3,3,"));
    }
  }

  @Test
  public void testSourceStartTimeAndEndTimeWorkingWithOrWithoutPattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)"
                  + " values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)",
              "insert into root.db.d2 (time, at1)"
                  + " values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)",
              "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.pattern", "root.db.d1");
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.start-time", "1970-01-01T08:00:02+08:00");
      // 1970-01-01T08:00:04+08:00
      extractorAttributes.put("source.end-time", "4000");

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

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),",
          Collections.singleton("3,"));

      extractorAttributes.remove("source.pattern");
      status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.at1),count(root.db.d2.at1),",
          Collections.singleton("3,3,"));
    }
  }

  @Test
  public void testHistoryLooseRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              // TsFile 1, extracted without parse
              "insert into root.db.d1 (time, at1, at2)" + " values (1000, 1, 2), (2000, 3, 4)",
              // TsFile 2, not extracted because pattern not overlapped
              "insert into root.db1.d1 (time, at1, at2)" + " values (1000, 1, 2), (2000, 3, 4)",
              "flush"))) {
        return;
      }

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              // TsFile 3, not extracted because time range not overlapped
              "insert into root.db.d1 (time, at1, at2)" + " values (3000, 1, 2), (4000, 3, 4)",
              "flush"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.path", "root.db.d1.at1");
      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.history.start-time", "1500");
      extractorAttributes.put("source.history.end-time", "2500");
      extractorAttributes.put("source.history.loose-range", "time, path");

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

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.** group by level=0",
          "count(root.*.*.*),",
          Collections.singleton("4,"));
    }
  }

  @Test
  public void testRealtimeLooseRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("source.path", "root.db.d1.at1");
    extractorAttributes.put("source.inclusion", "data.insert");
    extractorAttributes.put("source.realtime.loose-range", "time, path");
    extractorAttributes.put("source.start-time", "2000");
    extractorAttributes.put("source.end-time", "10000");
    extractorAttributes.put("source.realtime.mode", "batch");

    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1, at2)" + " values (1000, 1, 2), (3000, 3, 4)",
              "flush"))) {
        return;
      }

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db1.d1 (time, at1, at2)" + " values (1000, 1, 2), (3000, 3, 4)",
              "flush"))) {
        return;
      }

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, at1)" + " values (5000, 1), (16000, 3)",
              "insert into root.db.d1 (time, at1, at2)" + " values (5001, 1, 2), (6001, 3, 4)",
              "flush"))) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(at1) from root.db.d1 where time >= 2000 and time <= 10000",
          new HashMap<String, String>() {
            {
              put("count(root.db.d1.at1)", "4");
            }
          });
    }
  }

  private void assertTimeseriesCountOnReceiver(BaseEnv receiverEnv, int count) {
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton(count + ","));
  }

  private void assertPipeCount(int count) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(count, showPipeResult.size());
      // for (TShowPipeInfo showPipeInfo : showPipeResult) {
      //   System.out.println(showPipeInfo);
      // }
    }
  }
}
