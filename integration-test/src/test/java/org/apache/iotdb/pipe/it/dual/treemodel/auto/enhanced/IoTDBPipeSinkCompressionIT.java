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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeSinkCompressionIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    // Override to enable air-gap
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeAirGapReceiverEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @Test
  public void testCompression1() throws Exception {
    doTest("iotdb-thrift-sink", "stream", true, "snappy");
  }

  @Test
  public void testCompression2() throws Exception {
    doTest("iotdb-thrift-sink", "batch", true, "snappy, lzma2");
  }

  @Test
  public void testCompression3() throws Exception {
    doTest("iotdb-thrift-sync-sink", "stream", false, "snappy, snappy");
  }

  @Test
  public void testCompression4() throws Exception {
    doTest("iotdb-thrift-sync-sink", "batch", true, "gzip, zstd");
  }

  @Test
  public void testCompression5() throws Exception {
    doTest("iotdb-air-gap-sink", "stream", false, "lzma2, lz4");
  }

  @Test
  public void testCompression6() throws Exception {
    doTest("iotdb-air-gap-sink", "batch", true, "lzma2");
  }

  private void doTest(
      String sinkType, String realtimeMode, boolean useBatchMode, String compressionTypes)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort =
        sinkType.contains("air-gap")
            ? receiverDataNode.getPipeAirGapReceiverPort()
            : receiverDataNode.getPort();

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (2010-01-01T10:00:00+08:00, 1)",
              "insert into root.db.d1(time, s1) values (2010-01-02T10:00:00+08:00, 2)",
              "flush"),
          null);
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source", "iotdb-source");
      sourceAttributes.put("source.realtime.mode", realtimeMode);
      sourceAttributes.put("user", "root");

      processorAttributes.put("processor", "do-nothing-processor");

      sinkAttributes.put("sink", sinkType);
      sinkAttributes.put("sink.batch.enable", useBatchMode ? "true" : "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.user", "root");
      sinkAttributes.put("sink.password", "root");
      sinkAttributes.put("sink.compressor", compressionTypes);

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
          Collections.singleton("2,"),
          handleFailure);

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (now(), 3)",
              "insert into root.db.d1(time, s1) values (now(), 4)",
              "insert into root.db.d1(time, s1) values (now(), 5)",
              "insert into root.db.d1(time, s1) values (now(), 6)",
              "insert into root.db.d1(time, s1) values (now(), 7)",
              "insert into root.db.d1(time, s1) values (now(), 8)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("8,"),
          handleFailure);
    }
  }

  @Test
  public void testZstdCompressorLevel() throws Exception {
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
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (1, 1)",
              "insert into root.db.d1(time, s2) values (1, 1)",
              "insert into root.db.d1(time, s3) values (1, 1)",
              "insert into root.db.d1(time, s4) values (1, 1)",
              "insert into root.db.d1(time, s5) values (1, 1)",
              "flush"),
          null);

      // Create 5 pipes with different zstd compression levels, p4 and p5 should fail.

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p1"
                    + " with source ('source.pattern'='root.db.d1.s1')"
                    + " with sink ("
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.compressor'='zstd, zstd',"
                    + "'sink.compressor.zstd.level'='3')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p2"
                    + " with source ('source.pattern'='root.db.d1.s2')"
                    + " with sink ("
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.compressor'='zstd, zstd',"
                    + "'sink.compressor.zstd.level'='22')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p3"
                    + " with source ('source.pattern'='root.db.d1.s3')"
                    + " with sink ("
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.compressor'='zstd, zstd',"
                    + "'sink.compressor.zstd.level'='-131072')",
                receiverIp, receiverPort));
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p4"
                    + " with source ('source.pattern'='root.db.d1.s4')"
                    + " with sink ("
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.compressor'='zstd, zstd',"
                    + "'sink.compressor.zstd.level'='-131073')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException e) {
        // Make sure the error message in IoTDBConnector.java is returned
        Assert.assertTrue(e.getMessage().contains("Zstd compression level should be in the range"));
      }

      try (final Connection connection = senderEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p5"
                    + " with source ('source.pattern'='root.db.d1.s5')"
                    + " with sink ("
                    + "'sink.ip'='%s',"
                    + "'sink.port'='%s',"
                    + "'sink.compressor'='zstd, zstd',"
                    + "'sink.compressor.zstd.level'='23')",
                receiverIp, receiverPort));
        fail();
      } catch (SQLException e) {
        // Make sure the error message in IoTDBConnector.java is returned
        Assert.assertTrue(e.getMessage().contains("Zstd compression level should be in the range"));
      }

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(
          3,
          showPipeResult.stream()
              .filter(info -> !info.id.startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX))
              .count());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.db.**",
          "count(timeseries),",
          Collections.singleton("3,"),
          handleFailure);
    }
  }
}
