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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeSinkCompressionIT extends AbstractPipeTableModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  @Override
  protected void setupConfig() {
    // One LZMA2 encoder needs about 100 MB in addition to the node and in-flight callbacks.
    // Give the sender headroom above the default 256 MB while bounding sink concurrency below.
    senderEnv.getConfig().getDataNodeJVMConfig().setMaxHeapSize(512);
    // Override to enable air-gap
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        // Compression cases write only a few rows per database. Keep one DataRegion per database
        // so each case exercises compression without creating dozens of tiny TsFiles.
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnv.getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:3:0");
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setPipeAirGapReceiverEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
  }

  // The async thrift sink with stream extraction, batching, and Snappy must preserve tree and
  // table data.
  @Test
  public void testCompression1() throws Exception {
    doTest("iotdb-thrift-connector", "stream", true, "snappy");
  }

  // The async thrift sink with batch extraction and Snappy/LZMA2 compression must preserve tree
  // and table data.
  @Test
  public void testCompression2() throws Exception {
    doTest("iotdb-thrift-connector", "batch", true, "snappy, lzma2");
  }

  // The sync thrift sink with stream extraction and repeated Snappy compression must preserve
  // tree and table data.
  @Test
  public void testCompression3() throws Exception {
    doTest("iotdb-thrift-sync-connector", "stream", false, "snappy, snappy");
  }

  // The sync thrift sink with batch extraction and gzip/Zstd compression must preserve tree and
  // table data.
  @Test
  public void testCompression4() throws Exception {
    doTest("iotdb-thrift-sync-connector", "batch", true, "gzip, zstd");
  }

  // The air-gap sink with stream extraction and LZMA2/LZ4 compression must preserve tree and
  // table data.
  @Test
  public void testCompression5() throws Exception {
    doTest("iotdb-air-gap-connector", "stream", false, "lzma2, lz4");
  }

  // The air-gap sink with batch extraction and LZMA2 compression must preserve tree and table
  // data.
  @Test
  public void testCompression6() throws Exception {
    doTest("iotdb-air-gap-connector", "batch", true, "lzma2");
  }

  private void doTest(
      String connectorType, String realtimeMode, boolean useBatchMode, String compressionTypes)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort =
        connectorType.contains("air-gap")
            ? receiverDataNode.getPipeAirGapReceiverPort()
            : receiverDataNode.getPort();

    final String database = database("test");
    TableModelUtils.createDataBaseAndTable(senderEnv, "test", database);
    TableModelUtils.insertData(database, "test", 0, 50, senderEnv, true);

    try (final RetryFlushHandler handleFailure = new RetryFlushHandler();
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList(
                  "insert into root.db.d1(time, s1) values (2010-01-01T10:00:00+08:00, 1)",
                  "insert into root.db.d1(time, s1) values (2010-01-02T10:00:00+08:00, 2)",
                  "flush")),
          null);
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor", "iotdb-extractor");
      extractorAttributes.put("extractor.realtime.mode", realtimeMode);
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("mode.double-living", "true");
      extractorAttributes.put("extractor.database-name", database);
      extractorAttributes.put("extractor.table-name", "test");
      // The default pattern format matches literal prefixes; a wildcard would exclude the data.
      extractorAttributes.put("extractor.pattern", databasePath() + ".");
      extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

      processorAttributes.put("processor", "do-nothing-processor");

      connectorAttributes.put("connector", connectorType);
      connectorAttributes.put("connector.batch.enable", useBatchMode ? "true" : "false");
      // LZMA2 allocates a large dictionary per task. Bound its peak memory for these functional
      // round trips while retaining every codec and batching combination.
      connectorAttributes.put("connector.parallel.tasks", "1");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.user", SessionConfig.DEFAULT_USER);
      connectorAttributes.put("connector.password", SessionConfig.DEFAULT_PASSWORD);
      connectorAttributes.put("connector.compressor", compressionTypes);

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.s1),"),
          Collections.singleton("2,"),
          handleFailure);

      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList(
                  "insert into root.db.d1(time, s1) values (3, 3)",
                  "insert into root.db.d1(time, s1) values (4, 4)",
                  "insert into root.db.d1(time, s1) values (5, 5)",
                  "insert into root.db.d1(time, s1) values (6, 6)",
                  "insert into root.db.d1(time, s1) values (7, 7)",
                  "insert into root.db.d1(time, s1) values (8, 8)",
                  "flush")),
          null);

      TableModelUtils.insertData(database, "test", 50, 100, senderEnv, true);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          isolateDatabasePaths("select count(*) from root.db.**"),
          isolateDatabasePaths("count(root.db.d1.s1),"),
          Collections.singleton("8,"),
          handleFailure);

      TableModelUtils.assertCountData(database, "test", 100, receiverEnv, handleFailure);
    }
  }

  // Accept Zstd levels -131072, 3, and 22, reject levels -131073 and 23 on the same connection,
  // retain only the three valid pipes, and replicate data through a valid pipe.
  @Test
  public void testZstdCompressorLevel() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final RetryFlushHandler handleFailure = new RetryFlushHandler();
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          isolateDatabasePaths(
              Arrays.asList(
                  "insert into root.db.d1(time, s1) values (1, 1)",
                  "insert into root.db.d1(time, s2) values (1, 1)",
                  "insert into root.db.d1(time, s3) values (1, 1)",
                  "insert into root.db.d1(time, s4) values (1, 1)",
                  "insert into root.db.d1(time, s5) values (1, 1)",
                  "flush")),
          null);

      // Reuse the SQL client for all five levels, including after a rejected CREATE PIPE.

      try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create pipe p1"
                    + " with extractor ('extractor.pattern'='"
                    + isolateDatabasePaths("root.db.d1.s1")
                    + "','extractor.database-name'='"
                    + database("test")
                    + "','table-name'='test1','capture.table'='true','capture.tree'='true','mode.double-living'='true')"
                    + " with connector ("
                    + "'connector.ip'='%s',"
                    + "'connector.port'='%s',"
                    + "'connector.compressor'='zstd, zstd',"
                    + "'connector.compressor.zstd.level'='3')",
                receiverIp,
                receiverPort));

        statement.execute(
            String.format(
                "create pipe p2"
                    + " with extractor ('extractor.pattern'='"
                    + isolateDatabasePaths("root.db.d1.s2")
                    + "','extractor.database-name'='"
                    + database("test")
                    + "','table-name'='test2','capture.table'='true','capture.tree'='true','mode.double-living'='true')"
                    + " with connector ("
                    + "'connector.ip'='%s',"
                    + "'connector.port'='%s',"
                    + "'connector.compressor'='zstd, zstd',"
                    + "'connector.compressor.zstd.level'='22',"
                    + "'connector.rate-limit-bytes-per-second'='2048.0')",
                receiverIp,
                receiverPort));

        statement.execute(
            String.format(
                "create pipe p3"
                    + " with extractor ('extractor.pattern'='"
                    + isolateDatabasePaths("root.db.d1.s3")
                    + "','extractor.database-name'='"
                    + database("test")
                    + "','table-name'='test3','capture.table'='true','capture.tree'='true','mode.double-living'='true')"
                    + " with connector ("
                    + "'connector.ip'='%s',"
                    + "'connector.port'='%s',"
                    + "'connector.compressor'='zstd, zstd',"
                    + "'connector.compressor.zstd.level'='-131072')",
                receiverIp,
                receiverPort));

        try {
          statement.execute(
              String.format(
                  "create pipe p4"
                      + " with extractor ('extractor.pattern'='"
                      + isolateDatabasePaths("root.db.d1.s4")
                      + "','extractor.database-name'='"
                      + database("test")
                      + "','table-name'='test4','capture.table'='true','capture.tree'='true','mode.double-living'='true')"
                      + " with connector ("
                      + "'connector.ip'='%s',"
                      + "'connector.port'='%s',"
                      + "'connector.compressor'='zstd, zstd',"
                      + "'connector.compressor.zstd.level'='-131073')",
                  receiverIp,
                  receiverPort));
          fail();
        } catch (SQLException e) {
          // Make sure the error message in IoTDBConnector.java is returned
          Assert.assertTrue(
              e.getMessage().contains("Zstd compression level should be in the range"));
        }

        try {
          statement.execute(
              String.format(
                  "create pipe p5"
                      + " with extractor ('extractor.pattern'='"
                      + isolateDatabasePaths("root.db.d1.s5")
                      + "','extractor.database-name'='"
                      + database("test")
                      + "','table-name'='test5','capture.table'='true','capture.tree'='true','mode.double-living'='true')"
                      + " with connector ("
                      + "'connector.ip'='%s',"
                      + "'connector.port'='%s',"
                      + "'connector.compressor'='zstd, zstd',"
                      + "'connector.compressor.zstd.level'='23')",
                  receiverIp,
                  receiverPort));
          fail();
        } catch (SQLException e) {
          // Make sure the error message in IoTDBConnector.java is returned
          Assert.assertTrue(
              e.getMessage().contains("Zstd compression level should be in the range"));
        }
      }

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(
                  new TShowPipeReq().setIsTableModel(true).setUserName(SessionConfig.DEFAULT_USER))
              .pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(
          3,
          showPipeResult.stream()
              .filter(info -> !info.id.startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX))
              .count());

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test"));
      TableModelUtils.insertData(database("test"), "test1", 0, 50, senderEnv, true);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", database("test"));
      TableModelUtils.insertData(database("test"), "test2", 0, 50, senderEnv, true);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test3", database("test"));
      TableModelUtils.insertData(database("test"), "test3", 0, 50, senderEnv, true);

      TableModelUtils.assertCountData(database("test"), "test1", 50, receiverEnv, handleFailure);
    }
  }

  private final class RetryFlushHandler implements Consumer<String>, AutoCloseable {
    // Index 0 owns the sender Session and index 1 owns the receiver Session. Open lazily so a
    // successful first assertion poll needs no retry clients.
    private final ISession[] sessions = new ISession[2];

    @Override
    public void accept(final String ignored) {
      // Assertions poll every 100 ms; reuse connections instead of opening multi-node JDBC
      // connections for both environments on each failed poll.
      for (int i = 0; i < sessions.length; i++) {
        try {
          if (sessions[i] == null) {
            sessions[i] = (i == 0 ? senderEnv : receiverEnv).getSessionConnection();
          }
          sessions[i].executeNonQueryStatement("flush");
        } catch (Exception e) {
          throw new AssertionError(e);
        }
      }
    }

    @Override
    public void close() throws Exception {
      try {
        if (sessions[0] != null) {
          sessions[0].close();
        }
      } finally {
        if (sessions[1] != null) {
          sessions[1].close();
        }
      }
    }
  }

  private String database(final String name) {
    return (name + "_" + testName.getMethodName()).toLowerCase(Locale.ROOT);
  }

  private String databasePath() {
    return "root.db_" + testName.getMethodName().toLowerCase(Locale.ROOT);
  }

  private String isolateDatabasePaths(final String value) {
    return value.replace("root.db", databasePath());
  }

  private List<String> isolateDatabasePaths(final List<String> values) {
    return values.stream().map(this::isolateDatabasePaths).toList();
  }
}
