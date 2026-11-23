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
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeTreeModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeAutoDropIT extends AbstractPipeTreeModelSharedClusterIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    // Auto-drop waits for completion reports, so collect them frequently in this IT.
    senderEnv.getConfig().getCommonConfig().setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(1);
    senderEnv.getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");
  }

  @Override
  protected void cleanEnvironment() {
    try {
      // The unmatched-region scenario creates a table-model pipe while running on the shared
      // tree clusters. Drop it before the shared tree cleanup so it cannot recreate the table data.
      dropTablePipes(senderEnv);
      dropTablePipes(receiverEnv);
      super.cleanEnvironment();
      deleteTableDatabases(senderEnv);
      deleteTableDatabases(receiverEnv);
    } catch (Exception e) {
      throw new AssertionError("Failed to reset table state in shared auto-drop clusters", e);
    }
  }

  private static void dropTablePipes(final BaseEnv env) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      final TShowPipeResp response =
          client.showPipe(
              new TShowPipeReq().setIsTableModel(true).setUserName(SessionConfig.DEFAULT_USER));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      for (final TShowPipeInfo pipe : response.getPipeInfoList()) {
        if (!pipe.getId().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
          final int statusCode =
              client
                  .dropPipeExtended(new TDropPipeReq(pipe.getId()).setIsTableModel(true))
                  .getCode();
          // Automatic drop can remove a pipe after the SHOW PIPE snapshot.
          if (statusCode != TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode()) {
            Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), statusCode);
          }
        }
      }
    }
  }

  private static void deleteTableDatabases(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final List<String> databases = new ArrayList<>();
      try (final ResultSet result = statement.executeQuery("show databases")) {
        while (result.next()) {
          final String database = result.getString(1);
          if (!"information_schema".equals(database)) {
            databases.add(database);
          }
        }
      }
      for (final String database : databases) {
        TestUtils.dropDatabaseWithRetry(statement, "\"" + database.replace("\"", "\"\"") + "\"");
      }
    }
  }

  // A query-mode pipe must transfer the historical row and remove itself after completion.
  @Test
  public void testAutoDropInHistoricalTransfer() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Collections.singletonList("insert into root.db.d1(time, s1) values (1, 1)"),
          null);
      awaitUntilFlush(senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "query");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

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

      // Creation already starts finite pipes; another start can race with their auto-drop.

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      // Fetch a fresh ConfigNode snapshot on every poll while pipes are being removed.
      await()
          .pollInSameThread()
          .pollDelay(0L, TimeUnit.MILLISECONDS)
          .pollInterval(100L, TimeUnit.MILLISECONDS)
          .atMost(600, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final TShowPipeResp response =
                    client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER));
                Assert.assertEquals(
                    TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
                final List<TShowPipeInfo> showPipeResult = response.getPipeInfoList();
                showPipeResult.removeIf(
                    i -> i.getId().startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX));
                Assert.assertTrue(showPipeResult.isEmpty());
              });
    }
  }

  // With a fresh sender that has no DataRegions, query/history pipes must disappear while a
  // realtime-only pipe stays alive. Reusing a cluster could leave regions from another scenario.
  @Test
  public void testAutoDropFinitePipesWithoutDataRegion() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put("sink", "iotdb-thrift-sink");
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", receiverDataNode.getIp());
    sinkAttributes.put("sink.port", Integer.toString(receiverDataNode.getPort()));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> querySourceAttributes = new HashMap<>();
      querySourceAttributes.put("source.mode", "query");
      querySourceAttributes.put("user", SessionConfig.DEFAULT_USER);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("query_pipe_without_data_region", sinkAttributes)
                      .setExtractorAttributes(querySourceAttributes))
              .getCode());

      final Map<String, String> historySourceAttributes = new HashMap<>();
      historySourceAttributes.put("source.realtime.enable", Boolean.FALSE.toString());
      historySourceAttributes.put("user", SessionConfig.DEFAULT_USER);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("history_pipe_without_data_region", sinkAttributes)
                      .setExtractorAttributes(historySourceAttributes))
              .getCode());

      final Map<String, String> realtimeSourceAttributes = new HashMap<>();
      realtimeSourceAttributes.put("source.history.enable", Boolean.FALSE.toString());
      realtimeSourceAttributes.put("user", SessionConfig.DEFAULT_USER);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("realtime_pipe_without_data_region", sinkAttributes)
                      .setExtractorAttributes(realtimeSourceAttributes))
              .getCode());

      // Pipes start automatically after creation. Explicitly starting finite pipes can race with
      // their auto-drop when they have no DataRegions to process.
      await()
          .pollInSameThread()
          .pollDelay(0L, TimeUnit.MILLISECONDS)
          .pollInterval(100L, TimeUnit.MILLISECONDS)
          .atMost(600, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final TShowPipeResp response =
                    client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER));
                Assert.assertEquals(
                    TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
                final List<TShowPipeInfo> pipeInfoList = response.getPipeInfoList();
                Assert.assertFalse(
                    pipeInfoList.stream()
                        .anyMatch(info -> info.getId().equals("query_pipe_without_data_region")));
                Assert.assertFalse(
                    pipeInfoList.stream()
                        .anyMatch(info -> info.getId().equals("history_pipe_without_data_region")));
                Assert.assertTrue(
                    pipeInfoList.stream()
                        .anyMatch(
                            info -> info.getId().equals("realtime_pipe_without_data_region")));
              });

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipe("realtime_pipe_without_data_region").getCode());
    }
  }

  // Tree and table query pipes must transfer their matching row and auto-drop despite the sender's
  // other, unmatched DataRegions in both models.
  @Test
  public void testAutoDropIgnoredUnmatchedDataRegions() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    TableModelUtils.createDataBaseAndTable(senderEnv, "t1", "table_db");
    TableModelUtils.insertData("table_db", "t1", 0, 1, senderEnv);

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.other",
            "insert into root.other.d1(time, s1) values (1, 1)",
            "create database root.db",
            "insert into root.db.d1(time, s1) values (1, 1)",
            "flush"),
        null);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      // Tree-model pipe: only listens to root.db. root.other and the table database are
      // user-visible DataRegions that must not block the historical snapshot pipe from being
      // auto-dropped.
      final Map<String, String> treeSourceAttributes = new HashMap<>();
      treeSourceAttributes.put("source.mode", "query");
      treeSourceAttributes.put("source.path", "root.db.**");
      treeSourceAttributes.put("source.capture.tree", "true");
      treeSourceAttributes.put("source.capture.table", "false");
      treeSourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p_tree", sinkAttributes)
                  .setExtractorAttributes(treeSourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Table-model pipe: only listens to table_db, which must not be blocked by the tree
      // DataRegions either.
      final Map<String, String> tableSourceAttributes = new HashMap<>();
      tableSourceAttributes.put("source.mode", "query");
      tableSourceAttributes.put("source.database-name", "table_db");
      tableSourceAttributes.put("source.table-name", "t1");
      tableSourceAttributes.put("source.capture.tree", "false");
      tableSourceAttributes.put("source.capture.table", "true");
      tableSourceAttributes.put("__system.sql-dialect", "table");
      tableSourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      status =
          client.createPipe(
              new TCreatePipeReq("p_table", sinkAttributes)
                  .setExtractorAttributes(tableSourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));
      TableModelUtils.assertCountData("table_db", "t1", 1, receiverEnv);

      await()
          .pollInSameThread()
          .pollDelay(0L, TimeUnit.MILLISECONDS)
          .pollInterval(100L, TimeUnit.MILLISECONDS)
          .atMost(600, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final TShowPipeResp response =
                    client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER));
                Assert.assertEquals(
                    TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
                final List<TShowPipeInfo> showPipeResult = response.getPipeInfoList();
                showPipeResult.removeIf(
                    i -> i.getId().startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX));
                Assert.assertTrue(
                    showPipeResult.stream()
                        .noneMatch(
                            i ->
                                Objects.equals(i.getId(), "p_tree")
                                    || Objects.equals(i.getId(), "p_table")));
              });
    }
  }

  // Of five historical rows, the query pipe must transfer only the three in [2000, 4000] and then
  // remove itself, preserving the inclusive start/end-time boundaries.
  @Test
  public void testAutoDropInHistoricalTransferWithTimeRange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Collections.singletonList(
              "insert into root.db.d1(time, s1) values (1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)"),
          null);
      awaitUntilFlush(senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.mode", "query");
      sourceAttributes.put("source.start-time", "1970-01-01T08:00:02+08:00");
      sourceAttributes.put("source.end-time", "1970-01-01T08:00:04+08:00");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

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

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("3,"));

      await()
          .pollInSameThread()
          .pollDelay(0L, TimeUnit.MILLISECONDS)
          .pollInterval(100L, TimeUnit.MILLISECONDS)
          .atMost(600, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final TShowPipeResp response =
                    client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER));
                Assert.assertEquals(
                    TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
                final List<TShowPipeInfo> showPipeResult = response.getPipeInfoList();
                showPipeResult.removeIf(
                    i -> i.getId().startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX));
                Assert.assertTrue(showPipeResult.isEmpty());
              });
    }
  }
}
