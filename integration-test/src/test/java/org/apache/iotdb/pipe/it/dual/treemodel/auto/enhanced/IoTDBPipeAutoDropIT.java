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
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;
import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeAutoDropIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

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

      try (final Connection connection = makeItCloseQuietly(senderEnv.getConnection());
          final Statement statement = makeItCloseQuietly(connection.createStatement()); ) {
        ResultSet result = statement.executeQuery("show pipes");
        await()
            .pollInSameThread()
            .pollDelay(1L, TimeUnit.SECONDS)
            .pollInterval(1L, TimeUnit.SECONDS)
            .atMost(600, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  try {
                    int pipeNum = 0;
                    while (result.next()) {
                      if (!result
                          .getString(ColumnHeaderConstant.ID)
                          .contains(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
                        pipeNum++;
                      }
                    }
                    Assert.assertEquals(0, pipeNum);
                  } catch (Exception e) {
                    Assert.fail();
                  }
                });
      }
    }
  }

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
      treeSourceAttributes.put("user", "root");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p_tree", sinkAttributes)
                  .setExtractorAttributes(treeSourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p_tree").getCode());

      // Table-model pipe: only listens to table_db, which must not be blocked by the tree
      // DataRegions either.
      final Map<String, String> tableSourceAttributes = new HashMap<>();
      tableSourceAttributes.put("source.mode", "query");
      tableSourceAttributes.put("source.database-name", "table_db");
      tableSourceAttributes.put("source.table-name", "t1");
      tableSourceAttributes.put("source.capture.tree", "false");
      tableSourceAttributes.put("source.capture.table", "true");
      tableSourceAttributes.put("__system.sql-dialect", "table");
      tableSourceAttributes.put("user", "root");

      status =
          client.createPipe(
              new TCreatePipeReq("p_table", sinkAttributes)
                  .setExtractorAttributes(tableSourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p_table").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));
      TableModelUtils.assertCountData("table_db", "t1", 1, receiverEnv);

      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(600, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final List<TShowPipeInfo> showPipeResult =
                    client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER))
                        .pipeInfoList;
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
          Collections.singleton("3,"));

      try (final Connection connection = makeItCloseQuietly(senderEnv.getConnection());
          final Statement statement = makeItCloseQuietly(connection.createStatement()); ) {
        ResultSet result = statement.executeQuery("show pipes");
        await()
            .pollInSameThread()
            .pollDelay(1L, TimeUnit.SECONDS)
            .pollInterval(1L, TimeUnit.SECONDS)
            .atMost(600, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  try {
                    int pipeNum = 0;
                    while (result.next()) {
                      if (!result
                          .getString(ColumnHeaderConstant.ID)
                          .contains(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
                        pipeNum++;
                      }
                    }
                    Assert.assertEquals(0, pipeNum);
                  } catch (Exception e) {
                    Assert.fail();
                  }
                });
      }
    }
  }
}
