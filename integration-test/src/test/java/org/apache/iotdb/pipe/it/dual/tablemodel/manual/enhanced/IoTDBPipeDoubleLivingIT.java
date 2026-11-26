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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.PipeTestUtils.RetryFlushHandler;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeDoubleLivingIT extends AbstractPipeTableModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  private String databaseName() {
    return "double_" + testName.getMethodName().toLowerCase(Locale.ROOT);
  }

  private String treeDatabasePath() {
    return "root." + databaseName();
  }

  // Double-living accepts disabling either model but rejects forwarding that would create a cycle.
  @Test
  public void testDoubleLivingInvalidForwardingParameter() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.tree'='false',"
                  + "'double-living'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              "p1", receiverDataNode.getIpAndPortString()));
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.table'='false',"
                  + "'source.mode.double-living'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              "p2", receiverDataNode.getIpAndPortString()));

      try {
        statement.execute(
            String.format(
                "create pipe %s"
                    + " with source ("
                    + "'forwarding-pipe-requests'='true',"
                    + "'mode.double-living'='true')"
                    + " with sink ("
                    + "'node-urls'='%s')",
                "p3", receiverDataNode.getIpAndPortString()));
        fail();
      } catch (final SQLException ignored) {
        // Only rejection of the conflicting CREATE is expected; connection failures must fail.
      }
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(
                  new TShowPipeReq().setIsTableModel(true).setUserName(SessionConfig.DEFAULT_USER))
              .pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(2, showPipeResult.size());
    }
  }

  // Both models must merge disjoint history/realtime ranges from both clusters, then continue
  // replicating after both clusters restart. All 500 rows and both pipe directions remain checked.
  @Test
  public void testBasicDoubleLiving() throws Exception {
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    // Retain each JDBC session for writes and flushes until the explicit cluster restart.
    try (final Connection senderConnection = senderEnv.getConnection();
        final Connection receiverConnection = receiverEnv.getConnection();
        final Statement sender = senderConnection.createStatement();
        final Statement receiver = receiverConnection.createStatement();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(receiverEnv, senderEnv)) {
      insertTreeData(sender, 0, 100);
      TableModelUtils.createDataBaseAndTable(senderEnv, "test", databaseName());
      TableModelUtils.insertData(databaseName(), "test", 0, 100, senderEnv);
      sender.execute("flush");

      createDoubleLivingPipe(senderEnv, "p1", receiverDataNode);
      insertTreeData(sender, 100, 200);
      insertTreeData(receiver, 200, 300);
      TableModelUtils.insertData(databaseName(), "test", 100, 200, senderEnv);
      TableModelUtils.assertData(databaseName(), "test", 0, 200, receiverEnv, handleFailure);
      TableModelUtils.insertData(databaseName(), "test", 200, 300, receiverEnv);
      sender.execute("flush");

      createDoubleLivingPipe(receiverEnv, "p2", senderDataNode);
      insertTreeData(receiver, 300, 400);
      TableModelUtils.insertData(databaseName(), "test", 300, 400, receiverEnv);
      receiver.execute("flush");
      assertDoubleLivingData(400, handleFailure);
    }

    // Independent restarts can overlap. Propagate a restart failure so recovery is always verified.
    CompletableFuture.allOf(
            CompletableFuture.runAsync(() -> TestUtils.restartCluster(senderEnv)),
            CompletableFuture.runAsync(() -> TestUtils.restartCluster(receiverEnv)))
        .join();

    // The pre-restart handler is closed above. Recovery gets fresh retry Sessions on demand.
    try (final Connection connection = receiverEnv.getConnection();
        final Statement receiver = connection.createStatement();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(receiverEnv, senderEnv)) {
      insertTreeData(receiver, 400, 500);
      TableModelUtils.insertData(databaseName(), "test", 400, 500, receiverEnv);
      receiver.execute("flush");
      assertDoubleLivingData(500, handleFailure);
    }
  }

  private void createDoubleLivingPipe(
      final BaseEnv env, final String pipeName, final DataNodeWrapper target) throws SQLException {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'database-name'='%s',"
                  + "'table-name'='test',"
                  + "'path'='%s.d1.s1',"
                  + "'mode.double-living'='true')"
                  + " with sink ("
                  + "'batch.enable'='false',"
                  + "'node-urls'='%s')",
              pipeName, databaseName(), treeDatabasePath(), target.getIpAndPortString()));
    }
  }

  private void insertTreeData(final Statement statement, final int start, final int end)
      throws SQLException {
    // Ten rows per INSERT reduce RPCs to one tenth without changing data or phase boundaries.
    for (int batchStart = start; batchStart < end; batchStart += 10) {
      final StringBuilder sql =
          new StringBuilder("insert into " + treeDatabasePath() + ".d1(time, s1) values ");
      for (int i = batchStart; i < Math.min(batchStart + 10, end); ++i) {
        if (i > batchStart) {
          sql.append(',');
        }
        sql.append('(').append(i).append(",1)");
      }
      statement.execute(sql.toString());
    }
  }

  private void assertDoubleLivingData(final int end, final Consumer<String> handleFailure) {
    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < end; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    for (final BaseEnv env : new BaseEnv[] {senderEnv, receiverEnv}) {
      TestUtils.assertDataEventuallyOnEnv(
          env,
          "select * from " + treeDatabasePath() + ".**",
          "Time," + treeDatabasePath() + ".d1.s1,",
          expectedResSet);
      TableModelUtils.assertData(databaseName(), "test", 0, end, env, handleFailure);
    }
  }

  // Creating a double-living pipe exposes one pipe per dialect; dropping each via the opposite
  // dialect must leave exactly one pipe visible per dialect.
  @Test
  public void testDoubleLivingIsolation() throws Exception {
    final String treePipeName = "treePipe";
    final String tablePipeName = "tablePipe";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Retain a real SQL session per dialect so all visibility checks still exercise both parsers.
    try (final Connection treeConnection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Connection tableConnection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement treeStatement = treeConnection.createStatement();
        final Statement tableStatement = tableConnection.createStatement()) {
      // Create tree pipe
      treeStatement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'double-living'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              treePipeName, receiverDataNode.getIpAndPortString()));
      // Show tree pipe by tree session
      Assert.assertEquals(1, TableModelUtils.showPipesCount(treeStatement));

      // Show table pipe by table session
      Assert.assertEquals(1, TableModelUtils.showPipesCount(tableStatement));

      // Create table pipe
      tableStatement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'source.mode.double-living'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              tablePipeName, receiverDataNode.getIpAndPortString()));
      // Show tree pipe by tree session
      Assert.assertEquals(2, TableModelUtils.showPipesCount(treeStatement));

      // Show table pipe by table session
      Assert.assertEquals(2, TableModelUtils.showPipesCount(tableStatement));

      try (final SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
        // Drop pipe
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            client
                .dropPipeExtended(new TDropPipeReq(treePipeName).setIsTableModel(true))
                .getCode());
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            client
                .dropPipeExtended(new TDropPipeReq(tablePipeName).setIsTableModel(false))
                .getCode());
      }

      Assert.assertEquals(1, TableModelUtils.showPipesCount(treeStatement));
      Assert.assertEquals(1, TableModelUtils.showPipesCount(tableStatement));
    }
  }

  // A double-living pipe creates separate tree/table entries with the same name, distinct
  // creation times, and the correct dialect in each extractor configuration.
  @Test
  public void testDoubleLivingCreatesSameNamePipesWithSeparateDialects() throws Exception {
    final String pipeName = "double_living_same_name_pipe";
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ('extractor.double-living'='true')"
                  + " with sink ('node-urls'='%s')",
              pipeName, receiverDataNode.getIpAndPortString()));
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> treePipes = showPipes(client, false);
      final List<TShowPipeInfo> tablePipes = showPipes(client, true);

      Assert.assertEquals(1, treePipes.size());
      Assert.assertEquals(1, tablePipes.size());
      Assert.assertEquals(pipeName, treePipes.get(0).id);
      Assert.assertEquals(pipeName, tablePipes.get(0).id);
      Assert.assertNotEquals(treePipes.get(0).creationTime, tablePipes.get(0).creationTime);
      Assert.assertTrue(
          treePipes
              .get(0)
              .pipeExtractor
              .contains(
                  SystemConstant.SQL_DIALECT_KEY + "=" + SystemConstant.SQL_DIALECT_TREE_VALUE));
      Assert.assertTrue(
          tablePipes
              .get(0)
              .pipeExtractor
              .contains(
                  SystemConstant.SQL_DIALECT_KEY + "=" + SystemConstant.SQL_DIALECT_TABLE_VALUE));
    }
  }

  private List<TShowPipeInfo> showPipes(
      final SyncConfigNodeIServiceClient client, final boolean isTableModel) throws Exception {
    final List<TShowPipeInfo> showPipeResult =
        client.showPipe(
                new TShowPipeReq()
                    .setIsTableModel(isTableModel)
                    .setUserName(SessionConfig.DEFAULT_USER))
            .pipeInfoList;
    showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
    return showPipeResult;
  }
}
