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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopPipeReq;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeIsolationIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testWritePipeIsolation() throws Exception {
    final String treePipeName = "treePipe";
    final String tablePipeName = "tablePipe";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create tree pipe
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              treePipeName, receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create table pipe
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              tablePipeName, receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Start pipe
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client
              .startPipeExtended(new TStartPipeReq(treePipeName).setIsTableModel(true))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client
              .startPipeExtended(new TStartPipeReq(tablePipeName).setIsTableModel(false))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .startPipeExtended(new TStartPipeReq(treePipeName).setIsTableModel(false))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .startPipeExtended(new TStartPipeReq(tablePipeName).setIsTableModel(true))
              .getCode());

      // Stop pipe
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client.stopPipeExtended(new TStopPipeReq(treePipeName).setIsTableModel(true)).getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client
              .stopPipeExtended(new TStopPipeReq(tablePipeName).setIsTableModel(false))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.stopPipeExtended(new TStopPipeReq(treePipeName).setIsTableModel(false)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.stopPipeExtended(new TStopPipeReq(tablePipeName).setIsTableModel(true)).getCode());

      // Alter pipe
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client
              .alterPipe(
                  new TAlterPipeReq(
                          treePipeName,
                          Collections.emptyMap(),
                          Collections.emptyMap(),
                          false,
                          false)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setIsReplaceAllExtractorAttributes(false)
                      .setIsTableModel(true))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client
              .alterPipe(
                  new TAlterPipeReq(
                          tablePipeName,
                          Collections.emptyMap(),
                          Collections.emptyMap(),
                          false,
                          false)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setIsReplaceAllExtractorAttributes(false)
                      .setIsTableModel(false))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .alterPipe(
                  new TAlterPipeReq(
                          treePipeName,
                          Collections.emptyMap(),
                          Collections.emptyMap(),
                          false,
                          false)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setIsReplaceAllExtractorAttributes(false)
                      .setIsTableModel(false))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .alterPipe(
                  new TAlterPipeReq(
                          tablePipeName,
                          Collections.emptyMap(),
                          Collections.emptyMap(),
                          false,
                          false)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setIsReplaceAllExtractorAttributes(false)
                      .setIsTableModel(true))
              .getCode());

      // Drop pipe
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq(treePipeName).setIsTableModel(true)).getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
          client
              .dropPipeExtended(new TDropPipeReq(tablePipeName).setIsTableModel(false))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq(treePipeName).setIsTableModel(false)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq(tablePipeName).setIsTableModel(true)).getCode());
    }
  }

  @Test
  public void testReadPipeIsolation() {
    final String treePipeName = "treePipe";
    final String tablePipeName = "tablePipe";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // 1. Create tree pipe by tree session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              treePipeName, receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(0, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    // 2. Create table pipe by table session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              tablePipeName, receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));
  }

  @Test
  public void testCaptureTreeAndTableIgnoredByDialectIsolation() throws Exception {
    final String treePipeName = "tree_a2b";
    final String tablePipeName = "table_a2b";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // 1. Create tree pipe by tree session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              treePipeName, receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(0, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    // 2. Create table pipe by table session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              tablePipeName, receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    // 3. Drop pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq(treePipeName).setIsTableModel(false)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq(tablePipeName).setIsTableModel(true)).getCode());
    }
  }

  @Test
  public void testSameNameTreeOnlyAndTableOnlyPipeIsolation() throws Exception {
    final String pipeName = "same_name_pipe";
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              pipeName, receiverDataNode.getIpAndPortString()));
    }

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              pipeName, receiverDataNode.getIpAndPortString()));
    }

    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("stop pipe " + pipeName);
    }

    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("drop pipe " + pipeName);
    }

    Assert.assertEquals(0, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("start pipe " + pipeName);
      statement.execute("drop pipe " + pipeName);
    }

    Assert.assertEquals(0, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));
    Assert.assertEquals(0, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));
  }

  @Test
  public void testSameNamePipeWithCaptureAttributesStillIsolated() throws Exception {
    final String pipeName = "same_name_conflict_pipe";
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              pipeName, receiverDataNode.getIpAndPortString()));
    }

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ('capture.tree'='true','capture.table'='true')"
                  + " with sink ('node-urls'='%s')",
              pipeName, receiverDataNode.getIpAndPortString()));
    }

    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));
  }

  @Test
  public void testCaptureAttributesAreIgnoredByDialect() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // 1. Create tree pipe with capture attributes pointing to table data
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.tree'='false',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              "p1", receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(0, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    // 2. Create table pipe with capture attributes pointing to tree data
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.tree'='true',"
                  + "'capture.table'='false')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              "p2", receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    // 3. Create pipe with capture.tree and capture.table set to false
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.tree'='false',"
                  + "'capture.table'='false')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              "p3", receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(2, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));
  }

  @Test
  public void testDirectRpcCreationDialectCompatibility() throws Exception {
    final String pipeName = "rpc_same_name_pipe";
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put("sink", "iotdb-thrift-sink");
    sinkAttributes.put("sink.ip", receiverDataNode.getIp());
    sinkAttributes.put("sink.port", String.valueOf(receiverDataNode.getPort()));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.createPipe(new TCreatePipeReq(pipeName, sinkAttributes)).getCode());

      Assert.assertEquals(1, showPipes(client, false).size());
      Assert.assertEquals(0, showPipes(client, true).size());
      Assert.assertTrue(
          showPipes(client, false)
              .get(0)
              .pipeExtractor
              .contains(
                  SystemConstant.SQL_DIALECT_KEY + "=" + SystemConstant.SQL_DIALECT_TREE_VALUE));

      final Map<String, String> tableSourceAttributes = new HashMap<>();
      tableSourceAttributes.put(
          SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq(pipeName, sinkAttributes)
                      .setExtractorAttributes(tableSourceAttributes))
              .getCode());

      Assert.assertEquals(1, showPipes(client, false).size());
      Assert.assertEquals(1, showPipes(client, true).size());
      Assert.assertTrue(
          showPipes(client, true)
              .get(0)
              .pipeExtractor
              .contains(
                  SystemConstant.SQL_DIALECT_KEY + "=" + SystemConstant.SQL_DIALECT_TABLE_VALUE));

      Assert.assertNotEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.createPipe(new TCreatePipeReq(pipeName, sinkAttributes)).getCode());
    }
  }

  @Test
  public void testLegacyLifecycleRpcPrefersTreePipeThenTablePipe() throws Exception {
    final String pipeName = "legacy_same_name_pipe";
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              pipeName, receiverDataNode.getIpAndPortString()));
    }
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              pipeName, receiverDataNode.getIpAndPortString()));
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(1, showPipes(client, false).size());
      Assert.assertEquals(1, showPipes(client, true).size());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe(pipeName).getCode());
      Assert.assertEquals(0, showPipes(client, false).size());
      Assert.assertEquals(1, showPipes(client, true).size());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe(pipeName).getCode());
      Assert.assertEquals(0, showPipes(client, false).size());
      Assert.assertEquals(0, showPipes(client, true).size());
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
