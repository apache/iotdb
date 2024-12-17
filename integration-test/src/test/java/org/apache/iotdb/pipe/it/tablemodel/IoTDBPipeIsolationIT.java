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

package org.apache.iotdb.pipe.it.tablemodel;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopPipeReq;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2TableModel;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeIsolationIT extends AbstractPipeTableModelTestIT {

  @Test
  public void testWritePipeIsolation() throws Exception {
    final String treePipeName = "tree_a2b";
    final String tablePipeName = "table_a2b";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create tree pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("__system.sql-dialect", "tree");
      connectorAttributes.put("node-urls", receiverDataNode.getIpAndPortString());

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq(treePipeName, connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    // Create table pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("__system.sql-dialect", "table");
      connectorAttributes.put("node-urls", receiverDataNode.getIpAndPortString());

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq(tablePipeName, connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
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
    final String treePipeName = "tree_a2b";
    final String tablePipeName = "table_a2b";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create tree pipe by tree session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              treePipeName, receiverDataNode.getIpAndPortString()));
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(1, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show table pipe by table session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(0, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Create table pipe by table session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s with sink ('node-urls'='%s')",
              tablePipeName, receiverDataNode.getIpAndPortString()));
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(1, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show table pipe by table session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(1, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDoubleLivingIsolation() throws Exception {
    // TODO: consider 'mode.double-living'

    final String treePipeName = "tree_a2b";
    final String tablePipeName = "table_a2b";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create tree pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("__system.sql-dialect", "tree");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("capture.table", "true");
      connectorAttributes.put("node-urls", receiverDataNode.getIpAndPortString());

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq(treePipeName, connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    // Show tree pipe by tree session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(1, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show table pipe by table session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(1, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Create table pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("capture.tree", "true");
      extractorAttributes.put("capture.table", "true");
      connectorAttributes.put("node-urls", receiverDataNode.getIpAndPortString());

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq(tablePipeName, connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    // Show tree pipe by tree session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(2, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show table pipe by table session
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("show pipes");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(2, count);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Drop pipe
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropPipeExtended(new TDropPipeReq(treePipeName).setIsTableModel(true)).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .dropPipeExtended(new TDropPipeReq(tablePipeName).setIsTableModel(false))
              .getCode());
    }
  }
}
