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
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeDoubleLivingIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testDoubleLivingInvalidParameter() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.tree'='false',"
                  + "'mode.double-living'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              "p1", receiverDataNode.getIpAndPortString()));
      fail();
    } catch (final SQLException ignored) {
    }

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'capture.table'='false',"
                  + "'mode.double-living'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              "p2", receiverDataNode.getIpAndPortString()));
      fail();
    } catch (final SQLException ignored) {
    }

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
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
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(
                  new TShowPipeReq().setIsTableModel(true).setUserName(SessionConfig.DEFAULT_USER))
              .pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(0, showPipeResult.size());
    }
  }

  // combination of
  // org.apache.iotdb.pipe.it.tablemodel.autocreate.IoTDBPipeLifeCycleIT.testDoubleLiving and
  // org.apache.iotdb.pipe.it.autocreate.IoTDBPipeLifeCycleIT.testDoubleLiving
  @Test
  public void testBasicDoubleLiving() {

    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
        };

    // insertion on sender
    try (Connection conn = senderEnv.getConnection()) {
      for (int i = 0; i < 100; ++i) {
        TestUtils.executeNonQuery(
            senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i), conn);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

    TestUtils.executeNonQuery(senderEnv, "flush", null);

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'database-name'='test',"
                  + "'table-name'='test',"
                  + "'path'='root.db.d1.s1',"
                  + "'mode.double-living'='true')"
                  + " with sink ("
                  + "'batch.enable'='false',"
                  + "'node-urls'='%s')",
              "p1", receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (Connection conn = senderEnv.getConnection()) {
      // insertion on sender
      for (int i = 100; i < 200; ++i) {
        TestUtils.executeNonQuery(
            senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i), conn);
      }
      for (int i = 200; i < 300; ++i) {
        TestUtils.executeNonQuery(
            receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i), conn);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

    TableModelUtils.insertData("test", "test", 200, 300, receiverEnv);

    TestUtils.executeNonQuery(senderEnv, "flush", null);

    try (final Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'database-name'='test',"
                  + "'table-name'='test',"
                  + "'path'='root.db.d1.s1',"
                  + "'mode.double-living'='true')"
                  + " with sink ("
                  + "'batch.enable'='false',"
                  + "'node-urls'='%s')",
              "p2", senderDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (Connection conn = receiverEnv.getConnection()) {
      // insertion on receiver
      for (int i = 300; i < 400; ++i) {
        TestUtils.executeNonQuery(
            receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i), conn);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    TableModelUtils.insertData("test", "test", 300, 400, receiverEnv);

    TestUtils.executeNonQuery(receiverEnv, "flush", null);

    // check result
    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TableModelUtils.assertData("test", "test", 0, 400, senderEnv, handleFailure);
    TableModelUtils.assertData("test", "test", 0, 400, receiverEnv, handleFailure);

    // restart cluster
    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    try (Connection conn = receiverEnv.getConnection()) {
      // insertion on receiver
      for (int i = 400; i < 500; ++i) {
        TestUtils.executeNonQuery(
            receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i), conn);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    TableModelUtils.insertData("test", "test", 400, 500, receiverEnv);

    TestUtils.executeNonQuery(receiverEnv, "flush", null);

    // check result
    for (int i = 400; i < 500; ++i) {
      expectedResSet.add(i + ",1.0,");
    }
    TestUtils.assertDataEventuallyOnEnv(
        senderEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "select * from root.db.**", "Time,root.db.d1.s1,", expectedResSet);
    TableModelUtils.assertData("test", "test", 0, 500, senderEnv, handleFailure);
    TableModelUtils.assertData("test", "test", 0, 500, receiverEnv, handleFailure);
  }

  @Test
  public void testDoubleLivingIsolation() throws Exception {
    final String treePipeName = "treePipe";
    final String tablePipeName = "tablePipe";

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create tree pipe
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'mode.double-living'='true')"
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
    Assert.assertEquals(1, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

    // Create table pipe
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe %s"
                  + " with source ("
                  + "'mode.double-living'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              tablePipeName, receiverDataNode.getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Show tree pipe by tree session
    Assert.assertEquals(2, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TREE_SQL_DIALECT));

    // Show table pipe by table session
    Assert.assertEquals(2, TableModelUtils.showPipesCount(senderEnv, BaseEnv.TABLE_SQL_DIALECT));

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
