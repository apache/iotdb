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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBShowReceiversIT extends AbstractPipeSingleIT {

  @Test
  public void testShowReceiversInTreeAndTableModel() {
    createWriteBackPipe("root.show_receivers", "show_receivers_pipe");

    assertShowReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT);
    assertShowReceivers("select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT);
  }

  @Test
  public void testInformationSchemaReceiversWithProtocolFilter() {
    createWriteBackPipe("root.show_receivers_filter", "show_receivers_filter_pipe");

    assertShowReceivers(
        "select receiver_node_type, receiver_node_id, protocol, sender_cluster_id, "
            + "sender_address, user_name, sender_ports, connection_count, pipe_count, pipe_ids, "
            + "last_handshake_time, last_transfer_time "
            + "from information_schema.receivers where protocol = 'writeback'",
        BaseEnv.TABLE_SQL_DIALECT,
        "show_receivers_filter_pipe");
  }

  @Test
  public void testInformationSchemaReceiversProjectedColumns() {
    final String pipeName = "show_receivers_project_pipe";
    createWriteBackPipe("root.show_receivers_project", pipeName);

    assertProjectedShowReceivers(pipeName);
  }

  @Test
  public void testShowReceiversAggregatesMultiplePipesFromSameSender() {
    final String pipeName1 = "show_receivers_aggregate_pipe_1";
    final String pipeName2 = "show_receivers_aggregate_pipe_2";
    createTwoWriteBackPipes("root.show_receivers_aggregate", pipeName1, pipeName2);

    assertAggregatedShowReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName1, pipeName2);
    assertAggregatedShowReceivers(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        pipeName1,
        pipeName2);
  }

  @Test
  public void testShowReceiversWithStoppedDataNode() throws Exception {
    Assert.assertTrue(env.getDataNodeWrapperList().size() >= 3);
    createWriteBackPipe("root.show_receivers_ha", "show_receivers_ha_pipe");

    assertShowReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, "show_receivers_ha_pipe");
    assertShowReceivers(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        "show_receivers_ha_pipe");

    final int stoppedDataNodeIndex = 0;
    final DataNodeWrapper stoppedDataNode = env.getDataNodeWrapper(stoppedDataNodeIndex);
    final int stoppedDataNodeId = getDataNodeId(stoppedDataNode);
    final DataNodeWrapper queryDataNode = env.getDataNodeWrapper(1);

    env.shutdownDataNode(stoppedDataNodeIndex);
    env.ensureNodeStatus(
        Collections.singletonList(stoppedDataNode), Collections.singletonList(NodeStatus.Unknown));

    assertShowReceiversWithoutDataNode(
        "show receivers", BaseEnv.TREE_SQL_DIALECT, queryDataNode, stoppedDataNodeId);
    assertShowReceiversWithoutDataNode(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        queryDataNode,
        stoppedDataNodeId);
  }

  @Test
  public void testShowReceiversPermissionInTreeAndTableModel() {
    final String password = "Passwd123456@";
    final String normalUser = "show_receiver_normal";
    final String systemUser = "show_receiver_system";
    final String pipeName = "show_receivers_auth_pipe";

    TestUtils.executeNonQueries(
        env,
        Arrays.asList(
            "create user " + normalUser + " '" + password + "'",
            "create user " + systemUser + " '" + password + "'",
            "grant system on root.** to user " + systemUser),
        null);
    createWriteBackPipe("root.show_receivers_auth", pipeName);

    assertShowReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceivers(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);

    assertUserCannotSeeReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, normalUser, password);
    assertUserCannotSeeReceivers(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        normalUser,
        password);

    assertShowReceiversAsUser(
        "show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName, systemUser, password);
    assertShowReceiversAsUser(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        pipeName,
        systemUser,
        password);
  }

  @Test
  public void testNormalUserCanSeeOwnReceiverInTreeAndTableModel() {
    final String password = "Passwd123456@";
    final String sinkUser = "show_receiver_sink_user";
    final String otherUser = "show_receiver_other_user";
    final String pipeName = "show_receivers_own_user_pipe";

    TestUtils.executeNonQueries(
        env,
        Arrays.asList(
            "create user " + sinkUser + " '" + password + "'",
            "create user " + otherUser + " '" + password + "'",
            "grant write on root.** to user " + sinkUser),
        null);
    createWriteBackPipeWithSinkUser("root.show_receivers_own_user", pipeName, sinkUser, password);

    assertShowReceiversAsUser(
        "show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName, sinkUser, password, sinkUser);
    assertShowReceiversAsUser(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        pipeName,
        sinkUser,
        password,
        sinkUser);

    assertUserCannotSeeReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, otherUser, password);
    assertUserCannotSeeReceivers(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        otherUser,
        password);
  }

  @Test
  public void testOldSenderRuntimeShowsUnknownPipeIdsAndDisappearsAfterClientClose()
      throws Exception {
    final DataNodeWrapper receiverDataNode = env.getDataNodeWrapper(0);
    final int receiverDataNodeId = getDataNodeId(receiverDataNode);

    final IoTDBSyncClient client = createPipeTransferClient(receiverDataNode);
    try {
      sendOldDataNodeHandshake(client);

      assertOldSenderReceiverRuntime(
          "show receivers", BaseEnv.TREE_SQL_DIALECT, receiverDataNodeId);
      assertOldSenderReceiverRuntime(
          "select * from information_schema.receivers",
          BaseEnv.TABLE_SQL_DIALECT,
          receiverDataNodeId);
    } finally {
      client.close();
    }

    assertNoThriftReceiverRuntimeOnDataNode(
        "show receivers", BaseEnv.TREE_SQL_DIALECT, receiverDataNodeId);
    assertNoThriftReceiverRuntimeOnDataNode(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        receiverDataNodeId);
  }

  @Test
  public void testReceiverRuntimeClearedAfterDataNodeRestartAndCanReconnect() throws Exception {
    Assert.assertTrue(env.getDataNodeWrapperList().size() >= 2);
    final int restartedDataNodeIndex = 0;
    final DataNodeWrapper restartedDataNode = env.getDataNodeWrapper(restartedDataNodeIndex);
    final int restartedDataNodeId = getDataNodeId(restartedDataNode);

    IoTDBSyncClient client = createPipeTransferClient(restartedDataNode);
    try {
      sendOldDataNodeHandshake(client);
      assertOldSenderReceiverRuntime(
          "show receivers", BaseEnv.TREE_SQL_DIALECT, restartedDataNodeId);

      env.shutdownDataNode(restartedDataNodeIndex);
    } finally {
      client.close();
    }

    env.ensureNodeStatus(
        Collections.singletonList(restartedDataNode),
        Collections.singletonList(NodeStatus.Unknown));
    assertNoThriftReceiverRuntimeOnDataNode(
        "show receivers", BaseEnv.TREE_SQL_DIALECT, restartedDataNodeId);

    env.startDataNode(restartedDataNodeIndex);
    env.ensureNodeStatus(
        Collections.singletonList(restartedDataNode),
        Collections.singletonList(NodeStatus.Running));

    client = createPipeTransferClient(restartedDataNode);
    try {
      sendOldDataNodeHandshake(client);
      assertOldSenderReceiverRuntime(
          "select * from information_schema.receivers",
          BaseEnv.TABLE_SQL_DIALECT,
          restartedDataNodeId);
    } finally {
      client.close();
    }
  }

  private void createWriteBackPipe(final String database, final String pipeName) {
    TestUtils.executeNonQueries(
        env,
        Arrays.asList(
            "create database " + database,
            "create timeseries " + database + ".d1.s1 with datatype=INT32, encoding=PLAIN",
            "create pipe "
                + pipeName
                + " with source ('pattern'='"
                + database
                + "') with sink ('sink'='write-back-sink')",
            "insert into " + database + ".d1(time, s1) values (1, 1)",
            "flush"),
        null);
  }

  private void createWriteBackPipeWithSinkUser(
      final String database,
      final String pipeName,
      final String sinkUser,
      final String sinkPassword) {
    TestUtils.executeNonQueries(
        env,
        Arrays.asList(
            "create database " + database,
            "create timeseries " + database + ".d1.s1 with datatype=INT32, encoding=PLAIN",
            "create pipe "
                + pipeName
                + " with source ('pattern'='"
                + database
                + "') with sink ('sink'='write-back-sink', 'user'='"
                + sinkUser
                + "', 'password'='"
                + sinkPassword
                + "')",
            "insert into " + database + ".d1(time, s1) values (1, 1)",
            "flush"),
        null);
  }

  private void createTwoWriteBackPipes(
      final String database, final String firstPipeName, final String secondPipeName) {
    TestUtils.executeNonQueries(
        env,
        Arrays.asList(
            "create database " + database,
            "create timeseries " + database + ".d1.s1 with datatype=INT32, encoding=PLAIN",
            "create pipe "
                + firstPipeName
                + " with source ('pattern'='"
                + database
                + "') with sink ('sink'='write-back-sink')",
            "create pipe "
                + secondPipeName
                + " with source ('pattern'='"
                + database
                + "') with sink ('sink'='write-back-sink')",
            "insert into " + database + ".d1(time, s1) values (1, 1)",
            "flush"),
        null);
  }

  private void assertShowReceivers(final String sql, final String sqlDialect) {
    assertShowReceivers(sql, sqlDialect, "show_receivers_pipe");
  }

  private void assertShowReceivers(
      final String sql, final String sqlDialect, final String pipeName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(() -> Assert.assertTrue(hasExpectedReceiver(sql, sqlDialect, pipeName)));
  }

  private void assertShowReceiversAsUser(
      final String sql,
      final String sqlDialect,
      final String pipeName,
      final String userName,
      final String password) {
    assertShowReceiversAsUser(sql, sqlDialect, pipeName, userName, password, "root");
  }

  private void assertShowReceiversAsUser(
      final String sql,
      final String sqlDialect,
      final String pipeName,
      final String userName,
      final String password,
      final String expectedReceiverUserName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertTrue(
                    hasExpectedReceiver(
                        sql, sqlDialect, pipeName, userName, password, expectedReceiverUserName)));
  }

  private void assertAggregatedShowReceivers(
      final String sql,
      final String sqlDialect,
      final String firstPipeName,
      final String secondPipeName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertTrue(
                    hasAggregatedReceiver(sql, sqlDialect, firstPipeName, secondPipeName)));
  }

  private void assertProjectedShowReceivers(final String pipeName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(() -> Assert.assertTrue(hasProjectedReceiver(pipeName)));
  }

  private void assertUserCannotSeeReceivers(
      final String sql, final String sqlDialect, final String userName, final String password) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () -> Assert.assertFalse(hasAnyReceiver(sql, sqlDialect, userName, password)));
  }

  private IoTDBSyncClient createPipeTransferClient(final DataNodeWrapper dataNodeWrapper)
      throws Exception {
    return new IoTDBSyncClient(
        new ThriftClientProperty.Builder().build(),
        dataNodeWrapper.getIp(),
        dataNodeWrapper.getPort(),
        false,
        null,
        null);
  }

  private void sendOldDataNodeHandshake(final IoTDBSyncClient client) throws Exception {
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
                CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
  }

  private void assertOldSenderReceiverRuntime(
      final String sql, final String sqlDialect, final int receiverDataNodeId) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertTrue(
                    hasOldSenderReceiverRuntime(sql, sqlDialect, receiverDataNodeId)));
  }

  private boolean hasOldSenderReceiverRuntime(
      final String sql, final String sqlDialect, final int receiverDataNodeId) throws SQLException {
    try (final Connection connection =
            getAvailableReceiverQueryConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        if ("DataNode".equals(getString(resultSet, "ReceiverNodeType", "receiver_node_type"))
            && receiverDataNodeId == getInt(resultSet, "ReceiverNodeId", "receiver_node_id")
            && "thrift".equals(getString(resultSet, "Protocol", "protocol"))
            && "Unknown".equals(getString(resultSet, "SenderClusterId", "sender_cluster_id"))
            && SessionConfig.DEFAULT_USER.equals(getString(resultSet, "UserName", "user_name"))
            && getString(resultSet, "SenderAddress", "sender_address") != null
            && getString(resultSet, "SenderPorts", "sender_ports") != null
            && getInt(resultSet, "ConnectionCount", "connection_count") >= 1
            && getInt(resultSet, "PipeCount", "pipe_count") == 0
            && "Unknown".equals(getString(resultSet, "PipeIDs", "pipe_ids"))
            && getString(resultSet, "LastHandshakeTime", "last_handshake_time") != null
            && getString(resultSet, "LastTransferTime", "last_transfer_time") != null) {
          return true;
        }
      }
      return false;
    }
  }

  private void assertNoThriftReceiverRuntimeOnDataNode(
      final String sql, final String sqlDialect, final int receiverDataNodeId) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertFalse(
                    hasThriftReceiverRuntimeOnDataNode(sql, sqlDialect, receiverDataNodeId)));
  }

  private boolean hasThriftReceiverRuntimeOnDataNode(
      final String sql, final String sqlDialect, final int receiverDataNodeId) throws SQLException {
    try (final Connection connection =
            getAvailableReceiverQueryConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        if ("DataNode".equals(getString(resultSet, "ReceiverNodeType", "receiver_node_type"))
            && receiverDataNodeId == getInt(resultSet, "ReceiverNodeId", "receiver_node_id")
            && "thrift".equals(getString(resultSet, "Protocol", "protocol"))) {
          return true;
        }
      }
      return false;
    }
  }

  private boolean hasExpectedReceiver(
      final String sql, final String sqlDialect, final String pipeName) throws SQLException {
    return hasExpectedReceiver(
        sql, sqlDialect, pipeName, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  private boolean hasExpectedReceiver(
      final String sql,
      final String sqlDialect,
      final String pipeName,
      final String userName,
      final String password)
      throws SQLException {
    return hasExpectedReceiver(sql, sqlDialect, pipeName, userName, password, "root");
  }

  private boolean hasExpectedReceiver(
      final String sql,
      final String sqlDialect,
      final String pipeName,
      final String userName,
      final String password,
      final String expectedReceiverUserName)
      throws SQLException {
    try (final Connection connection = getReceiverQueryConnection(userName, password, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final String senderClusterId = getString(resultSet, "SenderClusterId", "sender_cluster_id");
        final String senderAddress = getString(resultSet, "SenderAddress", "sender_address");
        final String userNameInRow = getString(resultSet, "UserName", "user_name");
        final String senderPorts = getString(resultSet, "SenderPorts", "sender_ports");
        final String pipeIds = getString(resultSet, "PipeIDs", "pipe_ids");
        if ("DataNode".equals(getString(resultSet, "ReceiverNodeType", "receiver_node_type"))
            && "writeback".equals(getString(resultSet, "Protocol", "protocol"))
            && senderClusterId != null
            && !senderClusterId.isEmpty()
            && senderAddress != null
            && !senderAddress.isEmpty()
            && expectedReceiverUserName.equals(userNameInRow)
            && senderPorts != null
            && !senderPorts.isEmpty()
            && getInt(resultSet, "ConnectionCount", "connection_count") >= 1
            && getInt(resultSet, "PipeCount", "pipe_count") >= 1
            && pipeIds != null
            && pipeIds.contains(pipeName + "@")
            && getString(resultSet, "LastHandshakeTime", "last_handshake_time") != null
            && getString(resultSet, "LastTransferTime", "last_transfer_time") != null) {
          return true;
        }
      }
      return false;
    }
  }

  private boolean hasAggregatedReceiver(
      final String sql,
      final String sqlDialect,
      final String firstPipeName,
      final String secondPipeName)
      throws SQLException {
    try (final Connection connection =
            getReceiverQueryConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final String pipeIds = getString(resultSet, "PipeIDs", "pipe_ids");
        if ("DataNode".equals(getString(resultSet, "ReceiverNodeType", "receiver_node_type"))
            && "writeback".equals(getString(resultSet, "Protocol", "protocol"))
            && getInt(resultSet, "ConnectionCount", "connection_count") >= 1
            && getInt(resultSet, "PipeCount", "pipe_count") >= 2
            && pipeIds != null
            && pipeIds.contains(firstPipeName + "@")
            && pipeIds.contains(secondPipeName + "@")) {
          return true;
        }
      }
      return false;
    }
  }

  private boolean hasProjectedReceiver(final String pipeName) throws SQLException {
    try (final Connection connection =
            getReceiverQueryConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet =
            statement.executeQuery(
                "select receiver_node_type, sender_cluster_id, sender_address, "
                    + "connection_count, pipe_ids "
                    + "from information_schema.receivers where protocol = 'writeback'")) {
      while (resultSet.next()) {
        if ("DataNode".equals(resultSet.getString(1))
            && resultSet.getString(2) != null
            && !resultSet.getString(2).isEmpty()
            && resultSet.getString(3) != null
            && !resultSet.getString(3).isEmpty()
            && resultSet.getInt(4) >= 1
            && resultSet.getString(5) != null
            && resultSet.getString(5).contains(pipeName + "@")) {
          return true;
        }
      }
      return false;
    }
  }

  private boolean hasAnyReceiver(
      final String sql, final String sqlDialect, final String userName, final String password)
      throws SQLException {
    try (final Connection connection = getReceiverQueryConnection(userName, password, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      return resultSet.next();
    }
  }

  private Connection getReceiverQueryConnection(
      final String userName, final String password, final String sqlDialect) throws SQLException {
    // Receiver runtime rows contain volatile timestamps. A single coordinator still executes the
    // product cluster-level plan, but avoids the test wrapper comparing independently collected
    // snapshots from multiple client entry points.
    return env.getConnection(env.getDataNodeWrapper(0), userName, password, sqlDialect);
  }

  private Connection getAvailableReceiverQueryConnection(
      final String userName, final String password, final String sqlDialect) throws SQLException {
    for (final DataNodeWrapper dataNodeWrapper : env.getDataNodeWrapperList()) {
      if (dataNodeWrapper.isAlive()) {
        return env.getConnection(dataNodeWrapper, userName, password, sqlDialect);
      }
    }
    return getReceiverQueryConnection(userName, password, sqlDialect);
  }

  private int getDataNodeId(final DataNodeWrapper targetDataNode) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      final TShowDataNodesResp response = client.showDataNodes();
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      for (final TDataNodeInfo dataNodeInfo : response.getDataNodesInfoList()) {
        if (targetDataNode.getIp().equals(dataNodeInfo.getRpcAddresss())
            && targetDataNode.getPort() == dataNodeInfo.getRpcPort()) {
          return dataNodeInfo.getDataNodeId();
        }
      }
    }
    throw new AssertionError("Cannot find DataNodeId for " + targetDataNode.getIpAndPortString());
  }

  private static String getString(
      final ResultSet resultSet, final String treeColumnName, final String tableColumnName)
      throws SQLException {
    try {
      return resultSet.getString(treeColumnName);
    } catch (final SQLException ignored) {
      return resultSet.getString(tableColumnName);
    }
  }

  private static int getInt(
      final ResultSet resultSet, final String treeColumnName, final String tableColumnName)
      throws SQLException {
    try {
      return resultSet.getInt(treeColumnName);
    } catch (final SQLException ignored) {
      return resultSet.getInt(tableColumnName);
    }
  }

  private void assertShowReceiversWithoutDataNode(
      final String sql,
      final String sqlDialect,
      final DataNodeWrapper queryDataNode,
      final int excludedDataNodeId) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertQueryResultDoesNotContainDataNode(
                    sql, sqlDialect, queryDataNode, excludedDataNodeId));
  }

  private void assertQueryResultDoesNotContainDataNode(
      final String sql,
      final String sqlDialect,
      final DataNodeWrapper queryDataNode,
      final int excludedDataNodeId)
      throws SQLException {
    try (final Connection connection =
            env.getConnection(
                queryDataNode,
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final int receiverDataNodeId = resultSet.getInt(2);
        if (!resultSet.wasNull()) {
          Assert.assertNotEquals(excludedDataNodeId, receiverDataNodeId);
        }
      }
    }
  }
}
