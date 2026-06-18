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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBShowReceiversLifecycleIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv.getConfig().getCommonConfig().setPipeAirGapReceiverEnabled(true);
    receiverEnv.getConfig().getCommonConfig().setPipeAirGapReceiverEnabled(true);
  }

  @Test
  public void testShowReceiversPipeIdsDisappearAfterDropPipe() throws Exception {
    final String database = "root.show_receivers_lifecycle";
    final String pipeName = "show_receivers_lifecycle_pipe";

    createThriftPipe(database, pipeName);

    assertShowReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceivers(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);

    TestUtils.executeNonQueries(
        senderEnv, Collections.singletonList("drop pipe " + pipeName), null);

    assertShowReceiversDoesNotContainPipe("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceiversDoesNotContainPipe(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);
  }

  @Test
  public void testShowReceiversPipeIdsDisappearAfterStopPipe() throws Exception {
    final String database = "root.show_receivers_lifecycle_stop";
    final String pipeName = "show_receivers_lifecycle_stop_pipe";

    createThriftPipe(database, pipeName);

    assertShowReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceivers(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);

    TestUtils.executeNonQueries(
        senderEnv, Collections.singletonList("stop pipe " + pipeName), null);

    assertShowReceiversDoesNotContainPipe("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceiversDoesNotContainPipe(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);
  }

  @Test
  public void testShowReceiversIncludesDataNodeAndConfigNodeReceivers() throws Exception {
    final String database = "root.show_receivers_config_node";
    final String pipeName = "show_receivers_config_node_pipe";

    createThriftPipe(database, pipeName, "all");

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create timeseries " + database + ".d1.s2 with datatype=BOOLEAN, encoding=PLAIN",
            "insert into " + database + ".d1(time, s1, s2) values (2, 2, true)",
            "flush"),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries " + database + ".**",
        "count(timeseries),",
        Collections.singleton("2,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(s1) from " + database + ".d1",
        "count(" + database + ".d1.s1),",
        Collections.singleton("2,"));

    assertShowReceiversContainDataNodeAndConfigNode(
        "show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceiversContainDataNodeAndConfigNode(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);
  }

  @Test
  public void testShowReceiversShowsAirGapProtocol() throws Exception {
    final String database = "root.show_receivers_air_gap";
    final String pipeName = "show_receivers_air_gap_pipe";

    createAirGapPipe(database, pipeName);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(s1) from " + database + ".d1",
        "count(" + database + ".d1.s1),",
        Collections.singleton("1,"));

    assertShowReceiversContainProtocol(
        "show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName, "air_gap");
    assertShowReceiversContainProtocol(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        pipeName,
        "air_gap");
  }

  private void createThriftPipe(final String database, final String pipeName) throws Exception {
    createThriftPipe(database, pipeName, "data.insert");
  }

  private void createThriftPipe(
      final String database, final String pipeName, final String sourceInclusion) throws Exception {
    createPipe(database, pipeName, sourceInclusion, "iotdb-thrift-sink", false);
  }

  private void createAirGapPipe(final String database, final String pipeName) throws Exception {
    createPipe(database, pipeName, "data.insert", "iotdb-air-gap-sink", true);
  }

  private void createPipe(
      final String database,
      final String pipeName,
      final String sourceInclusion,
      final String sinkName,
      final boolean useAirGapPort)
      throws Exception {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database " + database,
            "create timeseries " + database + ".d1.s1 with datatype=INT32, encoding=PLAIN",
            "insert into " + database + ".d1(time, s1) values (1, 1)"),
        null);
    awaitUntilFlush(senderEnv);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final Map<String, String> sourceAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();

    sourceAttributes.put("source.path", database + ".**");
    sourceAttributes.put("source.inclusion", sourceInclusion);
    sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

    sinkAttributes.put("sink", sinkName);
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", receiverDataNode.getIp());
    sinkAttributes.put(
        "sink.port",
        Integer.toString(
            useAirGapPort
                ? receiverDataNode.getPipeAirGapReceiverPort()
                : receiverDataNode.getPort()));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TSStatus status =
          client.createPipe(
              new TCreatePipeReq(pipeName, sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      status = client.startPipe(pipeName);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
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

  private boolean hasExpectedReceiver(
      final String sql, final String sqlDialect, final String pipeName) throws SQLException {
    try (final Connection connection =
            receiverEnv.getConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        if ("DataNode".equals(resultSet.getString(1))
            && "thrift".equals(resultSet.getString(3))
            && resultSet.getString(4) != null
            && !resultSet.getString(4).isEmpty()
            && resultSet.getString(5) != null
            && !resultSet.getString(5).isEmpty()
            && resultSet.getInt(6) >= 1
            && resultSet.getInt(7) >= 1
            && resultSet.getString(8).contains(pipeName + "@")
            && SessionConfig.DEFAULT_USER.equals(resultSet.getString(9))
            && resultSet.getString(10) != null
            && !resultSet.getString(10).isEmpty()
            && resultSet.getString(11) != null
            && resultSet.getString(12) != null) {
          return true;
        }
      }
      return false;
    }
  }

  private void assertShowReceiversDoesNotContainPipe(
      final String sql, final String sqlDialect, final String pipeName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(() -> Assert.assertFalse(containsPipe(sql, sqlDialect, pipeName)));
  }

  private boolean containsPipe(final String sql, final String sqlDialect, final String pipeName)
      throws SQLException {
    try (final Connection connection =
            receiverEnv.getConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final String pipeIds = resultSet.getString(8);
        if (pipeIds != null && pipeIds.contains(pipeName + "@")) {
          return true;
        }
      }
      return false;
    }
  }

  private void assertShowReceiversContainDataNodeAndConfigNode(
      final String sql, final String sqlDialect, final String pipeName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () -> Assert.assertTrue(hasDataNodeAndConfigNodeReceivers(sql, sqlDialect, pipeName)));
  }

  private boolean hasDataNodeAndConfigNodeReceivers(
      final String sql, final String sqlDialect, final String pipeName) throws SQLException {
    boolean hasDataNode = false;
    boolean hasConfigNode = false;
    try (final Connection connection =
            receiverEnv.getConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final String receiverNodeType = resultSet.getString(1);
        final String protocol = resultSet.getString(3);
        final String pipeIds = resultSet.getString(8);
        if (!"thrift".equals(protocol) || pipeIds == null || !pipeIds.contains(pipeName + "@")) {
          continue;
        }
        if ("DataNode".equals(receiverNodeType)) {
          hasDataNode = true;
        } else if ("ConfigNode".equals(receiverNodeType)) {
          hasConfigNode = true;
        }
      }
    }
    return hasDataNode && hasConfigNode;
  }

  private void assertShowReceiversContainProtocol(
      final String sql, final String sqlDialect, final String pipeName, final String protocol) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertTrue(
                    hasDataNodeReceiverWithProtocol(sql, sqlDialect, pipeName, protocol)));
  }

  private boolean hasDataNodeReceiverWithProtocol(
      final String sql, final String sqlDialect, final String pipeName, final String protocol)
      throws SQLException {
    try (final Connection connection =
            receiverEnv.getConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final String pipeIds = resultSet.getString(8);
        if ("DataNode".equals(resultSet.getString(1))
            && protocol.equals(resultSet.getString(3))
            && pipeIds != null
            && pipeIds.contains(pipeName + "@")) {
          return true;
        }
      }
    }
    return false;
  }
}
