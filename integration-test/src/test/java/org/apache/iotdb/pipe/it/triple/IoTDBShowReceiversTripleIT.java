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

package org.apache.iotdb.pipe.it.triple;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT3;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT3.class})
public class IoTDBShowReceiversTripleIT {

  private BaseEnv senderEnv1;
  private BaseEnv senderEnv2;
  private BaseEnv receiverEnv;

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(3);
    senderEnv1 = MultiEnvFactory.getEnv(0);
    senderEnv2 = MultiEnvFactory.getEnv(1);
    receiverEnv = MultiEnvFactory.getEnv(2);

    setupConfig(senderEnv1);
    setupConfig(senderEnv2);
    setupConfig(receiverEnv);

    senderEnv1.initClusterEnvironment(1, 1);
    senderEnv2.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 3);
  }

  private static void setupConfig(final BaseEnv env) {
    env.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setEnforceStrongPassword(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    env.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  @After
  public void tearDown() {
    senderEnv1.cleanClusterEnvironment();
    senderEnv2.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }

  @Test
  public void testShowReceiversAggregatesMultipleSendersOnMultipleDataNodes() throws Exception {
    Assert.assertTrue(receiverEnv.getDataNodeWrapperList().size() >= 2);
    final DataNodeWrapper receiverDataNode1 = receiverEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode2 = receiverEnv.getDataNodeWrapper(1);
    final int receiverDataNodeId1 = getDataNodeId(receiverDataNode1);
    final int receiverDataNodeId2 = getDataNodeId(receiverDataNode2);
    final String pipeName1 = "show_receivers_sender_a_pipe";
    final String pipeName2 = "show_receivers_sender_b_pipe";
    final String database1 = "root.show_receivers_sender_a";
    final String database2 = "root.show_receivers_sender_b";

    createThriftPipe(senderEnv1, receiverDataNode1, database1, pipeName1);
    createThriftPipe(senderEnv2, receiverDataNode2, database2, pipeName2);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(s1) from " + database1 + ".d1",
        "count(" + database1 + ".d1.s1),",
        Collections.singleton("1,"));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(s1) from " + database2 + ".d1",
        "count(" + database2 + ".d1.s1),",
        Collections.singleton("1,"));

    assertShowReceiversContainPipesOnDifferentDataNodes(
        "show receivers",
        BaseEnv.TREE_SQL_DIALECT,
        pipeName1,
        receiverDataNodeId1,
        pipeName2,
        receiverDataNodeId2);
    assertShowReceiversContainPipesOnDifferentDataNodes(
        "select * from information_schema.receivers",
        BaseEnv.TABLE_SQL_DIALECT,
        pipeName1,
        receiverDataNodeId1,
        pipeName2,
        receiverDataNodeId2);
  }

  private void createThriftPipe(
      final BaseEnv senderEnv,
      final DataNodeWrapper receiverDataNode,
      final String database,
      final String pipeName)
      throws Exception {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database " + database,
            "create timeseries " + database + ".d1.s1 with datatype=INT32, encoding=PLAIN",
            "insert into " + database + ".d1(time, s1) values (1, 1)",
            "flush"),
        null);

    final Map<String, String> sourceAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();

    sourceAttributes.put("source.path", database + ".**");
    sourceAttributes.put("source.inclusion", "data.insert");
    sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

    sinkAttributes.put("sink", "iotdb-thrift-sink");
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", receiverDataNode.getIp());
    sinkAttributes.put("sink.port", Integer.toString(receiverDataNode.getPort()));

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

  private void assertShowReceiversContainPipesOnDifferentDataNodes(
      final String sql,
      final String sqlDialect,
      final String firstPipeName,
      final int firstReceiverDataNodeId,
      final String secondPipeName,
      final int secondReceiverDataNodeId) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertTrue(
                    hasReceiversOnDifferentDataNodes(
                        sql,
                        sqlDialect,
                        firstPipeName,
                        firstReceiverDataNodeId,
                        secondPipeName,
                        secondReceiverDataNodeId)));
  }

  private boolean hasReceiversOnDifferentDataNodes(
      final String sql,
      final String sqlDialect,
      final String firstPipeName,
      final int firstReceiverDataNodeId,
      final String secondPipeName,
      final int secondReceiverDataNodeId)
      throws SQLException {
    boolean hasFirstPipe = false;
    boolean hasSecondPipe = false;
    final Set<Integer> receiverDataNodeIds = new HashSet<>();
    try (final Connection connection =
            receiverEnv.getConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final String pipeIds = getString(resultSet, "PipeIDs", "pipe_ids");
        if (!"DataNode".equals(getString(resultSet, "ReceiverNodeType", "receiver_node_type"))
            || !"thrift".equals(getString(resultSet, "Protocol", "protocol"))
            || pipeIds == null) {
          continue;
        }
        final int receiverDataNodeId = getInt(resultSet, "ReceiverNodeId", "receiver_node_id");
        if (receiverDataNodeId == firstReceiverDataNodeId
            && pipeIds.contains(firstPipeName + "@")
            && getInt(resultSet, "ConnectionCount", "connection_count") >= 1
            && getInt(resultSet, "PipeCount", "pipe_count") >= 1) {
          hasFirstPipe = true;
          receiverDataNodeIds.add(receiverDataNodeId);
        }
        if (receiverDataNodeId == secondReceiverDataNodeId
            && pipeIds.contains(secondPipeName + "@")
            && getInt(resultSet, "ConnectionCount", "connection_count") >= 1
            && getInt(resultSet, "PipeCount", "pipe_count") >= 1) {
          hasSecondPipe = true;
          receiverDataNodeIds.add(receiverDataNodeId);
        }
      }
    }
    return hasFirstPipe && hasSecondPipe && receiverDataNodeIds.size() >= 2;
  }

  private int getDataNodeId(final DataNodeWrapper targetDataNode) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
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
}
