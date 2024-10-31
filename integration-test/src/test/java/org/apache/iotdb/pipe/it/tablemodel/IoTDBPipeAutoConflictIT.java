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
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeAutoConflictIT extends AbstractPipeTableModelTestIT {
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @Test
  public void testDoubleLivingAutoConflict() throws Exception {
    // Double living is two clusters each with a pipe connecting to the other.
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    createDataBaseAndTable(senderEnv);
    createDataBaseAndTable(receiverEnv);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    insertData("test", "test1", 0, 100, senderEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("capture.table", "true");
      // Add this property to avoid making self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

      connectorAttributes.put("sink", "iotdb-thrift-sink");
      connectorAttributes.put("sink.batch.enable", "false");
      connectorAttributes.put("sink.ip", receiverIp);
      connectorAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }

    insertData("test", "test1", 100, 200, senderEnv);
    insertData("test", "test1", 200, 300, receiverEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("capture.table", "true");
      // Add this property to avoid to make self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderIp);
      connectorAttributes.put("connector.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    insertData("test", "test1", 300, 400, receiverEnv);

    final Set<String> expectedResSet = new HashSet<>();
    for (int i = 0; i < 400; ++i) {
      expectedResSet.add(
          String.format(
              "t%s,%s,%s.0,%s,%s,",
              i, i, i, i, RpcUtils.formatDatetime("default", "ms", i, ZoneOffset.UTC)));
    }

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "select id1,s3,s2,s1,time from test1",
        "id1,s3,s2,s1,time,",
        expectedResSet,
        60,
        "test");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select id1,s3,s2,s1,time from test1",
        "id1,s3,s2,s1,time,",
        expectedResSet,
        60,
        "test");

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      fail(e.getMessage());
    }

    insertData("test", "test1", 400, 500, senderEnv);
    insertData("test", "test1", 500, 600, receiverEnv);

    for (int i = 400; i < 600; ++i) {
      expectedResSet.add(
          String.format(
              "t%s,%s,%s.0,%s,%s,",
              i, i, i, i, RpcUtils.formatDatetime("default", "ms", i, ZoneOffset.UTC)));
    }
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select id1,s3,s2,s1,time from test1",
        "id1,s3,s2,s1,time,",
        expectedResSet,
        60,
        "test");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select id1,s3,s2,s1,time from test1",
        "id1,s3,s2,s1,time,",
        expectedResSet,
        60,
        "test");
  }

  @Test
  public void testDoubleLivingAutoConflictTemplate() throws Exception {
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      createDataBaseAndTable(senderEnv);
      createDataBaseAndTable(receiverEnv);
      insertData("test", "test", 0, 100, senderEnv);
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }

    insertData("test", "test", 100, 200, senderEnv);
    insertData("test", "test1", 200, 300, receiverEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("source.forwarding-pipe-requests", "false");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "true");
      connectorAttributes.put("connector.ip", senderIp);
      connectorAttributes.put("connector.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    insertData("test", "test1", 300, 400, senderEnv);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        TableModelUtils.getQuerySql("test"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(0, 200),
        "test");

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        TableModelUtils.getQuerySql("test1"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(200, 400),
        "test");
  }

  private void createDataBaseAndTable(BaseEnv baseEnv) {
    try (Connection connection = baseEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database if not exists test");
      statement.execute("use test");
      statement.execute(
          "CREATE TABLE test(id1 string id, s1 int64 measurement, s2 float measurement, s3 string measurement)");
      statement.execute(
          "CREATE TABLE test1(id1 string id, s1 int64 measurement, s2 float measurement, s3 string measurement)");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void insertData(
      String dataBaseName, String tableName, int start, int end, BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (id1, s3, s2, s1, time) values ('t%s','%s', %s.0, %s, %s)",
              tableName, i, i, i, i, i));
    }
    list.add("flush");
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list)) {
      fail();
    }
  }
}
