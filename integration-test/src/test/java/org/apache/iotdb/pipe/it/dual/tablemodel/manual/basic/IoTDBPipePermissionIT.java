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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipePermissionIT extends AbstractPipeTableModelDualManualIT {
  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment(3, 3);
  }

  @Test
  public void testSourcePermission() {
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "create user `thulab` 'passwd'")) {
      return;
    }

    // Shall fail if username is specified without password
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'user'='thulab'"
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
      fail("When the 'user' or 'username' is specified, password must be specified too.");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Shall fail if password is wrong
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'user'='thulab'"
                  + "'password'='hack'"
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
      fail("Shall fail if password is wrong.");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Use current session, user is root
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2b"
                  + " with source ("
                  + "'inclusion'='all',"
                  + "'capture.tree'='true',"
                  + "'capture.table'='true')"
                  + " with sink ("
                  + "'node-urls'='%s')",
              receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()));
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    // Alter to another user, shall fail because of lack of password
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('username'='thulab')");
      fail("Alter pipe shall fail if only user is specified");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Successfully alter
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('username'='thulab', 'password'='passwd')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Alter pipe shall not fail if user and password are specified");
    }

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");

    // Shall not be transferred
    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv,
        "count databases",
        "count,",
        Collections.singleton("1,"),
        "information_schema");

    // Grant some privilege
    if (!TestUtils.tryExecuteNonQueryWithRetry(
        "test", BaseEnv.TABLE_SQL_DIALECT, senderEnv, "grant INSERT on any to user thulab")) {
      return;
    }

    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");

    // Shall not be transferred
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show tables from test1",
        "TableName,TTL(ms),",
        Collections.singleton("test1,INF,"),
        "information_schema");

    // Alter pipe, throw exception if no privileges
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ('skipif'='')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Write some data
    if (!TableModelUtils.insertData("test", "test", 0, 100, senderEnv)) {
      return;
    }

    TableModelUtils.createDataBaseAndTable(receiverEnv, "test", "test");

    // Exception, block here
    TableModelUtils.assertCountDataAlwaysOnEnv("test", "test", 0, receiverEnv);

    // Grant SELECT privilege
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        "test",
        BaseEnv.TABLE_SQL_DIALECT,
        senderEnv,
        Arrays.asList("grant SELECT on any to user thulab", "start pipe a2b"))) {
      return;
    }

    // Will finally pass
    TableModelUtils.assertCountData(
        "test",
        "test",
        100,
        receiverEnv,
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        });
  }

  @Test
  public void testReceiverPermission() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "create user testUser 'password'")) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      final String dbName = "test";
      final String tbName = "test";

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.capture.tree", "false");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.user", "testUser");
      connectorAttributes.put("connector.password", "password");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.startPipeExtended(new TStartPipeReq("testPipe").setIsTableModel(true)).getCode());

      TableModelUtils.createDataBaseAndTable(senderEnv, tbName, dbName);

      // Write some data
      if (!TableModelUtils.insertData(dbName, tbName, 0, 100, senderEnv)) {
        return;
      }

      // Shall not be transferred
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("information_schema,INF,null,null,null,"),
          (String) null);

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          "information_schema",
          BaseEnv.TABLE_SQL_DIALECT,
          receiverEnv,
          "grant insert,create on database test to user testUser")) {
        return;
      }

      // Will finally pass
      TableModelUtils.assertCountData(
          dbName,
          tbName,
          100,
          receiverEnv,
          o -> {
            TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
            TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          });

      // Alter pipe, skip if no privileges
      try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        statement.execute("alter pipe testPipe modify sink ('skipif'='no-privileges')");
      } catch (final SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      final String dbName2 = "test2";

      // Write some data
      if (!TableModelUtils.insertData(dbName2, tbName, 0, 100, senderEnv)) {
        return;
      }

      // Shall not be transferred
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("2,"), (String) null);

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          "information_schema",
          BaseEnv.TABLE_SQL_DIALECT,
          receiverEnv,
          "grant insert,create on database test2 to user testUser")) {
        return;
      }

      if (!TableModelUtils.insertData(dbName2, tbName, 100, 200, senderEnv)) {
        return;
      }

      // Will finally pass
      TableModelUtils.assertCountData(
          dbName2,
          tbName,
          100,
          receiverEnv,
          o -> {
            TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
            TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          });
    }
  }
}
