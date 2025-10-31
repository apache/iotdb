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
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
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

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment(3, 3);
  }

  @Test
  public void testSourcePermission() {
    TestUtils.executeNonQuery(senderEnv, "create user `thulab` 'passwD@123456'", null);

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
      statement.execute(
          "alter pipe a2b modify source ('username'='thulab', 'password'='passwD@123456')");
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
    TestUtils.executeNonQuery(
        "test", BaseEnv.TABLE_SQL_DIALECT, senderEnv, "grant INSERT on any to user thulab");

    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");

    // Shall be transferred
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
    TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

    try {
      TableModelUtils.createDataBaseAndTable(receiverEnv, "test", "test");
    } catch (final Exception | Error ignore) {
      // Ignore because the db/table may be transferred because sender user may see these
    }

    // Exception, block here
    TableModelUtils.assertCountDataAlwaysOnEnv("test", "test", 0, receiverEnv);

    // Grant SELECT privilege
    TestUtils.executeNonQueries(
        "test",
        BaseEnv.TABLE_SQL_DIALECT,
        senderEnv,
        Arrays.asList("grant SELECT on any to user thulab", "start pipe a2b"),
        null);

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

    // test showing pipe
    // Create another pipe, user is root
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe a2c"
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

    // A user shall only see its own pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(
          1,
          client
              .showPipe(new TShowPipeReq().setIsTableModel(true).setUserName("thulab"))
              .pipeInfoList
              .size());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testReceiverPermission() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQuery(receiverEnv, "create user testUser 'passwD@123456'", null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      final String dbName = "test";
      final String tbName = "test";

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.capture.tree", "false");
      sourceAttributes.put("source.capture.table", "true");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.user", "testUser");
      sinkAttributes.put("sink.password", "passwD@123456");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.startPipeExtended(new TStartPipeReq("testPipe").setIsTableModel(true)).getCode());

      TableModelUtils.createDataBaseAndTable(senderEnv, tbName, dbName);

      // Write some data
      TableModelUtils.insertData(dbName, tbName, 0, 100, senderEnv);

      // Shall not be transferred
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("information_schema,INF,null,null,null,"),
          (String) null);

      TestUtils.executeNonQuery(
          "information_schema",
          BaseEnv.TABLE_SQL_DIALECT,
          receiverEnv,
          "grant insert,create on database test to user testUser",
          null);

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
      TableModelUtils.createDataBaseAndTable(senderEnv, tbName, dbName2);

      // Write some data
      TableModelUtils.insertData(dbName2, tbName, 0, 100, senderEnv);

      TestUtils.executeNonQuery(null, "table", senderEnv, "drop pipe testPipe", null);

      sourceAttributes.put("start-time", "100");
      sourceAttributes.put("database-name", dbName2);
      sinkAttributes.put("skipif", "no-privileges");
      final TSStatus create =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), create.getCode());

      // Shall not be transferred
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "count databases", "count,", Collections.singleton("2,"), (String) null);

      TestUtils.executeNonQuery(
          "information_schema",
          BaseEnv.TABLE_SQL_DIALECT,
          receiverEnv,
          "grant insert,create on database test2 to user testUser",
          null);

      TableModelUtils.createDataBaseAndTable(receiverEnv, tbName, dbName2);
      TableModelUtils.insertData(dbName2, tbName, 100, 200, senderEnv);

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
