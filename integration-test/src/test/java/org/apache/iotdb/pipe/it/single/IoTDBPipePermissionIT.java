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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipePermissionIT extends AbstractPipeSingleIT {

  private static BaseEnv sharedEnv;

  @ClassRule
  public static final ExternalResource CLUSTER =
      new ExternalResource() {
        @Override
        protected void after() {
          try {
            if (sharedEnv != null) {
              sharedEnv.cleanClusterEnvironment();
            }
          } finally {
            sharedEnv = null;
          }
        }
      };

  @Before
  public void setUp() throws Exception {
    if (sharedEnv == null) {
      try {
        MultiEnvFactory.createEnv(1);
        env = MultiEnvFactory.getEnv(0);
        env.getConfig()
            .getCommonConfig()
            .setAutoCreateSchemaEnabled(true)
            .setDatanodeMemoryProportion("3:3:1:1:1:0")
            .setPipeMemoryManagementEnabled(false)
            .setDataReplicationFactor(1)
            .setSchemaReplicationFactor(1)
            .setIsPipeEnableMemoryCheck(false)
            .setPipeAutoSplitFullEnabled(false);
        // 1C1D to directly show the remaining count
        env.initClusterEnvironment(1, 1);
        sharedEnv = env;
      } catch (Exception | Error e) {
        if (env != null) {
          env.cleanClusterEnvironment();
        }
        throw e;
      }
    }
    env = sharedEnv;
  }

  @Override
  protected void cleanEnvironment() {
    if (sharedEnv == null) {
      return;
    }
    try {
      dropPipes(sharedEnv);
      resetCatalog(sharedEnv);
    } catch (Exception e) {
      throw new AssertionError("Failed to reset shared permission test cluster", e);
    }
  }

  private static void dropPipes(final BaseEnv env) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      for (final boolean tableModel : new boolean[] {false, true}) {
        final TShowPipeResp response =
            client.showPipe(
                new TShowPipeReq()
                    .setIsTableModel(tableModel)
                    .setUserName(SessionConfig.DEFAULT_USER));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
        for (final TShowPipeInfo pipe : response.getPipeInfoList()) {
          if (!pipe.getId().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
            Assert.assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                client
                    .dropPipeExtended(new TDropPipeReq(pipe.getId()).setIsTableModel(tableModel))
                    .getCode());
          }
        }
      }
    }
  }

  private static void resetCatalog(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final List<String> databases = new ArrayList<>();
      try (final ResultSet result = statement.executeQuery("show databases")) {
        while (result.next()) {
          final String database = result.getString(1);
          if (!"information_schema".equals(database)) {
            databases.add(database);
          }
        }
      }
      for (final String database : databases) {
        TestUtils.dropDatabaseWithRetry(statement, "\"" + database.replace("\"", "\"\"") + "\"");
      }
    }

    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      final List<String> databases = new ArrayList<>();
      try (final ResultSet result = statement.executeQuery("show databases")) {
        while (result.next()) {
          databases.add(result.getString(1));
        }
      }
      for (final String database : databases) {
        statement.execute("delete database " + database);
      }

      for (final String type : new String[] {"user", "role"}) {
        final List<String> names = new ArrayList<>();
        try (final ResultSet result = statement.executeQuery("list " + type)) {
          while (result.next()) {
            final String name = result.getString("user".equals(type) ? "User" : "Role");
            if (!"user".equals(type) || !SessionConfig.DEFAULT_USER.equals(name)) {
              names.add(name);
            }
          }
        }
        for (final String name : names) {
          statement.execute("drop " + type + " `" + name.replace("`", "``") + "`");
        }
      }
    }
  }

  @Test
  public void testSinkPermission() {
    TestUtils.executeNonQuery(env, "create user `thulab` 'StrngPsWd@623451'", null);

    // Shall fail if username is specified without password
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create pipe a2b with source ('capture.tree'='true') with sink ('user'='thulab', 'sink'='write-back-sink')");
      fail("When the 'user' or 'username' is specified, password must be specified too.");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Shall fail if password is wrong
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create pipe a2b ('user'='thulab', 'password'='hack', 'sink'='write-back-sink')");
      fail("Shall fail if password is wrong.");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Use current session, user is root
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create pipe a2b "
              + "with source ('database'='test1') "
              + "with processor('processor'='rename-database-processor', 'processor.new-db-name'='test') "
              + "with sink ('sink'='write-back-sink')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    // Alter to another user, shall fail because of lack of password
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('username'='thulab')");
      fail("Alter pipe shall fail if only user is specified");
    } catch (final SQLException ignore) {
      // Expected
    }

    // Successfully alter
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b modify sink ('sink.username'='thulab', 'sink.password'='StrngPsWd@623451')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Alter pipe shall not fail if user and password are specified");
    }

    TableModelUtils.createDataBaseAndTable(env, "test", "test1");
    TableModelUtils.createDataBaseAndTable(env, "test1", "test1");
    TableModelUtils.createDataBaseAndTable(env, "test", "test");
    TableModelUtils.createDataBaseAndTable(env, "test1", "test");

    // Write some data
    TableModelUtils.insertData("test1", "test", 0, 100, env);

    // Filter this
    TestUtils.executeNonQuery("test1", BaseEnv.TABLE_SQL_DIALECT, env, "flush", null);

    TableModelUtils.assertCountDataAlwaysOnEnv("test", "test", 0, env);

    // Continue, ensure that it won't block
    // Grant some privilege
    TestUtils.executeNonQuery(
        "test1", BaseEnv.TABLE_SQL_DIALECT, env, "grant INSERT on test.test1 to user thulab", null);
    TableModelUtils.insertData("test1", "test1", 0, 100, env);
    TableModelUtils.assertCountData("test", "test1", 100, env);

    // Clear data, avoid resending
    TestUtils.executeNonQuery("test", BaseEnv.TABLE_SQL_DIALECT, env, "drop database test1", null);

    // Alter pipe, throw exception if no privileges
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('skipif'='')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    TableModelUtils.createDataBaseAndTable(env, "test", "test1");
    TableModelUtils.createDataBaseAndTable(env, "test1", "test1");

    TableModelUtils.insertData("test1", "test", 0, 100, env);

    TableModelUtils.assertCountDataAlwaysOnEnv("test", "test", 0, env);

    // Grant some privilege
    TestUtils.executeNonQuery(
        "test", BaseEnv.TABLE_SQL_DIALECT, env, "grant INSERT on any to user thulab", null);

    TableModelUtils.assertCountData("test", "test", 100, env);

    // test showing pipe
    // Create another pipe, user is root
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create pipe a2c "
              + "with source ('database'='test1') "
              + "with processor('processor'='rename-database-processor', 'processor.new-db-name'='test') "
              + "with sink ('sink'='write-back-sink')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    // A user shall only see its own pipe
    try (final Connection connection =
            env.getConnection("thulab", "StrngPsWd@623451", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final ResultSet result = statement.executeQuery("show pipes");
      Assert.assertTrue(result.next());
      Assert.assertFalse(result.next());

      final ResultSet showCreateResult = statement.executeQuery("show create pipe a2b");
      Assert.assertTrue(showCreateResult.next());
      Assert.assertTrue(
          showCreateResult.getString("Create Pipe").contains("'sink.password'='******'"));
      Assert.assertFalse(showCreateResult.getString("Create Pipe").contains("StrngPsWd@623451"));
      Assert.assertFalse(showCreateResult.next());
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Tree model
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.test_sink");
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.test_sink.d1(s_int INT32,s_long INT64,s_float float,s_double double)");
      statement.execute(
          "INSERT INTO root.test_sink.d1(time,s_int,s_long,s_float,s_double) VALUES (2025-01-01 03:50:14,1,1,1,1),(2025-01-01 03:50:30,12,14,16.24,18.5),(2025-01-01 03:51:45,22,24,26.24,28.5),(2025-01-01 03:51:59,32,34,36.46,38.6),(2025-01-01 03:52:00,10,14,16.46,18.6)");
      statement.execute("CREATE USER user_new 'paSs1234@56789'");
      // Do not grant SYSTEM privilege to avoid auto creation
      statement.execute("GRANT write ON root.** TO USER user_new");
      statement.execute(
          "create pipe user_sink_pipe with source ('pattern'='root.test_sink.d1') with processor ('processor'='aggregate-processor', 'output.database'='root.user_sink_db', 'operators'='avg') with sink ('sink'='write-back-sink','user'='user_new','password'='paSs1234@56789')");

      final long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime <= 20_000L) {
        try (final ResultSet set = statement.executeQuery("show pipe user_sink_pipe")) {
          Assert.assertTrue(set.next());
          try {
            Assert.assertEquals("0", set.getString(8));
            return;
          } catch (final Throwable t) {
            // Retry
          }
        }
      }
      Assert.fail();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSinkPermissionWithHistoricalDataAndTablePattern() {
    TableModelUtils.createDataBaseAndTable(env, "test", "test1");
    TableModelUtils.createDataBaseAndTable(env, "test1", "test1");
    TableModelUtils.createDataBaseAndTable(env, "test", "test");
    TableModelUtils.createDataBaseAndTable(env, "test1", "test");

    TestUtils.executeNonQueries(
        "test",
        BaseEnv.TABLE_SQL_DIALECT,
        env,
        Arrays.asList(
            "create user thulab 'StrngPsWd@623451@123456'",
            "grant INSERT on test.test1 to user thulab"),
        null);

    // Write some data
    TableModelUtils.insertData("test1", "test", 0, 100, env);

    TableModelUtils.insertData("test1", "test1", 0, 100, env);

    // Use current session, user is root
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create pipe a2b "
              + "with source ('database'='test1', 'table'='test1') "
              + "with processor('processor'='rename-database-processor', 'processor.new-db-name'='test') "
              + "with sink ('sink'='write-back-sink', 'username'='thulab', 'password'='StrngPsWd@623451@123456')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    TableModelUtils.assertCountData("test", "test", 0, env);
    TableModelUtils.assertCountData("test", "test1", 100, env);
  }

  @Test
  public void testAuthenticatedRealtimePipesRemainWithoutDataRegion() throws Exception {
    final Set<String> expectedPipeNames =
        new HashSet<>(Arrays.asList("root_pipe", "source_pipe", "sink_pipe", "source_sink_pipe"));

    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER user_source 'paSs1234@56789'");
      statement.execute("CREATE USER user_sink 'paSs1234@56789'");
      statement.execute("CREATE DATABASE test_pipe_authentication");

      statement.execute(
          "create pipe root_pipe "
              + "with source ('forwarding-pipe-requests'='false', "
              + "'database-name'='test_pipe_authentication', 'table-name'='table_0') "
              + "with processor ('processor'='rename-database-processor', "
              + "'new-db-name'='pipe_newDB1') "
              + "with sink ('sink'='write-back-sink')");
      statement.execute(
          "create pipe source_pipe "
              + "with source ('forwarding-pipe-requests'='false', "
              + "'database-name'='test_pipe_authentication', 'table-name'='table_0', "
              + "'user'='user_source', 'password'='paSs1234@56789') "
              + "with processor ('processor'='rename-database-processor', "
              + "'new-db-name'='pipe_newDB2') "
              + "with sink ('sink'='write-back-sink')");
      statement.execute(
          "create pipe sink_pipe "
              + "with source ('forwarding-pipe-requests'='false', "
              + "'database-name'='test_pipe_authentication', 'table-name'='table_0') "
              + "with processor ('processor'='rename-database-processor', "
              + "'new-db-name'='pipe_newDB3') "
              + "with sink ('sink'='write-back-sink', 'user'='user_sink', "
              + "'password'='paSs1234@56789')");
      statement.execute(
          "create pipe source_sink_pipe "
              + "with source ('forwarding-pipe-requests'='false', "
              + "'database-name'='test_pipe_authentication', 'table-name'='table_0', "
              + "'user'='user_source', 'password'='paSs1234@56789') "
              + "with processor ('processor'='rename-database-processor', "
              + "'new-db-name'='pipe_newDB4') "
              + "with sink ('sink'='write-back-sink', 'user'='user_sink', "
              + "'password'='paSs1234@56789')");

      await()
          .pollInSameThread()
          .pollInterval(1, TimeUnit.SECONDS)
          .during(15, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final Set<String> actualPipeNames = new HashSet<>();
                try (final ResultSet resultSet = statement.executeQuery("SHOW PIPES")) {
                  while (resultSet.next()) {
                    actualPipeNames.add(resultSet.getString("ID"));
                  }
                }
                Assert.assertEquals(expectedPipeNames, actualPipeNames);
              });
    }
  }
}
