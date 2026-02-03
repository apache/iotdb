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

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;

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

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipePermissionIT extends AbstractPipeSingleIT {

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(1);
    env = MultiEnvFactory.getEnv(0);
    env.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    // 1C1D to directly show the remaining count
    env.initClusterEnvironment(1, 1);
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
          "alter pipe a2b modify sink ('username'='thulab', 'password'='StrngPsWd@623451')");
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
}
