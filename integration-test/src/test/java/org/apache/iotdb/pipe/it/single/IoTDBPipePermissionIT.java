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
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipePermissionIT extends AbstractPipeSingleIT {
  @Test
  public void testSinkPermission() {
    TestUtils.executeNonQuery(env, "create user `thulab` 'passwd'", null);

    // Shall fail if username is specified without password
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create pipe a2b ('user'='thulab', 'sink'='write-back-sink')");
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
      statement.execute("alter pipe a2b modify sink ('username'='thulab', 'password'='passwd')");
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
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
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
            "create user thulab 'passwD@123456'", "grant INSERT on test.test1 to user thulab"),
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
              + "with sink ('sink'='write-back-sink', 'username'='thulab', 'password'='passwD@123456')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    TableModelUtils.assertCountData("test", "test", 0, env);
    TableModelUtils.assertCountData("test", "test1", 100, env);
  }
}
