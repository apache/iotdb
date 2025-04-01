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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;

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
    if (!TestUtils.tryExecuteNonQueryWithRetry(env, "create user `thulab` 'passwd'")) {
      return;
    }

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
    if (!TableModelUtils.insertData("test1", "test", 0, 100, env)) {
      return;
    }

    // Filter this
    if (!TestUtils.tryExecuteNonQueryWithRetry("test1", BaseEnv.TABLE_SQL_DIALECT, env, "flush")) {
      return;
    }

    TableModelUtils.assertCountDataAlwaysOnEnv("test", "test", 0, env);

    // Continue, ensure that it won't block
    // Grant some privilege
    if (!TestUtils.tryExecuteNonQueryWithRetry(
        "test1", BaseEnv.TABLE_SQL_DIALECT, env, "grant INSERT on test.test1 to user thulab")) {
      return;
    }
    if (!TableModelUtils.insertData("test1", "test1", 0, 100, env)) {
      return;
    }
    TableModelUtils.assertCountData("test", "test1", 100, env);

    // Clear data, avoid resending
    if (!TestUtils.tryExecuteNonQueryWithRetry(
        "test", BaseEnv.TABLE_SQL_DIALECT, env, "drop database test1")) {
      return;
    }

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

    if (!TableModelUtils.insertData("test1", "test", 0, 100, env)) {
      return;
    }

    TableModelUtils.assertCountDataAlwaysOnEnv("test", "test", 0, env);

    // Grant some privilege
    if (!TestUtils.tryExecuteNonQueryWithRetry(
        "test", BaseEnv.TABLE_SQL_DIALECT, env, "grant INSERT on any to user thulab")) {
      return;
    }

    TableModelUtils.assertCountData("test", "test", 100, env);
  }

  @Test
  public void testSinkPermissionWithHistoricalDataAndTablePattern() {
    TableModelUtils.createDataBaseAndTable(env, "test", "test1");
    TableModelUtils.createDataBaseAndTable(env, "test1", "test1");
    TableModelUtils.createDataBaseAndTable(env, "test", "test");
    TableModelUtils.createDataBaseAndTable(env, "test1", "test");

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        "test",
        BaseEnv.TABLE_SQL_DIALECT,
        env,
        Arrays.asList(
            "create user thulab 'passwd'", "grant INSERT on test.test1 to user thulab"))) {
      return;
    }

    // Write some data
    if (!TableModelUtils.insertData("test1", "test", 0, 100, env)) {
      return;
    }

    if (!TableModelUtils.insertData("test1", "test1", 0, 100, env)) {
      return;
    }

    // Use current session, user is root
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create pipe a2b "
              + "with source ('database'='test1', 'table'='test1') "
              + "with processor('processor'='rename-database-processor', 'processor.new-db-name'='test') "
              + "with sink ('sink'='write-back-sink', 'username'='thulab', 'password'='passwd')");
    } catch (final SQLException e) {
      e.printStackTrace();
      fail("Create pipe without user shall succeed if use the current session");
    }

    TableModelUtils.assertCountData("test", "test", 0, env);
    TableModelUtils.assertCountData("test", "test1", 100, env);
  }
}
