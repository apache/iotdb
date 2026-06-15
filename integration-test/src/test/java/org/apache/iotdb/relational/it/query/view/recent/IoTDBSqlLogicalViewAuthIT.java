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

package org.apache.iotdb.relational.it.query.view.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSqlLogicalViewAuthIT {

  private static final String DATABASE = "sql_logical_view_auth_it";
  private static final String VIEW_USER = "view_user";
  private static final String OTHER_USER = "other_user";
  private static final String PASSWORD = "TimechoDB@2021";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(
        new String[] {
          "CREATE DATABASE " + DATABASE,
          "USE " + DATABASE,
          "CREATE TABLE secret_table (device_id STRING TAG, s1 INT32 FIELD)",
          "INSERT INTO secret_table(time, device_id, s1) VALUES (1, 'd1', 10)",
          "CREATE TABLE visibility_helper (device_id STRING TAG, s1 INT32 FIELD)",
          "INSERT INTO visibility_helper(time, device_id, s1) VALUES (1, 'd1', 1)",
          String.format("CREATE USER %s '%s'", VIEW_USER, PASSWORD),
          String.format("CREATE USER %s '%s'", OTHER_USER, PASSWORD),
          "GRANT SELECT ON TABLE visibility_helper TO USER " + VIEW_USER,
          "GRANT SELECT ON TABLE visibility_helper TO USER " + OTHER_USER
        });
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createViewAsAdmin(String viewName, String selectSql) {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE OR REPLACE VIEW " + viewName + " AS " + selectSql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateSqlLogicalViewAuth() {
    assertTableNonQueryTestFail(
        "CREATE VIEW create_view AS SELECT time, s1 FROM secret_table",
        "please add privilege CREATE ON " + DATABASE + ".create_view",
        VIEW_USER,
        PASSWORD,
        DATABASE);

    prepareTableData(
        new String[] {
          "USE " + DATABASE, "GRANT CREATE ON TABLE create_view TO USER " + VIEW_USER,
        });

    assertTableNonQueryTestFail(
        "CREATE VIEW create_view AS SELECT time, s1 FROM secret_table",
        "please add privilege SELECT ON " + DATABASE + ".secret_table",
        VIEW_USER,
        PASSWORD,
        DATABASE);

    prepareTableData(
        new String[] {
          "USE " + DATABASE, "GRANT SELECT ON TABLE secret_table TO USER " + VIEW_USER,
        });

    try (Connection connection =
            EnvFactory.getEnv().getConnection(VIEW_USER, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW create_view AS SELECT time, s1 FROM secret_table");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    prepareTableData(
        new String[] {
          "USE " + DATABASE, "GRANT SELECT ON TABLE create_view TO USER " + VIEW_USER,
        });

    tableResultSetEqualTest(
        "SELECT count(*) FROM create_view",
        new String[] {"_col0"},
        new String[] {"1,"},
        VIEW_USER,
        PASSWORD,
        DATABASE);
  }

  @Test
  public void testQuerySqlLogicalViewAuth() {
    createViewAsAdmin("auth_view", "SELECT time, s1 FROM secret_table");

    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "REVOKE SELECT ON TABLE secret_table FROM USER " + VIEW_USER,
          "GRANT SELECT ON TABLE auth_view TO USER " + VIEW_USER,
        });

    tableResultSetEqualTest(
        "SELECT * FROM auth_view ORDER BY time",
        new String[] {"time", "s1"},
        new String[] {"1970-01-01T00:00:00.001Z,10,"},
        VIEW_USER,
        PASSWORD,
        DATABASE);

    tableAssertTestFail(
        "SELECT * FROM secret_table",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON "
            + DATABASE
            + ".secret_table",
        VIEW_USER,
        PASSWORD,
        DATABASE);

    tableAssertTestFail(
        "SELECT * FROM auth_view",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON "
            + DATABASE
            + ".auth_view",
        OTHER_USER,
        PASSWORD,
        DATABASE);
  }

  @Test
  public void testQuerySqlLogicalViewAuthViaSubquery() {
    createViewAsAdmin("subquery_auth_view", "SELECT time, s1 FROM secret_table");

    prepareTableData(
        new String[] {
          "USE " + DATABASE,
          "REVOKE SELECT ON TABLE secret_table FROM USER " + VIEW_USER,
          "GRANT SELECT ON TABLE subquery_auth_view TO USER " + VIEW_USER,
        });

    tableResultSetEqualTest(
        "SELECT * FROM (SELECT * FROM subquery_auth_view) ORDER BY time",
        new String[] {"time", "s1"},
        new String[] {"1970-01-01T00:00:00.001Z,10,"},
        VIEW_USER,
        PASSWORD,
        DATABASE);

    tableAssertTestFail(
        "SELECT * FROM secret_table",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON "
            + DATABASE
            + ".secret_table",
        VIEW_USER,
        PASSWORD,
        DATABASE);

    tableAssertTestFail(
        "SELECT * FROM (SELECT * FROM subquery_auth_view)",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON "
            + DATABASE
            + ".subquery_auth_view",
        OTHER_USER,
        PASSWORD,
        DATABASE);
  }

  @Test
  public void testDropSqlLogicalViewAuth() {
    createViewAsAdmin("drop_view", "SELECT time FROM secret_table");

    assertTableNonQueryTestFail(
        "DROP VIEW drop_view",
        "please add privilege DROP ON " + DATABASE + ".drop_view",
        VIEW_USER,
        PASSWORD,
        DATABASE);

    prepareTableData(
        new String[] {
          "USE " + DATABASE, "GRANT DROP ON TABLE drop_view TO USER " + VIEW_USER,
        });

    try (Connection connection =
            EnvFactory.getEnv().getConnection(VIEW_USER, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("DROP VIEW drop_view");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    tableResultSetEqualTest(
        "SELECT table_name FROM tables WHERE database = '"
            + DATABASE
            + "' AND table_name = 'drop_view'",
        new String[] {"table_name"},
        new String[] {},
        "information_schema");
  }
}
