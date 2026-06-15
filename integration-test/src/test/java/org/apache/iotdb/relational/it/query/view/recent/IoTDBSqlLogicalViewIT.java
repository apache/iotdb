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
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqual;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSqlLogicalViewIT {

  private static final String DATABASE = "sql_logical_view_it";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE table1 (device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD)");
      statement.execute(
          "INSERT INTO table1(time, device_id, s1, s2) VALUES "
              + "(1, 'd1', 10, 100), (2, 'd1', 20, 200), (3, 'd2', 30, 300)");
      statement.execute(
          "CREATE WRITABLE VIEW writable_src AS SELECT device_id, s1, s2 FROM table1");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testQuerySqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE VIEW logical_view1 AS SELECT time, s1 + 1 AS s1_new, device_id FROM table1 "
              + "WHERE s1 >= 20");
    }
    tableResultSetEqualTest(
        "SELECT * FROM logical_view1 ORDER BY time",
        new String[] {"time", "s1_new", "device_id"},
        new String[] {
          "1970-01-01T00:00:00.002Z,21,d1,", "1970-01-01T00:00:00.003Z,31,d2,",
        },
        DATABASE);
  }

  @Test
  public void testNestedSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE VIEW nested_base AS SELECT time, s1 + 1 AS s1_new, device_id FROM table1 "
              + "WHERE s1 >= 20");
      statement.execute(
          "CREATE VIEW logical_view2 AS SELECT device_id, s1_new + 1 AS s1_plus FROM nested_base");
    }
    tableResultSetEqualTest(
        "SELECT count(*) FROM logical_view2",
        new String[] {"_col0"},
        new String[] {"2,"},
        DATABASE);
  }

  @Test
  public void testSqlLogicalViewOnWritableView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW logical_view4 AS SELECT s1 FROM writable_src WHERE s1 >= 20");
    }
    tableResultSetEqualTest(
        "SELECT count(*) FROM logical_view4 WHERE s1 = 30",
        new String[] {"_col0"},
        new String[] {"1,"},
        DATABASE);
  }

  @Test
  public void testCreateOrReplaceSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW replace_view AS SELECT time, s1 FROM table1");
      statement.execute(
          "CREATE OR REPLACE VIEW replace_view AS SELECT time, device_id FROM table1 WHERE s1 = 10");
    }
    tableResultSetEqualTest(
        "SELECT count(*) FROM replace_view", new String[] {"_col0"}, new String[] {"1,"}, DATABASE);
  }

  @Test
  public void testShowCreateSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW show_create_view AS SELECT time, s1 FROM table1");
      String ddl;
      try (ResultSet resultSet = statement.executeQuery("SHOW CREATE VIEW show_create_view")) {
        Assert.assertTrue(resultSet.next());
        ddl = resultSet.getString(2);
        Assert.assertTrue(ddl.contains("CREATE VIEW"));
        Assert.assertTrue(ddl.contains("SELECT"));
        Assert.assertTrue(ddl.contains("table1"));
        Assert.assertFalse(ddl.contains(" TIMESTAMP "));
        Assert.assertFalse(ddl.contains(" FIELD"));
      }
      statement.execute("DROP VIEW show_create_view");
      statement.execute(ddl);
      tableResultSetEqualTest(
          "SELECT count(*) FROM show_create_view",
          new String[] {"_col0"},
          new String[] {"3,"},
          DATABASE);
    }
  }

  @Test
  public void testShowTablesDetailsContainsSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW show_tables_view AS SELECT time FROM table1");
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT table_name, \"ttl(ms)\", status, comment, table_type, original_table_name "
                  + "FROM information_schema.tables WHERE database = '"
                  + DATABASE
                  + "' AND table_name = 'show_tables_view'")) {
        tableResultSetEqual(
            resultSet,
            new String[] {
              "table_name", "ttl(ms)", "status", "comment", "table_type", "original_table_name"
            },
            new String[] {"show_tables_view,INF,USING,null,VIEW,null,"});
      }
    }
  }

  @Test
  public void testInformationSchemaViews() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW ins_views_view AS SELECT time FROM table1");
    }
    tableResultSetEqualTest(
        "SELECT database, table_name FROM views WHERE database = '"
            + DATABASE
            + "' AND table_name = 'ins_views_view'",
        new String[] {"database", "table_name"},
        new String[] {DATABASE + ",ins_views_view,"},
        "information_schema");
  }

  @Test
  public void testDescribeSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW describe_view AS SELECT time, device_id FROM table1");
      try (ResultSet resultSet = statement.executeQuery("DESCRIBE describe_view")) {
        tableResultSetEqual(
            resultSet,
            new String[] {"ColumnName", "DataType", "Category"},
            new String[] {
              "time,TIMESTAMP,FIELD,", "device_id,STRING,FIELD,",
            });
      }
    }
  }

  @Test
  public void testRenameSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW rename_src AS SELECT time, s1 FROM table1");
      statement.execute("ALTER VIEW rename_src RENAME TO rename_dst");
    }
    tableResultSetEqualTest(
        "SELECT count(*) FROM rename_dst", new String[] {"_col0"}, new String[] {"3,"}, DATABASE);
  }

  @Test
  public void testCommentOnSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW comment_view AS SELECT time FROM table1");
      statement.execute("COMMENT ON VIEW comment_view IS 'logical view comment'");
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT table_name, \"ttl(ms)\", status, comment, table_type, original_table_name "
                  + "FROM information_schema.tables WHERE database = '"
                  + DATABASE
                  + "' AND table_name = 'comment_view'")) {
        tableResultSetEqual(
            resultSet,
            new String[] {
              "table_name", "ttl(ms)", "status", "comment", "table_type", "original_table_name"
            },
            new String[] {"comment_view,INF,USING,logical view comment,VIEW,null,"});
      }
    }
  }

  @Test
  public void testCreateSqlLogicalViewDuplicateColumnNames() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      IoTDBSQLException exception =
          Assert.assertThrows(
              IoTDBSQLException.class,
              () -> statement.execute("CREATE VIEW dup_col_view AS SELECT s1, s1 FROM table1"));
      Assert.assertTrue(
          exception.getMessage().contains("Column name 's1' specified more than once"));
    }
  }

  @Test
  public void testDropSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW drop_view AS SELECT time FROM table1");
      statement.execute("DROP VIEW drop_view");
      Assert.assertThrows(
          SQLException.class, () -> statement.executeQuery("SELECT * FROM drop_view"));
    }
  }

  @Test
  public void testRecursiveSqlLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE VIEW view_a AS SELECT time, s1 FROM table1");
      statement.execute("CREATE VIEW view_b AS SELECT time, s1 FROM view_a");
      IoTDBSQLException exception =
          Assert.assertThrows(
              IoTDBSQLException.class,
              () ->
                  statement.execute(
                      "CREATE OR REPLACE VIEW view_a AS SELECT time, s1 FROM view_b"));
      Assert.assertTrue(
          exception.getMessage().contains("recursive")
              || exception.getMessage().contains("Recursive"));
    }
  }

  @Test
  public void testStaleSqlLogicalView() throws SQLException {
    final String staleDb = DATABASE + "_stale";
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + staleDb);
      statement.execute("USE " + staleDb);
      statement.execute(
          "CREATE TABLE stale_table (device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD)");
      statement.execute(
          "INSERT INTO stale_table(time, device_id, s1, s2) VALUES (1, 'd1', 10, 100)");
      statement.execute("CREATE VIEW stale_view AS SELECT time, s1, s2 FROM stale_table");
      statement.execute("DROP TABLE stale_table");
      statement.execute(
          "CREATE TABLE stale_table (device_id STRING TAG, s3 INT32 FIELD, s4 INT64 FIELD)");
      SQLException exception =
          Assert.assertThrows(
              SQLException.class, () -> statement.executeQuery("SELECT * FROM stale_view"));
      Assert.assertTrue(
          exception.getMessage().contains("stale")
              || exception.getMessage().contains("mismatch")
              || exception.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testShowCreateViewQualifiesUnqualifiedTableNames() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE OR REPLACE VIEW show_create_qualify_view AS SELECT time, s1 FROM table1");
      try (ResultSet resultSet =
          statement.executeQuery("SHOW CREATE VIEW show_create_qualify_view")) {
        Assert.assertTrue(resultSet.next());
        final String showCreateSql = resultSet.getString(2);
        Assert.assertTrue(showCreateSql.contains(DATABASE + ".table1"));
      }
    }
  }

  @Test
  public void testCreateSqlLogicalViewWithCte() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE OR REPLACE VIEW cte_view AS WITH t1 AS (SELECT time, s1 FROM table1) "
              + "SELECT time, s1 FROM t1 WHERE s1 >= 20");
      try (ResultSet showCreateResult = statement.executeQuery("SHOW CREATE VIEW cte_view")) {
        Assert.assertTrue(showCreateResult.next());
        final String showCreateSql = showCreateResult.getString(2);
        Assert.assertTrue(showCreateSql.contains(DATABASE + ".table1"));
        Assert.assertFalse(showCreateSql.contains(DATABASE + ".t1"));
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM cte_view ORDER BY time")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(20, resultSet.getInt("s1"));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(30, resultSet.getInt("s1"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowCreateViewRejectsBaseTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      Assert.assertThrows(
          SQLException.class, () -> statement.executeQuery("SHOW CREATE VIEW table1"));
    }
  }

  @Test
  public void testSqlLogicalViewUsesCreatorSessionDatabase() throws SQLException {
    final String viewHomeDb = DATABASE + "_view_home";
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + viewHomeDb);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE VIEW "
              + viewHomeDb
              + ".resolution_view AS SELECT time, s1, device_id FROM table1 WHERE s1 = 10");
      statement.execute("USE " + viewHomeDb);
      try (ResultSet resultSet =
          statement.executeQuery("SELECT * FROM resolution_view ORDER BY time")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(10, resultSet.getInt("s1"));
        Assert.assertEquals("d1", resultSet.getString("device_id"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testQuerySqlLogicalViewFromDifferentDatabase() throws SQLException {
    final String otherDb = DATABASE + "_other";
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + otherDb);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE VIEW cross_db_view AS SELECT time, s1, device_id FROM table1 WHERE s1 = 10");
      statement.execute("USE " + otherDb);
      try (ResultSet resultSet =
          statement.executeQuery("SELECT * FROM " + DATABASE + ".cross_db_view ORDER BY time")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(10, resultSet.getInt("s1"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
