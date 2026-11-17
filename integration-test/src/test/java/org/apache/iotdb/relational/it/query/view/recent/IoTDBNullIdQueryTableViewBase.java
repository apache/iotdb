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
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetFuzzyTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class IoTDBNullIdQueryTableViewBase {
  private static final String TREE_DB_NAME = "root.test";
  private static final String DATABASE_NAME = "test_view";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + TREE_DB_NAME,
        "CREATE DATABASE root.db_view",
        "CREATE ALIGNED TIMESERIES root.test.testNullId(s1 INT32, s2 BOOLEAN, s3 DOUBLE)",
        "INSERT INTO root.test.testNullId(time,s1,s2,s3) " + " aligned values(1, 0, false, 11.1)",
        "CREATE ALIGNED TIMESERIES root.test.table1.d1(s1 INT32, s2 BOOLEAN, s3 INT64)",
        // in seq disk
        "INSERT INTO root.test.table1.d1(time,s1,s2,s3) " + "values(1, 1, false, 11)",
        "INSERT INTO root.test.table1.d1(time,s1) " + " aligned values(5, 5)",
        "FLUSH",
        // in uneq disk
        "INSERT INTO root.test.table1.d1(time,s1,s2,s3) " + "aligned values(4, 4, true, 44)",
        "INSERT INTO root.test.table1.d1(time,s1) " + " aligned values(3, 3)",
        "FLUSH",
        // in seq memtable
        "INSERT INTO root.test.table1.d1(time,s1,s2,s3) " + "aligned values(7, 7, false, 77)",
        "INSERT INTO root.test.table1.d1(time,s1) " + "aligned values(6, 6)",
        // in unseq memtable
        "INSERT INTO root.test.table1.d1(time,s1) " + " aligned values(2, 2)",
      };

  private static final String[] createTableViewSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE VIEW testNullId(id1 STRING TAG, id2 STRING TAG, s1 INT32 FIELD, s2 BOOLEAN FIELD, s3 DOUBLE FIELD) as root.test.testNullId.**",
        "CREATE VIEW table1(device_id STRING TAG, s1 INT32 FIELD, s2 BOOLEAN FIELD, s3 INT64 FIELD) as root.test.table1.**",
      };

  // The concrete table IT inherits this lifecycle so both fixture sets share one cluster.
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(createSqls);
    prepareTableData(createTableViewSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // Null-tag rows in a tree-backed table view remain queryable with IS NULL predicates.
  @Test
  public void viewNullFilterTest() throws Exception {
    String result = defaultFormatDataTime(1) + ",0,false,11.1";
    try (final Connection connectionIsNull =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connectionIsNull.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      ResultSet resultSet = statement.executeQuery("select * from testNullId where id1 is null");
      assertTrue(resultSet.next());
      String ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select * from testNullId where id1 is not null");
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select * from testNullId where id1 like '%'");
      assertFalse(resultSet.next());

      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null and id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // The second time we read from cache
      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null and id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test deduplication
      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null or id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test constant select item
      resultSet = statement.executeQuery("select *, 1 from testNullId");
      result = defaultFormatDataTime(1) + ",null,null,0,false,11.1,1";
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("id1")
              + ","
              + resultSet.getString("id2")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3")
              + ","
              + resultSet.getString("_col6");

      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test boolean between
      resultSet =
          statement.executeQuery("select * from testNullId where s2 between false and true");
      result = defaultFormatDataTime(1) + ",null,null,0,false,11.1";
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("id1")
              + ","
              + resultSet.getString("id2")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");

      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test boolean not between
      resultSet =
          statement.executeQuery("select * from testNullId where s2 not between false and true");
      assertFalse(resultSet.next());

      // Test same column name
      resultSet = statement.executeQuery("select time, s1 as a, s2 as a from testNullId");
      result = defaultFormatDataTime(1) + ",0,false";
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      assertEquals(3, resultSetMetaData.getColumnCount());
      assertEquals("time", resultSetMetaData.getColumnName(1));
      assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(1));
      assertEquals("a", resultSetMetaData.getColumnName(2));
      assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(2));
      assertEquals("a", resultSetMetaData.getColumnName(3));
      assertEquals(Types.BOOLEAN, resultSetMetaData.getColumnType(3));

      assertTrue(resultSet.next());
      ans = resultSet.getString(1) + "," + resultSet.getString(2) + "," + resultSet.getString(3);

      assertEquals(result, ans);
      assertFalse(resultSet.next());
    }
  }

  // Selecting fields from tree-backed table views preserves null values and row ordering.
  @Test
  public void viewNullSelectTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,",
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where time > 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,",
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where time < 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where time > 1 and time < 7 and s1 >= 3 and s1 <= 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where s2 is NULL",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 where s2 IS NOT NULL OR s3 IS NOT NULL",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // A view projection with no measurement columns still returns the expected time and tag rows.
  @Test
  public void viewNoMeasurementColumnsSelectTest() {
    String[] expectedHeader = new String[] {"time"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,",
          "1970-01-01T00:00:00.002Z,",
          "1970-01-01T00:00:00.003Z,",
          "1970-01-01T00:00:00.004Z,",
          "1970-01-01T00:00:00.005Z,",
          "1970-01-01T00:00:00.006Z,",
          "1970-01-01T00:00:00.007Z,"
        };
    tableResultSetEqualTest("select time from table1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"device_id"};
    retArray = new String[] {"d1,", "d1,", "d1,", "d1,", "d1,", "d1,"};
    tableResultSetEqualTest(
        "select device_id from table1 where time > 1", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,",
          "1970-01-01T00:00:00.002Z,d1,",
          "1970-01-01T00:00:00.003Z,d1,",
          "1970-01-01T00:00:00.004Z,d1,",
          "1970-01-01T00:00:00.005Z,d1,",
          "1970-01-01T00:00:00.006Z,d1,",
        };
    tableResultSetEqualTest(
        "select time, device_id from table1 where time < 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,",
          "1970-01-01T00:00:00.002Z,d1,",
          "1970-01-01T00:00:00.003Z,d1,",
          "1970-01-01T00:00:00.004Z,d1,",
          "1970-01-01T00:00:00.005Z,d1,",
          "1970-01-01T00:00:00.006Z,d1,",
        };
    tableResultSetEqualTest(
        "select time, device_id from table1 where time < 7",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"device_id"};
    retArray =
        new String[] {
          "d1,",
        };
    tableResultSetEqualTest(
        "select device_id from table1 where device_id='d1' limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,", "1970-01-01T00:00:00.004Z,", "1970-01-01T00:00:00.005Z,",
        };
    tableResultSetEqualTest(
        "select time from table1 where time >= 3 and time <= 5 and device_id='d1'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,", "1970-01-01T00:00:00.004Z,", "1970-01-01T00:00:00.005Z,",
        };
    tableResultSetEqualTest(
        "select time from table1 where time >= 3 and time < 6",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  public void viewLimitOffsetTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 OFFSET 2 limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 limit 1 OFFSET 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,null,null,",
          "1970-01-01T00:00:00.004Z,d1,true,44,",
          "1970-01-01T00:00:00.005Z,d1,null,null,",
          "1970-01-01T00:00:00.006Z,d1,null,null,",
          "1970-01-01T00:00:00.007Z,d1,false,77,"
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 OFFSET 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,false,11,", "1970-01-01T00:00:00.002Z,d1,null,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s2, s3 from table1 limit 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // SHOW statements report the active table dialect, database, user, and cluster metadata.
  @Test
  public void viewShowStatementTest() {
    String[] expectedHeader = new String[] {"CurrentSqlDialect"};
    String[] retArray =
        new String[] {
          "TABLE,",
        };
    tableResultSetEqualTest("show current_sql_dialect", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"CurrentUser"};
    retArray =
        new String[] {
          "root,",
        };
    tableResultSetEqualTest("show current_user", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"CurrentDatabase"};
    retArray =
        new String[] {
          DATABASE_NAME + ",",
        };
    tableResultSetEqualTest("show current_database", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"Version", "BuildInfo"};
    tableResultSetFuzzyTest("show version", expectedHeader, 1, DATABASE_NAME);

    expectedHeader = new String[] {"Variable", "Value"};
    tableResultSetFuzzyTest("show variables", expectedHeader, 15, DATABASE_NAME);

    expectedHeader = new String[] {"ClusterId"};
    tableResultSetFuzzyTest("show clusterid", expectedHeader, 1, DATABASE_NAME);

    expectedHeader = new String[] {"ClusterId"};
    tableResultSetFuzzyTest("show cluster_id", expectedHeader, 1, DATABASE_NAME);

    expectedHeader = new String[] {"CurrentTimestamp"};
    tableResultSetFuzzyTest("show current_timestamp", expectedHeader, 1, DATABASE_NAME);
  }

  // A user can switch one session between tree and table SQL dialects and observe each change.
  @Test
  public void viewSetSqlDialectTest() throws SQLException {
    createUser("tempuser_view", "TempView!2026");

    try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser_view", "TempView!2026");
        Statement userStmt = userCon.createStatement()) {
      assertCurrentSqlDialect(true, userStmt);

      // set Tree to Table
      userStmt.execute("set sql_dialect=table");
      assertCurrentSqlDialect(false, userStmt);

      // set Table to Tree
      userStmt.execute("set sql_dialect=tree");
      assertCurrentSqlDialect(true, userStmt);
    }
  }

  // Switching to tree dialect allows a tree insert after selecting a separate table database.
  @Test
  public void viewSetSqlDialectContextCleanTest() throws SQLException {
    try (Connection userCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement userStmt = userCon.createStatement()) {
      userStmt.execute("create database test1_view");
      userStmt.execute("use test1_view");
      userStmt.execute("set sql_dialect=tree");
      assertCurrentSqlDialect(true, userStmt);
      userStmt.execute("insert into root.db_view(time,s1) values (0,1), (1, 3), (2,5)");
    }
  }

  public static void assertCurrentSqlDialect(boolean expectedTree, Statement statement)
      throws SQLException {
    ResultSet resultSet = statement.executeQuery("show current_sql_dialect");
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals("CurrentSqlDialect", resultSetMetaData.getColumnName(1));
    int count = 0;
    while (resultSet.next()) {
      assertEquals(expectedTree ? "TREE" : "TABLE", resultSet.getString(1));
      count++;
    }
    assertEquals(1, count);
  }
}
