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

package org.apache.iotdb.relational.it.query.view.old;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class IoTDBFilterTableViewBase {
  protected static final int ITERATION_TIMES = 10;
  private static final String DATABASE_NAME = "test_view";

  protected static void insertViewData() {
    createTimeSeries();
    generateData();
    createTableView();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
      statement.execute("CREATE DATABASE root.testNullFilter");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s3 with datatype=TEXT,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testNullFilter.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testNullFilter.d1.s2 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.testNullFilter.d1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testNaN.d1.n1 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testNaN.d1.n2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testTimeSeries.d1.s1 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testTimeSeries.d1.s2 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testUDTF.d1.s1 with datatype=TEXT,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testUDTF.d1.s2 with datatype=DOUBLE,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; i++) {
        statement.execute(
            String.format(
                "insert into root.vehicle.testNaN.d1(timestamp,n1,n2) values(%d,%d,%d)", i, i, i));

        switch (i % 3) {
          case 0:
            statement.execute(
                String.format(
                    "insert into root.vehicle.testTimeSeries.d1(timestamp,s1,s2) values(%d,true,true)",
                    i));
            break;
          case 1:
            statement.execute(
                String.format(
                    "insert into root.vehicle.testTimeSeries.d1(timestamp,s1,s2) values(%d,true,false)",
                    i));
            break;
          case 2:
            statement.execute(
                String.format(
                    "insert into root.vehicle.testTimeSeries.d1(timestamp,s1,s2) values(%d,false,false)",
                    i));
            break;
        }
      }
      statement.execute(
          " insert into root.sg1.d1(time, s1, s2) aligned values (1,1, \"1\"), (2,2,\"2\")");
      statement.execute(
          " insert into root.vehicle.testUDTF.d1(time, s1, s2) values (1,\"ss\",0), (2,\"d\",3)");
      for (int i = 1; i <= ITERATION_TIMES; i++) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2,s3) values(%d,%d,%d,%s)", i, i, i, i));
      }
      statement.execute(
          "INSERT INTO root.testNullFilter.d1(timestamp,s2,s3) values(1, false, 11.1)");
      statement.execute("INSERT INTO root.testNullFilter.d1(timestamp,s1,s2) values(2, 22, true)");
      statement.execute("INSERT INTO root.testNullFilter.d1(timestamp,s1,s3) values(3, 23, 33.3)");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void createTableView() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW testNaN(device STRING TAG, n1 DOUBLE FIELD, n2 DOUBLE FIELD) as root.vehicle.testNaN.**");
      statement.execute(
          "CREATE VIEW testTimeSeries(device STRING TAG, s1 BOOLEAN FIELD, s2 BOOLEAN FIELD) as root.vehicle.testTimeSeries.**");
      statement.execute(
          "CREATE VIEW testUDTF(device STRING TAG, s1 TEXT FIELD, s2 DOUBLE FIELD) as root.vehicle.testUDTF.**");
      statement.execute(
          "CREATE VIEW sg1(device STRING TAG, s1 DOUBLE FIELD, s2 TEXT FIELD) as root.sg1.**");
      statement.execute(
          "CREATE VIEW table1 (device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD, s3 TEXT FIELD) as root.vehicle.d1.**");
      statement.execute(
          "CREATE VIEW testNullFilter(device STRING TAG, s1 INT32 FIELD, s2 BOOLEAN FIELD, s3 DOUBLE FIELD) as root.testNullFilter.**");

    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  // Checks boolean predicates against view fields and verifies the matching true rows.
  @Test
  public void testViewFilterBooleanSeries() {
    String[] expectedHeader = new String[] {"s1", "s2"};
    String[] retArray = new String[] {"true,true,", "true,true,", "true,true,", "true,true,"};
    tableResultSetEqualTest(
        "select s1, s2 from testTimeSeries " + "Where s2", expectedHeader, retArray, DATABASE_NAME);

    tableResultSetEqualTest(
        "select s1, s2 from testTimeSeries " + "Where s1 and s2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "true,true,",
          "true,false,",
          "true,true,",
          "true,false,",
          "true,true,",
          "true,false,",
          "true,true,"
        };
    tableResultSetEqualTest(
        "select s1, s2 from testTimeSeries " + "Where s1 or s2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select s1, s2 from testTimeSeries " + "Where s1 or false",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // Checks that a NaN comparison through a view does not return rows.
  @Test
  public void testViewFilterNaN() {
    String sqlStr = "select n1 from testNaN where n1/n2 > 0";
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery(sqlStr);
      int count = 0;
      while (resultSet.next()) {
        ++count;
      }

      // 0.0/0.0 is NaN which should not be kept.
      assertEquals(ITERATION_TIMES - 1, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  // Checks comparison coercion for differently typed constants over view columns.
  @Test
  public void testViewSameConstantWithDifferentType() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet =
          statement.executeQuery("select s2 from sg1 where s1 = 1 and s2 >= '1' and s2 <= '2'");
      int count = 0;
      while (resultSet.next()) {
        ++count;
      }
      assertEquals(1, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  // Checks equality and inequality predicates against NULL view values return no rows.
  @Test
  public void testViewCompareWithNull() {
    tableResultSetEqualTest(
        "select s1 from sg1 where s1 != null", new String[] {"s1"}, new String[] {}, DATABASE_NAME);
    tableResultSetEqualTest(
        "select s1 from sg1 where s1 <> null", new String[] {"s1"}, new String[] {}, DATABASE_NAME);
    tableResultSetEqualTest(
        "select s1 from sg1 where s1 = null", new String[] {"s1"}, new String[] {}, DATABASE_NAME);
  }

  // Checks arithmetic with NULL view values propagates NULL in the result.
  @Test
  public void testViewCalculateWithNull() {
    tableResultSetEqualTest(
        "select s1 + null from sg1",
        new String[] {"_col0"},
        new String[] {"null,", "null,"},
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select s1 - null from sg1",
        new String[] {"_col0"},
        new String[] {"null,", "null,"},
        DATABASE_NAME);
  }

  // Checks BETWEEN and NOT BETWEEN bounds on view fields and time, including literal expressions.
  @Test
  public void testViewBetweenExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      int start = 1;
      int end = 5;
      assertBetweenQuery(
          statement, "SELECT * FROM table1 WHERE s1 BETWEEN " + start + " AND " + end, start, end);
      assertBetweenQuery(
          statement,
          "SELECT * FROM table1 WHERE s1 NOT BETWEEN " + (end + 1) + " AND " + ITERATION_TIMES,
          start,
          end);
      assertBetweenQuery(
          statement,
          "SELECT * FROM table1 WHERE time BETWEEN " + start + " AND " + end,
          start,
          end);
      assertBetweenQuery(
          statement,
          "SELECT * FROM table1 WHERE time NOT BETWEEN " + (end + 1) + " AND " + ITERATION_TIMES,
          start,
          end);
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT * FROM table1 WHERE " + start + " BETWEEN time AND " + end)) {
        assertTrue(resultSet.next());
        assertBetweenRow(resultSet, 1);
        assertFalse(resultSet.next());
      }
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT * FROM table1 WHERE " + start + " NOT BETWEEN time AND " + end)) {
        for (int row = start + 1; row <= ITERATION_TIMES; row++) {
          assertTrue(resultSet.next());
          assertBetweenRow(resultSet, row);
        }
        assertFalse(resultSet.next());
      }
      String[] expectedRows = {"true,true,", "true,false,", "false,false,"};
      tableResultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from table1 where time between 1 and 3",
          new String[] {"_col0", "_col1"},
          expectedRows,
          DATABASE_NAME);
      tableResultSetEqualTest(
          "select s1 between 1 and 2 as r1, time between 0 and 1 as r2 from table1 where time between 1 and 3",
          new String[] {"r1", "r2"},
          expectedRows,
          DATABASE_NAME);
      tableResultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from table1 where time between 1 and 3 order by device",
          new String[] {"_col0", "_col1"},
          expectedRows,
          DATABASE_NAME);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void assertBetweenQuery(Statement statement, String query, int start, int end)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery(query)) {
      for (int row = start; row <= end; row++) {
        assertTrue(resultSet.next());
        assertBetweenRow(resultSet, row);
      }
      assertFalse(resultSet.next());
    }
  }

  private static void assertBetweenRow(ResultSet resultSet, int row) throws SQLException {
    assertEquals(defaultFormatDataTime(row), resultSet.getString("time"));
    assertEquals(String.valueOf(row), resultSet.getString("s1"));
    assertEquals(String.valueOf(row), resultSet.getString("s2"));
    assertEquals(String.valueOf(row), resultSet.getString("s3"));
  }

  // Checks IS NULL and IS NOT NULL view filters against rows with null values across columns.
  @Test
  public void testViewNullFilter() {
    String[] expectedRows = {
      defaultFormatDataTime(1) + ",null,false,11.1",
      defaultFormatDataTime(2) + ",22,true,null",
      defaultFormatDataTime(3) + ",23,null,33.3"
    };
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      int rowCount = 0;
      try (ResultSet resultSet =
          statement.executeQuery("select * from testNullFilter where s1 is null")) {
        while (resultSet.next()) {
          assertViewNullFilterRow(resultSet, expectedRows[rowCount++]);
        }
      }
      try (ResultSet resultSet =
          statement.executeQuery("select * from testNullFilter where s1 is not null")) {
        while (resultSet.next()) {
          assertViewNullFilterRow(resultSet, expectedRows[rowCount++]);
        }
      }
      assertEquals(expectedRows.length, rowCount);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void assertViewNullFilterRow(ResultSet resultSet, String expectedRow)
      throws SQLException {
    String actualRow =
        resultSet.getString("time")
            + ","
            + resultSet.getString("s1")
            + ","
            + resultSet.getString("s2")
            + ","
            + resultSet.getString("s3");
    assertEquals(expectedRow, actualRow);
  }
}
