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

package org.apache.iotdb.relational.it.query.old;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.relational.it.query.view.old.IoTDBFilterTableViewBase;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFilterTableIT extends IoTDBFilterTableViewBase {
  protected static final int ITERATION_TIMES = 10;
  private static final String DATABASE_NAME = "test";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
    // Table views use a separate database while reusing this cluster's topology and configuration.
    insertViewData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE testNaN(device STRING TAG, n1 DOUBLE FIELD, n2 DOUBLE FIELD)");
      statement.execute(
          "CREATE TABLE testTimeSeries(device STRING TAG, s1 BOOLEAN FIELD, s2 BOOLEAN FIELD)");
      statement.execute("CREATE TABLE testUDTF(device STRING TAG, s1 TEXT FIELD, s2 DOUBLE FIELD)");
      statement.execute("CREATE TABLE sg1(device STRING TAG, s1 DOUBLE FIELD, s2 TEXT FIELD)");
      statement.execute(
          "CREATE TABLE testNullFilter(device STRING TAG, s1 INT32 FIELD, s2 BOOLEAN FIELD, s3 DOUBLE FIELD)");
      statement.execute(
          "CREATE TABLE betweenTable (device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD, s3 TEXT FIELD)");

    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      for (int i = 0; i < ITERATION_TIMES; i++) {
        statement.execute(
            String.format("insert into testNaN(time,device,n1,n2) values(%d,'d1',%d,%d)", i, i, i));

        switch (i % 3) {
          case 0:
            statement.execute(
                String.format(
                    "insert into testTimeSeries(time,device,s1,s2) values(%d,'d1',true,true)", i));
            break;
          case 1:
            statement.execute(
                String.format(
                    "insert into testTimeSeries(time,device,s1,s2) values(%d,'d1',true,false)", i));
            break;
          case 2:
            statement.execute(
                String.format(
                    "insert into testTimeSeries(time,device,s1,s2) values(%d,'d1',false,false)",
                    i));
            break;
        }
      }
      statement.execute(" insert into sg1(time, device, s1, s2) values (1,'d1',1,'1')");
      statement.execute(" insert into sg1(time, device, s1, s2) values (2,'d1',2,'2')");
      statement.execute(" insert into testUDTF(time, device, s1, s2) values (1,'d1','ss',0)");
      statement.execute(" insert into testUDTF(time, device, s1, s2) values (2,'d1','d',3)");
      statement.execute(
          "insert into testNullFilter(time,device,s2,s3) values(1, 'd1', false, 11.1)");
      statement.execute("insert into testNullFilter(time,device,s1,s2) values(2, 'd1', 22, true)");
      statement.execute("insert into testNullFilter(time,device,s1,s3) values(3, 'd1', 23, 33.3)");
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into betweenTable(device,time,s1,s2,s3) values('d1',%d,%d,%d,%s)",
                i, i, i, i));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testFilterBooleanSeries() {
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

  @Test
  public void testFilterNaN() {
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

  @Test
  public void testSameConstantWithDifferentType() {
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

  @Test
  public void testMismatchedDataTypes() {
    tableAssertTestFail(
        "select s1 from sg1 where s1",
        "701: WHERE clause must evaluate to a boolean: actual type DOUBLE",
        DATABASE_NAME);
    // TODO After Aggregation supported
    /*assertTestFail(
        "select count(s1) from root.sg1.d1 group by ([0, 40), 5ms) having count(s1) + 1;",
        "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: DOUBLE.");
    assertTestFail(
        "select count(s1) from root.sg1.d1 group by ([0, 40), 5ms) having count(s1) + 1 align by device;",
        "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: DOUBLE.");*/
  }

  @Ignore // TODO After UDTF supported
  @Test
  public void testFilterWithUDTF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int containsCnt = 0;
      try (ResultSet containsResultSet =
          statement.executeQuery("select s1 from testUDTF where STRING_CONTAINS(s1, 's'='s')")) {
        while (containsResultSet.next()) {
          ++containsCnt;
        }
      }
      assertEquals(1, containsCnt);

      int sinCnt = 0;
      try (ResultSet sinResultSet =
          statement.executeQuery("select s1 from testUDTF where sin(s2) = 0")) {
        while (sinResultSet.next()) {
          ++sinCnt;
        }
      }
      assertEquals(1, sinCnt);
      assertTestFail(
          "select s1 from testUDTF where sin(s2)",
          "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: DOUBLE.");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCompareWithNull() {
    tableResultSetEqualTest(
        "select s1 from sg1 where s1 != null", new String[] {"s1"}, new String[] {}, DATABASE_NAME);
    tableResultSetEqualTest(
        "select s1 from sg1 where s1 <> null", new String[] {"s1"}, new String[] {}, DATABASE_NAME);
    tableResultSetEqualTest(
        "select s1 from sg1 where s1 = null", new String[] {"s1"}, new String[] {}, DATABASE_NAME);
  }

  @Test
  public void testCalculateWithNull() {
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

  // Checks BETWEEN and NOT BETWEEN bounds on fields and time, including literal expression bounds.
  @Test
  public void testBetweenExpression() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      int start = 1;
      int end = 5;
      String query = "SELECT * FROM betweenTable WHERE s1 BETWEEN " + start + " AND " + end;
      assertBetweenQuery(statement, query, start, end);

      query =
          "SELECT * FROM betweenTable WHERE s1 NOT BETWEEN "
              + (end + 1)
              + " AND "
              + ITERATION_TIMES;
      assertBetweenQuery(statement, query, start, end);

      query = "SELECT * FROM betweenTable WHERE time BETWEEN " + start + " AND " + end;
      assertBetweenQuery(statement, query, start, end);

      query =
          "SELECT * FROM betweenTable WHERE time NOT BETWEEN "
              + (end + 1)
              + " AND "
              + ITERATION_TIMES;
      assertBetweenQuery(statement, query, start, end);

      query = "SELECT * FROM betweenTable WHERE " + start + " BETWEEN time AND " + end;
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertTrue(resultSet.next());
        assertBetweenRow(resultSet, 1);
        Assert.assertFalse(resultSet.next());
      }

      query = "SELECT * FROM betweenTable WHERE " + start + " NOT BETWEEN time AND " + end;
      try (ResultSet resultSet = statement.executeQuery(query)) {
        for (int row = start + 1; row <= ITERATION_TIMES; row++) {
          Assert.assertTrue(resultSet.next());
          assertBetweenRow(resultSet, row);
        }
        Assert.assertFalse(resultSet.next());
      }

      String[] expectedHeader = {"_col0", "_col1"};
      String[] expectedRows = {"true,true,", "true,false,", "false,false,"};
      tableResultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from betweenTable where time between 1 and 3",
          expectedHeader,
          expectedRows,
          DATABASE_NAME);
      tableResultSetEqualTest(
          "select s1 between 1 and 2 as r1, time between 0 and 1 as r2 from betweenTable where time between 1 and 3",
          new String[] {"r1", "r2"},
          expectedRows,
          DATABASE_NAME);
      tableResultSetEqualTest(
          "select s1 between 1 and 2, time between 0 and 1 from betweenTable where time between 1 and 3 order by device",
          expectedHeader,
          expectedRows,
          DATABASE_NAME);
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private static void assertBetweenQuery(Statement statement, String query, int start, int end)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery(query)) {
      for (int row = start; row <= end; row++) {
        Assert.assertTrue(resultSet.next());
        assertBetweenRow(resultSet, row);
      }
      Assert.assertFalse(resultSet.next());
    }
  }

  private static void assertBetweenRow(ResultSet resultSet, int row) throws SQLException {
    Assert.assertEquals(defaultFormatDataTime(row), resultSet.getString("time"));
    Assert.assertEquals(String.valueOf(row), resultSet.getString("s1"));
    Assert.assertEquals(String.valueOf(row), resultSet.getString("s2"));
    Assert.assertEquals(String.valueOf(row), resultSet.getString("s3"));
  }

  // Checks IS NULL and IS NOT NULL partition rows with null values across several columns.
  @Test
  public void nullFilterTest() {
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
          assertNullFilterRow(resultSet, expectedRows[rowCount++]);
        }
      }
      try (ResultSet resultSet =
          statement.executeQuery("select * from testNullFilter where s1 is not null")) {
        while (resultSet.next()) {
          assertNullFilterRow(resultSet, expectedRows[rowCount++]);
        }
      }
      assertEquals(expectedRows.length, rowCount);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void assertNullFilterRow(ResultSet resultSet, String expectedRow)
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
