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

import org.junit.AfterClass;
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
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFilterTableIT {
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
          "CREATE TABLE testNaN(device STRING ID, n1 DOUBLE MEASUREMENT, n2 DOUBLE MEASUREMENT)");
      statement.execute(
          "CREATE TABLE testTimeSeries(device STRING ID, s1 BOOLEAN MEASUREMENT, s2 BOOLEAN MEASUREMENT)");
      statement.execute(
          "CREATE TABLE testUDTF(device STRING ID, s1 TEXT MEASUREMENT, s2 DOUBLE MEASUREMENT)");
      statement.execute(
          "CREATE TABLE sg1(device STRING ID, s1 DOUBLE MEASUREMENT, s2 TEXT MEASUREMENT)");

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
        Statement statement = connection.createStatement();
        ResultSet containsResultSet =
            statement.executeQuery("select s1 from testUDTF where STRING_CONTAINS(s1, 's'='s')");
        ResultSet sinResultSet =
            statement.executeQuery("select s1 from testUDTF where sin(s2) = 0")) {
      int containsCnt = 0;
      while (containsResultSet.next()) {
        ++containsCnt;
      }
      assertEquals(1, containsCnt);

      int sinCnt = 0;
      while (sinResultSet.next()) {
        ++sinCnt;
      }
      assertEquals(1, sinCnt);
      assertTestFail(
          "select s1 from testUDTF where sin(s2)",
          "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: DOUBLE.");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
