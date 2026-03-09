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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFilterTableViewIT {
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
    createTableView();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
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
}
