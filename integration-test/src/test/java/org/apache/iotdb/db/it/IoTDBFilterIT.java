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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBFilterIT {
  protected static final int ITERATION_TIMES = 10;

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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
      statement.execute(
          "create TIMESERIES root.vehicle.testNaN.d1 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testNaN.d2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testTimeSeries.s1 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute(
          "create TIMESERIES root.vehicle.testTimeSeries.s2 with datatype=BOOLEAN,encoding=PLAIN");
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
                "insert into root.vehicle.testNaN(timestamp,d1,d2) values(%d,%d,%d)", i, i, i));

        switch (i % 3) {
          case 0:
            statement.execute(
                String.format(
                    "insert into root.vehicle.testTimeSeries(timestamp,s1,s2) values(%d,true,true)",
                    i));
            break;
          case 1:
            statement.execute(
                String.format(
                    "insert into root.vehicle.testTimeSeries(timestamp,s1,s2) values(%d,true,false)",
                    i));
            break;
          case 2:
            statement.execute(
                String.format(
                    "insert into root.vehicle.testTimeSeries(timestamp,s1,s2) values(%d,false,false)",
                    i));
            break;
        }
      }
      statement.execute(
          " insert into root.sg1.d1(time, s1, s2) aligned values (1,1, \"1\"), (2,2,\"2\")");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testFilterBooleanSeries() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "root.vehicle.testTimeSeries.s1", "root.vehicle.testTimeSeries.s2"
        };
    String[] retArray =
        new String[] {"0,true,true,", "3,true,true,", "6,true,true,", "9,true,true,"};
    resultSetEqualTest(
        "select s1, s2 from root.vehicle.testTimeSeries " + "Where s2", expectedHeader, retArray);

    resultSetEqualTest(
        "select s1, s2 from root.vehicle.testTimeSeries " + "Where s1 && s2",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "0,true,true,",
          "1,true,false,",
          "3,true,true,",
          "4,true,false,",
          "6,true,true,",
          "7,true,false,",
          "9,true,true,"
        };
    resultSetEqualTest(
        "select s1, s2 from root.vehicle.testTimeSeries " + "Where s1 || s2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testFilterNaN() {
    String sqlStr = "select d1 from root.vehicle.testNaN where d1/d2 > 0";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr)) {
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select s2 from root.sg1.d1 where s1 = 1 and s2 >= \"1\" and s2 <= \"2\";")) {
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
    assertTestFail(
        "select s1 from root.sg1.d1 where s1;",
        "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: FLOAT.");
    assertTestFail(
        "select count(s1) from root.sg1.d1 group by ([0, 40), 5ms) having count(s1) + 1;",
        "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: DOUBLE.");
    assertTestFail(
        "select s1 from root.sg1.d1 where s1 align by device;",
        "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: FLOAT.");
    assertTestFail(
        "select count(s1) from root.sg1.d1 group by ([0, 40), 5ms) having count(s1) + 1 align by device;",
        "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: DOUBLE.");
  }
}
