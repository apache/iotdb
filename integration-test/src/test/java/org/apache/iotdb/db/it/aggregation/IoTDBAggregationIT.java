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

package org.apache.iotdb.db.it.aggregation;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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
import java.util.Locale;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.constant.TestConstant.maxTime;
import static org.apache.iotdb.db.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.constant.TestConstant.minTime;
import static org.apache.iotdb.db.constant.TestConstant.minValue;
import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationIT {

  private static final double DETLA = 1e-6;
  private static final String TIMESTAMP_STR = "Time";
  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";

  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE DATABASE root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.noDataRegion.s1 WITH DATATYPE=INT32"
      };
  private static final String[] dataSet2 =
      new String[] {
        "CREATE DATABASE root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)"
      };
  private static final String[] dataSet3 =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "insert into root.sg.d1(timestamp,s1) values(5,5)",
        "insert into root.sg.d1(timestamp,s1) values(12,12)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(15,15)",
        "insert into root.sg.d1(timestamp,s1) values(25,25)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(1,111)",
        "insert into root.sg.d1(timestamp,s1) values(20,200)",
        "flush",
      };
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // add test for part of points in page don't satisfy filter
  // details in: https://issues.apache.org/jira/projects/IOTDB/issues/IOTDB-54
  @Test
  public void test() {
    String[] retArray = new String[] {"0,2", "0,4", "0,3"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(temperature) FROM root.ln.wf01.wt01 WHERE time > 3")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(temperature) FROM root.ln.wf01.wt01 WHERE time > 3 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT min_time(temperature) FROM root.ln.wf01.wt01 WHERE time > 3")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minTime(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT min_time(temperature) FROM root.ln.wf01.wt01 WHERE temperature > 3")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minTime(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

      try {
        statement.executeQuery("SELECT max(root.temperature) from root.ln.wf01.wt01");
        fail();
      } catch (Exception ignored) {
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countTest() {
    String[] retArray = new String[] {"0,2001,2001,2001,2001", "0,7500,7500,7500,7500"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s0),count(s1),count(s2),count(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s0),count(s1),count(s2),count(s3) " + "FROM root.vehicle.d0")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s0),count(s1),count(s2),count(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s0),count(s1),count(s2),count(s3) "
                  + "FROM root.vehicle.d0 order by time desc")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void firstTest() {
    String[] retArray = new String[] {"0,2000,2000,2000.0,2000", "0,500,500,500.0,500"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT first_value(s0),first_value(s1),first_value(s2),first_value(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time <= 9000")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(firstValue(d0s0))
                  + ","
                  + resultSet.getString(firstValue(d0s1))
                  + ","
                  + resultSet.getString(firstValue(d0s2))
                  + ","
                  + resultSet.getString(firstValue(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT first_value(s0),first_value(s1),first_value(s2),first_value(s3) "
                  + "FROM root.vehicle.d0")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(firstValue(d0s0))
                  + ","
                  + resultSet.getString(firstValue(d0s1))
                  + ","
                  + resultSet.getString(firstValue(d0s2))
                  + ","
                  + resultSet.getString(firstValue(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT first_value(s0),first_value(s1),first_value(s2),first_value(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time <= 9000 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(firstValue(d0s0))
                  + ","
                  + resultSet.getString(firstValue(d0s1))
                  + ","
                  + resultSet.getString(firstValue(d0s2))
                  + ","
                  + resultSet.getString(firstValue(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastTest() {
    String[] retArray = new String[] {"0,8499,8499.0", "0,1499,1499.0", "0,2200,2200.0"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time < 9000")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue(d0s0))
                  + ","
                  + resultSet.getString(lastValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time <= 1600")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue(d0s0))
                  + ","
                  + resultSet.getString(lastValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time <= 2200")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue(d0s0))
                  + ","
                  + resultSet.getString(lastValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time <= 2200 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue(d0s0))
                  + ","
                  + resultSet.getString(lastValue(d0s2));
          Assert.assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      // keep the correctness of `order by time desc`
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time < 9000 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue(d0s0))
                  + ","
                  + resultSet.getString(lastValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void maxminTimeTest() {
    String[] retArray = new String[] {"0,8499,500", "0,2499,2000"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_time(s0),min_time(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxTime(d0s0))
                  + ","
                  + resultSet.getString(minTime(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_time(s0),min_time(s2) "
                  + "FROM root.vehicle.d0 WHERE time <= 2500 AND time > 1800")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxTime(d0s0))
                  + ","
                  + resultSet.getString(minTime(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_time(s0),min_time(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000 order by time desc")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxTime(d0s0))
                  + ","
                  + resultSet.getString(minTime(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void firstLastValueTest() throws SQLException {
    String[] retArray =
        new String[] {
          "0,2.2,4.4",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT first_value(temperature),last_value(temperature) "
                  + "FROM root.ln.wf01.wt01 WHERE time > 1 AND time < 5")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(1)
                  + ","
                  + resultSet.getString(2);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void maxminValueTest() {
    String[] retArray = new String[] {"0,8499,500.0", "0,2499,500.0"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_value(s0),min_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue(d0s0))
                  + ","
                  + resultSet.getString(minValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_value(s0),min_value(s2) " + "FROM root.vehicle.d0 WHERE time < 2500")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue(d0s0))
                  + ","
                  + resultSet.getString(minValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_value(s0),min_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000 order by time desc")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue(d0s0))
                  + ","
                  + resultSet.getString(minValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void avgSumTest() {
    double[][] retArray = {
      {0.0, 1.4508E7, 7250.374812593702},
      {0.0, 626750.0, 1250.9980039920158}
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s0),avg(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        while (resultSet.next()) {
          double[] ans = new double[3];
          ans[0] = Double.parseDouble(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.parseDouble(resultSet.getString(sum(d0s0)));
          ans[2] = Double.parseDouble(resultSet.getString(avg(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s0),avg(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 1000 AND time <= 2000")) {
        while (resultSet.next()) {
          double[] ans = new double[3];
          ans[0] = Double.parseDouble(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.parseDouble(resultSet.getString(sum(d0s0)));
          ans[2] = Double.parseDouble(resultSet.getString(avg(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s0),avg(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc")) {
        while (resultSet.next()) {
          double[] ans = new double[3];
          ans[0] = Double.parseDouble(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.parseDouble(resultSet.getString(sum(d0s0)));
          ans[2] = Double.parseDouble(resultSet.getString(avg(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void avgSumErrorTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT avg(s3) FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "Aggregate functions [AVG, SUM, EXTREME, MIN_VALUE, MAX_VALUE] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]"));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT sum(s3)" + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Aggregate functions [AVG, SUM, EXTREME, MIN_VALUE, MAX_VALUE] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]"));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT avg(s4)" + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Aggregate functions [AVG, SUM, EXTREME, MIN_VALUE, MAX_VALUE] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]"));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT sum(s4)" + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "Aggregate functions [AVG, SUM, EXTREME, MIN_VALUE, MAX_VALUE] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]"));
      }
      try {
        try (ResultSet resultSet =
            statement.executeQuery("SELECT avg(status) FROM root.ln.wf01.wt01")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "Aggregate functions [AVG, SUM, EXTREME, MIN_VALUE, MAX_VALUE] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** test aggregation query with more than one functions on one series */
  @Test
  public void mergeAggrOnOneSeriesTest() {
    double[][] retArray = {
      {0.0, 1.4508E7, 7250.374812593702, 7250.374812593702, 1.4508E7},
      {0.0, 626750.0, 1250.9980039920158, 1250.9980039920158, 626750.0},
      {0.0, 1.4508E7, 2001, 7250.374812593702, 7250.374812593702},
      {0.0, 1.4508E7, 2001, 7250.374812593702, 7250.374812593702, 2001, 1.4508E7}
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s0), avg(s2), avg(s0), sum(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        while (resultSet.next()) {
          double[] ans = new double[5];
          ans[0] = Double.parseDouble(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.parseDouble(resultSet.getString(sum(d0s0)));
          ans[2] = Double.parseDouble(resultSet.getString(avg(d0s2)));
          ans[3] = Double.parseDouble(resultSet.getString(avg(d0s0)));
          ans[4] = Double.parseDouble(resultSet.getString(sum(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s0), avg(s2), avg(s0), sum(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 1000 AND time <= 2000")) {
        while (resultSet.next()) {
          double[] ans = new double[5];
          ans[0] = Double.parseDouble(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.parseDouble(resultSet.getString(sum(d0s0)));
          ans[2] = Double.parseDouble(resultSet.getString(avg(d0s2)));
          ans[3] = Double.parseDouble(resultSet.getString(avg(d0s0)));
          ans[4] = Double.parseDouble(resultSet.getString(sum(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s0), count(s0), avg(s2), avg(s0)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        while (resultSet.next()) {
          double[] ans = new double[5];
          ans[0] = Double.parseDouble(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.parseDouble(resultSet.getString(sum(d0s0)));
          ans[2] = Double.parseDouble(resultSet.getString(count(d0s0)));
          ans[3] = Double.parseDouble(resultSet.getString(avg(d0s2)));
          ans[4] = Double.parseDouble(resultSet.getString(avg(d0s0)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s2), count(s0), avg(s2), avg(s1), count(s2),sum(s0)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000")) {
        while (resultSet.next()) {
          double[] ans = new double[7];
          ans[0] = Double.parseDouble(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.parseDouble(resultSet.getString(sum(d0s2)));
          ans[2] = Double.parseDouble(resultSet.getString(count(d0s0)));
          ans[3] = Double.parseDouble(resultSet.getString(avg(d0s2)));
          ans[4] = Double.parseDouble(resultSet.getString(avg(d0s1)));
          ans[5] = Double.parseDouble(resultSet.getString(count(d0s2)));
          ans[6] = Double.parseDouble(resultSet.getString(sum(d0s0)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(4, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void descAggregationWithUnseqData() {
    String[] retArray =
        new String[] {
          "0,12",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT max_time(s1) FROM root.sg.d1 where time < 15")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxTime("root.sg.d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      // prepare BufferWrite file
      for (int i = 5000; i < 7000; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("flush");

      for (int i = 7500; i < 8500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("flush");
      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("flush");
      for (int i = 3000; i < 6500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.executeBatch();
      statement.clearBatch();

      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.executeBatch();
      statement.clearBatch();
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.executeBatch();
      statement.clearBatch();

      for (String sql : dataSet3) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void noDataRegionTest() {
    String[] expectedHeader =
        new String[] {count("root.test.noDataRegion.s1"), sum("root.test.noDataRegion.s1")};
    String[] retArray = new String[] {"0,null,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1), sum(s1) from root.test.noDataRegion", expectedHeader, retArray);

    expectedHeader = new String[] {DEVICE, count("s1"), sum("s1")};
    retArray = new String[] {"root.test.noDataRegion,0,null,"};
    resultSetEqualTest(
        "select count(s1), sum(s1) from root.test.noDataRegion align by device",
        expectedHeader,
        retArray);
  }
}
