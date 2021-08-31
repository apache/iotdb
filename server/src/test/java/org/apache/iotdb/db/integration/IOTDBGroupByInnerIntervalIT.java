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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.apache.iotdb.db.constant.TestConstant.*;
import static org.junit.Assert.*;

public class IOTDBGroupByInnerIntervalIT {

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
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
            + "values(5, 5.5, false, 55)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(6, 6.6, false, 66)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(7, 7.7, true, 77)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(8, 8.8, false, 88)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(9, 9.9, false, 99)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(10, 10.0, false, 110)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(11, 11.1, false, 121)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(12, 12.2, true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(13, 13.3, false, 330)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(14, 14.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(15, 15.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(16, 16.6, false, 660)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(18, 18.8, true, 780)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(22, 22.2, false, 220 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(23, 23.3, false, 650)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(24, 24.4, false, 760)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(25, 25.5, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(26, 20.2, true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(27, 30.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(28, 40.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(29, 50.5, false, 550)",
      };

  private static final double DETLA = 1e-6;

  private static final String TIMESTAMP_STR = "Time";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void countSumAvgInnerIntervalTest() {
    double[][] retArray1 = {
      {1.0, 3.0, 6.6, 2.2},
      {6.0, 3.0, 23.1, 7.7},
      {11.0, 3.0, 36.6, 12.2},
      {16.0, 2.0, 35.4, 17.7},
      {21.0, 2.0, 45.5, 22.75},
      {26.0, 3.0, 90.9, 30.3}
    };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([1, 30), 3ms, 5ms)");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[4];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(count("root.ln.wf01.wt01.temperature")));
          ans[2] = Double.valueOf(resultSet.getString(sum("root.ln.wf01.wt01.temperature")));
          ans[3] = Double.valueOf(resultSet.getString(avg("root.ln.wf01.wt01.temperature")));
          assertArrayEquals(retArray1[cnt], ans, DETLA);

          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countSumAvgInnerIntervalTestWithValueFilter() {
    double[][] retArray1 = {
      {1.0, 1.0, 3.3, 3.3},
      {6.0, 3.0, 23.1, 7.7},
      {11.0, 3.0, 36.6, 12.2},
      {16.0, 2.0, 35.4, 17.7},
      {21.0, 2.0, 45.5, 22.75},
      {26.0, 3.0, 90.9, 30.3}
    };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3"
                  + " GROUP BY ([1, 30), 3ms, 5ms)");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[4];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(count("root.ln.wf01.wt01.temperature")));
          ans[2] = Double.valueOf(resultSet.getString(sum("root.ln.wf01.wt01.temperature")));
          ans[3] = Double.valueOf(resultSet.getString(avg("root.ln.wf01.wt01.temperature")));
          assertArrayEquals(retArray1[cnt], ans, DETLA);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }

      double[][] retArray2 = {
        {26.0, 3.0, 90.9, 30.3},
        {21.0, 2.0, 45.5, 22.75},
        {16.0, 2.0, 35.4, 17.7},
        {11.0, 3.0, 36.6, 12.2},
        {6.0, 3.0, 23.1, 7.7},
        {1.0, 1.0, 3.3, 3.3}
      };

      hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3"
                  + " GROUP BY ([1, 30), 3ms, 5ms) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[4];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(count("root.ln.wf01.wt01.temperature")));
          ans[2] = Double.valueOf(resultSet.getString(sum("root.ln.wf01.wt01.temperature")));
          ans[3] = Double.valueOf(resultSet.getString(avg("root.ln.wf01.wt01.temperature")));
          assertArrayEquals(retArray2[cnt], ans, DETLA);
          cnt++;
        }
        assertEquals(retArray2.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countSumAvgInnerIntervalTestWithTimeFilter() {
    String retArray = "1,0,0.0,null";
    double[][] retArray1 = {
      {6.0, 3.0, 23.1, 7.7},
      {11.0, 3.0, 36.6, 12.2},
      {16.0, 2.0, 35.4, 17.7},
      {21.0, 2.0, 45.5, 22.75},
      {26.0, 3.0, 90.9, 30.3}
    };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where time > 3"
                  + " GROUP BY ([1, 30), 3ms, 5ms)");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        if (resultSet.next()) {
          String res =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray, res);
        }
        cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[4];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(count("root.ln.wf01.wt01.temperature")));
          ans[2] = Double.valueOf(resultSet.getString(sum("root.ln.wf01.wt01.temperature")));
          ans[3] = Double.valueOf(resultSet.getString(avg("root.ln.wf01.wt01.temperature")));
          assertArrayEquals(retArray1[cnt], ans, DETLA);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void negativeOrZeroTimeInterval() {

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where time > 3"
                  + "GROUP BY ([1, 30), 0ms)");
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof IoTDBSQLException);
    }

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where time > 3"
                  + "GROUP BY ([1, 30), -1ms)");
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }

  @Test
  public void slidingStepLessThanTimeInterval() {

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where time > 3"
                  + "GROUP BY ([1, 30), 2ms, 1ms)");
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof IoTDBSQLException);
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
