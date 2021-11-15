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
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.junit.Assert.fail;

public class IoTDBGroupByMonthFillIT {

  private static final String TIMESTAMP_STR = "Time";
  private final DateFormat df = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss");

  @Before
  public void setUp() throws Exception {
    df.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /** Test StartTime: 2020-02-15, EndTime: 2020-11-15 PreviousFill beforeRange = 1mo */
  @Test
  public void previousFillTest1() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] retArray1 = {
        "02/15/2020:02:00:00", "1.0",
        "03/15/2020:02:00:00", "3.0",
        "04/15/2020:02:00:00", "3.0",
        "05/15/2020:02:00:00", null,
        "06/15/2020:02:00:00", "6.0",
        "07/15/2020:02:00:00", "6.0",
        "08/15/2020:02:00:00", null,
        "09/15/2020:02:00:00", "9.0",
        "10/15/2020:02:00:00", "9.0",
        "11/15/2020:02:00:00", null,
      };

      ((IoTDBConnection) connection).setTimeZone("GMT+00:00");
      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 "
                  + "GROUP BY ([1581732000000, 1607997600000), 1mo) "
                  + "FILL(ALL[previous, 1mo])");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String time = resultSet.getString(TIMESTAMP_STR);
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          Assert.assertEquals(retArray1[cnt++], df.format(Long.parseLong(time)));
          Assert.assertEquals(retArray1[cnt++], ans);
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test StartTime: 2020-02-15, EndTime: 2020-11-15 PreviousFill beforeRange = 2mo */
  @Test
  public void previousFillTest2() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] retArray1 = {
        "02/15/2020:02:00:00", "1.0",
        "03/15/2020:02:00:00", "3.0",
        "04/15/2020:02:00:00", "3.0",
        "05/15/2020:02:00:00", "3.0",
        "06/15/2020:02:00:00", "6.0",
        "07/15/2020:02:00:00", "6.0",
        "08/15/2020:02:00:00", "6.0",
        "09/15/2020:02:00:00", "9.0",
        "10/15/2020:02:00:00", "9.0",
        "11/15/2020:02:00:00", "9.0",
      };

      ((IoTDBConnection) connection).setTimeZone("GMT+00:00");
      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 "
                  + "GROUP BY ([1581732000000, 1607997600000), 1mo) "
                  + "FILL(ALL[previous, 2mo])");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String time = resultSet.getString(TIMESTAMP_STR);
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          Assert.assertEquals(retArray1[cnt++], df.format(Long.parseLong(time)));
          Assert.assertEquals(retArray1[cnt++], ans);
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test StartTime: 2020-02-15, EndTime: 2020-11-15 PreviousFill beforeRange = 1mo, afterRange =
   * 1mo
   */
  @Test
  public void LinearFillTest1() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] retArray1 = {
        "02/15/2020:02:00:00", "2.0",
        "03/15/2020:02:00:00", "3.0",
        "04/15/2020:02:00:00", null,
        "05/15/2020:02:00:00", null,
        "06/15/2020:02:00:00", "6.0",
        "07/15/2020:02:00:00", null,
        "08/15/2020:02:00:00", null,
        "09/15/2020:02:00:00", "9.0",
        "10/15/2020:02:00:00", null,
        "11/15/2020:02:00:00", null,
      };

      ((IoTDBConnection) connection).setTimeZone("GMT+00:00");
      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 "
                  + "GROUP BY ([1581732000000, 1607997600000), 1mo) "
                  + "FILL(ALL[linear, 1mo, 1mo])");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String time = resultSet.getString(TIMESTAMP_STR);
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          Assert.assertEquals(retArray1[cnt++], df.format(Long.parseLong(time)));
          if (retArray1[cnt] == null) {
            Assert.assertNull(ans);
          } else {
            Assert.assertEquals(Double.valueOf(retArray1[cnt]), Double.valueOf(ans), 0.1);
          }
          ++cnt;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test StartTime: 2020-02-15, EndTime: 2020-11-15 PreviousFill beforeRange = 2mo, afterRange =
   * 2mo
   */
  @Test
  public void LinearFillTest2() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] retArray1 = {
        "02/15/2020:02:00:00", "2.0",
        "03/15/2020:02:00:00", "3.0",
        "04/15/2020:02:00:00", "4.0",
        "05/15/2020:02:00:00", "5.0",
        "06/15/2020:02:00:00", "6.0",
        "07/15/2020:02:00:00", "7.0",
        "08/15/2020:02:00:00", "8.0",
        "09/15/2020:02:00:00", "9.0",
        "10/15/2020:02:00:00", "10.0",
        "11/15/2020:02:00:00", "11.0",
      };

      ((IoTDBConnection) connection).setTimeZone("GMT+00:00");
      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 "
                  + "GROUP BY ([1581732000000, 1607997600000), 1mo) "
                  + "FILL(ALL[linear, 2mo, 2mo])");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String time = resultSet.getString(TIMESTAMP_STR);
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          Assert.assertEquals(retArray1[cnt++], df.format(Long.parseLong(time)));
          Assert.assertEquals(Double.valueOf(retArray1[cnt++]), Double.valueOf(ans), 0.1);
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // 2020-01-15
      statement.execute(
          "insert into root.sg1.d1(timestamp, temperature) " + "values (1579053600000, 1)");
      // 2020-03-16
      statement.execute(
          "insert into root.sg1.d1(timestamp, temperature) " + "values (1584324000000, 3)");
      // 2020-06-17
      statement.execute(
          "insert into root.sg1.d1(timestamp, temperature) " + "values (1592359200000, 6)");
      // 2020-09-18
      statement.execute(
          "insert into root.sg1.d1(timestamp, temperature) " + "values (1600394400000, 9)");
      // 2020-12-19
      statement.execute(
          "insert into root.sg1.d1(timestamp, temperature) " + "values (1608343200000, 12)");

      statement.execute("flush");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
