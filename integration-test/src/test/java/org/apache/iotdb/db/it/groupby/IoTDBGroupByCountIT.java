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
package org.apache.iotdb.db.it.groupby;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByCountIT {
  // the data can be viewed in
  // https://docs.google.com/spreadsheets/d/1vsSmb41pdmK-BdBR1STwr8olg1Qc8baKVEWnfJB4mAg/edit#gid=0
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.sg.beijing.car01",
        "CREATE TIMESERIES root.sg.beijing.car01.charging_status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.beijing.car01.soc WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.beijing.car01.vehicle_status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(1, 1, 14, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2, 1, 16, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(3, 0, 16, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(4, 0, 16, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(5, 1, 18, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(6, 1, 24, 1)",
        "flush",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(7, 1, 36, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(8, null, 36, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(9, 1, 45, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(10, 1, 60, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(1100000000, null, 60, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(1200000000, null, 0, 0)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(1900000000, 1, 55, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2000000000, 1, 70, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2100000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2200000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2300000000, null, 69, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2400000000, 1, 80, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2500000000, 1, 100, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2600000000, 0, 101, 1)",
        "flush"
      };
  private static final String[] SQLs2 =
      new String[] {
        "CREATE DATABASE root.sg.beijing.car02",
        "CREATE TIMESERIES root.sg.beijing.car02.charging_status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.beijing.car02.soc WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.beijing.car02.vehicle_status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(1, 1, 14, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2, 1, 16, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(3, 0, 16, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(4, 0, 16, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(5, 1, 18, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(6, 1, 24, 1)",
        "flush",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(7, 1, 36, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(8, null, 36, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(9, 1, 45, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(10, 1, 60, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(1100000000, null, 60, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(1200000000, null, 0, 0)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(1900000000, 1, 55, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2000000000, 1, 70, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2100000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2200000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2300000000, null, 69, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2400000000, 1, 80, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2500000000, 1, 100, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2600000000, 0, 101, 1)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
    prepareData(SQLs2);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void checkHeader(ResultSetMetaData resultSetMetaData, String title) throws SQLException {
    String[] headers = title.split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  private void normalTest(String[][] res, String sql, boolean hasEndTime) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        String title =
            hasEndTime
                ? "Time,__endTime,sum(root.sg.beijing.car01.charging_status),count(root.sg.beijing.car01.vehicle_status),last_value(root.sg.beijing.car01.soc)"
                : "Time,sum(root.sg.beijing.car01.charging_status),count(root.sg.beijing.car01.vehicle_status),last_value(root.sg.beijing.car01.soc)";
        checkHeader(resultSetMetaData, title);
        int base = hasEndTime ? 1 : 0;
        int count = 0;
        while (resultSet.next()) {
          String startTime = resultSet.getString(1);
          String sum = resultSet.getString(2 + base);
          String countNum = resultSet.getString(3 + base);
          String lastValue = resultSet.getString(4 + base);
          assertEquals(res[count][0], startTime);
          assertEquals(res[count][2], sum);
          assertEquals(res[count][3], countNum);
          assertEquals(res[count][4], lastValue);
          if (hasEndTime) {
            String endTime = resultSet.getString(2);
            assertEquals(res[count][1], endTime);
          }
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void firstValueTest(String[][] res, String sql, boolean hasEndTime) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        String title =
            hasEndTime
                ? "Time,__endTime,first_value(root.sg.beijing.car01.soc)"
                : "Time,first_value(root.sg.beijing.car01.soc)";
        checkHeader(resultSetMetaData, title);
        int count = 0;
        while (resultSet.next()) {
          String startTime = resultSet.getString(1);
          assertEquals(res[count][0], startTime);
          if (hasEndTime) {
            String endTime = resultSet.getString(2);
            String first_value = resultSet.getString(3);
            assertEquals(res[count][1], endTime);
            assertEquals(res[count][2], first_value);
          } else {
            String first_value = resultSet.getString(2);
            assertEquals(res[count][2], first_value);
          }
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByCountNormalTest1() {
    String[][] res = {
      {"1", "5", "3.0", "5", "18"},
      {"6", "1900000000", "5.0", "5", "55"},
    };
    String sql =
        "select sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 5)";
    String sql2 =
        "select __endTime,sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 5)";
    normalTest(res, sql, false);
    normalTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest2() {
    String[][] res = {{"1", "2400000000", "10.0", "12", "80"}};
    String sql =
        "select sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 12)";
    String sql2 =
        "select __endTime,sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 12)";
    normalTest(res, sql, false);
    normalTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest3() {
    String[][] res = {
      {"1", "5", "14"},
      {"6", "1900000000", "24"},
    };
    String sql =
        "select first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 5)";
    String sql2 =
        "select __endTime,first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 5)";
    firstValueTest(res, sql, false);
    firstValueTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest4() {
    String[][] res = {
      {"1", "7", "14"},
      {"9", "2600000000", "45"},
    };
    String sql =
        "select first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 7)";
    String sql2 =
        "select __endTime,first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 7)";
    firstValueTest(res, sql, false);
    firstValueTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest5() {
    String[][] res = {
      {"1", "2", "14"},
      {"3", "4", "16"},
      {"5", "6", "18"},
      {"7", "9", "36"},
      {"10", "1900000000", "60"},
      {"2000000000", "2400000000", "70"},
      {"2500000000", "2600000000", "100"}
    };
    String sql =
        "select first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 2)";
    String sql2 =
        "select __endTime,first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 2)";
    firstValueTest(res, sql, false);
    firstValueTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest6() {
    String[][] res = {
      {"1", "2", "2.0", "2", "16"},
      {"3", "4", "0.0", "2", "16"},
      {"5", "6", "2.0", "2", "24"},
      {"7", "9", "2.0", "2", "45"},
      {"10", "1900000000", "2.0", "2", "55"},
      {"2000000000", "2400000000", "2.0", "2", "80"},
      {"2500000000", "2600000000", "1.0", "2", "101"}
    };
    String sql =
        "select sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 2)";
    String sql2 =
        "select __endTime,sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 2)";
    normalTest(res, sql, false);
    normalTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest7() {
    String[][] res = {
      {"3", "4", "0.0", "2", "16"},
      {"2500000000", "2600000000", "1.0", "2", "101"}
    };
    String sql =
        "select sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 2) having sum(charging_status)<2";
    normalTest(res, sql, false);
  }

  @Test
  public void groupByCountNormalTest8() {
    String[][] res = {
      {"1", "5", "3.0", "5", "18"},
      {"6", "10", "4.0", "5", "60"},
      {"1100000000", "2100000000", "2.0", "5", "70"},
      {"2200000000", "2600000000", "2.0", "5", "101"},
    };
    String sql =
        "select sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 5, ignoreNull=false)";
    String sql2 =
        "select __endTime,sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 5, ignoreNull=false)";
    normalTest(res, sql, false);
    normalTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest9() {
    String[][] res = {
      {"1", "2", "14"},
      {"3", "4", "16"},
      {"5", "6", "18"},
      {"7", "8", "36"},
      {"9", "10", "45"},
      {"1100000000", "1200000000", "60"},
      {"1900000000", "2000000000", "55"},
      {"2100000000", "2200000000", "70"},
      {"2300000000", "2400000000", "69"},
      {"2500000000", "2600000000", "100"}
    };
    String sql =
        "select first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 2, ignoreNull=false)";
    String sql2 =
        "select __endTime,first_value(soc) from root.sg.beijing.car01 group by count(charging_status, 2, ignoreNull=false)";
    firstValueTest(res, sql, false);
    firstValueTest(res, sql2, true);
  }

  @Test
  public void groupByCountNormalTest10() {
    String[][] res = {{"1", "1200000000", "7.0", "12", "0"}};
    String sql =
        "select sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 12, ignoreNull = false)";
    String sql2 =
        "select __endTime,sum(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by count(charging_status, 12, ignoreNull = false)";
    normalTest(res, sql, false);
    normalTest(res, sql2, true);
  }

  private void normalTestWithAlignByDevice(String[][] res, String sql, boolean hasEndTime) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        String title =
            hasEndTime
                ? "Time,Device,__endTime,sum(charging_status),count(vehicle_status),last_value(soc)"
                : "Time,Device,sum(charging_status),count(vehicle_status),last_value(soc)";
        checkHeader(resultSetMetaData, title);
        int count = 0;
        String expectedDevice = "root.sg.beijing.car01";
        int base = hasEndTime ? 1 : 0;
        while (resultSet.next()) {
          if (count == res.length) {
            count = 0;
            expectedDevice = "root.sg.beijing.car02";
          }
          String startTime = resultSet.getString(1);
          String device = resultSet.getString(2);
          String sum = resultSet.getString(3 + base);
          String countNum = resultSet.getString(4 + base);
          String lastValue = resultSet.getString(5 + base);
          assertEquals(expectedDevice, device);
          assertEquals(res[count][0], startTime);
          assertEquals(res[count][2], sum);
          assertEquals(res[count][3], countNum);
          assertEquals(res[count][4], lastValue);
          if (hasEndTime) {
            String endTime = resultSet.getString(3);
            assertEquals(res[count][1], endTime);
          }
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByCountAlignByDeviceTest() {
    String[][] res = {
      {"1", "2", "2.0", "2", "16"},
      {"3", "4", "0.0", "2", "16"},
      {"5", "6", "2.0", "2", "24"},
      {"7", "9", "2.0", "2", "45"},
      {"10", "1900000000", "2.0", "2", "55"},
      {"2000000000", "2400000000", "2.0", "2", "80"},
      {"2500000000", "2600000000", "1.0", "2", "101"}
    };
    String sql =
        "select sum(charging_status),count(vehicle_status),last_value(soc) from root.** group by count(charging_status, 2) align by device";
    String sql2 =
        "select __endTime,sum(charging_status),count(vehicle_status),last_value(soc) from root.** group by count(charging_status, 2) align by device";
    normalTestWithAlignByDevice(res, sql, false);
    normalTestWithAlignByDevice(res, sql2, true);
  }

  private void errorTest(String sql, String error) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
    } catch (Exception e) {
      assertEquals(error, e.getMessage());
    }
  }

  @Test
  public void errorTest1() {
    errorTest(
        "select count(temperature) from root.** group by count(soc, 2)",
        "701: root.**.soc in group by clause shouldn't refer to more than one timeseries.");
  }

  @Test
  public void errorTest2() {
    errorTest(
        "select count(soc) from root.sg.beijing.car01 group by count(count(soc),2)",
        "701: Aggregation expression shouldn't exist in group by clause");
  }

  @Test
  public void errorTest3() {
    errorTest(
        "select count(soc) from root.sg.beijing.car01 group by count(s1,2)",
        "701: root.sg.beijing.car01.s1 in group by clause doesn't exist.");
  }

  @Test
  public void errorTest4() {
    errorTest(
        "select count(soc) from root.sg.beijing.car01 group by count(s1,2) align by device",
        "701: s1 in group by clause doesn't exist.");
  }

  @Test
  public void errorTest5() {
    errorTest(
        "select count(soc) from root.sg.beijing.car01 group by count(root.sg.beijing.car01.soc,2) align by device",
        "701: ALIGN BY DEVICE: the suffix paths can only be measurement or one-level wildcard");
  }
}
