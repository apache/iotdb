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
public class IoTDBGroupByConditionIT {
  // the data can be viewed in
  // https://docs.google.com/spreadsheets/d/1vsSmb41pdmK-BdBR1STwr8olg1Qc8baKVEWnfJB4mAg/edit#gid=0
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.sg.beijing.car01",
        "CREATE TIMESERIES root.sg.beijing.car01.charging_status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.beijing.car01.soc WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
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
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(110000000, null, 60, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(120000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(130000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(140000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(150000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(160000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(170000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(180000000, null, null, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(1900000000, 1, 55, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2000000000, 1, 70, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2100000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2200000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2300000000, null, 69, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2400000000, 1, 80, 1)",
        "INSERT INTO root.sg.beijing.car01(timestamp, charging_status, soc, vehicle_status) values(2500000000, 1, 100, 1)",
        "flush"
      };

  private static final String[] SQLs2 =
      new String[] {
        "CREATE DATABASE root.sg.beijing.car02",
        "CREATE TIMESERIES root.sg.beijing.car02.charging_status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.beijing.car02.soc WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.beijing.car02.vehicle_status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(1, 1, 14, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2, 1, 16, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(3, 0, 16, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(4, 0, 16, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(5, 1, 18, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(6, 1, 24, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(7, 1, 36, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(8, null, 36, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(9, 1, 45, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(10, 1, 60, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(110000000, null, 60, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(120000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(130000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(140000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(150000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(160000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(170000000, null, null, 0)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(180000000, null, null, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(1900000000, 1, 55, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2000000000, 1, 70, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2100000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2200000000, null, 70, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2300000000, null, 69, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2400000000, 1, 80, 1)",
        "INSERT INTO root.sg.beijing.car02(timestamp, charging_status, soc, vehicle_status) values(2500000000, 1, 100, 1)",
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

  @Test
  public void groupByConditionTest1() {
    String[][] res =
        new String[][] {
          {"1", "2", "1.0", "2", "16.0"},
          {"5", "7", "2.0", "3", "36.0"},
          {"9", "10", "1.0", "2", "60.0"},
          {"1900000000", "2000000000", "100000000.0", "2", "70.0"},
          {"2400000000", "2500000000", "100000000.0", "2", "100.0"}
        };
    String sql =
        "select __endTime,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=1,KEEP>=2,ignoreNull=false)";
    normalTestWithEndTime(res, sql);
    String sql2 =
        "select max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=1,KEEP>=2,ignoreNull=false)";
    normalTest(res, sql2);
  }

  @Test
  public void groupByConditionTest2() {
    String[][] res =
        new String[][] {
          {"1", "2", "1.0", "2", "16.0"},
          {"5", "2500000000", "2499999995.0", "9", "100.0"}
        };
    String sql2 =
        "select max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=1,KEEP>=2,ignoreNull=true)";
    normalTest(res, sql2);
  }

  @Test
  public void groupByConditionTest3() {
    String[][] res =
        new String[][] {
          {"5", "2500000000", "2499999995.0", "9", "100.0"},
        };
    String sql =
        "select __endTime,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=1,KEEP>=3,ignoreNull=true)";
    normalTestWithEndTime(res, sql);
  }

  @Test
  public void groupByConditionTest4() {
    String[][] res =
        new String[][] {
          {"5", "7", "2.0", "3", "36.0"},
        };
    String sql2 =
        "select max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=1,KEEP>=3,ignoreNull=false)";
    normalTest(res, sql2);
  }

  @Test
  public void groupByConditionTest5() {
    String[][] res =
        new String[][] {
          {"3", "4", "1.0", "2", "16.0"},
        };
    String sql =
        "select __endTime,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=0,KEEP=2,ignoreNull=true)";
    normalTestWithEndTime(res, sql);
  }

  @Test
  public void groupByConditionTest6() {
    String[][] res =
        new String[][] {
          {"6", "2500000000", "2499999994", "13", "100.0"},
        };
    String sql =
        "select __endTime,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(soc>=24.0,KEEP<=15)";
    normalTestWithEndTime(res, sql);
  }

  private void normalTestWithEndTime(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,__endTime,max_time(root.sg.beijing.car01.charging_status) - min_time(root.sg.beijing.car01.charging_status),count(root.sg.beijing.car01.vehicle_status),last_value(root.sg.beijing.car01.soc)");
        int count = 0;
        while (resultSet.next()) {
          String startTime = resultSet.getString(1);
          String endTime = resultSet.getString(2);
          double duration = resultSet.getDouble(3);
          String countNum = resultSet.getString(4);
          String lastValue = resultSet.getString(5);
          assertEquals(res[count][0], startTime);
          assertEquals(res[count][1], endTime);
          assertEquals(Double.parseDouble(res[count][2]), duration, 0.0000000001);
          assertEquals(res[count][3], countNum);
          assertEquals(res[count][4], lastValue);
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void normalTest(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,max_time(root.sg.beijing.car01.charging_status) - min_time(root.sg.beijing.car01.charging_status),count(root.sg.beijing.car01.vehicle_status),last_value(root.sg.beijing.car01.soc)");
        int count = 0;
        while (resultSet.next()) {
          String startTime = resultSet.getString(1);
          double duration = resultSet.getDouble(2);
          String countNum = resultSet.getString(3);
          String lastValue = resultSet.getString(4);
          assertEquals(res[count][0], startTime);
          assertEquals(Double.parseDouble(res[count][2]), duration, 0.0000000001);
          assertEquals(res[count][3], countNum);
          assertEquals(res[count][4], lastValue);
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void normalTestWithEndTimeAlignByDevice(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,__endTime,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc)");
        int count = 0;
        int rowNum = res.length;
        String device = "root.sg.beijing.car01";
        while (resultSet.next()) {
          if (count == rowNum) {
            count = 0;
            device = "root.sg.beijing.car02";
          }
          String startTime = resultSet.getString(1);
          String deviceName = resultSet.getString(2);
          String endTime = resultSet.getString(3);
          double duration = resultSet.getDouble(4);
          String countNum = resultSet.getString(5);
          String lastValue = resultSet.getString(6);
          assertEquals(res[count][0], startTime);
          assertEquals(res[count][1], endTime);
          assertEquals(device, deviceName);
          assertEquals(Double.parseDouble(res[count][2]), duration, 0.0000000001);
          assertEquals(res[count][3], countNum);
          assertEquals(res[count][4], lastValue);
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void normalTestAlignByDevice(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc)");
        int count = 0;
        int rowNum = res.length;
        String device = "root.sg.beijing.car01";
        while (resultSet.next()) {
          if (count == rowNum) {
            count = 0;
            device = "root.sg.beijing.car02";
          }
          String startTime = resultSet.getString(1);
          String deviceName = resultSet.getString(2);
          double duration = resultSet.getDouble(3);
          String countNum = resultSet.getString(4);
          String lastValue = resultSet.getString(5);
          assertEquals(res[count][0], startTime);
          assertEquals(device, deviceName);
          assertEquals(Double.parseDouble(res[count][2]), duration, 0.0000000001);
          assertEquals(res[count][3], countNum);
          assertEquals(res[count][4], lastValue);
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
  public void groupByConditionTestAlignByDevice() {
    String[][] res =
        new String[][] {
          {"1", "2", "1.0", "2", "16.0"},
          {"5", "2500000000", "2499999995.0", "9", "100.0"}
        };
    String sql =
        "select __endTime,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.** group by condition(charging_status=1,KEEP>=2,ignoreNull=true) align by device";
    normalTestWithEndTimeAlignByDevice(res, sql);
    String sql2 =
        "select max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.** group by condition(charging_status=1,KEEP>=2,ignoreNull=true) align by device";
    normalTestAlignByDevice(res, sql2);
  }

  @Test
  public void groupByConditionTestWithHaving() {
    String[][] res =
        new String[][] {
          {"9", "10", "1.0", "2", "60.0"},
          {"1900000000", "2000000000", "100000000.0", "2", "70.0"},
          {"2400000000", "2500000000", "100000000.0", "2", "100.0"}
        };
    String sql =
        "select __endTime,max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=1,KEEP>=2,ignoreNull=false) having last_value(soc)>50";
    normalTestWithEndTime(res, sql);
    String sql2 =
        "select max_time(charging_status) - min_time(charging_status),count(vehicle_status),last_value(soc) from root.sg.beijing.car01 group by condition(charging_status=1,KEEP>=2,ignoreNull=false) having last_value(soc)>50";
    normalTest(res, sql2);
  }

  @Test
  public void groupByConditionFirstValueTest() {
    String[][] res = new String[][] {{"5", "7", "18.0"}};
    String sql =
        "select first_value(soc) from root.sg.beijing.car01 group by condition(charging_status!=0,KEEP>2,ignoreNull=false)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,first_value(root.sg.beijing.car01.soc)");
        int count = 0;
        while (resultSet.next()) {
          String startTime = resultSet.getString(1);
          String firstValue = resultSet.getString(2);
          assertEquals(res[count][0], startTime);
          assertEquals(res[count][2], firstValue);
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
