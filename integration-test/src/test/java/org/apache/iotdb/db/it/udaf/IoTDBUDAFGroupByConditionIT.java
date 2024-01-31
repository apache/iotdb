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

package org.apache.iotdb.db.it.udaf;

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
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFGroupByConditionIT {
  private final double DELTA = 1E-10;

  private static final String[] dataset =
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
        "flush",
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
    prepareData(dataset);
    registerUDAF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void registerUDAF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION count_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void UDAFGroupByConditionNotIgnoreNullTest() {
    String[][] expected =
        new String[][] {
          {"1", "2", "1.0", "2", "16.0"},
          {"5", "7", "2.0", "3", "36.0"},
          {"9", "10", "1.0", "2", "60.0"},
          {"1900000000", "2000000000", "100000000.0", "2", "70.0"},
          {"2400000000", "2500000000", "100000000.0", "2", "100.0"}
        };

    String sqlWithEndTime =
        "SELECT __endTime, max_time(charging_status) - min_time(charging_status), "
            + "count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY CONDITION(charging_status=1, KEEP>=2, ignoreNull=false)";
    UDAFGroupByConditionITChecker(sqlWithEndTime, expected, true);

    String sqlWithoutEndTime =
        "SELECT max_time(charging_status) - min_time(charging_status), "
            + "count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY CONDITION(charging_status=1, KEEP>=2, ignoreNull=false)";
    UDAFGroupByConditionITChecker(sqlWithoutEndTime, expected, false);
  }

  @Test
  public void UDAFGroupByConditionIgnoreNullTest() {
    String[][] expected =
        new String[][] {
          {"1", "2", "1.0", "2", "16.0"},
          {"5", "2500000000", "2499999995.0", "9", "100.0"}
        };

    String sql =
        "SELECT max_time(charging_status) - min_time(charging_status), "
            + "count_udaf(vehicle_status),last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY CONDITION(charging_status=1, KEEP>=2, ignoreNull=true)";
    UDAFGroupByConditionITChecker(sql, expected, false);
  }

  @Test
  public void UDAFGroupByConditionKeepTest() {
    String[][] expected =
        new String[][] {
          {"6", "2500000000", "2499999994", "13", "100.0"},
        };
    String sql =
        "SELECT __endTime, max_time(charging_status) - min_time(charging_status),"
            + "count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 GROUP BY CONDITION(soc>=24.0, KEEP<=15)";
    UDAFGroupByConditionITChecker(sql, expected, true);
  }

  @Test
  public void UDAFGroupByConditionTestAlignByDevice() {
    String[][] expected =
        new String[][] {
          {"1", "root.sg.beijing.car01", "2", "1.0", "2", "16.0"},
          {"5", "root.sg.beijing.car01", "2500000000", "2499999995.0", "9", "100.0"},
          {"1", "root.sg.beijing.car02", "2", "1.0", "2", "16.0"},
          {"5", "root.sg.beijing.car02", "2500000000", "2499999995.0", "9", "100.0"},
        };

    String sqlWithEndTime =
        "SELECT __endTime,max_time(charging_status) - min_time(charging_status),"
            + "count_udaf(vehicle_status),last_value(soc) "
            + "FROM root.** "
            + "GROUP BY CONDITION(charging_status=1, KEEP>=2, ignoreNull=true) "
            + "ALIGN BY DEVICE";
    UDAFGroupByConditionITAlignByDeviceChecker(sqlWithEndTime, expected, true);

    String sqlWithoutEndTime =
        "SELECT max_time(charging_status) - min_time(charging_status),"
            + "count_udaf(vehicle_status),last_value(soc) "
            + "FROM root.** "
            + "GROUP BY CONDITION(charging_status=1, KEEP>=2, ignoreNull=true) "
            + "ALIGN BY DEVICE";
    UDAFGroupByConditionITAlignByDeviceChecker(sqlWithoutEndTime, expected, false);
  }

  @Test
  public void UDAFGroupByConditionTestWithHaving() {
    String[][] expected =
        new String[][] {
          {"9", "10", "1.0", "2", "60.0"},
          {"1900000000", "2000000000", "100000000.0", "2", "70.0"},
          {"2400000000", "2500000000", "100000000.0", "2", "100.0"}
        };

    String sqlWithEndTime =
        "SELECT __endTime,max_time(charging_status) - min_time(charging_status),"
            + "count_udaf(vehicle_status),last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY CONDITION(charging_status=1, KEEP>=2, ignoreNull=false) "
            + "HAVING last_value(soc)>50";
    UDAFGroupByConditionITChecker(sqlWithEndTime, expected, true);

    String sqlWithoutEndTime =
        "SELECT max_time(charging_status) - min_time(charging_status),"
            + "count_udaf(vehicle_status),last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY CONDITION(charging_status=1, KEEP>=2, ignoreNull=false) "
            + "HAVING last_value(soc)>50";
    UDAFGroupByConditionITChecker(sqlWithoutEndTime, expected, false);
  }

  private void UDAFGroupByConditionITChecker(String sql, String[][] expected, boolean hasEndTime) {
    String targetSeries1 = "root.sg.beijing.car01.charging_status";
    String targetSeries2 = "root.sg.beijing.car01.vehicle_status";
    String targetSeries3 = "root.sg.beijing.car01.soc";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String startTime = resultSet.getString(TIMESTAMP_STR);
          String endTime = hasEndTime ? resultSet.getString(END_TIMESTAMP_STR) : null;
          double duration =
              resultSet.getDouble(maxTime(targetSeries1) + " - " + minTime(targetSeries1));
          String countNum = resultSet.getString(countUDAF(targetSeries2));
          String lastValue = resultSet.getString(lastValue(targetSeries3));

          assertEquals(expected[count][0], startTime);
          if (hasEndTime) {
            assertEquals(expected[count][1], endTime);
          }
          assertEquals(Double.parseDouble(expected[count][2]), duration, DELTA);
          assertEquals(expected[count][3], countNum);
          assertEquals(expected[count][4], lastValue);

          count++;
        }

        assertEquals(expected.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void UDAFGroupByConditionITAlignByDeviceChecker(
      String sql, String[][] expected, boolean hasEndTime) {
    String targetSeries1 = "charging_status";
    String targetSeries2 = "vehicle_status";
    String targetSeries3 = "soc";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String startTime = resultSet.getString(TIMESTAMP_STR);
          String deviceName = resultSet.getString(DEVICE);
          String endTime = hasEndTime ? resultSet.getString(END_TIMESTAMP_STR) : null;
          double duration =
              resultSet.getDouble(maxTime(targetSeries1) + " - " + minTime(targetSeries1));
          String countNum = resultSet.getString(countUDAF(targetSeries2));
          String lastValue = resultSet.getString(lastValue(targetSeries3));

          assertEquals(expected[count][0], startTime);
          assertEquals(expected[count][1], deviceName);
          if (hasEndTime) {
            assertEquals(expected[count][2], endTime);
          }
          assertEquals(Double.parseDouble(expected[count][3]), duration, DELTA);
          assertEquals(expected[count][4], countNum);
          assertEquals(expected[count][5], lastValue);

          count++;
        }

        assertEquals(expected.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
