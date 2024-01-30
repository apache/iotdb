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

import java.sql.*;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.apache.iotdb.itbase.constant.TestConstant.lastValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFGroupByCountIT {
  private static final String[] dataset =
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
        "flush",
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
      statement.execute(
          "CREATE FUNCTION sum_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFSum'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void UDAFGroupByCountTest() {
    String[][] expected = {
      {"1", "5", "3.0", "5", "18"},
      {"6", "1900000000", "5.0", "5", "55"},
    };
    String sqlWithoutEndTime =
        "SELECT sum_udaf(charging_status), count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY COUNT(charging_status, 5)";
    UDAFGroupByCountITChecker(sqlWithoutEndTime, expected, false);

    String sqlWithEndTime =
        "SELECT __endTime, sum_udaf(charging_status), count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY COUNT(charging_status, 5)";
    UDAFGroupByCountITChecker(sqlWithEndTime, expected, true);
  }

  @Test
  public void UDAFGroupByCountWithHavingTest() {
    String[][] expected = {
      {"3", "4", "0.0", "2", "16"},
      {"2500000000", "2600000000", "1.0", "2", "101"}
    };
    String sql =
        "SELECT sum_udaf(charging_status), count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY COUNT(charging_status, 2) "
            + "HAVING sum_udaf(charging_status) < 2";
    UDAFGroupByCountITChecker(sql, expected, false);
  }

  @Test
  public void UDAFGroupByCountIgnoreNullTest() {
    String[][] expected = {
      {"1", "5", "3.0", "5", "18"},
      {"6", "10", "4.0", "5", "60"},
      {"1100000000", "2100000000", "2.0", "5", "70"},
      {"2200000000", "2600000000", "2.0", "5", "101"},
    };

    String sqlWithoutEndTime =
        "SELECT sum_udaf(charging_status), count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY COUNT(charging_status, 5, ignoreNull=false)";
    UDAFGroupByCountITChecker(sqlWithoutEndTime, expected, false);

    String sqlWithEndTime =
        "SELECT __endTime, sum_udaf(charging_status), count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.sg.beijing.car01 "
            + "GROUP BY COUNT(charging_status, 5, ignoreNull=false)";
    UDAFGroupByCountITChecker(sqlWithEndTime, expected, true);
  }

  @Test
  public void UDAFGroupByCountAlignByDeviceTest() {
    String[][] expected = {
      {"1", "root.sg.beijing.car01", "2", "2.0", "2", "16"},
      {"3", "root.sg.beijing.car01", "4", "0.0", "2", "16"},
      {"5", "root.sg.beijing.car01", "6", "2.0", "2", "24"},
      {"7", "root.sg.beijing.car01", "9", "2.0", "2", "45"},
      {"10", "root.sg.beijing.car01", "1900000000", "2.0", "2", "55"},
      {"2000000000", "root.sg.beijing.car01", "2400000000", "2.0", "2", "80"},
      {"2500000000", "root.sg.beijing.car01", "2600000000", "1.0", "2", "101"},
      {"1", "root.sg.beijing.car02", "2", "2.0", "2", "16"},
      {"3", "root.sg.beijing.car02", "4", "0.0", "2", "16"},
      {"5", "root.sg.beijing.car02", "6", "2.0", "2", "24"},
      {"7", "root.sg.beijing.car02", "9", "2.0", "2", "45"},
      {"10", "root.sg.beijing.car02", "1900000000", "2.0", "2", "55"},
      {"2000000000", "root.sg.beijing.car02", "2400000000", "2.0", "2", "80"},
      {"2500000000", "root.sg.beijing.car02", "2600000000", "1.0", "2", "101"},
    };

    String sqlWithoutEndTime =
        "SELECT sum_udaf(charging_status), count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.** "
            + "GROUP BY COUNT(charging_status, 2) "
            + "ALIGN BY DEVICE";
    UDAFGroupByCountITAlignByDeviceChecker(sqlWithoutEndTime, expected, false);

    String sqlWithEndTime =
        "SELECT __endTime, sum_udaf(charging_status), count_udaf(vehicle_status), last_value(soc) "
            + "FROM root.** "
            + "GROUP BY COUNT(charging_status, 2) "
            + "ALIGN BY DEVICE";
    UDAFGroupByCountITAlignByDeviceChecker(sqlWithEndTime, expected, true);
  }

  private void UDAFGroupByCountITChecker(String sql, String[][] expected, boolean hasEndTime) {
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
          String sum = resultSet.getString(sumUDAF(targetSeries1));
          String countNum = resultSet.getString(countUDAF(targetSeries2));
          String lastValue = resultSet.getString(lastValue(targetSeries3));

          assertEquals(expected[count][0], startTime);
          if (hasEndTime) {
            assertEquals(expected[count][1], endTime);
          }
          assertEquals(expected[count][2], sum);
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

  private void UDAFGroupByCountITAlignByDeviceChecker(
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
          String sum = resultSet.getString(sumUDAF(targetSeries1));
          String countNum = resultSet.getString(countUDAF(targetSeries2));
          String lastValue = resultSet.getString(lastValue(targetSeries3));

          assertEquals(expected[count][0], startTime);
          assertEquals(expected[count][1], deviceName);
          if (hasEndTime) {
            assertEquals(expected[count][2], endTime);
          }
          assertEquals(expected[count][3], sum);
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
