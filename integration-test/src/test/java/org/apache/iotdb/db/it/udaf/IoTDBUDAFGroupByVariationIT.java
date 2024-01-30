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
import static org.junit.Assert.*;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFGroupByVariationIT {
  private final double DELTA = 0.00001;

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1, 35.7, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(2, 35.8,  true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(3, 35.4, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(4, 36.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(5, 36.8, false, 55)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(10, 36.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(20, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(30, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(40, 37.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(50, 37.9, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(100, 38.0, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(150, 38.8,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(200, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(250, 38.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(300, 38.3, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(400, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(440, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(480, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(500, 38.2, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(510, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(520, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(530, 36.8, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(540, 37.4, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(580, 37.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(590, 37.9,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(600, 36.9, true, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(610, 38.2, true, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(620, 39.2, true, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1500, 9.8, false, 666)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1550, 10.2, true, 888)",
        "flush",
        "CREATE DATABASE root.ln.wf01.wt02",
        "CREATE TIMESERIES root.ln.wf01.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt02.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(1, 35.7, false, 11)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(2, 35.8,  true, 22)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(3, 35.4, false, 33 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(4, 36.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(5, 36.8, false, 55)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(10, 36.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(20, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(30, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(40, 37.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(50, 37.9, false, 550)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(100, 38.0, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(150, 38.8,  true, 220)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(200, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(250, 38.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(300, 38.3, false, 550)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(400, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(440, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(480, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(500, 38.2, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(510, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(520, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(530, 36.8, false, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(540, 37.4, false, 550)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(580, 37.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(590, 37.9,  true, 220)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(600, 36.9, true, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(610, 38.2, true, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(620, 39.2, true, 550)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(1500, 9.8, false, 666)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(1550, 10.2, true, 888)",
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
          "CREATE FUNCTION avg_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFAvg'");
      statement.execute(
          "CREATE FUNCTION count_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.execute(
          "CREATE FUNCTION sum_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFSum'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void UDAFGroupByVariationTest() {
    String[][] res =
        new String[][] {
          {"1", "4", "35.825", "110"},
          {"5", "5", "37.26", "1155"},
          {"50", "9", "38.122222", "2860"},
          {"530", "3", "37.333333", "1100"},
          {"590", "3", "37.666667", "990"},
          {"620", "1", "39.2", "550"},
          {"1500", "2", "10", "1554"}
        };

    String sql =
        "SELECT count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf01.wt01 "
            + "GROUP BY VARIATION(temperature, 1, ignoreNull=true)";
    UDAFGroupByVariationITChecker(sql, res, false);
  }

  @Test
  public void UDAFGroupByVariationWithEndTimeTest() {
    String[][] res =
        new String[][] {
          {"1", "4", "4", "35.825", "110"},
          {"5", "40", "5", "37.26", "1155"},
          {"50", "520", "9", "38.122222", "2860"},
          {"530", "580", "3", "37.333333", "1100"},
          {"590", "610", "3", "37.666667", "990"},
          {"620", "620", "1", "39.2", "550"},
          {"1500", "1550", "2", "10", "1554"}
        };

    String sql =
        "SELECT __endTime, count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf01.wt01 "
            + "GROUP BY VARIATION(temperature, 1, ignoringNul=true)";
    UDAFGroupByVariationITChecker(sql, res, true);
  }

  @Test
  public void UDAFGroupByVariationEqualTest() {
    String[][] expected =
        new String[][] {
          {"1", "1", "35.7", "11"},
          {"2", "1", "35.8", "22"},
          {"3", "4", "36.35", "242"},
          {"20", "1", "37.8", "220"},
          {"30", "4", "37.7", "1430"},
          {"150", "1", "38.8", "220"},
          {"200", "4", "38.375", "1430"},
          {"510", "1", "37.5", "220"},
          {"520", "4", "37.35", "1430"},
          {"590", "4", "38.05", "1540"},
          {"1500", "1", "9.8", "666"},
          {"1550", "1", "10.2", "888"},
        };

    String sql =
        "SELECT count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf01.wt01 "
            + "GROUP BY VARIATION(status)";
    UDAFGroupByVariationITChecker(sql, expected, false);
  }

  @Test
  public void UDAFGroupByVariationEqualWithEndTimeTest() {
    String[][] expected =
        new String[][] {
          {"1", "1", "1", "35.7", "11"},
          {"2", "2", "1", "35.8", "22"},
          {"3", "10", "4", "36.35", "242"},
          {"20", "20", "1", "37.8", "220"},
          {"30", "100", "4", "37.7", "1430"},
          {"150", "150", "1", "38.8", "220"},
          {"200", "500", "4", "38.375", "1430"},
          {"510", "510", "1", "37.5", "220"},
          {"520", "580", "4", "37.35", "1430"},
          {"590", "620", "4", "38.05", "1540"},
          {"1500", "1500", "1", "9.8", "666"},
          {"1550", "1550", "1", "10.2", "888"},
        };

    String sql =
        "SELECT __endTime, count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf01.wt01 "
            + "GROUP BY VARIATION(status)";
    UDAFGroupByVariationITChecker(sql, expected, true);
  }

  @Test
  public void UDAFGroupByVariationWithoutIgnoreNullTest() {
    String[][] expected =
        new String[][] {
          {"1", "4", "35.825", "110"},
          {"5", "5", "37.26", "1155"},
          {"50", "6", "38.333333", "2200"},
          {"400", "3", "null", "0"},
          {"500", "3", "37.7", "660"},
          {"530", "3", "37.333333", "1100"},
          {"590", "3", "37.666667", "990"},
          {"620", "1", "39.2", "550"},
          {"1500", "2", "10", "1554"}
        };

    String sql =
        "SELECT count_udaf(hardware), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf01.wt01 "
            + "GROUP BY VARIATION(temperature, 1, ignoreNull=false)";
    UDAFGroupByVariationITNotIgnoreNullChecker(sql, expected);
  }

  @Test
  public void UDAFGroupByVariationEqualWithoutIgnoreNullTest() {
    String[][] expected =
        new String[][] {
          {"1", "1", "35.7", "11"},
          {"2", "1", "35.8", "22"},
          {"3", "4", "36.35", "242"},
          {"20", "1", "37.8", "220"},
          {"30", "4", "37.7", "1430"},
          {"150", "1", "38.8", "220"},
          {"200", "3", "38.433333", "1320"},
          {"400", "3", "null", "0"},
          {"500", "1", "38.2", "110"},
          {"510", "1", "37.5", "220"},
          {"520", "4", "37.35", "1430"},
          {"590", "4", "38.05", "1540"},
          {"1500", "1", "9.8", "666"},
          {"1550", "1", "10.2", "888"},
        };

    String sql =
        "SELECT count_udaf(hardware), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf01.wt01 "
            + "GROUP BY VARIATION(status, ignoreNull=false)";
    UDAFGroupByVariationITNotIgnoreNullChecker(sql, expected);
  }

  @Test
  public void UDAFGroupByVariationAlignByDeviceTest() {
    String[][] expected =
        new String[][] {
          {"1", "root.ln.wf01.wt01", "4", "4", "35.825", "110"},
          {"5", "root.ln.wf01.wt01", "40", "5", "37.26", "1155"},
          {"50", "root.ln.wf01.wt01", "520", "9", "38.122222", "2860"},
          {"530", "root.ln.wf01.wt01", "580", "3", "37.333333", "1100"},
          {"590", "root.ln.wf01.wt01", "610", "3", "37.666667", "990"},
          {"620", "root.ln.wf01.wt01", "620", "1", "39.2", "550"},
          {"1500", "root.ln.wf01.wt01", "1550", "2", "10", "1554"},
          {"1", "root.ln.wf01.wt02", "4", "4", "35.825", "110"},
          {"5", "root.ln.wf01.wt02", "40", "5", "37.26", "1155"},
          {"50", "root.ln.wf01.wt02", "520", "9", "38.122222", "2860"},
          {"530", "root.ln.wf01.wt02", "580", "3", "37.333333", "1100"},
          {"590", "root.ln.wf01.wt02", "610", "3", "37.666667", "990"},
          {"620", "root.ln.wf01.wt02", "620", "1", "39.2", "550"},
          {"1500", "root.ln.wf01.wt02", "1550", "2", "10", "1554"},
        };

    String sql =
        "SELECT __endTime, count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.** GROUP BY VARIATION(temperature,1) "
            + "ALIGN BY DEVICE";
    UDAFGroupByVariationITAlignByDeviceChecker(sql, expected);
  }

  @Test
  public void UDAFGroupByVariationEqualAlignByDeviceTest() {
    String[][] expected =
        new String[][] {
          {"1", "root.ln.wf01.wt01", "1", "1", "35.7", "11"},
          {"2", "root.ln.wf01.wt01", "2", "1", "35.8", "22"},
          {"3", "root.ln.wf01.wt01", "10", "4", "36.35", "242"},
          {"20", "root.ln.wf01.wt01", "20", "1", "37.8", "220"},
          {"30", "root.ln.wf01.wt01", "100", "4", "37.7", "1430"},
          {"150", "root.ln.wf01.wt01", "150", "1", "38.8", "220"},
          {"200", "root.ln.wf01.wt01", "500", "4", "38.375", "1430"},
          {"510", "root.ln.wf01.wt01", "510", "1", "37.5", "220"},
          {"520", "root.ln.wf01.wt01", "580", "4", "37.35", "1430"},
          {"590", "root.ln.wf01.wt01", "620", "4", "38.05", "1540"},
          {"1500", "root.ln.wf01.wt01", "1500", "1", "9.8", "666"},
          {"1550", "root.ln.wf01.wt01", "1550", "1", "10.2", "888"},
          {"1", "root.ln.wf01.wt02", "1", "1", "35.7", "11"},
          {"2", "root.ln.wf01.wt02", "2", "1", "35.8", "22"},
          {"3", "root.ln.wf01.wt02", "10", "4", "36.35", "242"},
          {"20", "root.ln.wf01.wt02", "20", "1", "37.8", "220"},
          {"30", "root.ln.wf01.wt02", "100", "4", "37.7", "1430"},
          {"150", "root.ln.wf01.wt02", "150", "1", "38.8", "220"},
          {"200", "root.ln.wf01.wt02", "500", "4", "38.375", "1430"},
          {"510", "root.ln.wf01.wt02", "510", "1", "37.5", "220"},
          {"520", "root.ln.wf01.wt02", "580", "4", "37.35", "1430"},
          {"590", "root.ln.wf01.wt02", "620", "4", "38.05", "1540"},
          {"1500", "root.ln.wf01.wt02", "1500", "1", "9.8", "666"},
          {"1550", "root.ln.wf01.wt02", "1550", "1", "10.2", "888"},
        };

    String sql =
        "SELECT __endTime, count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.** "
            + "GROUP BY VARIATION(status) "
            + "ALIGN BY DEVICE";
    UDAFGroupByVariationITAlignByDeviceChecker(sql, expected);
  }

  @Test
  public void UDAFGroupByVariationWithFPParameterTest() {
    String[][] expected =
        new String[][] {
          {"1", "root.ln.wf01.wt01", "4", "4", "35.825", "110"},
          {"5", "root.ln.wf01.wt01", "10", "2", "36.8", "165"},
          {"20", "root.ln.wf01.wt01", "100", "5", "37.72", "1650"},
          {"150", "root.ln.wf01.wt01", "500", "5", "38.46", "1650"},
          {"510", "root.ln.wf01.wt01", "610", "8", "37.4875", "2640"},
          {"620", "root.ln.wf01.wt01", "620", "1", "39.2", "550"},
          {"1500", "root.ln.wf01.wt01", "1550", "2", "10", "1554"},
          {"1", "root.ln.wf01.wt02", "4", "4", "35.825", "110"},
          {"5", "root.ln.wf01.wt02", "10", "2", "36.8", "165"},
          {"20", "root.ln.wf01.wt02", "100", "5", "37.72", "1650"},
          {"150", "root.ln.wf01.wt02", "500", "5", "38.46", "1650"},
          {"510", "root.ln.wf01.wt02", "610", "8", "37.4875", "2640"},
          {"620", "root.ln.wf01.wt02", "620", "1", "39.2", "550"},
          {"1500", "root.ln.wf01.wt02", "1550", "2", "10", "1554"},
        };

    String sql =
        "SELECT __endTime, count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.** "
            + "GROUP BY VARIATION(temperature, 0.8) "
            + "ALIGN BY DEVICE";
    UDAFGroupByVariationITAlignByDeviceChecker(sql, expected);
  }

  private void UDAFGroupByVariationITChecker(String sql, String[][] expected, boolean hasEndTime) {
    String targetSeries1 = "root.ln.wf01.wt01.status";
    String targetSeries2 = "root.ln.wf01.wt01.temperature";
    String targetSeries3 = "root.ln.wf01.wt01.hardware";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(TIMESTAMP_STR);
          String actualEndTime = hasEndTime ? resultSet.getString(END_TIMESTAMP_STR) : null;
          String actualCount = resultSet.getString(countUDAF(targetSeries1));
          double actualAvg = resultSet.getDouble(avgUDAF(targetSeries2));
          double actualSum = resultSet.getDouble(sumUDAF(targetSeries3));

          assertEquals(expected[count][0], actualTime);
          if (hasEndTime) {
            assertEquals(expected[count][1], actualEndTime);
            assertEquals(expected[count][2], actualCount);
            assertEquals(Double.parseDouble(expected[count][3]), actualAvg, DELTA);
            assertEquals(Double.parseDouble(expected[count][4]), actualSum, DELTA);
          } else {
            assertEquals(expected[count][1], actualCount);
            assertEquals(Double.parseDouble(expected[count][2]), actualAvg, DELTA);
            assertEquals(Double.parseDouble(expected[count][3]), actualSum, DELTA);
          }

          count++;
        }

        assertEquals(expected.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void UDAFGroupByVariationITAlignByDeviceChecker(String sql, String[][] expected) {
    String targetSeries1 = "status";
    String targetSeries2 = "temperature";
    String targetSeries3 = "hardware";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(TIMESTAMP_STR);
          String actualDevice = resultSet.getString(DEVICE);
          String actualEndTime = resultSet.getString(END_TIMESTAMP_STR);
          String actualCount = resultSet.getString(countUDAF(targetSeries1));
          double actualAvg = resultSet.getDouble(avgUDAF(targetSeries2));
          double actualSum = resultSet.getDouble(sumUDAF(targetSeries3));

          assertEquals(expected[count][0], actualTime);
          assertEquals(expected[count][1], actualDevice);
          assertEquals(expected[count][2], actualEndTime);
          assertEquals(expected[count][3], actualCount);
          assertEquals(Double.parseDouble(expected[count][4]), actualAvg, DELTA);
          assertEquals(Double.parseDouble(expected[count][5]), actualSum, DELTA);

          count++;
        }

        assertEquals(expected.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void UDAFGroupByVariationITNotIgnoreNullChecker(String sql, String[][] expected) {
    String targetSeries1 = "root.ln.wf01.wt01.hardware";
    String targetSeries2 = "root.ln.wf01.wt01.temperature";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(TIMESTAMP_STR);
          String actualCount = resultSet.getString(countUDAF(targetSeries1));
          double actualAvg = resultSet.getDouble(avgUDAF(targetSeries2));
          boolean wasNull = resultSet.wasNull();
          double actualSum = resultSet.getDouble(sumUDAF(targetSeries1));

          assertEquals(expected[count][0], actualTime);
          assertEquals(expected[count][1], actualCount);
          if (expected[count][2].equals("null")) {
            assertTrue(wasNull);
          } else {
            assertEquals(Double.parseDouble(expected[count][2]), actualAvg, DELTA);
          }
          assertEquals(Double.parseDouble(expected[count][3]), actualSum, DELTA);

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
