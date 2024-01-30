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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFGroupBySessionIT {
  private final double DELTA = 1E-3;

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.ln.wf02.wt01",
        "CREATE TIMESERIES root.ln.wf02.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(1000, 35.7, false, 11)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(2000, 35.8,  true, 22)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(3000, 35.4, false, 33 )",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(4000, 36.4, false, 44)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(5000, 36.8, false, 55)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(10000, 36.8, false, 110)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(20000, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(30000, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(40000, 37.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(50000, 37.9, false, 550)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(100000, 38.0, false, 110)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(150000, 38.8,  true, 220)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(200000, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(260000, 38.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(320000, 38.3, false, 550)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(400000, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(470000, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(480000, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86881000, 38.2, false, 110)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86882000, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86883000, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86884000, 36.8, false, 440)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86885000, 37.4, false, 550)",
        "flush",
        "CREATE DATABASE root.ln.wf02.wt02",
        "CREATE TIMESERIES root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt02.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(1, 35.7, false, 11)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(2, 35.8,  true, 22)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(3, 35.4, false, 33 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(4, 36.4, false, 44)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(5, 36.8, false, 55)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(10, 36.8, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(20, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(30, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(40, 37.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(50, 37.9, false, 550)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(100, 38.0, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(150, 38.8,  true, 220)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(200, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(250, 38.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(300, 38.3, false, 550)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(400, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(440, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(480, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(500, 38.2, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(510, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(520, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(530, 36.8, false, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(540, 37.4, false, 550)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(580, 37.8, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(590, 37.9,  true, 220)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(600, 36.9, true, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(610, 38.2, true, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(620, 39.2, true, 550)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(1500, 9.8, false, 666)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(1550, 10.2, true, 888)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(3550, 10.8, true, 999)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(5550, 10.6, false, 1888)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(7550, 10.2, true, 2888)",
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
  public void UDAFGroupBySessionTest() {
    String[][] expected =
        new String[][] {
          {"1", "15", "37.3067", "3465"},
          {"400", "10", "37.73", "3300"},
          {"1500", "2", "10", "1554"},
          {"3550", "1", "10.8", "999"},
          {"5550", "1", "10.6", "1888"},
          {"7550", "1", "10.2", "2888"}
        };

    String sql =
        "SELECT count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf02.wt02 "
            + "GROUP BY SESSION(99ms)";
    UDAFGroupBySessionITChecker(sql, expected);
  }

  @Test
  public void UDAFGroupBySessionTestWithHaving() {
    String[][] expected =
        new String[][] {
          {"1", "15", "37.3067", "3465"},
          {"400", "10", "37.73", "3300"},
        };

    String sql =
        "SELECT count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf02.wt02 "
            + "GROUP BY SESSION(99ms) "
            + "HAVING avg_udaf(temperature) > 30";
    UDAFGroupBySessionITChecker(sql, expected);
  }

  @Test
  public void UDAFGroupBySessionOrderByTest() {
    String[][] expected =
        new String[][] {
          {"7550", "1", "10.2", "2888"},
          {"5550", "1", "10.6", "1888"},
          {"3550", "1", "10.8", "999"},
          {"1", "27", "35.441", "8319"}
        };

    String sql =
        "SELECT count_udaf(status), avg_udaf(temperature), sum_udaf(hardware) "
            + "FROM root.ln.wf02.wt02 "
            + "GROUP BY SESSION(1s) "
            + "ORDER BY TIME DESC";
    UDAFGroupBySessionITChecker(sql, expected);
  }

  @Test
  public void UDAFGroupBySessionAlignByDeviceTest() {
    String[][] expected =
        new String[][] {
          {"1000", "root.ln.wf02.wt01", "480000", "15", "37.3066666667", "3465", "11"},
          {"86881000", "root.ln.wf02.wt01", "86885000", "5", "37.46", "1650", "110"},
          {"1", "root.ln.wf02.wt02", "7550", "30", "32.95", "14094", "11"}
        };
    String sql =
        "SELECT __endTime, count_udaf(status), avg_udaf(temperature), sum_udaf(hardware), first_value(hardware) "
            + "FROM root.ln.** "
            + "GROUP BY SESSION(1d) "
            + "ALIGN BY DEVICE";
    UDAFGroupBySessionITAlignByDeviceChecker(sql, expected);
  }

  @Test
  public void UDAFGroupBySessionAlignByDeviceWithHavingTest() {
    String[][] expected =
        new String[][] {
          {"1000", "root.ln.wf02.wt01", "200000", "13", "37.1461538462", "2475", "11"},
          {"1", "root.ln.wf02.wt02", "7550", "30", "32.95", "14094", "11"}
        };
    String sql =
        "SELECT __endTime, count_udaf(status), avg_udaf(temperature), sum_udaf(hardware), first_value(hardware) "
            + "FROM root.ln.** "
            + "GROUP BY SESSION(50s) "
            + "HAVING count_udaf(status) > 5 "
            + "ALIGN BY DEVICE";
    UDAFGroupBySessionITAlignByDeviceChecker(sql, expected);
  }

  private void UDAFGroupBySessionITChecker(String sql, String[][] expected) {
    String targetSeries1 = "root.ln.wf02.wt02.status";
    String targetSeries2 = "root.ln.wf02.wt02.temperature";
    String targetSeries3 = "root.ln.wf02.wt02.hardware";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(TIMESTAMP_STR);
          String actualCount = resultSet.getString(countUDAF(targetSeries1));
          double actualAvg = resultSet.getDouble(avgUDAF(targetSeries2));
          double actualSum = resultSet.getDouble(sumUDAF(targetSeries3));

          assertEquals(expected[count][0], actualTime);
          assertEquals(expected[count][1], actualCount);
          assertEquals(Double.parseDouble(expected[count][2]), actualAvg, DELTA);
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

  private void UDAFGroupBySessionITAlignByDeviceChecker(String sql, String[][] expected) {
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
          String actualFirstValue = resultSet.getString(firstValue(targetSeries3));

          assertEquals(expected[count][0], actualTime);
          assertEquals(expected[count][1], actualDevice);
          assertEquals(expected[count][2], actualEndTime);
          assertEquals(expected[count][3], actualCount);
          assertEquals(Double.parseDouble(expected[count][4]), actualAvg, DELTA);
          assertEquals(Double.parseDouble(expected[count][5]), actualSum, DELTA);
          assertEquals(expected[count][6], actualFirstValue);

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
