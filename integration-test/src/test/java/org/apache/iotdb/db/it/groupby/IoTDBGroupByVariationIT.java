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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByVariationIT {

  // original data and aggregation results can be directly viewed in online doc:
  // https://docs.google.com/spreadsheets/d/11YSt061_JON8OyQ1EqntSwJKiyMrV3Pepz-GaCHQrA4
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
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(10, 36.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(20, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(30, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(40, 37.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(50, 37.9, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(100, 38.0, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(150, 38.8,  true, 220)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(200, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(250, 38.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(300, 38.3, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(400, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(440, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(480, null, null, 0)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(500, 38.2, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(510, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(520, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(530, 36.8, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(540, 37.4, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(580, 37.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(590, 37.9,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(600, 36.9, true, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(610, 38.2, true, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(620, 39.2, true, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1500, 9.8, false, 666)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1550, 10.2, true, 888)",
        "flush"
      };

  private static final String[] SQLs2 =
      new String[] {
        "CREATE DATABASE root.ln.wf01.wt02",
        "CREATE TIMESERIES root.ln.wf01.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt02.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(1, 35.7, false, 11)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(2, 35.8,  true, 22)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(3, 35.4, false, 33 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(4, 36.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(5, 36.8, false, 55)",
        "flush",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(10, 36.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(20, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(30, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(40, 37.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(50, 37.9, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(100, 38.0, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(150, 38.8,  true, 220)",
        "flush",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(200, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(250, 38.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(300, 38.3, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(400, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(440, null, null, 0)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(480, null, null, 0)",
        "flush",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(500, 38.2, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(510, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(520, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(530, 36.8, false, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(540, 37.4, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(580, 37.8, false, 110)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(590, 37.9,  true, 220)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(600, 36.9, true, 330 )",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(610, 38.2, true, 440)",
        "INSERT INTO root.ln.wf01.wt02(timestamp, temperature, status, hardware) values(620, 39.2, true, 550)",
        "flush",
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

  private void normalTest(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,count(root.ln.wf01.wt01.status),avg(root.ln.wf01.wt01.temperature),sum(root.ln.wf01.wt01.hardware)");
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          String actualCount = resultSet.getString(2);
          double actualAvg = resultSet.getDouble(3);
          double actualSum = resultSet.getDouble(4);

          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualCount);
          assertEquals(Double.parseDouble(res[count][2]), actualAvg, 0.00001);
          assertEquals(Double.parseDouble(res[count][3]), actualSum, 0.00001);
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void normalTestWithEndTime(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,__endTime,count(root.ln.wf01.wt01.status),avg(root.ln.wf01.wt01.temperature),sum(root.ln.wf01.wt01.hardware)");
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          String actualEndTime = resultSet.getString(2);
          String actualCount = resultSet.getString(3);
          double actualAvg = resultSet.getDouble(4);
          double actualSum = resultSet.getDouble(5);

          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualEndTime);
          assertEquals(res[count][2], actualCount);
          assertEquals(Double.parseDouble(res[count][3]), actualAvg, 0.00001);
          assertEquals(Double.parseDouble(res[count][4]), actualSum, 0.00001);
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
  public void groupByVariationTest() {

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
        "select count(status),avg(temperature),sum(hardware) from root.ln.wf01.wt01 group by variation(temperature,1,ignoreNull=true)";
    normalTest(res, sql);
  }

  @Test
  public void groupByVariationTestWithEndTime() {

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
        "select __endTime,count(status),avg(temperature),sum(hardware) from root.ln.wf01.wt01 group by variation(temperature,1,ignoringNul=true)";
    normalTestWithEndTime(res, sql);
  }

  @Test
  public void groupByVariationEqualTest() {

    String[][] res =
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
        "select count(status),avg(temperature),sum(hardware) from root.ln.wf01.wt01 group by variation(status)";
    normalTest(res, sql);
  }

  @Test
  public void groupByVariationEqualTestWithEndTime() {

    String[][] res =
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
        "select __endTime,count(status),avg(temperature),sum(hardware) from root.ln.wf01.wt01 group by variation(status)";
    normalTestWithEndTime(res, sql);
  }

  private void normalTestWithoutIgnoreNull(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,count(root.ln.wf01.wt01.hardware),avg(root.ln.wf01.wt01.temperature),sum(root.ln.wf01.wt01.hardware)");
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          String actualCount = resultSet.getString(2);
          double actualAvg = resultSet.getDouble(3);
          boolean wasNull = resultSet.wasNull();
          double actualSum = resultSet.getDouble(4);

          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualCount);
          if (res[count][2].equals("null")) assertTrue(wasNull);
          else assertEquals(Double.parseDouble(res[count][2]), actualAvg, 0.00001);
          assertEquals(Double.parseDouble(res[count][3]), actualSum, 0.00001);
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
  public void groupByVariationTestWithoutIgnoreNull() {

    String[][] res =
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
        "select count(hardware),avg(temperature),sum(hardware) from root.ln.wf01.wt01 group by variation(temperature,1,ignoreNull=false)";
    normalTestWithoutIgnoreNull(res, sql);
  }

  @Test
  public void groupByVariationEqualTestWithoutIgnoreNull() {

    String[][] res =
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
        "select count(hardware),avg(temperature),sum(hardware) from root.ln.wf01.wt01 group by variation(status,ignoreNull=false)";
    normalTestWithoutIgnoreNull(res, sql);
  }

  private void errorTest(String sql, String error) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(sql);
    } catch (Exception e) {
      assertEquals(error, e.getMessage());
    }
  }

  private void normalTestWithEndTimeAlignByDevice(String[][] res, String sql) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,__endTime,count(status),avg(temperature),sum(hardware)");
        int count = 0;
        int rowNum = res.length;
        String device = "root.ln.wf01.wt01";
        while (resultSet.next()) {
          if (rowNum == 0) {
            device = "root.ln.wf01.wt02";
            count = 0;
          }
          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          String actualEndTime = resultSet.getString(3);
          String actualCount = resultSet.getString(4);
          double actualAvg = resultSet.getDouble(5);
          double actualSum = resultSet.getDouble(6);
          assertEquals(device, actualDevice);
          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualEndTime);
          assertEquals(res[count][2], actualCount);
          assertEquals(Double.parseDouble(res[count][3]), actualAvg, 0.00001);
          assertEquals(Double.parseDouble(res[count][4]), actualSum, 0.00001);
          count++;
          rowNum--;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByVariationAlignByDeviceTest() {
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
        "select __endTime,count(status),avg(temperature),sum(hardware) from root.** group by variation(temperature,1) align by device";
    normalTestWithEndTimeAlignByDevice(res, sql);
  }

  @Test
  public void groupByVariationEqualAlignByDeviceTest() {

    String[][] res =
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
        "select __endTime,count(status),avg(temperature),sum(hardware) from root.** group by variation(status) align by device";
    normalTestWithEndTimeAlignByDevice(res, sql);
  }

  @Test
  public void errorTest1() {
    errorTest(
        "select avg(temperature) from root.ln.wf01.wt01 group by variation(*)",
        "701: Expression in group by should indicate one value");
  }

  @Test
  public void errorTest2() {
    errorTest(
        "select avg(temperature) from root.ln.wf01.wt01 group by variation(avg(temperature))",
        "701: Aggregation expression shouldn't exist in group by clause");
  }

  @Test
  public void groupByVariationWithDoubleTest() {
    String[][] res =
        new String[][] {
          {"1", "4", "4", "35.825", "110"},
          {"5", "10", "2", "36.8", "165"},
          {"20", "100", "5", "37.72", "1650"},
          {"150", "500", "5", "38.46", "1650"},
          {"510", "610", "8", "37.4875", "2640"},
          {"620", "620", "1", "39.2", "550"},
          {"1500", "1550", "2", "10", "1554"}
        };
    String sql =
        "select __endTime,count(status),avg(temperature),sum(hardware) from root.** group by variation(temperature,0.8) align by device";
    normalTestWithEndTimeAlignByDevice(res, sql);
  }

  @Test
  public void groupByVariationWithDoubleTest2() {
    String[][] res =
        new String[][] {
          {"20", "100", "5", "37.72", "1650"},
          {"150", "500", "5", "38.46", "1650"},
          {"510", "610", "8", "37.4875", "2640"},
        };
    String sql =
        "select __endTime,count(status),avg(temperature),sum(hardware) from root.** group by variation(temperature,0.8) having count(status)>=5 align by device";
    normalTestWithEndTimeAlignByDevice(res, sql);
  }

  @Test
  public void groupByVariationWithDoubleTest3() {
    String[][] res =
        new String[][] {
          {"20", "100", "5", "37.72", "1650"},
          {"150", "500", "5", "38.46", "1650"},
          {"510", "610", "8", "37.4875", "2640"},
        };
    String sql =
        "select __endTime,count(status),avg(temperature),sum(hardware) from root.ln.wf01.wt01 group by variation(temperature,0.8) having count(status)>=5";
    normalTestWithEndTime(res, sql);
  }

  @Test
  public void groupByVariationWithDoubleTest4() {
    String[][] res =
        new String[][] {
          {"1", "35.7"},
          {"5", "36.8"},
          {"50", "37.9"},
          {"530", "36.8"},
          {"590", "37.9"},
          {"620", "39.2"},
          {"1500", "9.8"}
        };
    String sql =
        "select first_value(temperature) from root.ln.wf01.wt01 group by variation(temperature,1)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "Time,first_value(root.ln.wf01.wt01.temperature)");
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          double actualFirst = resultSet.getDouble(2);

          assertEquals(res[count][0], actualTime);
          assertEquals(Double.parseDouble(res[count][1]), actualFirst, 0.00001);
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
