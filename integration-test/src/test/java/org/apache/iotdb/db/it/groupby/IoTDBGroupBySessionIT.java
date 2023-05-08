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
public class IoTDBGroupBySessionIT {

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.ln.wf02.wt01",
        "CREATE TIMESERIES root.ln.wf02.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(1000, 35.7, false, 11)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(2000, 35.8,  true, 22)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(3000, 35.4, false, 33 )",
        "flush",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(4000, 36.4, false, 44)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(5000, 36.8, false, 55)",
        "flush",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(10000, 36.8, false, 110)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(20000, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(30000, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(40000, 37.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(50000, 37.9, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(100000, 38.0, false, 110)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(150000, 38.8,  true, 220)",
        "flush",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(200000, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(260000, 38.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(320000, 38.3, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(400000, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(470000, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(480000, null, null, 0)",
        "flush",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86881000, 38.2, false, 110)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86882000, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86883000, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86884000, 36.8, false, 440)",
        "INSERT INTO root.ln.wf02.wt01(timestamp, temperature, status, hardware) values(86885000, 37.4, false, 550)",
      };

  private static final String[] SQLs2 =
      new String[] {
        "CREATE DATABASE root.ln.wf02.wt02",
        "CREATE TIMESERIES root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt02.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(1, 35.7, false, 11)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(2, 35.8,  true, 22)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(3, 35.4, false, 33 )",
        "flush",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(4, 36.4, false, 44)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(5, 36.8, false, 55)",
        "flush",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(10, 36.8, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(20, 37.8,  true, 220)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(30, 37.5, false, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(40, 37.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(50, 37.9, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(100, 38.0, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(150, 38.8,  true, 220)",
        "flush",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(200, 38.6, false, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(250, 38.4, false, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(300, 38.3, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(400, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(440, null, null, 0)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(480, null, null, 0)",
        "flush",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(500, 38.2, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(510, 37.5,  true, 220)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(520, 37.4, false, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(530, 36.8, false, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(540, 37.4, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(580, 37.8, false, 110)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(590, 37.9,  true, 220)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(600, 36.9, true, 330 )",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(610, 38.2, true, 440)",
        "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(620, 39.2, true, 550)",
        "flush",
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
            "Time,count(root.ln.wf02.wt02.status),avg(root.ln.wf02.wt02.temperature),sum(root.ln.wf02.wt02.hardware)");
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          String actualCount = resultSet.getString(2);
          double actualAvg = resultSet.getDouble(3);
          double actualSum = resultSet.getDouble(4);

          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualCount);
          assertEquals(Double.parseDouble(res[count][2]), actualAvg, 0.01);
          assertEquals(Double.parseDouble(res[count][3]), actualSum, 0.01);
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
  public void groupBySessionTest1() {
    String[][] res =
        new String[][] {
          {"1", "15", "37.3067", "3465"},
          {"400", "10", "37.73", "3300"},
          {"1500", "2", "10", "1554"},
          {"3550", "1", "10.8", "999"},
          {"5550", "1", "10.6", "1888"},
          {"7550", "1", "10.2", "2888"}
        };

    String sql =
        "select count(status), avg(temperature), sum(hardware) from root.ln.wf02.wt02 group by session(99ms)";
    normalTest(res, sql);
  }

  @Test
  public void groupBySessionTest1WithHaving() {
    String[][] res =
        new String[][] {
          {"1", "15", "37.3067", "3465"},
          {"400", "10", "37.73", "3300"},
        };

    String sql =
        "select count(status), avg(temperature), sum(hardware) from root.ln.wf02.wt02 group by session(99ms) having avg(temperature) > 30";
    normalTest(res, sql);
  }

  @Test
  public void groupBySessionTest2() {
    String[][] res =
        new String[][] {
          {"1", "10"}, {"100", "1"}, {"150", "1"}, {"200", "1"}, {"250", "1"},
          {"300", "1"}, {"500", "5"}, {"580", "5"}, {"1500", "1"}, {"1550", "1"},
          {"3550", "1"}, {"5550", "1"}, {"7550", "1"}
        };

    String sql = "select count(temperature) from root.ln.wf02.wt02 group by session(10ms)";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          String actualCount = resultSet.getString(2);

          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualCount);
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
  public void groupBySessionTest3() {
    String[][] res =
        new String[][] {
          {"1", "10", "36.75", "1815"},
          {"100", "1", "38", "110"},
          {"150", "1", "38.8", "220"},
          {"200", "1", "38.6", "330"},
          {"250", "1", "38.4", "440"},
          {"300", "1", "38.3", "550"},
          {"400", "10", "37.73", "3300"},
          {"1500", "1", "9.8", "666"},
          {"1550", "1", "10.2", "888"},
          {"3550", "1", "10.8", "999"},
          {"5550", "1", "10.6", "1888"},
          {"7550", "1", "10.2", "2888"}
        };

    String sql =
        "select count(status), avg(temperature), sum(hardware) from root.ln.wf02.wt02 group by session(49ms)";
    normalTest(res, sql);
  }

  public void groupBySessionFirstValueTest(String sql, String[][] res) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          double firstValue = resultSet.getDouble(2);

          assertEquals(res[count][0], actualTime);
          assertEquals(Double.parseDouble(res[count][1]), firstValue, 0.01);
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
  public void groupBySessionTest4() {
    String[][] res =
        new String[][] {
          {"1", "35.7"}, {"100", "38"}, {"150", "38.8"}, {"200", "38.6"}, {"250", "38.4"},
          {"300", "38.3"}, {"500", "38.2"}, {"580", "37.8"}, {"1500", "9.8"}, {"1550", "10.2"},
          {"3550", "10.8"}, {"5550", "10.6"}, {"7550", "10.2"}
        };

    String sql = "select first_value(temperature) from root.ln.wf02.wt02 group by session(10ms)";
    groupBySessionFirstValueTest(sql, res);
  }

  @Test
  public void groupBySessionTest5() {
    String[][] res =
        new String[][] {
          {"1", "27", "35.441", "8319"},
          {"3550", "1", "10.8", "999"},
          {"5550", "1", "10.6", "1888"},
          {"7550", "1", "10.2", "2888"}
        };

    String sql =
        "select count(status), avg(temperature), sum(hardware) from root.ln.wf02.wt02 group by session(1s)";
    normalTest(res, sql);
  }

  @Test
  public void groupBySessionTest6() {
    String[][] res =
        new String[][] {
          {"7550", "1", "10.2", "2888"},
          {"5550", "1", "10.6", "1888"},
          {"3550", "1", "10.8", "999"},
          {"1", "27", "35.441", "8319"}
        };

    String sql =
        "select count(status), avg(temperature), sum(hardware) from root.ln.wf02.wt02 group by session(1s) order by time desc";
    normalTest(res, sql);
  }

  @Test
  public void groupBySessionTest7() {
    String[][] res =
        new String[][] {
          {"7550", "1", "10.2", "2888"},
          {"5550", "1", "10.6", "1888"},
          {"3550", "1", "10.8", "999"},
          {"1550", "1", "10.2", "888"},
          {"1500", "1", "9.8", "666"},
          {"400", "10", "37.73", "3300"},
          {"300", "1", "38.3", "550"},
          {"250", "1", "38.4", "440"},
          {"200", "1", "38.6", "330"},
          {"150", "1", "38.8", "220"},
          {"100", "1", "38", "110"},
          {"1", "10", "36.75", "1815"}
        };

    String sql =
        "select count(status), avg(temperature), sum(hardware) from root.ln.wf02.wt02 group by session(49ms) order by time desc";
    normalTest(res, sql);
  }

  @Test
  public void GroupBySessionAlignByDeviceTest() {
    String[][] res =
        new String[][] {
          {"1000", "200000", "13", "37.1461538462", "2475", "11"},
          {"1", "7550", "30", "32.95", "14094", "11"}
        };
    String sql =
        "select __endTime,count(status), avg(temperature), sum(hardware), first_value(hardware) from root.ln.** group by session(50s) having count(status)>5 align by device";
    normalTestAlignByDevice(res, sql, 1);
  }

  @Test
  public void GroupBySessionAlignByDeviceTest2() {
    String[][] res =
        new String[][] {
          {"1000", "480000", "15", "37.3066666667", "3465", "11"},
          {"86881000", "86885000", "5", "37.46", "1650", "110"},
          {"1", "7550", "30", "32.95", "14094", "11"}
        };
    String sql =
        "select __endTime,count(status), avg(temperature), sum(hardware), first_value(hardware) from root.ln.** group by session(1d) align by device";
    normalTestAlignByDevice(res, sql, 2);
  }

  @Test
  public void GroupBySessionAlignByDeviceTest3() {
    String[][] res =
        new String[][] {
          {"1000", "200000", "11"},
          {"260000", "260000", "440"},
          {"320000", "320000", "550"},
          {"400000", "400000", "0"},
          {"470000", "480000", "0"},
          {"86881000", "86885000", "110"},
          {"1", "7550", "11"}
        };
    String sql =
        "select __endTime,first_value(hardware) from root.ln.** group by session(50s) align by device";
    normalTestAlignByDevice2(res, sql, 6, "Time,Device,__endTime,first_value(hardware)");
  }

  @Test
  public void GroupBySessionFirstValueTest() {
    String[][] res = new String[][] {{"1", "35.7"}};
    String sql = "select first_value(temperature) from root.ln.wf02.wt02 group by session(50s)";
    groupBySessionFirstValueTest(sql, res);
  }

  @Test
  public void GroupBySessionAlignByDeviceTest4() {
    String[][] res =
        new String[][] {
          {"1000", "480000", "18"},
          {"86881000", "86885000", "5"},
          {"1", "7550", "33"}
        };
    String sql =
        "select __endTime,count(hardware) from root.ln.** group by session(1d) align by device";
    normalTestAlignByDevice2(res, sql, 2, "Time,Device,__endTime,count(hardware)");
  }

  private void normalTestAlignByDevice(String[][] res, String sql, int split) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(
            resultSetMetaData,
            "Time,Device,__endTime,count(status),avg(temperature),sum(hardware),first_value(hardware)");
        int count = 0;
        String device = "root.ln.wf02.wt01";
        while (resultSet.next()) {
          if (count == split) {
            device = "root.ln.wf02.wt02";
          }
          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          String actualEndTime = resultSet.getString(3);
          String actualCount = resultSet.getString(4);
          double actualAvg = resultSet.getDouble(5);
          double actualSum = resultSet.getDouble(6);
          String actualFirstValue = resultSet.getString(7);

          assertEquals(device, actualDevice);
          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualEndTime);
          assertEquals(res[count][2], actualCount);
          assertEquals(Double.parseDouble(res[count][3]), actualAvg, 0.01);
          assertEquals(Double.parseDouble(res[count][4]), actualSum, 0.01);
          assertEquals(res[count][5], actualFirstValue);
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void normalTestAlignByDevice2(String[][] res, String sql, int split, String title) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, title);
        int count = 0;
        String device = "root.ln.wf02.wt01";
        while (resultSet.next()) {
          if (count == split) {
            device = "root.ln.wf02.wt02";
          }
          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          String actualEndTime = resultSet.getString(3);
          String actualValue = resultSet.getString(4);

          assertEquals(device, actualDevice);
          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualEndTime);
          assertEquals(res[count][2], actualValue);
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
