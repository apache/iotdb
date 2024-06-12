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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFOrderByIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBUDAFOrderByIT.class);

  protected static final String[] dataset =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d.num WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d.bigNum WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d.floatNum WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d.str WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d.bool WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(0,3,2947483648,231.2121,\"coconut\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(20,2,2147483648,434.12,\"pineapple\",TRUE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(40,1,2247483648,12.123,\"apricot\",TRUE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(80,9,2147483646,43.12,\"apple\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(100,8,2147483964,4654.231,\"papaya\",TRUE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(31536000000,6,2147483650,1231.21,\"banana\",TRUE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(31536000100,10,3147483648,231.55,\"pumelo\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(31536000500,4,2147493648,213.1,\"peach\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(31536001000,5,2149783648,56.32,\"orange\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(31536010000,7,2147983648,213.112,\"lemon\",TRUE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(31536100000,11,2147468648,54.121,\"pitaya\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(41536000000,12,2146483648,45.231,\"strawberry\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(41536000020,14,2907483648,231.34,\"cherry\",FALSE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(41536900000,13,2107483648,54.12,\"lychee\",TRUE)",
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(51536000000,15,3147483648,235.213,\"watermelon\",TRUE)",
        "flush",
        "CREATE TIMESERIES root.sg.d2.num WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.bigNum WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.floatNum WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d2.str WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d2.bool WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(0,3,2947483648,231.2121,\"coconut\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(20,2,2147483648,434.12,\"pineapple\",TRUE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(40,1,2247483648,12.123,\"apricot\",TRUE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(80,9,2147483646,43.12,\"apple\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(100,8,2147483964,4654.231,\"papaya\",TRUE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(31536000000,6,2147483650,1231.21,\"banana\",TRUE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(31536000100,10,3147483648,231.55,\"pumelo\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(31536000500,4,2147493648,213.1,\"peach\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(31536001000,5,2149783648,56.32,\"orange\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(31536010000,7,2147983648,213.112,\"lemon\",TRUE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(31536100000,11,2147468648,54.121,\"pitaya\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(41536000000,12,2146483648,45.231,\"strawberry\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(41536000020,14,2907483648,231.34,\"cherry\",FALSE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(41536900000,13,2107483648,54.12,\"lychee\",TRUE)",
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(51536000000,15,3147483648,235.213,\"watermelon\",TRUE)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setSortBufferSize(1024 * 1024L);
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
  public void UDAFOrderByWithAggregationTest() {
    String sql =
        "SELECT avg_udaf(num) FROM root.sg.d "
            + "GROUP BY SESSION(10000ms) "
            + "ORDER BY avg_udaf(num) DESC";
    double[][] ans = new double[][] {{15.0}, {13.0}, {13.0}, {11.0}, {6.4}, {4.6}};
    long[] times =
        new long[] {51536000000L, 41536000000L, 41536900000L, 31536100000L, 31536000000L, 0L};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          double actualAvg = resultSet.getDouble(2);
          assertEquals(times[i], actualTime);
          assertEquals(ans[i][0], actualAvg, 0.0001);
          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void UDAFOrderByWithAggregationTest2() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.sg.d "
            + "GROUP BY SESSION(10000ms) "
            + "ORDER BY max_value(floatNum)";
    double[][] ans =
        new double[][] {
          {13.0, 54.12},
          {11.0, 54.121},
          {13.0, 231.34},
          {15.0, 235.213},
          {6.4, 1231.21},
          {4.6, 4654.231}
        };
    long[] times =
        new long[] {41536900000L, 31536100000L, 41536000000L, 51536000000L, 31536000000L, 0L};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          double actualAvg = resultSet.getDouble(2);
          assertEquals(times[i], actualTime);
          assertEquals(ans[i][0], actualAvg, 0.0001);
          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void UDAFOrderByWithAggregationTest3() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.sg.d GROUP BY SESSION(10000ms) "
            + "ORDER BY avg_udaf(num) DESC, max_value(floatNum)";
    double[] ans = new double[] {15.0, 13.0, 13.0, 11.0, 6.4, 4.6};
    long[] times =
        new long[] {51536000000L, 41536900000L, 41536000000L, 31536100000L, 31536000000L, 0L};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          double actualAvg = resultSet.getDouble(2);
          assertEquals(times[i], actualTime);
          assertEquals(ans[i], actualAvg, 0.0001);
          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void UDAFOrderByWithAggregationTest4() {
    String sql =
        "SELECT avg_udaf(num) + avg_udaf(floatNum) "
            + "FROM root.sg.d "
            + "GROUP BY SESSION(10000ms) "
            + "ORDER BY avg_udaf(num) + avg_udaf(floatNum)";
    double[][] ans =
        new double[][] {{1079.56122}, {395.4584}, {65.121}, {151.2855}, {67.12}, {250.213}};
    long[] times =
        new long[] {0L, 31536000000L, 31536100000L, 41536000000L, 41536900000L, 51536000000L};
    int[] order = new int[] {2, 4, 3, 5, 1, 0};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          double actualAvg = resultSet.getDouble(2);
          assertEquals(times[order[i]], actualTime);
          assertEquals(ans[order[i]][0], actualAvg, 0.0001);
          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "GROUP BY SESSION(10000ms) "
            + "ORDER BY avg_udaf(num) "
            + "ALIGN BY DEVICE";

    double[] ans = {4.6, 4.6, 6.4, 6.4, 11.0, 11.0, 13.0, 13.0, 13.0, 13.0, 15.0, 15.0};
    long[] times =
        new long[] {
          0L,
          0L,
          31536000000L,
          31536000000L,
          31536100000L,
          31536100000L,
          41536000000L,
          41536900000L,
          41536000000L,
          41536900000L,
          51536000000L,
          51536000000L
        };
    String[] device =
        new String[] {
          "root.sg.d",
          "root.sg.d2",
          "root.sg.d",
          "root.sg.d2",
          "root.sg.d",
          "root.sg.d2",
          "root.sg.d",
          "root.sg.d",
          "root.sg.d2",
          "root.sg.d2",
          "root.sg.d",
          "root.sg.d2"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          String actualDevice = resultSet.getString(2);
          double actualAvg = resultSet.getDouble(3);

          assertEquals(device[i], actualDevice);
          assertEquals(times[i], actualTime);
          assertEquals(ans[i], actualAvg, 0.0001);
          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest2() {
    String sql =
        "SELECT avg_udaf(num) " + "FROM root.** " + "ORDER BY avg_udaf(num) " + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest3() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY max_value(floatNum) + avg_udaf(num) "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest4() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY max_value(floatNum) + avg_udaf(num), device ASC, time DESC "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest5() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY max_value(floatNum) + avg_udaf(num), time ASC, device DESC "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, false);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest6() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY time ASC, max_value(floatNum) + avg_udaf(num), device DESC "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, false);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest7() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY device ASC, max_value(floatNum) + avg_udaf(num), time DESC "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest8() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY max_value(floatNum) DESC, time ASC, avg_udaf(num) ASC, device DESC "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, false);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest9() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY max_value(floatNum) DESC, device ASC, avg_udaf(num) ASC, time DESC "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest10() {
    String sql =
        "SELECT avg_udaf(num+floatNum) "
            + "FROM root.** "
            + "ORDER BY time, avg_udaf(num+floatNum) "
            + "ALIGN BY DEVICE";
    String value = "537.34154";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest11() {
    String sql =
        "SELECT avg_udaf(num) "
            + "FROM root.** "
            + "ORDER BY time, avg_udaf(num+floatNum) "
            + "ALIGN BY DEVICE";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void UDAFOrderByWithAggregationAlignByDeviceTest12() {
    String sql =
        "SELECT avg_udaf(num+floatNum) "
            + "FROM root.** "
            + "ORDER BY time, avg_udaf(num) "
            + "ALIGN BY DEVICE";
    String value = "537.34154";
    checkSingleDouble(sql, value, true);
  }

  private void checkSingleDouble(String sql, Object value, boolean deviceAsc) {
    String device = "root.sg.d";
    if (!deviceAsc) device = "root.sg.d2";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        List<String> actualDeviceList = new ArrayList<>();
        List<Double> actualValueList = new ArrayList<>();
        while (resultSet.next()) {
          String actualDevice = resultSet.getString(1);
          double actualValue = resultSet.getDouble(2);

          actualDeviceList.add(actualDevice);
          actualValueList.add(actualValue);

          count++;
        }

        LOGGER.info(actualDeviceList.toString());
        LOGGER.info(actualValueList.toString());

        assertEquals(count, 2);
        assertEquals(device, actualDeviceList.get(0));
        assertEquals(Double.parseDouble(value.toString()), actualValueList.get(0), 1);
        // Change device name
        if (device.equals("root.sg.d")) {
          device = "root.sg.d2";
        } else {
          device = "root.sg.d";
        }
        assertEquals(device, actualDeviceList.get(1));
        assertEquals(Double.parseDouble(value.toString()), actualValueList.get(1), 1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
