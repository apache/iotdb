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

package org.apache.iotdb.db.it.orderBy;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.bouncycastle.util.Arrays;
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
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBOrderByIT {

  // the data can be viewed in
  // https://docs.google.com/spreadsheets/d/1OWA1bKraArCwWVnuTjuhJ5yLG0PFLdD78gD6FjquepI/edit#gid=0
  private static final String[] sql =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d.num WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d.bigNum WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d.floatNum WITH DATATYPE=DOUBLE, ENCODING=RLE, 'MAX_POINT_NUMBER'='5'",
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
        "insert into root.sg.d(timestamp,num,bigNum,floatNum,str,bool) values(51536000000,15,3147483648,235.213,\"watermelon\",TRUE)"
      };

  private static final String[] sql2 =
      new String[] {
        "CREATE TIMESERIES root.sg.d2.num WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.bigNum WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.floatNum WITH DATATYPE=DOUBLE, ENCODING=RLE, 'MAX_POINT_NUMBER'='5'",
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
        "insert into root.sg.d2(timestamp,num,bigNum,floatNum,str,bool) values(51536000000,15,3147483648,235.213,\"watermelon\",TRUE)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setSortBufferSize(1024 * 1024L);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sql) {
        statement.execute(sql);
      }
      for (String sql : sql2) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // ordinal data
  String[][] res =
      new String[][] {
        {"0", "3", "2947483648", "231.2121", "coconut", "false"},
        {"20", "2", "2147483648", "434.12", "pineapple", "true"},
        {"40", "1", "2247483648", "12.123", "apricot", "true"},
        {"80", "9", "2147483646", "43.12", "apple", "false"},
        {"100", "8", "2147483964", "4654.231", "papaya", "true"},
        {"31536000000", "6", "2147483650", "1231.21", "banana", "true"},
        {"31536000100", "10", "3147483648", "231.55", "pumelo", "false"},
        {"31536000500", "4", "2147493648", "213.1", "peach", "false"},
        {"31536001000", "5", "2149783648", "56.32", "orange", "false"},
        {"31536010000", "7", "2147983648", "213.112", "lemon", "true"},
        {"31536100000", "11", "2147468648", "54.121", "pitaya", "false"},
        {"41536000000", "12", "2146483648", "45.231", "strawberry", "false"},
        {"41536000020", "14", "2907483648", "231.34", "cherry", "false"},
        {"41536900000", "13", "2107483648", "54.12", "lychee", "true"},
        {"51536000000", "15", "3147483648", "235.213", "watermelon", "true"},
      };

  private void checkHeader(ResultSetMetaData resultSetMetaData, String[] title)
      throws SQLException {
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(title[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  private void testNormalOrderBy(String sql, int[] ans) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(
            metaData,
            new String[] {
              "Time",
              "root.sg.d.num",
              "root.sg.d.bigNum",
              "root.sg.d.floatNum",
              "root.sg.d.str",
              "root.sg.d.bool"
            });
        int i = 0;
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          String actualNum = resultSet.getString(2);
          String actualBigNum = resultSet.getString(3);
          double actualFloatNum = resultSet.getDouble(4);
          String actualStr = resultSet.getString(5);
          String actualBool = resultSet.getString(6);

          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(res[ans[i]][1], actualNum);
          assertEquals(res[ans[i]][2], actualBigNum);
          assertEquals(Double.parseDouble(res[ans[i]][3]), actualFloatNum, 0.0001);
          assertEquals(res[ans[i]][4], actualStr);
          assertEquals(res[ans[i]][5], actualBool);

          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  // 1. One-level order by test
  @Test
  public void orderByTest1() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by num";
    int[] ans = {2, 1, 0, 7, 8, 5, 9, 4, 3, 6, 10, 11, 13, 12, 14};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest2() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by bigNum,time";
    int[] ans = {13, 11, 10, 3, 1, 5, 4, 7, 9, 8, 2, 12, 0, 6, 14};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest3() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by floatNum";
    int[] ans = {2, 3, 11, 13, 10, 8, 7, 9, 0, 12, 6, 14, 1, 5, 4};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest4() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by str";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest5() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by num desc";
    int[] ans = {2, 1, 0, 7, 8, 5, 9, 4, 3, 6, 10, 11, 13, 12, 14};
    testNormalOrderBy(sql, Arrays.reverse(ans));
  }

  @Test
  public void orderByTest6() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.sg.d order by bigNum desc, time asc";
    int[] ans = {6, 14, 0, 12, 2, 8, 9, 7, 4, 5, 1, 3, 10, 11, 13};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest7() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by floatNum desc";
    int[] ans = {2, 3, 11, 13, 10, 8, 7, 9, 0, 12, 6, 14, 1, 5, 4};
    testNormalOrderBy(sql, Arrays.reverse(ans));
  }

  @Test
  public void orderByTest8() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by str desc";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderBy(sql, Arrays.reverse(ans));
  }

  @Test
  public void orderByTest17() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by str desc, str asc";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderBy(sql, Arrays.reverse(ans));
  }

  @Test
  public void orderByTest18() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by str, str";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest15() {
    String sql = "select num+bigNum,floatNum from root.sg.d order by str";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(
            metaData,
            new String[] {"Time", "root.sg.d.num + root.sg.d.bigNum", "root.sg.d.floatNum"});
        int i = 0;
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          double actualNum = resultSet.getDouble(2);
          double actualFloat = resultSet.getDouble(3);

          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(
              Long.parseLong(res[ans[i]][1]) + Long.parseLong(res[ans[i]][2]), actualNum, 0.0001);
          assertEquals(Double.parseDouble(res[ans[i]][3]), actualFloat, 0.0001);

          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  // 2. Multi-level order by test
  @Test
  public void orderByTest9() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by bool asc, str asc";
    int[] ans = {3, 12, 0, 8, 7, 10, 6, 11, 2, 5, 9, 13, 4, 1, 14};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest10() {
    String sql = "select num,bigNum,floatNum,str,bool from root.sg.d order by bool asc, num desc";
    int[] ans = {12, 11, 10, 6, 3, 8, 7, 0, 14, 13, 4, 9, 5, 1, 2};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest11() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.sg.d order by bigNum desc, floatNum desc";
    int[] ans = {14, 6, 0, 12, 2, 8, 9, 7, 4, 5, 1, 3, 10, 11, 13};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest12() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.sg.d order by str desc, floatNum desc";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderBy(sql, Arrays.reverse(ans));
  }

  @Test
  public void orderByTest13() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.sg.d order by num+floatNum desc, floatNum desc";
    int[] ans = {4, 5, 1, 14, 12, 6, 0, 9, 7, 13, 10, 8, 11, 3, 2};
    testNormalOrderBy(sql, ans);
  }

  @Test
  public void orderByTest14() {
    String sql = "select num+bigNum from root.sg.d order by num+floatNum desc, floatNum desc";
    int[] ans = {4, 5, 1, 14, 12, 6, 0, 9, 7, 13, 10, 8, 11, 3, 2};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(metaData, new String[] {"Time", "root.sg.d.num + root.sg.d.bigNum"});
        int i = 0;
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          double actualNum = resultSet.getDouble(2);

          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(
              Long.parseLong(res[ans[i]][1]) + Long.parseLong(res[ans[i]][2]), actualNum, 0.001);

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
  public void orderByTest16() {
    String sql = "select num+floatNum from root.sg.d order by floatNum+num desc, floatNum desc";
    int[] ans = {4, 5, 1, 14, 12, 6, 0, 9, 7, 13, 10, 8, 11, 3, 2};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(metaData, new String[] {"Time", "root.sg.d.num + root.sg.d.floatNum"});
        int i = 0;
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          double actualNum = resultSet.getDouble(2);

          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(
              Long.parseLong(res[ans[i]][1]) + Double.parseDouble(res[ans[i]][3]),
              actualNum,
              0.001);

          i++;
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  // 3. aggregation query
  @Test
  public void orderByInAggregationTest() {
    String sql = "select avg(num) from root.sg.d group by session(10000ms) order by avg(num) desc";
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
  public void orderByInAggregationTest2() {
    String sql =
        "select avg(num) from root.sg.d group by session(10000ms) order by max_value(floatNum)";
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
  public void orderByInAggregationTest3() {
    String sql =
        "select avg(num) from root.sg.d group by session(10000ms) order by avg(num) desc,max_value(floatNum)";
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
  public void orderByInAggregationTest4() {
    String sql =
        "select avg(num)+avg(floatNum) from root.sg.d group by session(10000ms) order by avg(num)+avg(floatNum)";
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
  public void orderByInAggregationTest5() {
    String sql =
        "select min_value(bigNum) from root.sg.d group by session(10000ms) order by avg(num)+avg(floatNum)";
    long[] ans =
        new long[] {2147483646L, 2147483650L, 2147468648L, 2146483648L, 2107483648L, 3147483648L};
    long[] times =
        new long[] {0L, 31536000000L, 31536100000L, 41536000000L, 41536900000L, 51536000000L};
    int[] order = new int[] {2, 4, 3, 5, 1, 0};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          long actualMinValue = resultSet.getLong(2);
          assertEquals(times[order[i]], actualTime);
          assertEquals(ans[order[i]], actualMinValue, 0.0001);
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
  public void orderByInAggregationTest6() {
    String sql =
        "select min_value(num)+min_value(bigNum) from root.sg.d group by session(10000ms) order by avg(num)+avg(floatNum)";
    long[] ans =
        new long[] {2147483647L, 2147483654L, 2147468659L, 2146483660L, 2107483661L, 3147483663L};
    long[] times =
        new long[] {0L, 31536000000L, 31536100000L, 41536000000L, 41536900000L, 51536000000L};
    int[] order = new int[] {2, 4, 3, 5, 1, 0};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          long actualTime = resultSet.getLong(1);
          double actualMinValue = resultSet.getDouble(2);
          assertEquals(times[order[i]], actualTime);
          assertEquals(ans[order[i]], actualMinValue, 0.0001);
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
  public void orderByInAggregationTest7() {
    String sql =
        "select avg(num)+min_value(floatNum) from root.sg.d group by session(10000ms) order by max_value(floatNum)";
    double[][] ans =
        new double[][] {
          {13.0, 54.12, 54.12},
          {11.0, 54.121, 54.121},
          {13.0, 231.34, 45.231},
          {15.0, 235.213, 235.213},
          {6.4, 1231.21, 56.32},
          {4.6, 4654.231, 12.123}
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
          assertEquals(ans[i][0] + ans[i][2], actualAvg, 0.0001);
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
  public void orderByInAggregationTest8() {
    String sql =
        "select avg(num)+avg(floatNum) from root.sg.d group by session(10000ms) order by avg(floatNum)+avg(num)";
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

  // 4. raw data query with align by device
  private void testNormalOrderByAlignByDevice(String sql, int[] ans) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(
            metaData, new String[] {"Time", "Device", "num", "bigNum", "floatNum", "str", "bool"});
        int i = 0;
        int total = 0;
        String device = "root.sg.d";
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          String actualNum = resultSet.getString(3);
          String actualBigNum = resultSet.getString(4);
          double actualFloatNum = resultSet.getDouble(5);
          String actualStr = resultSet.getString(6);
          String actualBool = resultSet.getString(7);

          assertEquals(device, actualDevice);
          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(res[ans[i]][1], actualNum);
          assertEquals(res[ans[i]][2], actualBigNum);
          assertEquals(Double.parseDouble(res[ans[i]][3]), actualFloatNum, 0.0001);
          assertEquals(res[ans[i]][4], actualStr);
          assertEquals(res[ans[i]][5], actualBool);

          if (device.equals("root.sg.d")) {
            device = "root.sg.d2";
          } else {
            device = "root.sg.d";
            i++;
          }
          total++;
        }
        assertEquals(i, ans.length);
        assertEquals(total, ans.length * 2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void alignByDeviceOrderByTest1() {
    String sql =
        "select num+bigNum from root.** order by num+floatNum desc, floatNum desc align by device";
    int[] ans = {4, 5, 1, 14, 12, 6, 0, 9, 7, 13, 10, 8, 11, 3, 2};
    String device = "root.sg.d";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          double actualNum = resultSet.getDouble(3);

          assertEquals(device, actualDevice);
          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(
              Long.parseLong(res[ans[i]][1]) + Long.parseLong(res[ans[i]][2]), actualNum, 0.0001);
          if (device.equals("root.sg.d")) {
            device = "root.sg.d2";
          } else {
            device = "root.sg.d";
            i++;
          }
        }
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void alignByDeviceOrderByTest2() {
    String sql = "select num,bigNum,floatNum,str,bool from root.** order by num align by device";
    int[] ans = {2, 1, 0, 7, 8, 5, 9, 4, 3, 6, 10, 11, 13, 12, 14};
    testNormalOrderByAlignByDevice(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest3() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by floatNum align by device";
    int[] ans = {2, 3, 11, 13, 10, 8, 7, 9, 0, 12, 6, 14, 1, 5, 4};
    testNormalOrderByAlignByDevice(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest4() {
    String sql = "select num,bigNum,floatNum,str,bool from root.** order by str align by device";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderByAlignByDevice(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest5() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by num desc align by device";
    int[] ans = {2, 1, 0, 7, 8, 5, 9, 4, 3, 6, 10, 11, 13, 12, 14};
    testNormalOrderByAlignByDevice(sql, Arrays.reverse(ans));
  }

  @Test
  public void alignByDeviceOrderByTest6() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by str desc align by device";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderByAlignByDevice(sql, Arrays.reverse(ans));
  }

  @Test
  public void alignByDeviceOrderByTest7() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by bool asc, num desc align by device";
    int[] ans = {12, 11, 10, 6, 3, 8, 7, 0, 14, 13, 4, 9, 5, 1, 2};
    testNormalOrderByAlignByDevice(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest8() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by bigNum desc, floatNum desc align by device";
    int[] ans = {14, 6, 0, 12, 2, 8, 9, 7, 4, 5, 1, 3, 10, 11, 13};
    testNormalOrderByAlignByDevice(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest9() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by str desc, floatNum desc align by device";
    int[] ans = {3, 2, 5, 12, 0, 9, 13, 8, 4, 7, 1, 10, 6, 11, 14};
    testNormalOrderByAlignByDevice(sql, Arrays.reverse(ans));
  }

  private void testNormalOrderByMixAlignBy(String sql, int[] ans) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(
            metaData, new String[] {"Time", "Device", "num", "bigNum", "floatNum", "str", "bool"});
        int i = 0;
        int total = 0;
        String device = "root.sg.d";
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          String actualNum = resultSet.getString(3);
          String actualBigNum = resultSet.getString(4);
          double actualFloatNum = resultSet.getDouble(5);
          String actualStr = resultSet.getString(6);
          String actualBool = resultSet.getString(7);

          assertEquals(device, actualDevice);
          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(res[ans[i]][1], actualNum);
          assertEquals(res[ans[i]][2], actualBigNum);
          assertEquals(Double.parseDouble(res[ans[i]][3]), actualFloatNum, 0.0001);
          assertEquals(res[ans[i]][4], actualStr);
          assertEquals(res[ans[i]][5], actualBool);

          if (device.equals("root.sg.d2")) {
            i++;
            device = "root.sg.d";
          } else {
            device = "root.sg.d2";
          }

          total++;
        }
        assertEquals(total, ans.length * 2);
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void testDeviceViewOrderByMixAlignBy(String sql, int[] ans) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(
            metaData, new String[] {"Time", "Device", "num", "bigNum", "floatNum", "str", "bool"});
        int i = 0;
        int total = 0;
        String device = "root.sg.d2";
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          String actualNum = resultSet.getString(3);
          String actualBigNum = resultSet.getString(4);
          double actualFloatNum = resultSet.getDouble(5);
          String actualStr = resultSet.getString(6);
          String actualBool = resultSet.getString(7);

          assertEquals(device, actualDevice);
          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(res[ans[i]][1], actualNum);
          assertEquals(res[ans[i]][2], actualBigNum);
          assertEquals(Double.parseDouble(res[ans[i]][3]), actualFloatNum, 0.0001);
          assertEquals(res[ans[i]][4], actualStr);
          assertEquals(res[ans[i]][5], actualBool);

          i++;
          total++;
          if (total == ans.length) {
            i = 0;
            if (device.equals("root.sg.d2")) {
              device = "root.sg.d";
            } else {
              device = "root.sg.d2";
            }
          }
        }
        assertEquals(total, ans.length * 2);
        assertEquals(i, ans.length);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void orderByBigNumAlignByDevice(String sql, int[] ans) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        checkHeader(
            metaData, new String[] {"Time", "Device", "num", "bigNum", "floatNum", "str", "bool"});
        int i = 0;
        int total = 0;
        String device = "root.sg.d";
        while (resultSet.next()) {

          String actualTime = resultSet.getString(1);
          String actualDevice = resultSet.getString(2);
          String actualNum = resultSet.getString(3);
          String actualBigNum = resultSet.getString(4);
          double actualFloatNum = resultSet.getDouble(5);
          String actualStr = resultSet.getString(6);
          String actualBool = resultSet.getString(7);

          if (total < 4) {
            i = total % 2;
            if (total < 2) {
              device = "root.sg.d2";
            } else {
              device = "root.sg.d";
            }
          }

          assertEquals(device, actualDevice);
          assertEquals(res[ans[i]][0], actualTime);
          assertEquals(res[ans[i]][1], actualNum);
          assertEquals(res[ans[i]][2], actualBigNum);
          assertEquals(Double.parseDouble(res[ans[i]][3]), actualFloatNum, 0.0001);
          assertEquals(res[ans[i]][4], actualStr);
          assertEquals(res[ans[i]][5], actualBool);

          if (device.equals("root.sg.d2")) {
            device = "root.sg.d";
          } else {
            i++;
            device = "root.sg.d2";
          }

          total++;
        }
        assertEquals(i, ans.length);
        assertEquals(total, ans.length * 2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void alignByDeviceOrderByTest12() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by bigNum desc, device desc, time asc align by device";
    int[] ans = {6, 14, 0, 12, 2, 8, 9, 7, 4, 5, 1, 3, 10, 11, 13};
    orderByBigNumAlignByDevice(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest13() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by bigNum desc, time desc, device asc align by device";
    int[] ans = {14, 6, 0, 12, 2, 8, 9, 7, 4, 5, 1, 3, 10, 11, 13};
    testNormalOrderByMixAlignBy(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest14() {
    int[] ans = {14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by time desc, bigNum desc, device asc align by device";
    testNormalOrderByMixAlignBy(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest15() {
    int[] ans = {6, 14, 0, 12, 2, 8, 9, 7, 4, 5, 1, 3, 10, 11, 13};
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by device desc, bigNum desc, time asc align by device";
    testDeviceViewOrderByMixAlignBy(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest16() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by device desc, time asc, bigNum desc align by device";
    int[] ans = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
    testDeviceViewOrderByMixAlignBy(sql, ans);
  }

  @Test
  public void alignByDeviceOrderByTest17() {
    String sql =
        "select num,bigNum,floatNum,str,bool from root.** order by bigNum desc, device desc, num asc, time asc align by device";
    int[] ans = {6, 14, 0, 12, 2, 8, 9, 7, 4, 5, 1, 3, 10, 11, 13};
    orderByBigNumAlignByDevice(sql, ans);
  }

  // 5. aggregation query align by device
  @Test
  public void orderByInAggregationAlignByDeviceTest() {
    String sql =
        "select avg(num) from root.** group by session(10000ms) order by avg(num) align by device";

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
  public void orderByInAggregationAlignByDeviceTest2() {
    String sql = "select avg(num) from root.** order by avg(num) align by device";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  private void checkSingleDouble(String sql, Object value, boolean deviceAsc) {
    String device = "root.sg.d";
    if (!deviceAsc) device = "root.sg.d2";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          String deviceName = resultSet.getString(1);
          double actualVal = resultSet.getDouble(2);
          assertEquals(deviceName, device);
          assertEquals(Double.parseDouble(value.toString()), actualVal, 1);
          if (device.equals("root.sg.d")) device = "root.sg.d2";
          else device = "root.sg.d";
          i++;
        }
        assertEquals(i, 2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest3() {
    String sql =
        "select avg(num)+avg(bigNum) from root.** order by max_value(floatNum) align by device";
    long value = 2388936669L + 8;
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest4() {

    String sql =
        "select avg(num)+avg(bigNum) from root.** order by max_value(floatNum)+min_value(num) align by device";
    long value = 2388936669L + 8;
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest5() {
    String sql =
        "select avg(num) from root.** order by max_value(floatNum)+avg(num) align by device";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest6() {
    String sql =
        "select avg(num) from root.** order by max_value(floatNum)+avg(num), device asc, time desc align by device";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest7() {
    String sql =
        "select avg(num) from root.** order by max_value(floatNum)+avg(num), time asc, device desc align by device";
    String value = "8";
    checkSingleDouble(sql, value, false);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest8() {
    String sql =
        "select avg(num) from root.** order by time asc, max_value(floatNum)+avg(num), device desc align by device";
    String value = "8";
    checkSingleDouble(sql, value, false);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest9() {
    String sql =
        "select avg(num) from root.** order by device asc, max_value(floatNum)+avg(num), time desc align by device";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest10() {
    String sql =
        "select avg(num) from root.** order by max_value(floatNum) desc,time asc, avg(num) asc, device desc align by device";
    String value = "8";
    checkSingleDouble(sql, value, false);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest11() {
    String sql =
        "select avg(num) from root.** order by max_value(floatNum) desc,device asc, avg(num) asc, time desc align by device";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest12() {
    String sql =
        "select avg(num+floatNum) from root.** order by time,avg(num+floatNum) align by device";
    String value = "537.34154";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest13() {
    String sql = "select avg(num) from root.** order by time,avg(num+floatNum) align by device";
    String value = "8";
    checkSingleDouble(sql, value, true);
  }

  @Test
  public void orderByInAggregationAlignByDeviceTest14() {
    String sql = "select avg(num+floatNum) from root.** order by time,avg(num) align by device";
    String value = "537.34154";
    checkSingleDouble(sql, value, true);
  }

  String[][] UDFRes =
      new String[][] {
        {"0", "3", "0", "0"},
        {"20", "2", "0", "0"},
        {"40", "1", "0", "0"},
        {"80", "9", "0", "0"},
        {"100", "8", "0", "0"},
        {"31536000000", "6", "0", "0"},
        {"31536000100", "10", "0", "0"},
        {"31536000500", "4", "0", "0"},
        {"31536001000", "5", "0", "0"},
        {"31536010000", "7", "0", "0"},
        {"31536100000", "11", "0", "0"},
        {"41536000000", "12", "2146483648", "0"},
        {"41536000020", "14", "0", "14"},
        {"41536900000", "13", "2107483648", "0"},
        {"51536000000", "15", "0", "15"},
      };

  // UDF Test
  private void orderByUDFTest(String sql, int[] ans) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          String time = resultSet.getString(1);
          String num = resultSet.getString(2);
          String topK = resultSet.getString(3);
          String bottomK = resultSet.getString(4);

          assertEquals(time, UDFRes[ans[i]][0]);
          assertEquals(num, UDFRes[ans[i]][1]);
          if (Objects.equals(UDFRes[ans[i]][3], "0")) {
            assertNull(topK);
          } else {
            assertEquals(topK, UDFRes[ans[i]][3]);
          }

          if (Objects.equals(UDFRes[ans[i]][2], "0")) {
            assertNull(bottomK);
          } else {
            assertEquals(bottomK, UDFRes[ans[i]][2]);
          }

          i++;
        }
        assertEquals(i, 15);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void orderByUDFTest1() {
    String sql =
        "select num, top_k(num, 'k'='2'), bottom_k(bigNum, 'k'='2') from root.sg.d order by top_k(num, 'k'='2') nulls first, bottom_k(bigNum, 'k'='2') nulls first";
    int[] ans = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 11, 12, 14};
    orderByUDFTest(sql, ans);
  }

  @Test
  public void orderByUDFTest2() {
    String sql =
        "select num, top_k(num, 'k'='2'), bottom_k(bigNum, 'k'='2') from root.sg.d order by top_k(num, 'k'='2'), bottom_k(bigNum, 'k'='2')";
    int[] ans = {12, 14, 13, 11, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    orderByUDFTest(sql, ans);
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
        "select num from root.sg.d order by avg(bigNum)",
        "701: Raw data and aggregation hybrid query is not supported.");
  }

  @Test
  public void errorTest2() {
    errorTest(
        "select avg(num) from root.sg.d order by bigNum",
        "701: Raw data and aggregation hybrid query is not supported.");
  }

  @Test
  public void errorTest3() {
    errorTest(
        "select bigNum,floatNum from root.sg.d order by s1",
        "701: root.sg.d.s1 in order by clause doesn't exist.");
  }

  @Test
  public void errorTest4() {
    errorTest(
        "select bigNum,floatNum from root.** order by bigNum",
        "701: root.**.bigNum in order by clause shouldn't refer to more than one timeseries.");
  }

  @Test
  public void errorTest5() {
    errorTest(
        "select bigNum,floatNum from root.** order by s1 align by device",
        "701: s1 in order by clause doesn't exist.");
  }

  @Test
  public void errorTest6() {
    errorTest(
        "select bigNum,floatNum from root.** order by root.sg.d.bigNum align by device",
        "701: ALIGN BY DEVICE: the suffix paths can only be measurement or one-level wildcard");
  }

  @Test
  public void errorTest7() {
    errorTest(
        "select last bigNum,floatNum from root.** order by root.sg.d.bigNum",
        "701: root.sg.d.bigNum in order by clause doesn't exist in the result of last query.");
  }

  // last query
  public void testLastQueryOrderBy(String sql, String[][] ans) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int i = 0;
        while (resultSet.next()) {
          String time = resultSet.getString(1);
          String num = resultSet.getString(2);
          String value = resultSet.getString(3);
          String dataType = resultSet.getString(4);

          assertEquals(time, ans[0][i]);
          assertEquals(num, ans[1][i]);
          assertEquals(value, ans[2][i]);
          assertEquals(dataType, ans[3][i]);

          i++;
        }
        assertEquals(i, 4);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void lastQueryOrderBy() {
    String[][] ans =
        new String[][] {
          {"51536000000", "51536000000", "51536000000", "51536000000"},
          {"root.sg.d.num", "root.sg.d2.num", "root.sg.d.bigNum", "root.sg.d2.bigNum"},
          {"15", "15", "3147483648", "3147483648"},
          {"INT32", "INT32", "INT64", "INT64"}
        };
    String sql = "select last bigNum,num from root.** order by value, timeseries";
    testLastQueryOrderBy(sql, ans);
  }

  @Test
  public void lastQueryOrderBy2() {
    String[][] ans =
        new String[][] {
          {"51536000000", "51536000000", "51536000000", "51536000000"},
          {"root.sg.d2.num", "root.sg.d2.bigNum", "root.sg.d.num", "root.sg.d.bigNum"},
          {"15", "3147483648", "15", "3147483648"},
          {"INT32", "INT64", "INT32", "INT64"}
        };
    String sql = "select last bigNum,num from root.** order by timeseries desc";
    testLastQueryOrderBy(sql, ans);
  }

  @Test
  public void lastQueryOrderBy3() {
    String[][] ans =
        new String[][] {
          {"51536000000", "51536000000", "51536000000", "51536000000"},
          {"root.sg.d2.num", "root.sg.d2.bigNum", "root.sg.d.num", "root.sg.d.bigNum"},
          {"15", "3147483648", "15", "3147483648"},
          {"INT32", "INT64", "INT32", "INT64"}
        };
    String sql = "select last bigNum,num from root.** order by timeseries desc, value asc";
    testLastQueryOrderBy(sql, ans);
  }

  @Test
  public void lastQueryOrderBy4() {
    String[][] ans =
        new String[][] {
          {"51536000000", "51536000000", "51536000000", "51536000000"},
          {"root.sg.d2.num", "root.sg.d.num", "root.sg.d2.bigNum", "root.sg.d.bigNum"},
          {"15", "15", "3147483648", "3147483648"},
          {"INT32", "INT32", "INT64", "INT64"}
        };
    String sql = "select last bigNum,num from root.** order by value, timeseries desc";
    testLastQueryOrderBy(sql, ans);
  }

  @Test
  public void lastQueryOrderBy5() {
    String[][] ans =
        new String[][] {
          {"51536000000", "51536000000", "51536000000", "51536000000"},
          {"root.sg.d2.num", "root.sg.d.num", "root.sg.d2.bigNum", "root.sg.d.bigNum"},
          {"15", "15", "3147483648", "3147483648"},
          {"INT32", "INT32", "INT64", "INT64"}
        };
    String sql = "select last bigNum,num from root.** order by datatype, timeseries desc";
    testLastQueryOrderBy(sql, ans);
  }
}
