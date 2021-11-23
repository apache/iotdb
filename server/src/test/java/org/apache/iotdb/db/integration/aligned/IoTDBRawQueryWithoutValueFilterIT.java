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
package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IoTDBRawQueryWithoutValueFilterIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    // TODO When the aligned time series support compaction, we need to set compaction to true
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void selectAllAlignedWithoutValueFilterTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "1,1.0,1,1,true,aligned_test1",
          "2,2.0,2,2,null,aligned_test2",
          "3,30000.0,null,30000,true,aligned_unseq_test3",
          "4,4.0,4,null,true,aligned_test4",
          "5,5.0,5,null,true,aligned_test5",
          "6,6.0,6,6,true,null",
          "7,7.0,7,7,false,aligned_test7",
          "8,8.0,8,8,null,aligned_test8",
          "9,9.0,9,9,false,aligned_test9",
          "10,null,10,10,true,aligned_test10",
          "11,11.0,11,11,null,null",
          "12,12.0,12,12,null,null",
          "13,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,14.0,14,14,null,null",
          "15,15.0,15,15,null,null",
          "16,16.0,16,16,null,null",
          "17,17.0,17,17,null,null",
          "18,18.0,18,18,null,null",
          "19,19.0,19,19,null,null",
          "20,20.0,20,20,null,null",
          "21,null,null,21,true,null",
          "22,null,null,22,true,null",
          "23,230000.0,null,230000,false,null",
          "24,null,null,24,true,null",
          "25,null,null,25,true,null",
          "26,null,null,26,false,null",
          "27,null,null,27,false,null",
          "28,null,null,28,false,null",
          "29,null,null,29,false,null",
          "30,null,null,30,false,null",
          "31,null,31,null,null,aligned_test31",
          "32,null,32,null,null,aligned_test32",
          "33,null,33,null,null,aligned_test33",
          "34,null,34,null,null,aligned_test34",
          "35,null,35,null,null,aligned_test35",
          "36,null,36,null,null,aligned_test36",
          "37,null,37,null,null,aligned_test37",
          "38,null,38,null,null,aligned_test38",
          "39,null,39,null,null,aligned_test39",
          "40,null,40,null,null,aligned_test40",
        };

    String[] columnNames = {
      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.sg1.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedAndNonAlignedTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "1,1.0,1,1,true,aligned_test1,1.0,1,1,true,non_aligned_test1",
          "2,2.0,2,2,null,aligned_test2,2.0,2,2,null,non_aligned_test2",
          "3,30000.0,null,30000,true,aligned_unseq_test3,3.0,null,3,false,non_aligned_test3",
          "4,4.0,4,null,true,aligned_test4,4.0,4,null,true,non_aligned_test4",
          "5,5.0,5,null,true,aligned_test5,5.0,5,null,true,non_aligned_test5",
          "6,6.0,6,6,true,null,6.0,6,6,true,null",
          "7,7.0,7,7,false,aligned_test7,7.0,7,7,false,non_aligned_test7",
          "8,8.0,8,8,null,aligned_test8,8.0,8,8,null,non_aligned_test8",
          "9,9.0,9,9,false,aligned_test9,9.0,9,9,false,non_aligned_test9",
          "10,null,10,10,true,aligned_test10,null,10,10,true,non_aligned_test10",
          "11,11.0,11,11,null,null,11.0,11,11,null,null",
          "12,12.0,12,12,null,null,12.0,12,12,null,null",
          "13,130000.0,130000,130000,true,aligned_unseq_test13,13.0,13,13,null,null",
          "14,14.0,14,14,null,null,14.0,14,14,null,null",
          "15,15.0,15,15,null,null,15.0,15,15,null,null",
          "16,16.0,16,16,null,null,16.0,16,16,null,null",
          "17,17.0,17,17,null,null,17.0,17,17,null,null",
          "18,18.0,18,18,null,null,18.0,18,18,null,null",
          "19,19.0,19,19,null,null,19.0,19,19,null,null",
          "20,20.0,20,20,null,null,20.0,20,20,null,null",
          "21,null,null,21,true,null,null,null,21,true,null",
          "22,null,null,22,true,null,null,null,22,true,null",
          "23,230000.0,null,230000,false,null,null,null,23,true,null",
          "24,null,null,24,true,null,null,null,24,true,null",
          "25,null,null,25,true,null,null,null,25,true,null",
          "26,null,null,26,false,null,null,null,26,false,null",
          "27,null,null,27,false,null,null,null,27,false,null",
          "28,null,null,28,false,null,null,null,28,false,null",
          "29,null,null,29,false,null,null,null,29,false,null",
          "30,null,null,30,false,null,null,null,30,false,null",
          "31,null,31,null,null,aligned_test31,null,31,null,null,non_aligned_test31",
          "32,null,32,null,null,aligned_test32,null,32,null,null,non_aligned_test32",
          "33,null,33,null,null,aligned_test33,null,33,null,null,non_aligned_test33",
          "34,null,34,null,null,aligned_test34,null,34,null,null,non_aligned_test34",
          "35,null,35,null,null,aligned_test35,null,35,null,null,non_aligned_test35",
          "36,null,36,null,null,aligned_test36,null,36,null,null,non_aligned_test36",
          "37,null,37,null,null,aligned_test37,null,37,null,null,non_aligned_test37",
          "38,null,38,null,null,aligned_test38,null,38,null,null,non_aligned_test38",
          "39,null,39,null,null,aligned_test39,null,39,null,null,non_aligned_test39",
          "40,null,40,null,null,aligned_test40,null,40,null,null,non_aligned_test40",
        };

    String[] columnNames = {
      "root.sg1.d1.s1",
      "root.sg1.d1.s2",
      "root.sg1.d1.s3",
      "root.sg1.d1.s4",
      "root.sg1.d1.s5",
      "root.sg1.d2.s1",
      "root.sg1.d2.s2",
      "root.sg1.d2.s3",
      "root.sg1.d2.s4",
      "root.sg1.d2.s5"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.sg1.*");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedWithTimeFilterTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "9,9.0,9,9,false,aligned_test9",
          "10,null,10,10,true,aligned_test10",
          "11,11.0,11,11,null,null",
          "12,12.0,12,12,null,null",
          "13,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,14.0,14,14,null,null",
          "15,15.0,15,15,null,null",
          "16,16.0,16,16,null,null",
          "17,17.0,17,17,null,null",
          "18,18.0,18,18,null,null",
          "19,19.0,19,19,null,null",
          "20,20.0,20,20,null,null",
          "21,null,null,21,true,null",
          "22,null,null,22,true,null",
          "23,230000.0,null,230000,false,null",
          "24,null,null,24,true,null",
          "25,null,null,25,true,null",
          "26,null,null,26,false,null",
          "27,null,null,27,false,null",
          "28,null,null,28,false,null",
          "29,null,null,29,false,null",
          "30,null,null,30,false,null",
          "31,null,31,null,null,aligned_test31",
          "32,null,32,null,null,aligned_test32",
          "33,null,33,null,null,aligned_test33",
        };

    String[] columnNames = {
      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select * from root.sg1.d1 where time >= 9 and time <= 33");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedWithoutValueFilterTest1() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "1,1.0,true,aligned_test1",
          "2,2.0,null,aligned_test2",
          "3,30000.0,true,aligned_unseq_test3",
          "4,4.0,true,aligned_test4",
          "5,5.0,true,aligned_test5",
          "6,6.0,true,null",
          "7,7.0,false,aligned_test7",
          "8,8.0,null,aligned_test8",
          "9,9.0,false,aligned_test9",
          "10,null,true,aligned_test10",
          "11,11.0,null,null",
          "12,12.0,null,null",
          "13,130000.0,true,aligned_unseq_test13",
          "14,14.0,null,null",
          "15,15.0,null,null",
          "16,16.0,null,null",
          "17,17.0,null,null",
          "18,18.0,null,null",
          "19,19.0,null,null",
          "20,20.0,null,null",
          "21,null,true,null",
          "22,null,true,null",
          "23,230000.0,false,null",
          "24,null,true,null",
          "25,null,true,null",
          "26,null,false,null",
          "27,null,false,null",
          "28,null,false,null",
          "29,null,false,null",
          "30,null,false,null",
          "31,null,null,aligned_test31",
          "32,null,null,aligned_test32",
          "33,null,null,aligned_test33",
          "34,null,null,aligned_test34",
          "35,null,null,aligned_test35",
          "36,null,null,aligned_test36",
          "37,null,null,aligned_test37",
          "38,null,null,aligned_test38",
          "39,null,null,aligned_test39",
          "40,null,null,aligned_test40",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select s1,s4,s5 from root.sg1.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedWithoutValueFilterTest2() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "1,1.0,true",
          "2,2.0,null",
          "3,30000.0,true",
          "4,4.0,true",
          "5,5.0,true",
          "6,6.0,true",
          "7,7.0,false",
          "8,8.0,null",
          "9,9.0,false",
          "10,null,true",
          "11,11.0,null",
          "12,12.0,null",
          "13,130000.0,true",
          "14,14.0,null",
          "15,15.0,null",
          "16,16.0,null",
          "17,17.0,null",
          "18,18.0,null",
          "19,19.0,null",
          "20,20.0,null",
          "21,null,true",
          "22,null,true",
          "23,230000.0,false",
          "24,null,true",
          "25,null,true",
          "26,null,false",
          "27,null,false",
          "28,null,false",
          "29,null,false",
          "30,null,false",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select s1,s4 from root.sg1.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedWithTimeFilterTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "16,16.0,null,null",
          "17,17.0,null,null",
          "18,18.0,null,null",
          "19,19.0,null,null",
          "20,20.0,null,null",
          "21,null,true,null",
          "22,null,true,null",
          "23,230000.0,false,null",
          "24,null,true,null",
          "25,null,true,null",
          "26,null,false,null",
          "27,null,false,null",
          "28,null,false,null",
          "29,null,false,null",
          "30,null,false,null",
          "31,null,null,aligned_test31",
          "32,null,null,aligned_test32",
          "33,null,null,aligned_test33",
          "34,null,null,aligned_test34",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select s1,s4,s5 from root.sg1.d1 where time >= 16 and time <= 34");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedAndNonAlignedWithTimeFilterTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "16,null,null,16.0,null,null,16.0",
          "17,null,null,17.0,null,null,17.0",
          "18,null,null,18.0,null,null,18.0",
          "19,null,null,19.0,null,null,19.0",
          "20,null,null,20.0,null,null,20.0",
          "21,null,true,null,null,true,null",
          "22,null,true,null,null,true,null",
          "23,null,false,null,null,true,230000.0",
          "24,null,true,null,null,true,null",
          "25,null,true,null,null,true,null",
          "26,null,false,null,null,false,null",
          "27,null,false,null,null,false,null",
          "28,null,false,null,null,false,null",
          "29,null,false,null,null,false,null",
          "30,null,false,null,null,false,null",
          "31,non_aligned_test31,null,null,aligned_test31,null,null",
          "32,non_aligned_test32,null,null,aligned_test32,null,null",
          "33,non_aligned_test33,null,null,aligned_test33,null,null",
          "34,non_aligned_test34,null,null,aligned_test34,null,null",
        };

    String[] columnNames = {
      "root.sg1.d2.s5",
      "root.sg1.d1.s4",
      "root.sg1.d2.s1",
      "root.sg1.d1.s5",
      "root.sg1.d2.s4",
      "root.sg1.d1.s1"
    };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // 1 4 5
      boolean hasResultSet =
          statement.execute(
              "select d2.s5, d1.s4, d2.s1, d1.s5, d2.s4, d1.s1 from root.sg1 where time >= 16 and time <= 34");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
