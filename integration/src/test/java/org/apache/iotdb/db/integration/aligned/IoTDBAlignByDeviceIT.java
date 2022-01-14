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
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.constant.TestConstant.maxTime;
import static org.apache.iotdb.db.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.constant.TestConstant.minTime;
import static org.apache.iotdb.db.constant.TestConstant.minValue;
import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test is for aligned time series exactly, which is different from
 * integration/IoTDBAlignByDeviceIT.
 */
@Category({LocalStandaloneTest.class})
public class IoTDBAlignByDeviceIT {

  private static final double DELTA = 1e-6;
  private static final String TIMESTAMP_STR = "Time";
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
          "1,root.sg1.d1,1.0,1,1,true,aligned_test1",
          "2,root.sg1.d1,2.0,2,2,null,aligned_test2",
          "3,root.sg1.d1,30000.0,null,30000,true,aligned_unseq_test3",
          "4,root.sg1.d1,4.0,4,null,true,aligned_test4",
          "5,root.sg1.d1,5.0,5,null,true,aligned_test5",
          "6,root.sg1.d1,6.0,6,6,true,null",
          "7,root.sg1.d1,7.0,7,7,false,aligned_test7",
          "8,root.sg1.d1,8.0,8,8,null,aligned_test8",
          "9,root.sg1.d1,9.0,9,9,false,aligned_test9",
          "10,root.sg1.d1,null,10,10,true,aligned_test10",
          "11,root.sg1.d1,11.0,11,11,null,null",
          "12,root.sg1.d1,12.0,12,12,null,null",
          "13,root.sg1.d1,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,root.sg1.d1,14.0,14,14,null,null",
          "15,root.sg1.d1,15.0,15,15,null,null",
          "16,root.sg1.d1,16.0,16,16,null,null",
          "17,root.sg1.d1,17.0,17,17,null,null",
          "18,root.sg1.d1,18.0,18,18,null,null",
          "19,root.sg1.d1,19.0,19,19,null,null",
          "20,root.sg1.d1,20.0,20,20,null,null",
          "21,root.sg1.d1,null,null,21,true,null",
          "22,root.sg1.d1,null,null,22,true,null",
          "23,root.sg1.d1,230000.0,null,230000,false,null",
          "24,root.sg1.d1,null,null,24,true,null",
          "25,root.sg1.d1,null,null,25,true,null",
          "26,root.sg1.d1,null,null,26,false,null",
          "27,root.sg1.d1,null,null,27,false,null",
          "28,root.sg1.d1,null,null,28,false,null",
          "29,root.sg1.d1,null,null,29,false,null",
          "30,root.sg1.d1,null,null,30,false,null",
          "31,root.sg1.d1,null,31,null,null,aligned_test31",
          "32,root.sg1.d1,null,32,null,null,aligned_test32",
          "33,root.sg1.d1,null,33,null,null,aligned_test33",
          "34,root.sg1.d1,null,34,null,null,aligned_test34",
          "35,root.sg1.d1,null,35,null,null,aligned_test35",
          "36,root.sg1.d1,null,36,null,null,aligned_test36",
          "37,root.sg1.d1,null,37,null,null,aligned_test37",
          "38,root.sg1.d1,null,38,null,null,aligned_test38",
          "39,root.sg1.d1,null,39,null,null,aligned_test39",
          "40,root.sg1.d1,null,40,null,null,aligned_test40",
        };

    String[] columnNames = {"Device", "s1", "s2", "s3", "s4", "s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.sg1.d1 align by device");
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
          "1,root.sg1.d1,1.0,1,1,true,aligned_test1",
          "2,root.sg1.d1,2.0,2,2,null,aligned_test2",
          "3,root.sg1.d1,30000.0,null,30000,true,aligned_unseq_test3",
          "4,root.sg1.d1,4.0,4,null,true,aligned_test4",
          "5,root.sg1.d1,5.0,5,null,true,aligned_test5",
          "6,root.sg1.d1,6.0,6,6,true,null",
          "7,root.sg1.d1,7.0,7,7,false,aligned_test7",
          "8,root.sg1.d1,8.0,8,8,null,aligned_test8",
          "9,root.sg1.d1,9.0,9,9,false,aligned_test9",
          "10,root.sg1.d1,null,10,10,true,aligned_test10",
          "11,root.sg1.d1,11.0,11,11,null,null",
          "12,root.sg1.d1,12.0,12,12,null,null",
          "13,root.sg1.d1,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,root.sg1.d1,14.0,14,14,null,null",
          "15,root.sg1.d1,15.0,15,15,null,null",
          "16,root.sg1.d1,16.0,16,16,null,null",
          "17,root.sg1.d1,17.0,17,17,null,null",
          "18,root.sg1.d1,18.0,18,18,null,null",
          "19,root.sg1.d1,19.0,19,19,null,null",
          "20,root.sg1.d1,20.0,20,20,null,null",
          "21,root.sg1.d1,null,null,21,true,null",
          "22,root.sg1.d1,null,null,22,true,null",
          "23,root.sg1.d1,230000.0,null,230000,false,null",
          "24,root.sg1.d1,null,null,24,true,null",
          "25,root.sg1.d1,null,null,25,true,null",
          "26,root.sg1.d1,null,null,26,false,null",
          "27,root.sg1.d1,null,null,27,false,null",
          "28,root.sg1.d1,null,null,28,false,null",
          "29,root.sg1.d1,null,null,29,false,null",
          "30,root.sg1.d1,null,null,30,false,null",
          "31,root.sg1.d1,null,31,null,null,aligned_test31",
          "32,root.sg1.d1,null,32,null,null,aligned_test32",
          "33,root.sg1.d1,null,33,null,null,aligned_test33",
          "34,root.sg1.d1,null,34,null,null,aligned_test34",
          "35,root.sg1.d1,null,35,null,null,aligned_test35",
          "36,root.sg1.d1,null,36,null,null,aligned_test36",
          "37,root.sg1.d1,null,37,null,null,aligned_test37",
          "38,root.sg1.d1,null,38,null,null,aligned_test38",
          "39,root.sg1.d1,null,39,null,null,aligned_test39",
          "40,root.sg1.d1,null,40,null,null,aligned_test40",
          "1,root.sg1.d2,1.0,1,1,true,non_aligned_test1",
          "2,root.sg1.d2,2.0,2,2,null,non_aligned_test2",
          "3,root.sg1.d2,3.0,null,3,false,non_aligned_test3",
          "4,root.sg1.d2,4.0,4,null,true,non_aligned_test4",
          "5,root.sg1.d2,5.0,5,null,true,non_aligned_test5",
          "6,root.sg1.d2,6.0,6,6,true,null",
          "7,root.sg1.d2,7.0,7,7,false,non_aligned_test7",
          "8,root.sg1.d2,8.0,8,8,null,non_aligned_test8",
          "9,root.sg1.d2,9.0,9,9,false,non_aligned_test9",
          "10,root.sg1.d2,null,10,10,true,non_aligned_test10",
          "11,root.sg1.d2,11.0,11,11,null,null",
          "12,root.sg1.d2,12.0,12,12,null,null",
          "13,root.sg1.d2,13.0,13,13,null,null",
          "14,root.sg1.d2,14.0,14,14,null,null",
          "15,root.sg1.d2,15.0,15,15,null,null",
          "16,root.sg1.d2,16.0,16,16,null,null",
          "17,root.sg1.d2,17.0,17,17,null,null",
          "18,root.sg1.d2,18.0,18,18,null,null",
          "19,root.sg1.d2,19.0,19,19,null,null",
          "20,root.sg1.d2,20.0,20,20,null,null",
          "21,root.sg1.d2,null,null,21,true,null",
          "22,root.sg1.d2,null,null,22,true,null",
          "23,root.sg1.d2,null,null,23,true,null",
          "24,root.sg1.d2,null,null,24,true,null",
          "25,root.sg1.d2,null,null,25,true,null",
          "26,root.sg1.d2,null,null,26,false,null",
          "27,root.sg1.d2,null,null,27,false,null",
          "28,root.sg1.d2,null,null,28,false,null",
          "29,root.sg1.d2,null,null,29,false,null",
          "30,root.sg1.d2,null,null,30,false,null",
          "31,root.sg1.d2,null,31,null,null,non_aligned_test31",
          "32,root.sg1.d2,null,32,null,null,non_aligned_test32",
          "33,root.sg1.d2,null,33,null,null,non_aligned_test33",
          "34,root.sg1.d2,null,34,null,null,non_aligned_test34",
          "35,root.sg1.d2,null,35,null,null,non_aligned_test35",
          "36,root.sg1.d2,null,36,null,null,non_aligned_test36",
          "37,root.sg1.d2,null,37,null,null,non_aligned_test37",
          "38,root.sg1.d2,null,38,null,null,non_aligned_test38",
          "39,root.sg1.d2,null,39,null,null,non_aligned_test39",
          "40,root.sg1.d2,null,40,null,null,non_aligned_test40",
        };

    String[] columnNames = {"Device", "s1", "s2", "s3", "s4", "s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.sg1.* align by device");
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
          "9,root.sg1.d1,9.0,9,9,false,aligned_test9",
          "10,root.sg1.d1,null,10,10,true,aligned_test10",
          "11,root.sg1.d1,11.0,11,11,null,null",
          "12,root.sg1.d1,12.0,12,12,null,null",
          "13,root.sg1.d1,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,root.sg1.d1,14.0,14,14,null,null",
          "15,root.sg1.d1,15.0,15,15,null,null",
          "16,root.sg1.d1,16.0,16,16,null,null",
          "17,root.sg1.d1,17.0,17,17,null,null",
          "18,root.sg1.d1,18.0,18,18,null,null",
          "19,root.sg1.d1,19.0,19,19,null,null",
          "20,root.sg1.d1,20.0,20,20,null,null",
          "21,root.sg1.d1,null,null,21,true,null",
          "22,root.sg1.d1,null,null,22,true,null",
          "23,root.sg1.d1,230000.0,null,230000,false,null",
          "24,root.sg1.d1,null,null,24,true,null",
          "25,root.sg1.d1,null,null,25,true,null",
          "26,root.sg1.d1,null,null,26,false,null",
          "27,root.sg1.d1,null,null,27,false,null",
          "28,root.sg1.d1,null,null,28,false,null",
          "29,root.sg1.d1,null,null,29,false,null",
          "30,root.sg1.d1,null,null,30,false,null",
          "31,root.sg1.d1,null,31,null,null,aligned_test31",
          "32,root.sg1.d1,null,32,null,null,aligned_test32",
          "33,root.sg1.d1,null,33,null,null,aligned_test33",
        };

    String[] columnNames = {"Device", "s1", "s2", "s3", "s4", "s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select * from root.sg1.d1 where time >= 9 and time <= 33 align by device");
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
          "1,root.sg1.d1,1.0,true,aligned_test1",
          "2,root.sg1.d1,2.0,null,aligned_test2",
          "3,root.sg1.d1,30000.0,true,aligned_unseq_test3",
          "4,root.sg1.d1,4.0,true,aligned_test4",
          "5,root.sg1.d1,5.0,true,aligned_test5",
          "6,root.sg1.d1,6.0,true,null",
          "7,root.sg1.d1,7.0,false,aligned_test7",
          "8,root.sg1.d1,8.0,null,aligned_test8",
          "9,root.sg1.d1,9.0,false,aligned_test9",
          "10,root.sg1.d1,null,true,aligned_test10",
          "11,root.sg1.d1,11.0,null,null",
          "12,root.sg1.d1,12.0,null,null",
          "13,root.sg1.d1,130000.0,true,aligned_unseq_test13",
          "14,root.sg1.d1,14.0,null,null",
          "15,root.sg1.d1,15.0,null,null",
          "16,root.sg1.d1,16.0,null,null",
          "17,root.sg1.d1,17.0,null,null",
          "18,root.sg1.d1,18.0,null,null",
          "19,root.sg1.d1,19.0,null,null",
          "20,root.sg1.d1,20.0,null,null",
          "21,root.sg1.d1,null,true,null",
          "22,root.sg1.d1,null,true,null",
          "23,root.sg1.d1,230000.0,false,null",
          "24,root.sg1.d1,null,true,null",
          "25,root.sg1.d1,null,true,null",
          "26,root.sg1.d1,null,false,null",
          "27,root.sg1.d1,null,false,null",
          "28,root.sg1.d1,null,false,null",
          "29,root.sg1.d1,null,false,null",
          "30,root.sg1.d1,null,false,null",
          "31,root.sg1.d1,null,null,aligned_test31",
          "32,root.sg1.d1,null,null,aligned_test32",
          "33,root.sg1.d1,null,null,aligned_test33",
          "34,root.sg1.d1,null,null,aligned_test34",
          "35,root.sg1.d1,null,null,aligned_test35",
          "36,root.sg1.d1,null,null,aligned_test36",
          "37,root.sg1.d1,null,null,aligned_test37",
          "38,root.sg1.d1,null,null,aligned_test38",
          "39,root.sg1.d1,null,null,aligned_test39",
          "40,root.sg1.d1,null,null,aligned_test40",
        };

    String[] columnNames = {"Device", "s1", "s4", "s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select s1,s4,s5 from root.sg1.d1 align by device");
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
          "1,root.sg1.d1,1.0,true",
          "2,root.sg1.d1,2.0,null",
          "3,root.sg1.d1,30000.0,true",
          "4,root.sg1.d1,4.0,true",
          "5,root.sg1.d1,5.0,true",
          "6,root.sg1.d1,6.0,true",
          "7,root.sg1.d1,7.0,false",
          "8,root.sg1.d1,8.0,null",
          "9,root.sg1.d1,9.0,false",
          "10,root.sg1.d1,null,true",
          "11,root.sg1.d1,11.0,null",
          "12,root.sg1.d1,12.0,null",
          "13,root.sg1.d1,130000.0,true",
          "14,root.sg1.d1,14.0,null",
          "15,root.sg1.d1,15.0,null",
          "16,root.sg1.d1,16.0,null",
          "17,root.sg1.d1,17.0,null",
          "18,root.sg1.d1,18.0,null",
          "19,root.sg1.d1,19.0,null",
          "20,root.sg1.d1,20.0,null",
          "21,root.sg1.d1,null,true",
          "22,root.sg1.d1,null,true",
          "23,root.sg1.d1,230000.0,false",
          "24,root.sg1.d1,null,true",
          "25,root.sg1.d1,null,true",
          "26,root.sg1.d1,null,false",
          "27,root.sg1.d1,null,false",
          "28,root.sg1.d1,null,false",
          "29,root.sg1.d1,null,false",
          "30,root.sg1.d1,null,false",
        };

    String[] columnNames = {"Device", "s1", "s4"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select s1,s4 from root.sg1.d1 align by device");
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
          "16,root.sg1.d1,16.0,null,null",
          "17,root.sg1.d1,17.0,null,null",
          "18,root.sg1.d1,18.0,null,null",
          "19,root.sg1.d1,19.0,null,null",
          "20,root.sg1.d1,20.0,null,null",
          "21,root.sg1.d1,null,true,null",
          "22,root.sg1.d1,null,true,null",
          "23,root.sg1.d1,230000.0,false,null",
          "24,root.sg1.d1,null,true,null",
          "25,root.sg1.d1,null,true,null",
          "26,root.sg1.d1,null,false,null",
          "27,root.sg1.d1,null,false,null",
          "28,root.sg1.d1,null,false,null",
          "29,root.sg1.d1,null,false,null",
          "30,root.sg1.d1,null,false,null",
          "31,root.sg1.d1,null,null,aligned_test31",
          "32,root.sg1.d1,null,null,aligned_test32",
          "33,root.sg1.d1,null,null,aligned_test33",
          "34,root.sg1.d1,null,null,aligned_test34",
        };

    String[] columnNames = {"Device", "s1", "s4", "s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select s1,s4,s5 from root.sg1.d1 where time >= 16 and time <= 34 align by device");
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
  public void countAllAlignedWithoutTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"root.sg1.d1", "20", "29", "28", "19", "20"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select count(*) from root.sg1.d1 align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countAllAlignedAndNonAlignedWithoutTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"root.sg1.d1,20,29,28,19,20,", "root.sg1.d2,19,29,28,18,19,"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select count(*) from root.sg1.* align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(resultSet.getString(index)).append(",");
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
  public void countAllAlignedWithTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"root.sg1.d1", "12", "15", "22", "13", "6"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(*) from root.sg1.d1 where time >= 9 and time <= 33 align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** aggregate multi columns of aligned timeseries in one SQL */
  @Test
  public void aggregateSomeAlignedWithoutTimeFilterTest() throws ClassNotFoundException {
    double[] retArray =
        new double[] {
          20, 29, 28, 390184, 130549, 390417, 19509.2, 4501.689655172413, 13943.464285714286
        };
    String[] columnNames = {
      "Device",
      "count(s1)",
      "count(s2)",
      "count(s3)",
      "sum(s1)",
      "sum(s2)",
      "sum(s3)",
      "avg(s1)",
      "avg(s2)",
      "avg(s3)",
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(s1),count(s2),count(s3),sum(s1),sum(s2),sum(s3),avg(s1),avg(s2),avg(s3) from root.sg1.d1 align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 1; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i - 1] = Double.parseDouble(resultSet.getString(index));
            assertEquals(retArray[i - 1], ans[i - 1], DELTA);
          }
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"root.sg1.d1", "11"};
    String[] columnNames = {"Device", "count(s4)"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select count(s4) from root.sg1.d1 where s4 = true align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregationFuncAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {"root.sg1.d1", "8", "42.0", "5.25", "1.0", "9.0", "1", "9", "1.0", "9.0"};
    String[] columnNames = {
      "Device",
      "count(s1)",
      "sum(s1)",
      "avg(s1)",
      "first_value(s1)",
      "last_value(s1)",
      "min_time(s1)",
      "max_time(s1)",
      "min_value(s1)",
      "max_value(s1)",
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s1), avg(s1), "
                  + "first_value(s1), last_value(s1), "
                  + "min_time(s1), max_time(s1),"
                  + "max_value(s1), min_value(s1) from root.sg1.d1 where s1 < 10 "
                  + "align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countAllAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"root.sg1.d1", "6", "6", "9", "11", "6"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(*) from root.sg1.d1 where s4 = true " + "align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregationAllAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"root.sg1.d1", "160016.0", "11", "1", "13"};
    String[] columnNames = {
      "Device", "sum(s1)", "count(s4)", "min_value(s3)", "max_time(s2)",
    };
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select sum(s1), count(s4), min_value(s3), max_time(s2) from root.sg1.d1 where s4 = true "
                  + "align by device");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>(); // used to adjust result sequence
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String[] ans = new String[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = resultSet.getString(index);
          }
          assertArrayEquals(retArray, ans);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countSumAvgGroupByTimeTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,4,40.0,7.5",
          "11,root.sg1.d1,10,130142.0,13014.2",
          "21,root.sg1.d1,1,null,230000.0",
          "31,root.sg1.d1,0,355.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(count("s1"))
                  + ","
                  + resultSet.getString(sum("s2"))
                  + ","
                  + resultSet.getString(avg("s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void maxMinValueGroupByTimeTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,10,6.0,10,6",
          "11,root.sg1.d1,130000,11.0,20,11",
          "21,root.sg1.d1,230000,230000.0,null,21",
          "31,root.sg1.d1,null,null,40,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(maxValue("s3"))
                  + ","
                  + resultSet.getString(minValue("s1"))
                  + ","
                  + resultSet.getString(maxTime("s2"))
                  + ","
                  + resultSet.getString(minTime("s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void firstLastGroupByTimeTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,null,null", "6,root.sg1.d1,true,aligned_test7",
              "11,root.sg1.d1,true,aligned_unseq_test13", "16,root.sg1.d1,null,null",
          "21,root.sg1.d1,true,null", "26,root.sg1.d1,false,null",
              "31,root.sg1.d1,null,aligned_test31", "36,root.sg1.d1,null,aligned_test36"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(lastValue("s4"))
                  + ","
                  + resultSet.getString(firstValue("s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void groupByWithWildcardTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,9,9,8,8,9,9.0,10,10,true,aligned_test10",
          "11,root.sg1.d1,10,10,10,1,1,20.0,20,20,true,aligned_unseq_test13",
          "21,root.sg1.d1,1,0,10,10,0,230000.0,null,30,false,null",
          "31,root.sg1.d1,0,10,0,0,10,null,40,null,null,aligned_test40"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(*), last_value(*) from root.sg1.d1 GROUP BY ([1, 41), 10ms) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(count("s1"))
                  + ","
                  + resultSet.getString(count("s2"))
                  + ","
                  + resultSet.getString(count("s3"))
                  + ","
                  + resultSet.getString(count("s4"))
                  + ","
                  + resultSet.getString(count("s5"))
                  + ","
                  + resultSet.getString(lastValue("s1"))
                  + ","
                  + resultSet.getString(lastValue("s2"))
                  + ","
                  + resultSet.getString(lastValue("s3"))
                  + ","
                  + resultSet.getString(lastValue("s4"))
                  + ","
                  + resultSet.getString(lastValue("s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void groupByWithNonAlignedTimeseriesTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,0,null,null",
          "7,root.sg1.d1,3,34.0,8.0",
          "13,root.sg1.d1,4,130045.0,32511.25",
          "19,root.sg1.d1,2,39.0,19.5",
          "25,root.sg1.d1,0,null,null",
          "31,root.sg1.d1,0,130.0,null",
          "37,root.sg1.d1,0,154.0,null",
          "1,root.sg1.d2,0,null,null",
          "7,root.sg1.d2,3,34.0,8.0",
          "13,root.sg1.d2,4,58.0,14.5",
          "19,root.sg1.d2,2,39.0,19.5",
          "25,root.sg1.d2,0,null,null",
          "31,root.sg1.d2,0,130.0,null",
          "37,root.sg1.d2,0,154.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.* "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(count("s1"))
                  + ","
                  + resultSet.getString(sum("s2"))
                  + ","
                  + resultSet.getString(avg("s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void countSumAvgPreviousFillTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,4,40.0,7.5",
          "11,root.sg1.d1,10,130142.0,13014.2",
          "21,root.sg1.d1,1,130142.0,230000.0",
          "31,root.sg1.d1,0,355.0,230000.0"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms) FILL (previous, 15ms) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(count("s1"))
                  + ","
                  + resultSet.getString(sum("s2"))
                  + ","
                  + resultSet.getString(avg("s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void countSumAvgValueFillTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,1,3.14,30000.0",
          "6,root.sg1.d1,4,40.0,7.5",
          "11,root.sg1.d1,5,130052.0,26010.4",
          "16,root.sg1.d1,5,90.0,18.0",
          "21,root.sg1.d1,1,3.14,230000.0",
          "26,root.sg1.d1,0,3.14,3.14",
          "31,root.sg1.d1,0,3.14,3.14"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where s3 > 5 and time < 30 GROUP BY ([1, 36), 5ms) FILL (3.14) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(count("s1"))
                  + ","
                  + resultSet.getString(sum("s2"))
                  + ","
                  + resultSet.getString(avg("s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void maxMinValueTimePreviousUntilLastFillTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,30000,6.0,9,3",
          "11,root.sg1.d1,130000,11.0,20,11",
          "21,root.sg1.d1,230000,230000.0,null,23",
          "31,root.sg1.d1,null,null,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where s1 > 5 and time < 35 GROUP BY ([1, 41), 10ms) FILL(previousUntilLast) align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(maxValue("s3"))
                  + ","
                  + resultSet.getString(minValue("s1"))
                  + ","
                  + resultSet.getString(maxTime("s2"))
                  + ","
                  + resultSet.getString(minTime("s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }

  @Test
  public void maxMinValueTimeValueFillTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,30000,30000.0,null,3",
          "6,root.sg1.d1,10,6.0,10,6",
          "11,root.sg1.d1,130000,11.0,15,11",
          "16,root.sg1.d1,20,16.0,20,16",
          "21,root.sg1.d1,230000,230000.0,null,21",
          "26,root.sg1.d1,29,null,null,26"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where s3 > 5 and time < 30 GROUP BY ([1, 31), 5ms) FILL ('fill string') align by device");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString(maxValue("s3"))
                  + ","
                  + resultSet.getString(minValue("s1"))
                  + ","
                  + resultSet.getString(maxTime("s2"))
                  + ","
                  + resultSet.getString(minTime("s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    }
  }
}
