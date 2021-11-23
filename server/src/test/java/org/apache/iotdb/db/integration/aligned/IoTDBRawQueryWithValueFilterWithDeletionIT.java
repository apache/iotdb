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

public class IoTDBRawQueryWithValueFilterWithDeletionIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

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
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // TODO currently aligned data in memory doesn't support deletion, so we flush all data to
      // disk before doing deletion
      statement.execute("flush");
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("delete from root.sg1.d1.s1 where time <= 21");
    } catch (Exception e) {
      e.printStackTrace();
    }
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
  public void selectAllAlignedWithValueFilterTest1() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "1,null,1,true,aligned_test1",
          "3,null,30000,true,aligned_unseq_test3",
          "4,null,null,true,aligned_test4",
          "5,null,null,true,aligned_test5",
          "6,null,6,true,null",
          "10,null,10,true,aligned_test10",
          "13,null,130000,true,aligned_unseq_test13",
          "21,null,21,true,null",
          "22,null,22,true,null",
          "24,null,24,true,null",
          "25,null,25,true,null",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.sg1.d1 where s4 = true");
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
  public void selectAllAlignedWithValueFilterTest2() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "7,null,7,false,aligned_test7",
          "9,null,9,false,aligned_test9",
          "26,null,26,false,null",
          "27,null,27,false,null",
          "28,null,28,false,null",
          "29,null,29,false,null",
          "30,null,30,false,null",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select * from root.sg1.d1 where s4 = false and s3 <= 33");
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
  public void selectAllAlignedWithValueFilterTest3() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "1,null,1,true,aligned_test1",
          "2,null,2,null,aligned_test2",
          "6,null,6,true,null",
          "7,null,7,false,aligned_test7",
          "8,null,8,null,aligned_test8",
          "9,null,9,false,aligned_test9",
          "10,null,10,true,aligned_test10",
          "11,null,11,null,null",
          "12,null,12,null,null",
          "14,null,14,null,null",
          "15,null,15,null,null",
          "16,null,16,null,null",
          "17,null,17,null,null",
          "18,null,18,null,null",
          "19,null,19,null,null",
          "20,null,20,null,null",
          "21,null,21,true,null",
          "22,null,22,true,null",
          "23,230000.0,230000,false,null",
          "24,null,24,true,null",
          "25,null,25,true,null",
          "26,null,26,false,null",
          "27,null,27,false,null",
          "28,null,28,false,null",
          "29,null,29,false,null",
          "30,null,30,false,null",
          "40,null,null,null,aligned_test40",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select * from root.sg1.d1 where s5 = 'aligned_test40' or s4 = false or s3 <= 33");
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
  public void selectAllAlignedAndNonAlignedTest1() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "3,null,30000,true,aligned_unseq_test3,3.0,null,3,false,non_aligned_test3",
          "13,null,130000,true,aligned_unseq_test13,13.0,13,13,null,null",
          "17,null,17,null,null,17.0,17,17,null,null",
          "18,null,18,null,null,18.0,18,18,null,null",
          "19,null,19,null,null,19.0,19,19,null,null",
          "20,null,20,null,null,20.0,20,20,null,null",
          "21,null,21,true,null,null,null,21,true,null",
          "22,null,22,true,null,null,null,22,true,null",
          "23,230000.0,230000,false,null,null,null,23,true,null",
          "24,null,24,true,null,null,null,24,true,null",
          "25,null,25,true,null,null,null,25,true,null",
          "26,null,26,false,null,null,null,26,false,null",
          "27,null,27,false,null,null,null,27,false,null",
          "28,null,28,false,null,null,null,28,false,null",
          "29,null,29,false,null,null,null,29,false,null",
          "30,null,30,false,null,null,null,30,false,null",
        };

    String[] columnNames = {
      "root.sg1.d1.s1",
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

      boolean hasResultSet =
          statement.execute(
              "select * from root.sg1.* where root.sg1.d1.s3 > 16 and root.sg1.d2.s3 <= 36");
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
  public void selectAllAlignedAndNonAlignedTest2() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "3,null,30000,true,aligned_unseq_test3,3.0,null,3,false,non_aligned_test3",
          "7,null,7,false,aligned_test7,7.0,7,7,false,non_aligned_test7",
          "9,null,9,false,aligned_test9,9.0,9,9,false,non_aligned_test9",
          "13,null,130000,true,aligned_unseq_test13,13.0,13,13,null,null",
          "17,null,17,null,null,17.0,17,17,null,null",
          "18,null,18,null,null,18.0,18,18,null,null",
          "19,null,19,null,null,19.0,19,19,null,null",
          "20,null,20,null,null,20.0,20,20,null,null",
          "21,null,21,true,null,null,null,21,true,null",
          "22,null,22,true,null,null,null,22,true,null",
          "23,230000.0,230000,false,null,null,null,23,true,null",
          "24,null,24,true,null,null,null,24,true,null",
          "25,null,25,true,null,null,null,25,true,null",
          "26,null,26,false,null,null,null,26,false,null",
          "27,null,27,false,null,null,null,27,false,null",
          "28,null,28,false,null,null,null,28,false,null",
          "29,null,29,false,null,null,null,29,false,null",
          "30,null,30,false,null,null,null,30,false,null",
        };

    String[] columnNames = {
      "root.sg1.d1.s1",
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

      boolean hasResultSet =
          statement.execute(
              "select * from root.sg1.* where root.sg1.d1.s3 > 16 or root.sg1.d2.s4 = false");
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
  public void selectAllAlignedWithTimeAndValueFilterTest1() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "9,null,9,false,aligned_test9",
          "10,null,10,true,aligned_test10",
          "11,null,11,null,null",
          "12,null,12,null,null",
          "14,null,14,null,null",
          "15,null,15,null,null",
          "16,null,16,null,null",
          "17,null,17,null,null",
          "18,null,18,null,null",
          "19,null,19,null,null",
          "20,null,20,null,null",
          "21,null,21,true,null",
          "22,null,22,true,null",
          "24,null,24,true,null",
          "25,null,25,true,null",
          "26,null,26,false,null",
          "27,null,27,false,null",
          "28,null,28,false,null",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select * from root.sg1.d1 where time >= 9 and time <= 33 and s3 < 29");
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
  public void selectAllAlignedWithTimeAndValueFilterTest2() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "9,null,9,false,aligned_test9",
          "10,null,10,true,aligned_test10",
          "11,null,11,null,null",
          "12,null,12,null,null",
          "13,null,130000,true,aligned_unseq_test13",
          "14,null,14,null,null",
          "15,null,15,null,null",
          "16,null,16,null,null",
          "17,null,17,null,null",
          "18,null,18,null,null",
          "19,null,19,null,null",
          "20,null,20,null,null",
          "21,null,21,true,null",
          "22,null,22,true,null",
          "23,230000.0,230000,false,null",
          "24,null,24,true,null",
          "25,null,25,true,null",
          "26,null,26,false,null",
          "27,null,27,false,null",
          "28,null,28,false,null",
          "29,null,29,false,null",
          "30,null,30,false,null",
          "31,null,null,null,aligned_test31",
          "32,null,null,null,aligned_test32",
          "33,null,null,null,aligned_test33",
          "36,null,null,null,aligned_test36",
          "37,null,null,null,aligned_test37",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select * from root.sg1.d1 where time >= 9 and time <= 33 or s5 = 'aligned_test36' or s5 = 'aligned_test37'");
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
  public void selectSomeAlignedWithValueFilterTest1() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "7,null,false,aligned_test7",
          "9,null,false,aligned_test9",
          "23,230000.0,false,null",
          "26,null,false,null",
          "27,null,false,null",
          "28,null,false,null",
          "29,null,false,null",
          "30,null,false,null",
          "34,null,null,aligned_test34",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select s1,s4,s5 from root.sg1.d1 where s4 = false or s5 = 'aligned_test34'");
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
  public void selectSomeAlignedWithValueFilterTest2() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "23,230000.0,false",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select s1,s4 from root.sg1.d1 where s1 > 29 and s4 = false");
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
  public void selectSomeAlignedWithTimeAndValueFilterTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "23,230000.0,false,null",
          "26,null,false,null",
          "27,null,false,null",
          "28,null,false,null",
          "29,null,false,null",
          "30,null,false,null",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select s1,s4,s5 from root.sg1.d1 where time >= 16 and time <= 34 and s4=false");
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
  public void selectSomeAlignedAndNonAlignedWithTimeAndValueFilterTest()
      throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "21,null,true,null,null,true,null",
          "22,null,true,null,null,true,null",
          "23,null,false,null,null,true,230000.0",
          "24,null,true,null,null,true,null",
          "25,null,true,null,null,true,null",
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
              "select d2.s5, d1.s4, d2.s1, d1.s5, d2.s4, d1.s1 from root.sg1 where time >= 16 and time <= 34 and (d1.s5 = 'aligned_test31' or d1.s5 = 'aligned_test32' or d1.s5 = 'aligned_test33' or d1.s5 = 'aligned_test34' or d2.s4 = true)");
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
