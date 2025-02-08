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
package org.apache.iotdb.relational.it.query.old.aligned;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.utils.constant.TestConstant.avg;
import static org.apache.iotdb.db.utils.constant.TestConstant.count;
import static org.apache.iotdb.db.utils.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.maxTime;
import static org.apache.iotdb.db.utils.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.minTime;
import static org.apache.iotdb.db.utils.constant.TestConstant.minValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.sum;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.relational.it.query.old.aligned.TableUtils.USE_DB;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAlignedSeriesQueryTableIT {

  private static final double DELTA = 1e-6;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv().initClusterEnvironment();
    TableUtils.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ------------------------------Raw Query Without Value Filter----------------------------------
  @Test
  public void selectAllAlignedWithoutValueFilterTest() {

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

    String[] columnNames = {"s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2,s3,s4,s5 from table0 where device='d1' order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithTimeFilterTest() {

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

    String[] columnNames = {"s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2,s3,s4,s5 from table0 where device='d1' and time >= 9 and time <= 33 order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithoutValueFilterTest1() {

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

    String[] columnNames = {"s1", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery("select Time,s1,s4,s5 from table0 where device='d1'")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithoutValueFilterTest2() {

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
          "31,null,null",
          "32,null,null",
          "33,null,null",
          "34,null,null",
          "35,null,null",
          "36,null,null",
          "37,null,null",
          "38,null,null",
          "39,null,null",
          "40,null,null",
        };

    String[] columnNames = {"s1", "s4"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery("select Time,s1,s4 from table0 where device='d1'")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithTimeFilterTest() {

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

    String[] columnNames = {"s1", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s4,s5 from table0 where device='d1' and time >= 16 and time <= 34")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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

  // ------------------------------Raw Query With Value Filter-------------------------------------
  @Test
  public void selectAllAlignedWithValueFilterTest1() {

    String[] retArray =
        new String[] {
          "1,1.0,1,1,true,aligned_test1",
          "3,30000.0,null,30000,true,aligned_unseq_test3",
          "4,4.0,4,null,true,aligned_test4",
          "5,5.0,5,null,true,aligned_test5",
          "6,6.0,6,6,true,null",
          "10,null,10,10,true,aligned_test10",
          "13,130000.0,130000,130000,true,aligned_unseq_test13",
          "21,null,null,21,true,null",
          "22,null,null,22,true,null",
          "24,null,null,24,true,null",
          "25,null,null,25,true,null",
        };

    String[] columnNames = {"s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2,s3,s4,s5 from table0 where device='d1' and s4 = true")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithValueFilterTest2() {

    String[] retArray =
        new String[] {
          "12,12.0,12,12,null,null",
          "14,14.0,14,14,null,null",
          "15,15.0,15,15,null,null",
          "16,16.0,16,16,null,null",
          "17,17.0,17,17,null,null",
          "18,18.0,18,18,null,null",
          "19,19.0,19,19,null,null",
          "20,20.0,20,20,null,null",
        };

    String[] columnNames = {"s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2,s3,s4,s5 from table0 where device='d1' and s1 > 11.0 and s2 <= 33 order by time asc")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithValueFilterTest3() {

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
          "23,230000.0,null,230000,false,null",
          "31,null,31,null,null,aligned_test31",
          "32,null,32,null,null,aligned_test32",
        };

    String[] columnNames = {"s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2,s3,s4,s5 from table0 where device='d1' and (s1 >= 13.0 or s2 < 33) order by time asc")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithTimeAndValueFilterTest1() {

    String[] retArray =
        new String[] {
          "9,9.0,9,9,false,aligned_test9",
          "11,11.0,11,11,null,null",
          "12,12.0,12,12,null,null",
          "14,14.0,14,14,null,null",
          "15,15.0,15,15,null,null",
          "16,16.0,16,16,null,null",
          "17,17.0,17,17,null,null",
          "18,18.0,18,18,null,null",
        };

    String[] columnNames = {"s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2,s3,s4,s5 from table0 where device='d1' and time >= 9 and time <= 33 and s1 < 19.0")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithLimitOffsetTest() {

    String[] retArray =
        new String[] {
          "14,14.0,14,14,null,null",
          "15,15.0,15,15,null,null",
          "16,16.0,16,16,null,null",
          "17,17.0,17,17,null,null",
          "18,18.0,18,18,null,null",
        };

    String[] columnNames = {"s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2,s3,s4,s5 from table0 where device='d1' and time >= 9 and time <= 33 offset 5 limit 5")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithValueFilterTest1() {

    String[] retArray =
        new String[] {
          "1,1.0,true,aligned_test1",
          "2,2.0,null,aligned_test2",
          "4,4.0,true,aligned_test4",
          "5,5.0,true,aligned_test5",
          "6,6.0,true,null",
          "7,7.0,false,aligned_test7",
          "8,8.0,null,aligned_test8",
          "9,9.0,false,aligned_test9",
          "11,11.0,null,null",
          "12,12.0,null,null",
          "14,14.0,null,null",
          "15,15.0,null,null",
          "16,16.0,null,null",
          "34,null,null,aligned_test34",
        };

    String[] columnNames = {"s1", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s4,s5 from table0 where device='d1' and s1 < 17.0 or s5 = 'aligned_test34'")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithValueFilterTest2() {

    String[] retArray =
        new String[] {
          "7,7.0,false", "9,9.0,false",
        };

    String[] columnNames = {"s1", "s4"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s4 from table0 where device='d1' and s1 < 19.0 and s4 = false")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithTimeAndValueFilterTest() {

    String[] retArray =
        new String[] {
          "23,230000.0,false,null",
          "26,null,false,null",
          "27,null,false,null",
          "28,null,false,null",
          "29,null,false,null",
          "30,null,false,null",
        };

    String[] columnNames = {"s1", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s4,s5 from table0 where device='d1' and time >= 16 and time <= 34 and s4=false")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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

  // --------------------------Aggregation Query Without Value Filter---------------------------
  @Ignore
  @Test
  public void countAllAlignedWithoutTimeFilterTest() {
    String[] retArray = new String[] {"20", "29", "28", "19", "20"};
    String[] columnNames = {
      "count(d1.s1)", "count(d1.s2)", "count(d1.s3)", "count(d1.s4)", "count(d1.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select count(*) from d1")) {
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

  @Ignore
  @Test
  public void countAllAlignedAndNonAlignedWithoutTimeFilterTest() {
    String[] retArray = new String[] {"20", "29", "28", "19", "20", "19", "29", "28", "18", "19"};
    String[] columnNames = {
      "count(d1.s1)",
      "count(d1.s2)",
      "count(d1.s3)",
      "count(d1.s4)",
      "count(d1.s5)",
      "count(d2.s1)",
      "count(d2.s2)",
      "count(d2.s3)",
      "count(d2.s4)",
      "count(d2.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select count(*) from root.sg1.*")) {
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

  @Ignore
  @Test
  public void countAllAlignedWithTimeFilterTest() {
    String[] retArray = new String[] {"12", "15", "22", "13", "6"};
    String[] columnNames = {
      "count(d1.s1)", "count(d1.s2)", "count(d1.s3)", "count(d1.s4)", "count(d1.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(*) from d1 where time >= 9 and time <= 33")) {
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
  @Ignore
  @Test
  public void aggregateSomeAlignedWithoutTimeFilterTest() {
    double[] retArray =
        new double[] {
          20, 29, 28, 390184, 130549, 390417, 19509.2, 4501.689655172413, 13943.464285714286
        };
    String[] columnNames = {
      "count(d1.s1)",
      "count(d1.s2)",
      "count(d1.s3)",
      "sum(d1.s1)",
      "sum(d1.s2)",
      "sum(d1.s3)",
      "avg(d1.s1)",
      "avg(d1.s2)",
      "avg(d1.s3)",
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1),count(s2),count(s3),sum(s1),sum(s2),sum(s3),avg(s1),avg(s2),avg(s3) from d1")) {
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
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = Double.parseDouble(resultSet.getString(index));
          }
          assertArrayEquals(retArray, ans, DELTA);
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
  @Ignore
  @Test
  public void aggregateSomeAlignedWithTimeFilterTest() {
    double[] retArray =
        new double[] {
          6, 9, 15, 230090, 220, 230322, 38348.333333333336, 24.444444444444443, 15354.8
        };
    String[] columnNames = {
      "count(d1.s1)",
      "count(d1.s2)",
      "count(d1.s3)",
      "sum(d1.s1)",
      "sum(d1.s2)",
      "sum(d1.s3)",
      "avg(d1.s1)",
      "avg(d1.s2)",
      "avg(d1.s3)",
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1),count (s2),count (s3),sum(s1),sum(s2),sum(s3),avg(s1),avg(s2),avg(s3) from d1 where time>=16 and time<=34")) {
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
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = Double.parseDouble(resultSet.getString(index));
          }
          assertArrayEquals(retArray, ans, DELTA);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void countSingleAlignedWithTimeFilterTest() {
    String[] retArray = new String[] {"9"};
    String[] columnNames = {"count(d1.s2)"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(s2) from d1 where time>=16 and time<=34")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          // No need to add time column for aggregation query
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            if (builder.length() != 0) {
              builder.append(",");
            }
            builder.append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void sumSingleAlignedWithTimeFilterTest() {
    String[] retArray = new String[] {"230322.0"};
    String[] columnNames = {"sum(d1.s3)"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select sum(s3) from d1 where time>=16 and time<=34")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          // No need to add time column for aggregation query
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            if (builder.length() != 0) {
              builder.append(",");
            }
            builder.append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void avgSingleAlignedWithTimeFilterTest() {
    double[][] retArray = {{24.444444444444443}};
    String[] columnNames = {"avg(d1.s2)"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select avg(s2) from d1 where time>=16 and time<=34")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[columnNames.length];
          StringBuilder builder = new StringBuilder();
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            ans[i] = Double.parseDouble(resultSet.getString(index));
          }
          assertArrayEquals(retArray[cnt], ans, DELTA);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // --------------------------------Aggregation with value filter------------------------------
  @Ignore
  @Test
  public void countAlignedWithValueFilterTest() {
    String[] retArray = new String[] {"11"};
    String[] columnNames = {"count(d1.s4)"};
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(s4) from d1 where s4 = true")) {
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

  @Ignore
  @Test
  public void aggregationFuncAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"8", "42.0", "5.25", "1.0", "9.0", "1", "9", "1.0", "9.0"};
    String[] columnNames = {
      "count(d1.s1)",
      "sum(d1.s1)",
      "avg(d1.s1)",
      "first_value(d1.s1)",
      "last_value(d1.s1)",
      "min_time(d1.s1)",
      "max_time(d1.s1)",
      "min_value(d1.s1)",
      "max_value(d1.s1)",
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s1), avg(s1), "
                  + "first_value(s1), last_value(s1), "
                  + "min_time(s1), max_time(s1),"
                  + "max_value(s1), min_value(s1) from d1 where s1 < 10")) {
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

  @Ignore
  @Test
  public void countAllAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"6", "6", "9", "11", "6"};
    String[] columnNames = {
      "count(d1.s1)", "count(d1.s2)", "count(d1.s3)", "count(d1.s4)", "count(d1.s5)"
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(*) from d1 where s4 = true")) {
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

  @Ignore
  @Test
  public void aggregationAllAlignedWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"160016.0", "11", "1", "13"};
    String[] columnNames = {
      "sum(d1.s1)", "count(d1.s4)", "min_value(d1.s3)", "max_time(d1.s2)",
    };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select sum(s1), count(s4), min_value(s3), max_time(s2) from d1 where s4 = true")) {
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

  // ---------------------------------Align by device query ------------------------------------
  @Test
  public void selectAllAlignedWithoutValueFilterAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "1,d1,1.0,1,1,true,aligned_test1",
          "2,d1,2.0,2,2,null,aligned_test2",
          "3,d1,30000.0,null,30000,true,aligned_unseq_test3",
          "4,d1,4.0,4,null,true,aligned_test4",
          "5,d1,5.0,5,null,true,aligned_test5",
          "6,d1,6.0,6,6,true,null",
          "7,d1,7.0,7,7,false,aligned_test7",
          "8,d1,8.0,8,8,null,aligned_test8",
          "9,d1,9.0,9,9,false,aligned_test9",
          "10,d1,null,10,10,true,aligned_test10",
          "11,d1,11.0,11,11,null,null",
          "12,d1,12.0,12,12,null,null",
          "13,d1,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,d1,14.0,14,14,null,null",
          "15,d1,15.0,15,15,null,null",
          "16,d1,16.0,16,16,null,null",
          "17,d1,17.0,17,17,null,null",
          "18,d1,18.0,18,18,null,null",
          "19,d1,19.0,19,19,null,null",
          "20,d1,20.0,20,20,null,null",
          "21,d1,null,null,21,true,null",
          "22,d1,null,null,22,true,null",
          "23,d1,230000.0,null,230000,false,null",
          "24,d1,null,null,24,true,null",
          "25,d1,null,null,25,true,null",
          "26,d1,null,null,26,false,null",
          "27,d1,null,null,27,false,null",
          "28,d1,null,null,28,false,null",
          "29,d1,null,null,29,false,null",
          "30,d1,null,null,30,false,null",
          "31,d1,null,31,null,null,aligned_test31",
          "32,d1,null,32,null,null,aligned_test32",
          "33,d1,null,33,null,null,aligned_test33",
          "34,d1,null,34,null,null,aligned_test34",
          "35,d1,null,35,null,null,aligned_test35",
          "36,d1,null,36,null,null,aligned_test36",
          "37,d1,null,37,null,null,aligned_test37",
          "38,d1,null,38,null,null,aligned_test38",
          "39,d1,null,39,null,null,aligned_test39",
          "40,d1,null,40,null,null,aligned_test40",
        };

    String[] columnNames = {"device", "s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,device,s1,s2,s3,s4,s5 from table0 where device='d1' order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedAndNonAlignedAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "1,d1,1.0,1,1,true,aligned_test1",
          "2,d1,2.0,2,2,null,aligned_test2",
          "3,d1,30000.0,null,30000,true,aligned_unseq_test3",
          "4,d1,4.0,4,null,true,aligned_test4",
          "5,d1,5.0,5,null,true,aligned_test5",
          "6,d1,6.0,6,6,true,null",
          "7,d1,7.0,7,7,false,aligned_test7",
          "8,d1,8.0,8,8,null,aligned_test8",
          "9,d1,9.0,9,9,false,aligned_test9",
          "10,d1,null,10,10,true,aligned_test10",
          "11,d1,11.0,11,11,null,null",
          "12,d1,12.0,12,12,null,null",
          "13,d1,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,d1,14.0,14,14,null,null",
          "15,d1,15.0,15,15,null,null",
          "16,d1,16.0,16,16,null,null",
          "17,d1,17.0,17,17,null,null",
          "18,d1,18.0,18,18,null,null",
          "19,d1,19.0,19,19,null,null",
          "20,d1,20.0,20,20,null,null",
          "21,d1,null,null,21,true,null",
          "22,d1,null,null,22,true,null",
          "23,d1,230000.0,null,230000,false,null",
          "24,d1,null,null,24,true,null",
          "25,d1,null,null,25,true,null",
          "26,d1,null,null,26,false,null",
          "27,d1,null,null,27,false,null",
          "28,d1,null,null,28,false,null",
          "29,d1,null,null,29,false,null",
          "30,d1,null,null,30,false,null",
          "31,d1,null,31,null,null,aligned_test31",
          "32,d1,null,32,null,null,aligned_test32",
          "33,d1,null,33,null,null,aligned_test33",
          "34,d1,null,34,null,null,aligned_test34",
          "35,d1,null,35,null,null,aligned_test35",
          "36,d1,null,36,null,null,aligned_test36",
          "37,d1,null,37,null,null,aligned_test37",
          "38,d1,null,38,null,null,aligned_test38",
          "39,d1,null,39,null,null,aligned_test39",
          "40,d1,null,40,null,null,aligned_test40",
          "1,d2,1.0,1,1,true,non_aligned_test1",
          "2,d2,2.0,2,2,null,non_aligned_test2",
          "3,d2,3.0,null,3,false,non_aligned_test3",
          "4,d2,4.0,4,null,true,non_aligned_test4",
          "5,d2,5.0,5,null,true,non_aligned_test5",
          "6,d2,6.0,6,6,true,null",
          "7,d2,7.0,7,7,false,non_aligned_test7",
          "8,d2,8.0,8,8,null,non_aligned_test8",
          "9,d2,9.0,9,9,false,non_aligned_test9",
          "10,d2,null,10,10,true,non_aligned_test10",
          "11,d2,11.0,11,11,null,null",
          "12,d2,12.0,12,12,null,null",
          "13,d2,13.0,13,13,null,null",
          "14,d2,14.0,14,14,null,null",
          "15,d2,15.0,15,15,null,null",
          "16,d2,16.0,16,16,null,null",
          "17,d2,17.0,17,17,null,null",
          "18,d2,18.0,18,18,null,null",
          "19,d2,19.0,19,19,null,null",
          "20,d2,20.0,20,20,null,null",
          "21,d2,null,null,21,true,null",
          "22,d2,null,null,22,true,null",
          "23,d2,null,null,23,true,null",
          "24,d2,null,null,24,true,null",
          "25,d2,null,null,25,true,null",
          "26,d2,null,null,26,false,null",
          "27,d2,null,null,27,false,null",
          "28,d2,null,null,28,false,null",
          "29,d2,null,null,29,false,null",
          "30,d2,null,null,30,false,null",
          "31,d2,null,31,null,null,non_aligned_test31",
          "32,d2,null,32,null,null,non_aligned_test32",
          "33,d2,null,33,null,null,non_aligned_test33",
          "34,d2,null,34,null,null,non_aligned_test34",
          "35,d2,null,35,null,null,non_aligned_test35",
          "36,d2,null,36,null,null,non_aligned_test36",
          "37,d2,null,37,null,null,non_aligned_test37",
          "38,d2,null,38,null,null,non_aligned_test38",
          "39,d2,null,39,null,null,non_aligned_test39",
          "40,d2,null,40,null,null,non_aligned_test40",
        };

    String[] columnNames = {"device", "s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,device,s1,s2,s3,s4,s5 from table0 order by device, time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithTimeFilterAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "9,d1,9.0,9,9,false,aligned_test9",
          "10,d1,null,10,10,true,aligned_test10",
          "11,d1,11.0,11,11,null,null",
          "12,d1,12.0,12,12,null,null",
          "13,d1,130000.0,130000,130000,true,aligned_unseq_test13",
          "14,d1,14.0,14,14,null,null",
          "15,d1,15.0,15,15,null,null",
          "16,d1,16.0,16,16,null,null",
          "17,d1,17.0,17,17,null,null",
          "18,d1,18.0,18,18,null,null",
          "19,d1,19.0,19,19,null,null",
          "20,d1,20.0,20,20,null,null",
          "21,d1,null,null,21,true,null",
          "22,d1,null,null,22,true,null",
          "23,d1,230000.0,null,230000,false,null",
          "24,d1,null,null,24,true,null",
          "25,d1,null,null,25,true,null",
          "26,d1,null,null,26,false,null",
          "27,d1,null,null,27,false,null",
          "28,d1,null,null,28,false,null",
          "29,d1,null,null,29,false,null",
          "30,d1,null,null,30,false,null",
          "31,d1,null,31,null,null,aligned_test31",
          "32,d1,null,32,null,null,aligned_test32",
          "33,d1,null,33,null,null,aligned_test33",
        };

    String[] columnNames = {"device", "s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,device,s1,s2,s3,s4,s5 from table0 where device='d1' and time >= 9 and time <= 33 order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithoutValueFilterAlignByDeviceTest1() {
    String[] retArray =
        new String[] {
          "1,d1,1.0,true,aligned_test1",
          "2,d1,2.0,null,aligned_test2",
          "3,d1,30000.0,true,aligned_unseq_test3",
          "4,d1,4.0,true,aligned_test4",
          "5,d1,5.0,true,aligned_test5",
          "6,d1,6.0,true,null",
          "7,d1,7.0,false,aligned_test7",
          "8,d1,8.0,null,aligned_test8",
          "9,d1,9.0,false,aligned_test9",
          "10,d1,null,true,aligned_test10",
          "11,d1,11.0,null,null",
          "12,d1,12.0,null,null",
          "13,d1,130000.0,true,aligned_unseq_test13",
          "14,d1,14.0,null,null",
          "15,d1,15.0,null,null",
          "16,d1,16.0,null,null",
          "17,d1,17.0,null,null",
          "18,d1,18.0,null,null",
          "19,d1,19.0,null,null",
          "20,d1,20.0,null,null",
          "21,d1,null,true,null",
          "22,d1,null,true,null",
          "23,d1,230000.0,false,null",
          "24,d1,null,true,null",
          "25,d1,null,true,null",
          "26,d1,null,false,null",
          "27,d1,null,false,null",
          "28,d1,null,false,null",
          "29,d1,null,false,null",
          "30,d1,null,false,null",
          "31,d1,null,null,aligned_test31",
          "32,d1,null,null,aligned_test32",
          "33,d1,null,null,aligned_test33",
          "34,d1,null,null,aligned_test34",
          "35,d1,null,null,aligned_test35",
          "36,d1,null,null,aligned_test36",
          "37,d1,null,null,aligned_test37",
          "38,d1,null,null,aligned_test38",
          "39,d1,null,null,aligned_test39",
          "40,d1,null,null,aligned_test40",
        };

    String[] columnNames = {"device", "s1", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery("select Time,device,s1,s4,s5 from table0 where device='d1'")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithoutValueFilterAlignByDeviceTest2() {
    String[] retArray =
        new String[] {
          "1,d1,1.0,true",
          "2,d1,2.0,null",
          "3,d1,30000.0,true",
          "4,d1,4.0,true",
          "5,d1,5.0,true",
          "6,d1,6.0,true",
          "7,d1,7.0,false",
          "8,d1,8.0,null",
          "9,d1,9.0,false",
          "10,d1,null,true",
          "11,d1,11.0,null",
          "12,d1,12.0,null",
          "13,d1,130000.0,true",
          "14,d1,14.0,null",
          "15,d1,15.0,null",
          "16,d1,16.0,null",
          "17,d1,17.0,null",
          "18,d1,18.0,null",
          "19,d1,19.0,null",
          "20,d1,20.0,null",
          "21,d1,null,true",
          "22,d1,null,true",
          "23,d1,230000.0,false",
          "24,d1,null,true",
          "25,d1,null,true",
          "26,d1,null,false",
          "27,d1,null,false",
          "28,d1,null,false",
          "29,d1,null,false",
          "30,d1,null,false",
          "31,d1,null,null",
          "32,d1,null,null",
          "33,d1,null,null",
          "34,d1,null,null",
          "35,d1,null,null",
          "36,d1,null,null",
          "37,d1,null,null",
          "38,d1,null,null",
          "39,d1,null,null",
          "40,d1,null,null",
        };

    String[] columnNames = {"Device", "s1", "s4"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery("select Time,Device,s1,s4 from table0 where device='d1'")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithTimeFilterAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "16,d1,16.0,null,null",
          "17,d1,17.0,null,null",
          "18,d1,18.0,null,null",
          "19,d1,19.0,null,null",
          "20,d1,20.0,null,null",
          "21,d1,null,true,null",
          "22,d1,null,true,null",
          "23,d1,230000.0,false,null",
          "24,d1,null,true,null",
          "25,d1,null,true,null",
          "26,d1,null,false,null",
          "27,d1,null,false,null",
          "28,d1,null,false,null",
          "29,d1,null,false,null",
          "30,d1,null,false,null",
          "31,d1,null,null,aligned_test31",
          "32,d1,null,null,aligned_test32",
          "33,d1,null,null,aligned_test33",
          "34,d1,null,null,aligned_test34",
        };

    String[] columnNames = {"Device", "s1", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,Device,s1,s4,s5 from table0 where device='d1' and time >= 16 and time <= 34 order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithValueFilterAlignByDeviceTest1() {
    String[] retArray =
        new String[] {
          "1,d1,1.0,1,1,true,aligned_test1",
          "3,d1,30000.0,null,30000,true,aligned_unseq_test3",
          "4,d1,4.0,4,null,true,aligned_test4",
          "5,d1,5.0,5,null,true,aligned_test5",
          "6,d1,6.0,6,6,true,null",
          "10,d1,null,10,10,true,aligned_test10",
          "13,d1,130000.0,130000,130000,true,aligned_unseq_test13",
          "21,d1,null,null,21,true,null",
          "22,d1,null,null,22,true,null",
          "24,d1,null,null,24,true,null",
          "25,d1,null,null,25,true,null",
        };

    String[] columnNames = {"Device", "s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);

      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,Device,s1,s2,s3,s4,s5 from table0 where device='d1' and s4 = true order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithValueFilterAlignByDeviceTest2() {
    String[] retArray =
        new String[] {
          "12,d1,12.0,12,12,null,null",
          "14,d1,14.0,14,14,null,null",
          "15,d1,15.0,15,15,null,null",
          "16,d1,16.0,16,16,null,null",
          "17,d1,17.0,17,17,null,null",
          "18,d1,18.0,18,18,null,null",
          "19,d1,19.0,19,19,null,null",
          "20,d1,20.0,20,20,null,null",
        };

    String[] columnNames = {"Device", "s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,Device,s1,s2,s3,s4,s5 from table0 where device='d1' and s1 > 11.0 and s2 <= 33 order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectAllAlignedWithTimeAndValueFilterAlignByDeviceTest1() {
    String[] retArray =
        new String[] {
          "9,d1,9.0,9,9,false,aligned_test9",
          "11,d1,11.0,11,11,null,null",
          "12,d1,12.0,12,12,null,null",
          "14,d1,14.0,14,14,null,null",
          "15,d1,15.0,15,15,null,null",
          "16,d1,16.0,16,16,null,null",
          "17,d1,17.0,17,17,null,null",
          "18,d1,18.0,18,18,null,null",
        };

    String[] columnNames = {"Device", "s1", "s2", "s3", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,Device,s1,s2,s3,s4,s5 from table0 where device='d1' and time >= 9 and time <= 33 and s1 < 19.0 order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithValueFilterAlignByDeviceTest1() {
    String[] retArray =
        new String[] {
          "1,d1,1.0,true,aligned_test1",
          "2,d1,2.0,null,aligned_test2",
          "4,d1,4.0,true,aligned_test4",
          "5,d1,5.0,true,aligned_test5",
          "6,d1,6.0,true,null",
          "7,d1,7.0,false,aligned_test7",
          "8,d1,8.0,null,aligned_test8",
          "9,d1,9.0,false,aligned_test9",
          "11,d1,11.0,null,null",
          "12,d1,12.0,null,null",
          "14,d1,14.0,null,null",
          "15,d1,15.0,null,null",
          "16,d1,16.0,null,null",
          "34,d1,null,null,aligned_test34",
        };

    String[] columnNames = {"Device", "s1", "s4", "s5"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,Device,s1,s4,s5 from table0 where device='d1' and s1 < 17.0 or s5 = 'aligned_test34' order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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
  public void selectSomeAlignedWithValueFilterAlignByDeviceTest2() {
    String[] retArray =
        new String[] {
          "7,d1,7.0,false", "9,d1,9.0,false",
        };

    String[] columnNames = {"Device", "s1", "s4"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,Device,s1,s4 from table0 where device='d1' and s1 < 19.0 and s4 = false order by time")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
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

  @Ignore
  @Test
  public void countAllAlignedWithoutTimeFilterAlignByDeviceTest() {
    String[] retArray = new String[] {"d1", "20", "29", "28", "19", "20"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(*) from d1 align by device")) {
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

  @Ignore
  @Test
  public void countAllAlignedAndNonAlignedWithoutTimeFilterAlignByDeviceTest() {
    String[] retArray = new String[] {"d1,20,29,28,19,20,", "d2,19,29,28,18,19,"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(*) from root.sg1.* align by device")) {
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

  @Ignore
  @Test
  public void countAllAlignedWithTimeFilterAlignByDeviceTest() {
    String[] retArray = new String[] {"d1", "12", "15", "22", "13", "6"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) from d1 where time >= 9 and time <= 33 align by device")) {
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
  @Ignore
  @Test
  public void aggregateSomeAlignedWithoutTimeFilterAlignByDeviceTest() {
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

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1),count(s2),count(s3),sum(s1),sum(s2),sum(s3),avg(s1),avg(s2),avg(s3) from d1 align by device")) {
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

  @Ignore
  @Test
  public void countAlignedWithValueFilterAlignByDeviceTest() {
    String[] retArray = new String[] {"d1", "11"};
    String[] columnNames = {"Device", "count(s4)"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(s4) from d1 where s4 = true align by device")) {
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

  @Ignore
  @Test
  public void aggregationFuncAlignedWithValueFilterAlignByDeviceTest() {
    String[] retArray =
        new String[] {"d1", "8", "42.0", "5.25", "1.0", "9.0", "1", "9", "1.0", "9.0"};
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

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s1), avg(s1), "
                  + "first_value(s1), last_value(s1), "
                  + "min_time(s1), max_time(s1),"
                  + "max_value(s1), min_value(s1) from d1 where s1 < 10 "
                  + "align by device")) {
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

  @Ignore
  @Test
  public void countAllAlignedWithValueFilterAlignByDeviceTest() {
    String[] retArray = new String[] {"d1", "6", "6", "9", "11", "6"};
    String[] columnNames = {
      "Device", "count(s1)", "count(s2)", "count(s3)", "count(s4)", "count(s5)"
    };

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(*) from d1 where s4 = true " + "align by device")) {
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

  @Ignore
  @Test
  public void aggregationAllAlignedWithValueFilterAlignByDeviceTest() {
    String[] retArray = new String[] {"d1", "160016.0", "11", "1", "13"};
    String[] columnNames = {
      "Device", "sum(s1)", "count(s4)", "min_value(s3)", "max_time(s2)",
    };

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select sum(s1), count(s4), min_value(s3), max_time(s2) from d1 where s4 = true "
                  + "align by device")) {
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

  @Ignore
  @Test
  public void countSumAvgGroupByTimeAlignByDeviceTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,d1,4,40.0,7.5",
          "11,d1,10,130142.0,13014.2",
          "21,d1,1,null,230000.0",
          "31,d1,0,355.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
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

  @Ignore
  @Test
  public void maxMinValueGroupByTimeAlignByDeviceTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,d1,10,6.0,10,6",
          "11,d1,130000,11.0,20,11",
          "21,d1,230000,230000.0,null,21",
          "31,d1,null,null,40,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
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

  @Ignore
  @Test
  public void firstLastGroupByTimeAlignByDeviceTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,d1,null,null", "6,d1,true,aligned_test7",
          "11,d1,true,aligned_unseq_test13", "16,d1,null,null",
          "21,d1,true,null", "26,d1,false,null",
          "31,d1,null,aligned_test31", "36,d1,null,aligned_test36"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s4), first_value(s5) from d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
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

  @Ignore
  @Test
  public void groupByWithWildcardAlignByDeviceTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,d1,9,9,8,8,9,9.0,10,10,true,aligned_test10",
          "11,d1,10,10,10,1,1,20.0,20,20,true,aligned_unseq_test13",
          "21,d1,1,0,10,10,0,230000.0,null,30,false,null",
          "31,d1,0,10,0,0,10,null,40,null,null,aligned_test40"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*), last_value(*) from d1 GROUP BY ([1, 41), 10ms) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
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

  @Ignore
  @Test
  public void groupByWithNonAlignedTimeseriesAlignByDeviceTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,d1,0,null,null",
          "7,d1,3,34.0,8.0",
          "13,d1,4,130045.0,32511.25",
          "19,d1,2,39.0,19.5",
          "25,d1,0,null,null",
          "31,d1,0,130.0,null",
          "37,d1,0,154.0,null",
          "1,d2,0,null,null",
          "7,d2,3,34.0,8.0",
          "13,d2,4,58.0,14.5",
          "19,d2,2,39.0,19.5",
          "25,d2,0,null,null",
          "31,d2,0,130.0,null",
          "37,d2,0,154.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from root.sg1.* "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
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

  @Ignore
  @Test
  public void countSumAvgPreviousFillAlignByDeviceTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,d1,4,40.0,7.5",
          "11,d1,10,130142.0,13014.2",
          "21,d1,1,130142.0,230000.0",
          "31,d1,0,355.0,230000.0"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms) FILL (previous) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
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

  @Ignore
  @Test
  public void countSumAvgValueFillAlignByDeviceTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,d1,1,3.0,30000.0",
          "6,d1,4,40.0,7.5",
          "11,d1,5,130052.0,26010.4",
          "16,d1,5,90.0,18.0",
          "21,d1,1,3.0,230000.0",
          "26,d1,0,3.0,3.0",
          "31,d1,0,3.0,3.0"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + "where s3 > 5 and time < 30 GROUP BY ([1, 36), 5ms) FILL (3) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DEVICE)
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

  // --------------------------GroupByWithoutValueFilter--------------------------
  @Ignore
  @Test
  public void countSumAvgGroupByTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,4,40.0,7.5", "11,10,130142.0,13014.2", "21,1,null,230000.0", "31,0,355.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d1.s1"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void countSumAvgGroupByTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,null,null", "6,4,40.0,7.5", "11,5,130052.0,26010.4", "16,5,90.0,18.0",
          "21,1,null,230000.0", "26,0,null,null", "31,0,165.0,null", "36,0,73.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d1.s1"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void countSumAvgGroupByWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,null,null",
          "7,3,34.0,8.0",
          "13,4,130045.0,32511.25",
          "19,2,39.0,19.5",
          "25,0,null,null",
          "31,0,130.0,null",
          "37,0,154.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s1) from d1 "
                  + " where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d1.s1"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void countSumAvgGroupByWithNonAlignedTimeseriesTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,null,null,0,null,null",
          "7,3,34.0,8.0,4,34.0,8.5",
          "13,4,58.0,14.5,4,130045.0,14.5",
          "19,2,39.0,19.5,4,39.0,20.5",
          "25,0,null,null,4,null,26.5",
          "31,0,130.0,null,0,130.0,null",
          "37,0,154.0,null,0,154.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(d1.s1), sum(d2.s2), avg(d2.s1), count(d1.s3), sum(d1.s2), avg(d2.s3) "
                  + "from root.sg1 where time > 5 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d2.s2"))
                  + ","
                  + resultSet.getString(avg("d2.s1"))
                  + ","
                  + resultSet.getString(count("d1.s3"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d2.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(d1.s1), sum(d2.s2), avg(d2.s1), count(d1.s3), sum(d1.s2), avg(d2.s3) "
                  + "from root.sg1 where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(sum("d2.s2"))
                  + ","
                  + resultSet.getString(avg("d2.s1"))
                  + ","
                  + resultSet.getString(count("d1.s3"))
                  + ","
                  + resultSet.getString(sum("d1.s2"))
                  + ","
                  + resultSet.getString(avg("d2.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void maxMinValueTimeGroupByTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,10,6.0,10,6",
          "11,130000,11.0,20,11",
          "21,230000,230000.0,null,21",
          "31,null,null,40,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from d1 "
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void maxMinValueTimeGroupByTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null",
          "6,10,6.0,10,6",
          "11,130000,11.0,15,11",
          "16,20,16.0,20,16",
          "21,230000,230000.0,null,21",
          "26,30,null,null,26",
          "31,null,null,35,null",
          "36,null,null,37,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void maxMinValueTimeGroupByWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null",
          "7,10,7.0,10,7",
          "13,130000,14.0,16,13",
          "19,22,19.0,20,19",
          "25,28,null,null,25",
          "31,null,null,34,null",
          "37,null,null,40,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from d1 "
                  + " where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void maxMinValueTimeGroupByWithNonAlignedTimeseriesTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null,null,null,null,null",
          "7,10,7.0,10,7,10,7.0,10,7",
          "13,16,14.0,16,13,130000,13.0,16,13",
          "19,22,19.0,20,19,22,19.0,20,19",
          "25,28,null,null,25,28,null,null,25",
          "31,null,null,34,null,null,null,34,null",
          "37,null,null,37,null,null,null,37,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(d2.s3), min_value(d1.s1), max_time(d2.s2), min_time(d1.s3), "
                  + "max_value(d1.s3), min_value(d2.s1), max_time(d1.s2), min_time(d2.s3) "
                  + "from root.sg1 where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d2.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d2.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"))
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d2.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d2.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(d2.s3), min_value(d1.s1), max_time(d2.s2), min_time(d1.s3), "
                  + "max_value(d1.s3), min_value(d2.s1), max_time(d1.s2), min_time(d2.s3) "
                  + "from root.sg1 where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("d2.s3"))
                  + ","
                  + resultSet.getString(minValue("d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("d2.s2"))
                  + ","
                  + resultSet.getString(minTime("d1.s3"))
                  + ","
                  + resultSet.getString(maxValue("d1.s3"))
                  + ","
                  + resultSet.getString(minValue("d2.s1"))
                  + ","
                  + resultSet.getString(maxTime("d1.s2"))
                  + ","
                  + resultSet.getString(minTime("d2.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void firstLastGroupByTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,true,aligned_test7",
          "11,true,aligned_unseq_test13",
          "21,false,null",
          "31,null,aligned_test31"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s4), first_value(s5) from d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s4), first_value(s5) from d1 "
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void firstLastGroupByTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null", "6,true,aligned_test7", "11,true,aligned_unseq_test13", "16,null,null",
          "21,true,null", "26,false,null", "31,null,aligned_test31", "36,null,aligned_test36"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s4), first_value(s5) from d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s4), first_value(s5) from d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void firstLastGroupByWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null",
          "7,true,aligned_test7",
          "13,true,aligned_unseq_test13",
          "19,true,null",
          "25,false,null",
          "31,null,aligned_test31",
          "37,null,aligned_test37"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s4), first_value(s5) from d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s4), first_value(s5) from d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void firstLastGroupByWithNonAlignedTimeseriesTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null",
          "7,non_aligned_test10,false,aligned_test10,false",
          "13,null,true,aligned_unseq_test13,null",
          "19,null,true,null,true",
          "25,null,true,null,true",
          "31,non_aligned_test34,null,aligned_test34,null",
          "37,non_aligned_test37,null,aligned_test37,null"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(d2.s5), first_value(d1.s4), last_value(d1.s5), first_value(d2.s4) "
                  + "from root.sg1 where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d2.s5"))
                  + ","
                  + resultSet.getString(firstValue("d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("d1.s5"))
                  + ","
                  + resultSet.getString(firstValue("d2.s4"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(d2.s5), first_value(d1.s4), last_value(d1.s5), first_value(d2.s4) "
                  + "from root.sg1 where time > 5 and time < 38 "
                  + "GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d2.s5"))
                  + ","
                  + resultSet.getString(firstValue("d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("d1.s5"))
                  + ","
                  + resultSet.getString(firstValue("d2.s4"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void groupByWithWildcardTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,9,9,8,8,9,9.0,10,10,true,aligned_test10",
          "11,10,10,10,1,1,20.0,20,20,true,aligned_unseq_test13",
          "21,1,0,10,10,0,230000.0,null,30,false,null",
          "31,0,10,0,0,10,null,40,null,null,aligned_test40"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*), last_value(*) from d1 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(count("d1.s2"))
                  + ","
                  + resultSet.getString(count("d1.s3"))
                  + ","
                  + resultSet.getString(count("d1.s4"))
                  + ","
                  + resultSet.getString(count("d1.s5"))
                  + ","
                  + resultSet.getString(lastValue("d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("d1.s2"))
                  + ","
                  + resultSet.getString(lastValue("d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*), last_value(*) from d1 "
                  + "GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(count("d1.s2"))
                  + ","
                  + resultSet.getString(count("d1.s3"))
                  + ","
                  + resultSet.getString(count("d1.s4"))
                  + ","
                  + resultSet.getString(count("d1.s5"))
                  + ","
                  + resultSet.getString(lastValue("d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("d1.s2"))
                  + ","
                  + resultSet.getString(lastValue("d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void groupByWithWildcardTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,0,0,0,0",
          "5,2,2,2,2,1",
          "9,2,3,3,2,2",
          "13,3,3,3,1,1",
          "17,3,3,3,0,0",
          "21,1,0,3,3,0",
          "25,0,0,3,3,0",
          "29,0,1,2,2,1",
          "33,0,3,0,0,3",
          "37,0,1,0,0,1"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) from d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(count("d1.s2"))
                  + ","
                  + resultSet.getString(count("d1.s3"))
                  + ","
                  + resultSet.getString(count("d1.s4"))
                  + ","
                  + resultSet.getString(count("d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) from d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("d1.s1"))
                  + ","
                  + resultSet.getString(count("d1.s2"))
                  + ","
                  + resultSet.getString(count("d1.s3"))
                  + ","
                  + resultSet.getString(count("d1.s4"))
                  + ","
                  + resultSet.getString(count("d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void groupByWithWildcardTest3() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null,null",
          "5,7.0,7,7,false,aligned_test7",
          "9,11.0,11,11,true,aligned_test10",
          "13,15.0,15,15,true,aligned_unseq_test13",
          "17,19.0,19,19,null,null",
          "21,230000.0,null,230000,false,null",
          "25,null,null,27,false,null",
          "29,null,31,30,false,aligned_test31",
          "33,null,35,null,null,aligned_test35",
          "37,null,37,null,null,aligned_test37"
        };
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("d1.s2"))
                  + ","
                  + resultSet.getString(lastValue("d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("d1.s2"))
                  + ","
                  + resultSet.getString(lastValue("d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Ignore
  @Test
  public void groupByWithoutAggregationFuncTest() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      statement.executeQuery("select s1 from d1 group by ([0, 100), 5ms)");

      fail("No expected exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains(
                  "Common queries and aggregated queries are not allowed to appear at the same time"));
    }
  }

  @Ignore
  @Test
  public void negativeOrZeroTimeIntervalTest() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      statement.executeQuery(
          "select count(s1), sum(s2), avg(s1) from  d1 "
              + "where time > 5 GROUP BY ([1, 41), 0ms)");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains("The second parameter time interval should be a positive integer."));
    }
  }
}
