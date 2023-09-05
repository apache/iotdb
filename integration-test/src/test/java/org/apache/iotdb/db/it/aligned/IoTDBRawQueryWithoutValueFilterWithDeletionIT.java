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
package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
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
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBRawQueryWithoutValueFilterWithDeletionIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    AlignedWriteUtil.insertData();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("delete from root.sg1.d1.s2 where time <= 40");
      statement.execute("delete from root.sg1.d1.s1 where time <= 21");
      statement.execute("delete from root.sg1.d1.s5 where time <= 31 and time > 20");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void selectAllAlignedWithoutValueFilterTest() {

    String[] retArray =
        new String[] {
          "1,null,1,true,aligned_test1",
          "2,null,2,null,aligned_test2",
          "3,null,30000,true,aligned_unseq_test3",
          "4,null,null,true,aligned_test4",
          "5,null,null,true,aligned_test5",
          "6,null,6,true,null",
          "7,null,7,false,aligned_test7",
          "8,null,8,null,aligned_test8",
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
          "32,null,null,null,aligned_test32",
          "33,null,null,null,aligned_test33",
          "34,null,null,null,aligned_test34",
          "35,null,null,null,aligned_test35",
          "36,null,null,null,aligned_test36",
          "37,null,null,null,aligned_test37",
          "38,null,null,null,aligned_test38",
          "39,null,null,null,aligned_test39",
          "40,null,null,null,aligned_test40",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from root.sg1.d1")) {

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

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedAndNonAlignedTest() {

    String[] retArray =
        new String[] {
          "1,null,1,true,aligned_test1,1.0,1,1,true,non_aligned_test1",
          "2,null,2,null,aligned_test2,2.0,2,2,null,non_aligned_test2",
          "3,null,30000,true,aligned_unseq_test3,3.0,null,3,false,non_aligned_test3",
          "4,null,null,true,aligned_test4,4.0,4,null,true,non_aligned_test4",
          "5,null,null,true,aligned_test5,5.0,5,null,true,non_aligned_test5",
          "6,null,6,true,null,6.0,6,6,true,null",
          "7,null,7,false,aligned_test7,7.0,7,7,false,non_aligned_test7",
          "8,null,8,null,aligned_test8,8.0,8,8,null,non_aligned_test8",
          "9,null,9,false,aligned_test9,9.0,9,9,false,non_aligned_test9",
          "10,null,10,true,aligned_test10,null,10,10,true,non_aligned_test10",
          "11,null,11,null,null,11.0,11,11,null,null",
          "12,null,12,null,null,12.0,12,12,null,null",
          "13,null,130000,true,aligned_unseq_test13,13.0,13,13,null,null",
          "14,null,14,null,null,14.0,14,14,null,null",
          "15,null,15,null,null,15.0,15,15,null,null",
          "16,null,16,null,null,16.0,16,16,null,null",
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
          "31,null,null,null,null,null,31,null,null,non_aligned_test31",
          "32,null,null,null,aligned_test32,null,32,null,null,non_aligned_test32",
          "33,null,null,null,aligned_test33,null,33,null,null,non_aligned_test33",
          "34,null,null,null,aligned_test34,null,34,null,null,non_aligned_test34",
          "35,null,null,null,aligned_test35,null,35,null,null,non_aligned_test35",
          "36,null,null,null,aligned_test36,null,36,null,null,non_aligned_test36",
          "37,null,null,null,aligned_test37,null,37,null,null,non_aligned_test37",
          "38,null,null,null,aligned_test38,null,38,null,null,non_aligned_test38",
          "39,null,null,null,aligned_test39,null,39,null,null,non_aligned_test39",
          "40,null,null,null,aligned_test40,null,40,null,null,non_aligned_test40",
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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from root.sg1.*")) {

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

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedWithTimeFilterTest() {

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
          "32,null,null,null,aligned_test32",
          "33,null,null,null,aligned_test33",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select * from root.sg1.d1 where time >= 9 and time <= 33")) {

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

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedWithoutValueFilterTest1() {

    String[] retArray =
        new String[] {
          "1,null,true,aligned_test1",
          "2,null,null,aligned_test2",
          "3,null,true,aligned_unseq_test3",
          "4,null,true,aligned_test4",
          "5,null,true,aligned_test5",
          "6,null,true,null",
          "7,null,false,aligned_test7",
          "8,null,null,aligned_test8",
          "9,null,false,aligned_test9",
          "10,null,true,aligned_test10",
          "13,null,true,aligned_unseq_test13",
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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select s1,s4,s5 from root.sg1.d1")) {

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

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedWithoutValueFilterTest2() {

    String[] retArray =
        new String[] {
          "1,null,true",
          "3,null,true",
          "4,null,true",
          "5,null,true",
          "6,null,true",
          "7,null,false",
          "9,null,false",
          "10,null,true",
          "13,null,true",
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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select s1,s4 from root.sg1.d1")) {

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

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedWithTimeFilterTest() {

    String[] retArray =
        new String[] {
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
          "32,null,null,aligned_test32",
          "33,null,null,aligned_test33",
          "34,null,null,aligned_test34",
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select s1,s4,s5 from root.sg1.d1 where time >= 16 and time <= 34")) {

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

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedAndNonAlignedWithTimeFilterTest() {

    String[] retArray =
        new String[] {
          "16,null,null,16.0,null,null,null",
          "17,null,null,17.0,null,null,null",
          "18,null,null,18.0,null,null,null",
          "19,null,null,19.0,null,null,null",
          "20,null,null,20.0,null,null,null",
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
          "31,non_aligned_test31,null,null,null,null,null",
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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select d2.s5, d1.s4, d2.s1, d1.s5, d2.s4, d1.s1 from root.sg1 where time >= 16 and time <= 34")) {

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

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
