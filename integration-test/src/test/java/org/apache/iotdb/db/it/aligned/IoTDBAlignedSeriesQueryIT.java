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

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.itbase.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESEIRES_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignedSeriesQueryIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @BeforeClass
  public static void setUp() throws Exception {
    enableSeqSpaceCompaction = ConfigFactory.getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction = ConfigFactory.getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction = ConfigFactory.getConfig().isEnableCrossSpaceCompaction();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initBeforeClass();
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
  }

  // ----------------------------------------Last Query-----------------------------------------
  @Test
  public void selectAllAlignedLastTest() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "40,root.sg1.d1.s2,40,INT32",
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select last * from root.sg1.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedAndNonAlignedLastTest() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "40,root.sg1.d1.s2,40,INT32",
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT",
                "20,root.sg1.d2.s1,20.0,FLOAT",
                "40,root.sg1.d2.s2,40,INT32",
                "30,root.sg1.d2.s3,30,INT64",
                "30,root.sg1.d2.s4,false,BOOLEAN",
                "40,root.sg1.d2.s5,non_aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select last * from root.sg1.*")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedLastWithTimeFilterTest() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList("40,root.sg1.d1.s2,40,INT32", "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg1.d1 where time > 30")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastTest1() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last s1, s4, s5 from root.sg1.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastTest2() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList("23,root.sg1.d1.s1,230000.0,FLOAT", "30,root.sg1.d1.s4,false,BOOLEAN"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select last s1, s4 from root.sg1.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastWithTimeFilterTest() {

    Set<String> retSet =
        new HashSet<>(Collections.singletonList("40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last s1, s4, s5 from root.sg1.d1 where time > 30")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedAndNonAlignedLastWithTimeFilterTest() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "40,root.sg1.d1.s5,aligned_test40,TEXT",
                "40,root.sg1.d2.s5,non_aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // 1 4 5
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last d2.s5, d1.s4, d2.s1, d1.s5, d2.s4, d1.s1 from root.sg1 where time > 30")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
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

    String[] columnNames = {
      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select * from root.sg1.d1")) {
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
  public void selectAllAlignedAndNonAlignedTest() {

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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select * from root.sg1.*")) {
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

    String[] columnNames = {
      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
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

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select s1,s4,s5 from root.sg1.d1")) {
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
        };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select s1,s4 from root.sg1.d1")) {
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

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
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
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedAndNonAlignedWithTimeFilterTest() {

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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
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
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ------------------------------Raw Query With Value Filter-------------------------------------
  // TODO add these back when value filter is done in new cluster
  //
  //  @Test
  //  public void selectAllAlignedWithValueFilterTest1() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "1,1.0,1,1,true,aligned_test1",
  //          "3,30000.0,null,30000,true,aligned_unseq_test3",
  //          "4,4.0,4,null,true,aligned_test4",
  //          "5,5.0,5,null,true,aligned_test5",
  //          "6,6.0,6,6,true,null",
  //          "10,null,10,10,true,aligned_test10",
  //          "13,130000.0,130000,130000,true,aligned_unseq_test13",
  //          "21,null,null,21,true,null",
  //          "22,null,null,22,true,null",
  //          "24,null,null,24,true,null",
  //          "25,null,null,25,true,null",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery("select * from root.sg1.d1 where s4 = true")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectAllAlignedWithValueFilterTest2() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "12,12.0,12,12,null,null",
  //          "14,14.0,14,14,null,null",
  //          "15,15.0,15,15,null,null",
  //          "16,16.0,16,16,null,null",
  //          "17,17.0,17,17,null,null",
  //          "18,18.0,18,18,null,null",
  //          "19,19.0,19,19,null,null",
  //          "20,20.0,20,20,null,null",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery("select * from root.sg1.d1 where s1 > 11 and s2 <= 33")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectAllAlignedWithValueFilterTest3() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "1,1.0,1,1,true,aligned_test1",
  //          "2,2.0,2,2,null,aligned_test2",
  //          "3,30000.0,null,30000,true,aligned_unseq_test3",
  //          "4,4.0,4,null,true,aligned_test4",
  //          "5,5.0,5,null,true,aligned_test5",
  //          "6,6.0,6,6,true,null",
  //          "7,7.0,7,7,false,aligned_test7",
  //          "8,8.0,8,8,null,aligned_test8",
  //          "9,9.0,9,9,false,aligned_test9",
  //          "10,null,10,10,true,aligned_test10",
  //          "11,11.0,11,11,null,null",
  //          "12,12.0,12,12,null,null",
  //          "13,130000.0,130000,130000,true,aligned_unseq_test13",
  //          "14,14.0,14,14,null,null",
  //          "15,15.0,15,15,null,null",
  //          "16,16.0,16,16,null,null",
  //          "17,17.0,17,17,null,null",
  //          "18,18.0,18,18,null,null",
  //          "19,19.0,19,19,null,null",
  //          "20,20.0,20,20,null,null",
  //          "23,230000.0,null,230000,false,null",
  //          "31,null,31,null,null,aligned_test31",
  //          "32,null,32,null,null,aligned_test32",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery("select * from root.sg1.d1 where s1 >= 13 or s2 < 33")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectAllAlignedAndNonAlignedTest1() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "13,130000.0,130000,130000,true,aligned_unseq_test13,13.0,13,13,null,null",
  //          "17,17.0,17,17,null,null,17.0,17,17,null,null",
  //          "18,18.0,18,18,null,null,18.0,18,18,null,null",
  //          "19,19.0,19,19,null,null,19.0,19,19,null,null",
  //          "20,20.0,20,20,null,null,20.0,20,20,null,null",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d1.s1",
  //      "root.sg1.d1.s2",
  //      "root.sg1.d1.s3",
  //      "root.sg1.d1.s4",
  //      "root.sg1.d1.s5",
  //      "root.sg1.d2.s1",
  //      "root.sg1.d2.s2",
  //      "root.sg1.d2.s3",
  //      "root.sg1.d2.s4",
  //      "root.sg1.d2.s5"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select * from root.sg1.* where root.sg1.d1.s2 > 16 and root.sg1.d2.s3 <= 36")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectAllAlignedAndNonAlignedTest2() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "3,30000.0,null,30000,true,aligned_unseq_test3,3.0,null,3,false,non_aligned_test3",
  //          "7,7.0,7,7,false,aligned_test7,7.0,7,7,false,non_aligned_test7",
  //          "9,9.0,9,9,false,aligned_test9,9.0,9,9,false,non_aligned_test9",
  //          "13,130000.0,130000,130000,true,aligned_unseq_test13,13.0,13,13,null,null",
  //          "17,17.0,17,17,null,null,17.0,17,17,null,null",
  //          "18,18.0,18,18,null,null,18.0,18,18,null,null",
  //          "19,19.0,19,19,null,null,19.0,19,19,null,null",
  //          "20,20.0,20,20,null,null,20.0,20,20,null,null",
  //          "26,null,null,26,false,null,null,null,26,false,null",
  //          "27,null,null,27,false,null,null,null,27,false,null",
  //          "28,null,null,28,false,null,null,null,28,false,null",
  //          "29,null,null,29,false,null,null,null,29,false,null",
  //          "30,null,null,30,false,null,null,null,30,false,null",
  //          "31,null,31,null,null,aligned_test31,null,31,null,null,non_aligned_test31",
  //          "32,null,32,null,null,aligned_test32,null,32,null,null,non_aligned_test32",
  //          "33,null,33,null,null,aligned_test33,null,33,null,null,non_aligned_test33",
  //          "34,null,34,null,null,aligned_test34,null,34,null,null,non_aligned_test34",
  //          "35,null,35,null,null,aligned_test35,null,35,null,null,non_aligned_test35",
  //          "36,null,36,null,null,aligned_test36,null,36,null,null,non_aligned_test36",
  //          "37,null,37,null,null,aligned_test37,null,37,null,null,non_aligned_test37",
  //          "38,null,38,null,null,aligned_test38,null,38,null,null,non_aligned_test38",
  //          "39,null,39,null,null,aligned_test39,null,39,null,null,non_aligned_test39",
  //          "40,null,40,null,null,aligned_test40,null,40,null,null,non_aligned_test40",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d1.s1",
  //      "root.sg1.d1.s2",
  //      "root.sg1.d1.s3",
  //      "root.sg1.d1.s4",
  //      "root.sg1.d1.s5",
  //      "root.sg1.d2.s1",
  //      "root.sg1.d2.s2",
  //      "root.sg1.d2.s3",
  //      "root.sg1.d2.s4",
  //      "root.sg1.d2.s5"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select * from root.sg1.* where root.sg1.d1.s2 > 16 or root.sg1.d2.s4 = false")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectAllAlignedWithTimeAndValueFilterTest1() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "9,9.0,9,9,false,aligned_test9",
  //          "11,11.0,11,11,null,null",
  //          "12,12.0,12,12,null,null",
  //          "14,14.0,14,14,null,null",
  //          "15,15.0,15,15,null,null",
  //          "16,16.0,16,16,null,null",
  //          "17,17.0,17,17,null,null",
  //          "18,18.0,18,18,null,null",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select * from root.sg1.d1 where time >= 9 and time <= 33 and s1 < 19")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectAllAlignedWithTimeAndValueFilterTest2() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "9,9.0,9,9,false,aligned_test9",
  //          "10,null,10,10,true,aligned_test10",
  //          "11,11.0,11,11,null,null",
  //          "12,12.0,12,12,null,null",
  //          "13,130000.0,130000,130000,true,aligned_unseq_test13",
  //          "14,14.0,14,14,null,null",
  //          "15,15.0,15,15,null,null",
  //          "16,16.0,16,16,null,null",
  //          "17,17.0,17,17,null,null",
  //          "18,18.0,18,18,null,null",
  //          "19,19.0,19,19,null,null",
  //          "20,20.0,20,20,null,null",
  //          "21,null,null,21,true,null",
  //          "22,null,null,22,true,null",
  //          "23,230000.0,null,230000,false,null",
  //          "24,null,null,24,true,null",
  //          "25,null,null,25,true,null",
  //          "26,null,null,26,false,null",
  //          "27,null,null,27,false,null",
  //          "28,null,null,28,false,null",
  //          "29,null,null,29,false,null",
  //          "30,null,null,30,false,null",
  //          "31,null,31,null,null,aligned_test31",
  //          "32,null,32,null,null,aligned_test32",
  //          "33,null,33,null,null,aligned_test33",
  //          "36,null,36,null,null,aligned_test36",
  //          "37,null,37,null,null,aligned_test37",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select * from root.sg1.d1 where time >= 9 and time <= 33 or s5 = 'aligned_test36'
  // or s5 = 'aligned_test37'")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectSomeAlignedWithValueFilterTest1() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "1,1.0,true,aligned_test1",
  //          "2,2.0,null,aligned_test2",
  //          "4,4.0,true,aligned_test4",
  //          "5,5.0,true,aligned_test5",
  //          "6,6.0,true,null",
  //          "7,7.0,false,aligned_test7",
  //          "8,8.0,null,aligned_test8",
  //          "9,9.0,false,aligned_test9",
  //          "11,11.0,null,null",
  //          "12,12.0,null,null",
  //          "14,14.0,null,null",
  //          "15,15.0,null,null",
  //          "16,16.0,null,null",
  //          "34,null,null,aligned_test34",
  //        };
  //
  //    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select s1,s4,s5 from root.sg1.d1 where s1 < 17 or s5 = 'aligned_test34'")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectSomeAlignedWithValueFilterTest2() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "7,7.0,false", "9,9.0,false",
  //        };
  //
  //    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4"};
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery("select s1,s4 from root.sg1.d1 where s1 < 19 and s4 = false"))
  // {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectSomeAlignedWithTimeAndValueFilterTest() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "23,230000.0,false,null",
  //          "26,null,false,null",
  //          "27,null,false,null",
  //          "28,null,false,null",
  //          "29,null,false,null",
  //          "30,null,false,null",
  //        };
  //
  //    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5"};
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select s1,s4,s5 from root.sg1.d1 where time >= 16 and time <= 34 and s4=false"))
  // {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void selectSomeAlignedAndNonAlignedWithTimeAndValueFilterTest() {
  //
  //    String[] retArray =
  //        new String[] {
  //          "18,null,null,18.0,null,null,18.0",
  //          "19,null,null,19.0,null,null,19.0",
  //          "20,null,null,20.0,null,null,20.0",
  //          "21,null,true,null,null,true,null",
  //          "22,null,true,null,null,true,null",
  //          "23,null,false,null,null,true,230000.0",
  //          "24,null,true,null,null,true,null",
  //          "25,null,true,null,null,true,null",
  //        };
  //
  //    String[] columnNames = {
  //      "root.sg1.d2.s5",
  //      "root.sg1.d1.s4",
  //      "root.sg1.d2.s1",
  //      "root.sg1.d1.s5",
  //      "root.sg1.d2.s4",
  //      "root.sg1.d1.s1"
  //    };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select d2.s5, d1.s4, d2.s1, d1.s5, d2.s4, d1.s1 from root.sg1 where time >= 16
  // and time <= 34 and (d1.s1 >= 18 or d2.s4 = true)")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        Map<String, Integer> map = new HashMap<>();
  //        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //          map.put(resultSetMetaData.getColumnName(i), i);
  //        }
  //        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          StringBuilder builder = new StringBuilder();
  //          builder.append(resultSet.getString(1));
  //          for (String columnName : columnNames) {
  //            int index = map.get(columnName);
  //            builder.append(",").append(resultSet.getString(index));
  //          }
  //          assertEquals(retArray[cnt], builder.toString());
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //    } catch (SQLException e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
}
