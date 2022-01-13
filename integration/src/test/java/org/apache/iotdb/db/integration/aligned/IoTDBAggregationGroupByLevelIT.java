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
import org.apache.iotdb.db.qp.logical.crud.AggregationQueryOperator;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBAggregationGroupByLevelIT {

  private static final double DELTA = 1e-6;
  private static final double NULL = Double.MIN_VALUE;
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
      statement.execute("SET STORAGE GROUP TO root.sg2");
      statement.execute(
          "create aligned timeseries root.sg2.d1(s1 FLOAT encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY, s3 INT64)");
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "insert into root.sg2.d1(time, s1) aligned values(%d,%f)", i, (double) i));
      }
      for (int i = 11; i <= 20; i++) {
        statement.execute(
            String.format("insert into root.sg2.d1(time, s2) aligned values(%d,%d)", i, i));
      }
      for (int i = 21; i <= 30; i++) {
        statement.execute(
            String.format("insert into root.sg2.d1(time, s3) aligned values(%d,%d)", i, i));
      }
      for (int i = 31; i <= 40; i++) {
        statement.execute(
            String.format(
                "insert into root.sg2.d1(time, s1, s2, s3) aligned values(%d,%f,%d,%d)",
                i, (double) i, i, i));
      }
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
  public void countFuncByLevelTest() throws ClassNotFoundException {
    // level = 1
    double[][] retArray1 = new double[][] {{39, 20}};
    String[] columnNames1 = {"count(root.sg1.*.s1)", "count(root.sg2.*.s1)"};
    test("select count(s1) from root.*.* group by level=1", retArray1, columnNames1);
    // level = 2
    double[][] retArray2 = new double[][] {{40, 19}};
    String[] columnNames2 = {"count(root.*.d1.s1)", "count(root.*.d2.s1)"};
    // level = 3
    test("select count(s1) from root.*.* group by level=2", retArray2, columnNames2);
    double[][] retArray3 = new double[][] {{59}};
    String[] columnNames3 = {"count(root.*.*.s1)"};
    test("select count(s1) from root.*.* group by level=3", retArray3, columnNames3);
    // multi level = 1,2
    double[][] retArray4 = new double[][] {{20, 19, 20}};
    String[] columnNames4 = {
      "count(root.sg1.d1.s1)", "count(root.sg1.d2.s1)", "count(root.sg2.d1.s1)"
    };
    test("select count(s1) from root.*.* group by level=1,2", retArray4, columnNames4);
    // level=2 with time filter
    double[][] retArray5 =
        new double[][] {
          {17, 8},
          {10, 10},
          {1, 0},
          {10, 0}
        };
    String[] columnNames5 = {"count(root.*.d1.s1)", "count(root.*.d2.s1)"};
    test(
        "select count(s1) from root.*.* where time>=2 group by ([1,41),10ms), level=2",
        retArray5,
        columnNames5);
  }

  @Test
  public void sumFuncByLevelTest() throws ClassNotFoundException {
    // level = 1
    double[][] retArray1 = new double[][] {{131111, 510}};
    String[] columnNames1 = {"sum(root.sg1.*.s2)", "sum(root.sg2.*.s2)"};
    test("select sum(s2) from root.*.* group by level=1", retArray1, columnNames1);
    // level = 2
    double[][] retArray2 = new double[][] {{131059, 562}};
    String[] columnNames2 = {"sum(root.*.d1.s2)", "sum(root.*.d2.s2)"};
    // level = 3
    test("select sum(s2) from root.*.* group by level=2", retArray2, columnNames2);
    double[][] retArray3 = new double[][] {{131621}};
    String[] columnNames3 = {"sum(root.*.*.s2)"};
    test("select sum(s2) from root.*.* group by level=3", retArray3, columnNames3);
    // multi level = 1,2
    double[][] retArray4 = new double[][] {{130549, 562, 510}};
    String[] columnNames4 = {"sum(root.sg1.d1.s2)", "sum(root.sg1.d2.s2)", "sum(root.sg2.d1.s2)"};
    test("select sum(s2) from root.*.* group by level=1,2", retArray4, columnNames4);
    // level=2 with time filter
    double[][] retArray5 =
        new double[][] {
          {51, 51},
          {130297, 155},
          {NULL, NULL},
          {710, 355}
        };
    String[] columnNames5 = {"sum(root.*.d1.s2)", "sum(root.*.d2.s2)"};
    test(
        "select sum(s2) from root.*.* where time>=2 group by ([1,41),10ms), level=2",
        retArray5,
        columnNames5);
  }

  @Test
  public void avgFuncByLevelTest() throws ClassNotFoundException {
    // level = 1
    double[][] retArray1 = new double[][] {{2260.53448275862, 25.5}};
    String[] columnNames1 = {"avg(root.sg1.*.s2)", "avg(root.sg2.*.s2)"};
    test("select avg(s2) from root.*.* group by level=1", retArray1, columnNames1);
    // level = 2
    double[][] retArray2 = new double[][] {{2674.6734693877547, 19.379310344827587}};
    String[] columnNames2 = {"avg(root.*.d1.s2)", "avg(root.*.d2.s2)"};
    // level = 3
    test("select avg(s2) from root.*.* group by level=2", retArray2, columnNames2);
    double[][] retArray3 = new double[][] {{1687.4487179487176}};
    String[] columnNames3 = {"avg(root.*.*.s2)"};
    test("select avg(s2) from root.*.* group by level=3", retArray3, columnNames3);
    // multi level = 1,2
    double[][] retArray4 = new double[][] {{4501.68965517241, 19.379310344827587, 25.5}};
    String[] columnNames4 = {"avg(root.sg1.d1.s2)", "avg(root.sg1.d2.s2)", "avg(root.sg2.d1.s2)"};
    test("select avg(s2) from root.*.* group by level=1,2", retArray4, columnNames4);
    //     level=2 with time filter
    double[][] retArray5 =
        new double[][] {
          {6.375, 6.375},
          {6514.85, 15.5},
          {NULL, NULL},
          {35.5, 35.5}
        };
    String[] columnNames5 = {"avg(root.*.d1.s2)", "avg(root.*.d2.s2)"};
    test(
        "select avg(s2) from root.*.* where time>=2 group by ([1,41),10ms), level=2",
        retArray5,
        columnNames5);
  }

  @Test
  public void timeFuncGroupByLevelTest() throws ClassNotFoundException {
    double[][] retArray1 = new double[][] {{1, 40, 1, 30}};
    String[] columnNames1 = {
      "min_time(root.*.d1.s3)",
      "max_time(root.*.d1.s3)",
      "min_time(root.*.d2.s3)",
      "max_time(root.*.d2.s3)"
    };
    test(
        "select min_time(s3),max_time(s3) from root.*.* group by level=2", retArray1, columnNames1);
  }

  @Test
  public void valueFuncGroupByLevelTest() throws ClassNotFoundException {
    double[][] retArray1 = new double[][] {{40, 230000, 30, 30}};
    String[] columnNames1 = {
      "last_value(root.*.d1.s3)",
      "max_value(root.*.d1.s3)",
      "last_value(root.*.d2.s3)",
      "max_value(root.*.d2.s3)"
    };
    test(
        "select last_value(s3),max_value(s3) from root.*.* group by level=2",
        retArray1,
        columnNames1);
  }

  /**
   * Test group by without aggregation function used in select clause. The expected situation is
   * throwing an exception.
   */
  @Test
  public void groupByWithoutAggregationFuncTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("select s2 from root.*.* group by ([0, 40), 10ms)");
      fail("No expected exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(AggregationQueryOperator.ERROR_MESSAGE1));
    }
  }

  public void test(String sql, double[][] retArray, String[] columnNames)
      throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute(sql);
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        // if result has more than one rows, "Time" is included in columnNames
        assertEquals(
            retArray.length > 1 ? columnNames.length + 1 : columnNames.length,
            resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          double[] ans = new double[columnNames.length];
          // No need to add time column for aggregation query
          for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int index = map.get(columnName);
            String result = resultSet.getString(index);
            ans[i] = result == null ? NULL : Double.parseDouble(result);
          }
          assertArrayEquals(retArray[cnt], ans, DELTA);
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
