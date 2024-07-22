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

package org.apache.iotdb.db.it.schema.regionscan;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBActiveRegionScanIT extends AbstractSchemaIT {
  // Data can be viewed in
  // https://docs.google.com/spreadsheets/d/11tNRIaHmNdFWc0RC4yAloO6JJbmo9S5qxwAqWxeVY6c/edit#gid=0
  public static final String[] common_insert_sqls =
      new String[] {
        "create aligned timeseries root.sg.aligned.d1(s1 INT32 encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY)",
        "create aligned timeseries root.sg.aligned.d2(s3 BOOLEAN, s4 TEXT)",
        "create timeseries root.sg.unaligned.d2.s1 WITH DATATYPE=INT64, encoding=RLE",
        "create timeseries root.sg.unaligned.d2.s2 WITH DATATYPE=INT32, encoding=Gorilla",
        "create timeseries root.sg.unaligned.d3.s4 WITH DATATYPE=INT32, encoding=Gorilla",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(1, 1, 2)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(2, 2, 3)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(3, 3, 4)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(5, 5, 6)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(6, 6, 7)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(7, 7, 8)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(8, null, 9)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(9, 9, 10)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(10, 10, 11)",
        "flush",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(5, FALSE, 'aligned_test1')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(6, TRUE, 'aligned_test2')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(7, TRUE, 'aligned_test3')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(8, null, 'aligned_test4')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(9, TRUE, 'aligned_test5')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(10, TRUE, 'aligned_test6')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(12, TRUE, 'aligned_test8')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(13, TRUE, null)",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(14, TRUE, 'aligned_test10')",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(11, 11, 12)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(12, 12, 13)",
        "insert into root.sg.unaligned.d2(time, s1) values(1, 1)",
        "insert into root.sg.unaligned.d2(time, s1) values(2, 2)",
        "insert into root.sg.unaligned.d2(time, s1) values(3, 3)",
        "insert into root.sg.unaligned.d2(time, s1) values(4, 4)",
        "insert into root.sg.unaligned.d2(time, s1) values(5, 5)",
        "insert into root.sg.unaligned.d2(time, s1) values(7, null)",
        "insert into root.sg.unaligned.d2(time, s1) values(8, 8)",
        "flush",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(20, 1, 2)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(22, 2, 3)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(23, 3, 4)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(25, 5, 6)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(26, 6, 7)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(27, 7, 8)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(28, null, 9)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(29, 9, 10)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(30, 10, 11)",
        "insert into root.sg.unaligned.d2(time, s2) values(22, 1)",
        "insert into root.sg.unaligned.d2(time, s2) values(24, 2)",
        "insert into root.sg.unaligned.d3(time, s4) values(22, 1)",
        "insert into root.sg.unaligned.d3(time, s4) values(23, 2)",
        "insert into root.sg.unaligned.d3(time, s4) values(24, 3)",
        "insert into root.sg.unaligned.d3(time, s4) values(25, 4)",
        "insert into root.sg.unaligned.d3(time, s4) values(26, 5)",
        "flush",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(40, TRUE, 'aligned_test1')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(42, TRUE, 'aligned_test2')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(41, TRUE, 'aligned_test3')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(43, TRUE, 'aligned_test4')",
        "insert into root.sg.aligned.d2(time, s3, s4) aligned values(44, TRUE, 'aligned_test5')",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(40, 1, 2)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(42, 2, 3)",
        "insert into root.sg.aligned.d1(time, s1, s2) aligned values(44, 3, 4)",
        "insert into root.sg.unaligned.d2(time, s1) values(40, 1)",
        "insert into root.sg.unaligned.d2(time, s1) values(41, 2)",
        "insert into root.sg.unaligned.d2(time, s1) values(42, 3)",
        "insert into root.sg.unaligned.d2(time, s1) values(43, 4)",
        "insert into root.sg.unaligned.d2(time, s1) values(44, 5)",
        "insert into root.sg.unaligned.d2(time, s1) values(45, 6)",
        "insert into root.sg.unaligned.d2(time, s2) values(40, 1)",
        "insert into root.sg.unaligned.d2(time, s2) values(41, 2)",
        "insert into root.sg.unaligned.d2(time, s2) values(42, 3)",
        "insert into root.sg.unaligned.d2(time, s2) values(43, 4)",
        "insert into root.sg.unaligned.d3(time, s4) values(40, 1)",
        "insert into root.sg.unaligned.d3(time, s4) values(41, 2)",
        "insert into root.sg.unaligned.d3(time, s4) values(42, 3)",
      };

  public static final String[] SHOW_DEVICES_COLUMN_NAMES =
      new String[] {"Device", "IsAligned", "Template", "TTL"};
  public static final String[] SHOW_TIMESERIES_COLUMN_NAMES =
      new String[] {
        "Timeseries",
        "Alias",
        "Database",
        "DataType",
        "Encoding",
        "Compression",
        "Tags",
        "Attributes",
        "Deadband",
        "DeadbandParameters",
        "ViewType"
      };
  public static final String COUNT_TIMESERIES_COLUMN_NAMES = "count(timeseries)";
  public static final String COUNT_DEVICES_COLUMN_NAMES = "count(devices)";

  public IoTDBActiveRegionScanIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  public static void insertData() {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      // create aligned and non-aligned time series
      for (final String sql : common_insert_sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    tearDownEnvironment();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void showActiveDeviceTest() {
    String sql = "show devices where time = 4";
    String[] retArray =
        new String[] {
          "root.sg.unaligned.d2",
        };
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time = 4";
    long value = 1;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDeviceTest2() {
    String sql = "show devices where time >= 6 and time <=10";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1", "root.sg.aligned.d2", "root.sg.unaligned.d2",
        };
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time >= 6 and time <=10";
    long value = 3;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDeviceTest3() {
    String sql = "show devices where time >= 6 and time <=30";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1", "root.sg.aligned.d2", "root.sg.unaligned.d2", "root.sg.unaligned.d3"
        };
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time >= 6 and time <=30";
    long value = 4;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDeviceTest4() {
    String sql = "show devices where time >= 25 and time <= 28";
    String[] retArray = new String[] {"root.sg.aligned.d1", "root.sg.unaligned.d3"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time >= 25 and time <= 28";
    long value = 2;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDeviceTest5() {
    String sql = "show devices where time >= 40 and time <= 44";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1", "root.sg.aligned.d2", "root.sg.unaligned.d2", "root.sg.unaligned.d3"
        };
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time >= 40 and time <= 44";
    long value = 4;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDeviceTest6() {
    String sql = "show devices where time >= 43 and time < 44";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d2", "root.sg.unaligned.d2",
        };
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveDeviceTest7() {
    String sql = "show devices where time > 45";
    String[] retArray = new String[] {};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveDeviceTest8() {
    String sql = "show devices where time > 44 or time < 4";
    String[] retArray = new String[] {"root.sg.aligned.d1", "root.sg.unaligned.d2"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveDeviceTest9() {
    String sql = "show devices where time = 8";
    String[] retArray =
        new String[] {"root.sg.aligned.d1", "root.sg.aligned.d2", "root.sg.unaligned.d2"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveDeviceTest10() {
    String sql = "show devices where time < 50";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1", "root.sg.aligned.d2", "root.sg.unaligned.d3", "root.sg.unaligned.d2"
        };
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveDeviceEmptyTest() {
    String sql = "show devices root.empty where time < 50";
    String[] retArray = new String[] {};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices root.empty where time < 50";
    long value = 0;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveTimeseriesEmptyTest() {
    String sql = "show timeseries root.empty where time < 50";
    String[] retArray = new String[] {};
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries root.empty where time < 50";
    long value = 0;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveTimeseriesTest() {
    String sql = "show timeseries where time = 4";
    String[] retArray =
        new String[] {
          "root.sg.unaligned.d2.s1",
        };
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveTimeseriesTest2() {
    String sql = "show timeseries where time >= 6 and time <=10";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1.s1",
          "root.sg.aligned.d1.s2",
          "root.sg.aligned.d2.s3",
          "root.sg.aligned.d2.s4",
          "root.sg.unaligned.d2.s1",
        };

    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveTimeseriesTest3() {
    String sql = "show timeseries where time >= 6 and time <=30";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1.s1",
          "root.sg.aligned.d1.s2",
          "root.sg.aligned.d2.s3",
          "root.sg.aligned.d2.s4",
          "root.sg.unaligned.d2.s1",
          "root.sg.unaligned.d2.s2",
          "root.sg.unaligned.d3.s4"
        };
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveTimeseriesTest4() {
    String sql = "show timeseries where time >= 25 and time <= 28";
    String[] retArray =
        new String[] {"root.sg.aligned.d1.s1", "root.sg.aligned.d1.s2", "root.sg.unaligned.d3.s4"};
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);
  }

  @Test
  public void showActiveTimeseriesTest5() {
    String sql = "show timeseries where time >= 40 and time <= 44";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1.s1",
          "root.sg.aligned.d1.s2",
          "root.sg.aligned.d2.s3",
          "root.sg.aligned.d2.s4",
          "root.sg.unaligned.d2.s1",
          "root.sg.unaligned.d2.s2",
          "root.sg.unaligned.d3.s4"
        };
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time >= 40 and time <= 44";
    long value = 7;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveTimeseriesTest6() {
    String sql = "show timeseries where time >= 43 and time < 44";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d2.s3",
          "root.sg.aligned.d2.s4",
          "root.sg.unaligned.d2.s1",
          "root.sg.unaligned.d2.s2"
        };
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time >= 43 and time < 44";
    long value = 4;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveTimeseriesTest7() {
    String sql = "show timeseries where time > 45";
    String[] retArray = new String[] {};
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time > 45";
    long value = 0;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveTimeseriesTest8() {
    String sql = "show timeseries where time > 44 or time < 4";
    String[] retArray =
        new String[] {"root.sg.aligned.d1.s1", "root.sg.aligned.d1.s2", "root.sg.unaligned.d2.s1"};
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time > 44 or time < 4";
    long value = 3;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveTimeseriesTest9() {
    String sql = "show timeseries where time = 8";
    String[] retArray =
        new String[] {"root.sg.aligned.d1.s2", "root.sg.aligned.d2.s4", "root.sg.unaligned.d2.s1"};
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time = 8";
    long value = 3;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveTimeseriesTest10() {
    String sql = "show timeseries where time < 50";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1.s1",
          "root.sg.aligned.d1.s2",
          "root.sg.aligned.d2.s3",
          "root.sg.aligned.d2.s4",
          "root.sg.unaligned.d2.s1",
          "root.sg.unaligned.d2.s2",
          "root.sg.unaligned.d3.s4"
        };
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time < 50";
    long value = 7;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  public static void basicShowActiveDeviceTest(
      String sql, String[] columnNames, String[] retArray) {

    Set<String> set = new HashSet<>();
    Collections.addAll(set, retArray);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          String id = resultSet.getString(1);
          assertTrue(set.contains(id));
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  public static void basicCountActiveDeviceTest(String sql, String columnName, long value) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        assertEquals(1, resultSetMetaData.getColumnCount());
        assertEquals(columnName, resultSetMetaData.getColumnName(1));
        int cnt = 0;
        while (resultSet.next()) {
          long count = resultSet.getLong(1);
          assertEquals(value, count);
          cnt++;
        }
        assertEquals(1, cnt);
      }

    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }
}
