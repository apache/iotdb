/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.aggregation;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBTagAggregationIT {
  private final double E = 0.00001D;
  protected static final String[] DATASET =
      new String[] {
        "CREATE DATABASE root.sg.a;",
        "CREATE DATABASE root.sg.b;",
        "CREATE DATABASE root.sg2.c;",
        "CREATE DATABASE root.case2;",
        "create timeseries root.sg.a.d1.t with datatype=FLOAT tags(k1=k1v1, k2=k2v1, k3=k3v1);",
        "create timeseries root.sg.b.d2.t with datatype=FLOAT tags(k1=k1v1, k2=k2v2);",
        "create timeseries root.sg.a.d3.t with datatype=FLOAT tags(k1=k1v2, k2=k2v1);",
        "create timeseries root.sg.b.d4.t with datatype=FLOAT tags(k1=k1v2, k2=k2v2);",
        "create timeseries root.sg.a.d5.t with datatype=FLOAT tags(k1=k1v1);",
        "create timeseries root.sg.b.d6.t with datatype=FLOAT tags(k2=k2v1);",
        "create timeseries root.sg.a.d7.t with datatype=FLOAT;",
        "create timeseries root.sg2.c.d8.t with datatype=TEXT tags(k3=k3v1);",
        "insert into root.sg.a.d1(time, t) values(1, 1.1);",
        "insert into root.sg.b.d2(time, t) values(1, 1.2);",
        "insert into root.sg.a.d3(time, t) values(1, 1.3);",
        "insert into root.sg.b.d4(time, t) values(1, 1.4);",
        "insert into root.sg.a.d5(time, t) values(1, 1.5);",
        "insert into root.sg.b.d6(time, t) values(1, 1.6);",
        "insert into root.sg.a.d7(time, t) values(1, 1.7);",
        "insert into root.sg2.c.d8(time, t) values(1, 'abc');",
        "insert into root.sg.a.d1(time, t) values(10, 2.1);",
        "insert into root.sg.b.d2(time, t) values(10, 3.2);",
        "insert into root.sg.a.d3(time, t) values(10, 4.3);",
        "insert into root.sg.b.d4(time, t) values(10, 5.4);",
        "insert into root.sg.a.d5(time, t) values(10, 6.5);",
        "insert into root.sg.b.d6(time, t) values(10, 7.6);",
        "insert into root.sg.a.d7(time, t) values(10, 8.7);",

        // test multi value with multi aggregation column
        "create timeseries root.case2.d1.s1 with datatype=FLOAT tags(k1=v1);",
        "create timeseries root.case2.d2.s1 with datatype=FLOAT tags(k1=v1);",
        "create timeseries root.case2.d1.s2 with datatype=FLOAT tags(k1=v2);",
        "create timeseries root.case2.d3.s1 with datatype=FLOAT tags(k1=v2);",
        "insert into root.case2.d1(time, s1) values(10, 8.8);",
        "insert into root.case2.d2(time, s1) values(10, 7.7);",
        "insert into root.case2.d1(time, s2) values(10, 6.6);",
        "insert into root.case2.d3(time, s1) values(10, 9.9);",
      };

  protected static final double DELTA = 0.001D;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : DATASET) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAggregateFunctions() {
    String query =
        "SELECT COUNT(t), AVG(t), MAX_TIME(t), MIN_TIME(t), MAX_VALUE(t), MIN_VALUE(t), EXTREME(t) FROM root.sg.** GROUP BY TAGS(k1)";
    // Expected result set:
    // +----+--------+------------------+-----------+-----------+------------+------------+----------+
    // |  k1|count(t)|
    // avg(t)|max_time(t)|min_time(t)|max_value(t)|min_value(t)|extreme(t)|
    // +----+--------+------------------+-----------+-----------+------------+------------+----------+
    // |k1v2|       4|3.1000000536441803|         10|          1|         5.4|         1.3|
    // 5.4|
    // |k1v1|       6| 2.600000003973643|         10|          1|         6.5|         1.1|
    // 6.5|
    // |NULL|       4|  4.89999994635582|         10|          1|         8.7|         1.6|
    // 8.7|
    // +----+--------+------------------+-----------+-----------+------------+------------+----------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(8, resultSet.getMetaData().getColumnCount());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("k1v2", resultSet.getString("k1"));
        Assert.assertEquals(4L, resultSet.getLong("count(t)"));
        Assert.assertEquals(3.1D, resultSet.getDouble("avg(t)"), DELTA);
        Assert.assertEquals(10L, resultSet.getLong("max_time(t)"));
        Assert.assertEquals(1L, resultSet.getLong("min_time(t)"));
        Assert.assertEquals(5.4F, resultSet.getFloat("max_value(t)"), DELTA);
        Assert.assertEquals(1.3F, resultSet.getFloat("min_value(t)"), DELTA);
        Assert.assertEquals(5.4F, resultSet.getFloat("extreme(t)"), DELTA);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("k1v1", resultSet.getString(1));
        Assert.assertEquals(6L, resultSet.getLong(2));
        Assert.assertEquals(2.6D, resultSet.getDouble(3), DELTA);
        Assert.assertEquals(10L, resultSet.getLong(4));
        Assert.assertEquals(1L, resultSet.getLong(5));
        Assert.assertEquals(6.5F, resultSet.getFloat(6), DELTA);
        Assert.assertEquals(1.1F, resultSet.getFloat(7), DELTA);
        Assert.assertEquals(6.5F, resultSet.getFloat(8), DELTA);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("NULL", resultSet.getString(1));
        Assert.assertEquals(4L, resultSet.getLong(2));
        Assert.assertEquals(4.9D, resultSet.getDouble(3), DELTA);
        Assert.assertEquals(10L, resultSet.getLong(4));
        Assert.assertEquals(1L, resultSet.getLong(5));
        Assert.assertEquals(8.7D, resultSet.getFloat(6), DELTA);
        Assert.assertEquals(1.6D, resultSet.getFloat(7), DELTA);
        Assert.assertEquals(8.7D, resultSet.getFloat(8), DELTA);
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Ignore
  public void testAggregateFunctionsWithNestedExpression() {
    String query = "SELECT COUNT(t + 1), AVG(t + 1) FROM root.sg.** GROUP BY TAGS(k1)";
    // Expected result set:
    // +----+------------+------------------+
    // |  k1|count(t + 1)|        avg(t + 1)|
    // +----+------------+------------------+
    // |k1v2|           4|3.1000000536441803|
    // |k1v1|           6| 3.600000003973643|
    // |NULL|           4|  5.89999994635582|
    // +----+------------+------------------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(8, resultSet.getMetaData().getColumnCount());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("k1v2", resultSet.getString("k1"));
        Assert.assertEquals(4L, resultSet.getLong("count(t)"));
        Assert.assertEquals(3.1D, resultSet.getDouble("avg(t)"), DELTA);
        Assert.assertEquals(10L, resultSet.getLong("max_time(t)"));
        Assert.assertEquals(1L, resultSet.getLong("min_time(t)"));
        Assert.assertEquals(5.4F, resultSet.getFloat("max_value(t)"), DELTA);
        Assert.assertEquals(1.3F, resultSet.getFloat("min_value(t)"), DELTA);
        Assert.assertEquals(5.4F, resultSet.getFloat("extreme(t)"), DELTA);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("k1v1", resultSet.getString(1));
        Assert.assertEquals(6L, resultSet.getLong(2));
        Assert.assertEquals(2.6D, resultSet.getDouble(3), DELTA);
        Assert.assertEquals(10L, resultSet.getLong(4));
        Assert.assertEquals(1L, resultSet.getLong(5));
        Assert.assertEquals(6.5F, resultSet.getFloat(6), DELTA);
        Assert.assertEquals(1.1F, resultSet.getFloat(7), DELTA);
        Assert.assertEquals(6.5F, resultSet.getFloat(8), DELTA);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("NULL", resultSet.getString(1));
        Assert.assertEquals(4L, resultSet.getLong(2));
        Assert.assertEquals(4.9D, resultSet.getDouble(3), DELTA);
        Assert.assertEquals(10L, resultSet.getLong(4));
        Assert.assertEquals(1L, resultSet.getLong(5));
        Assert.assertEquals(8.7D, resultSet.getFloat(6), DELTA);
        Assert.assertEquals(1.6D, resultSet.getFloat(7), DELTA);
        Assert.assertEquals(8.7D, resultSet.getFloat(8), DELTA);
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  @Ignore // TODO: support having in later commits
  public void testAggregateFunctionsWithHaving() {
    String query =
        "SELECT COUNT(t), AVG(t), MAX_TIME(t), MIN_TIME(t), MAX_VALUE(t), MIN_VALUE(t), EXTREME(t) FROM root.sg.** GROUP BY TAGS(k1) HAVING avg(t) > 3";
    // Expected result set:
    // +----+--------+------------------+-----------+-----------+------------+------------+----------+
    // |  k1|count(t)|
    // avg(t)|max_time(t)|min_time(t)|max_value(t)|min_value(t)|extreme(t)|
    // +----+--------+------------------+-----------+-----------+------------+------------+----------+
    // |k1v2|       4|3.1000000536441803|         10|          1|         5.4|         1.3|
    // 5.4|
    // |NULL|       4|  4.89999994635582|         10|          1|         8.7|         1.6|
    // 8.7|
    // +----+--------+------------------+-----------+-----------+------------+------------+----------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(8, resultSet.getMetaData().getColumnCount());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("k1v2", resultSet.getString("k1"));
        Assert.assertEquals(4L, resultSet.getLong("count(t)"));
        Assert.assertEquals(3.1D, resultSet.getDouble("avg(t)"), DELTA);
        Assert.assertEquals(10L, resultSet.getLong("max_time(t)"));
        Assert.assertEquals(1L, resultSet.getLong("min_time(t)"));
        Assert.assertEquals(5.4F, resultSet.getFloat("max_value(t)"), DELTA);
        Assert.assertEquals(1.3F, resultSet.getFloat("min_value(t)"), DELTA);
        Assert.assertEquals(5.4F, resultSet.getFloat("extreme(t)"), DELTA);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("NULL", resultSet.getString(1));
        Assert.assertEquals(4L, resultSet.getLong(2));
        Assert.assertEquals(4.9D, resultSet.getDouble(3), DELTA);
        Assert.assertEquals(10L, resultSet.getLong(4));
        Assert.assertEquals(1L, resultSet.getLong(5));
        Assert.assertEquals(8.7D, resultSet.getFloat(6), DELTA);
        Assert.assertEquals(1.6D, resultSet.getFloat(7), DELTA);
        Assert.assertEquals(8.7D, resultSet.getFloat(8), DELTA);
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMultipleAggregationKeys() {
    String query = "SELECT COUNT(t) FROM root.sg.** GROUP BY TAGS(k1, k2)";
    // Expected result set:
    // +----+----+--------+
    // |  k1|  k2|count(t)|
    // +----+----+--------+
    // |NULL|NULL|       2|
    // |NULL|k2v1|       2|
    // |k1v1|NULL|       2|
    // |k1v2|k2v1|       2|
    // |k1v1|k2v2|       2|
    // |k1v1|k2v1|       2|
    // |k1v2|k2v2|       2|
    // +----+----+--------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        Set<List<String>> groups = new HashSet<>();
        for (int i = 0; i < 7; i++) {
          Assert.assertTrue(resultSet.next());
          List<String> tagValues = new ArrayList<>(2);
          tagValues.add(resultSet.getString("k1"));
          tagValues.add(resultSet.getString("k2"));
          groups.add(tagValues);
          Assert.assertEquals(2L, resultSet.getLong("count(t)"));
        }
        Assert.assertFalse(resultSet.next());
        Assert.assertEquals(7, groups.size());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlongWithTimeAggregation() {
    String query = "SELECT COUNT(t) from root.sg.** GROUP BY ([0, 20), 10ms), TAGS(k1)";
    // Expected result set:
    // +-----------------------------+----+--------+
    // |                         Time|  k1|count(t)|
    // +-----------------------------+----+--------+
    // |1970-01-01T08:00:00.000+08:00|k1v2|       2|
    // |1970-01-01T08:00:00.000+08:00|k1v1|       3|
    // |1970-01-01T08:00:00.000+08:00|NULL|       2|
    // |1970-01-01T08:00:00.010+08:00|k1v2|       2|
    // |1970-01-01T08:00:00.010+08:00|k1v1|       3|
    // |1970-01-01T08:00:00.010+08:00|NULL|       2|
    // +-----------------------------+----+--------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        Set<String> groups = new HashSet<>();
        for (int i = 0; i < 6; i++) {
          Assert.assertTrue(resultSet.next());
          if (i < 3) {
            Assert.assertEquals(0L, resultSet.getLong("Time"));
          } else {
            Assert.assertEquals(10L, resultSet.getLong("Time"));
          }
          String tagValue = resultSet.getString("k1");
          switch (tagValue) {
            case "k1v1":
              Assert.assertEquals(3L, resultSet.getLong("count(t)"));
              break;
            case "k1v2":
              Assert.assertEquals(2L, resultSet.getLong("count(t)"));
              break;
            case "NULL":
              Assert.assertEquals(2L, resultSet.getLong("count(t)"));
              break;
            default:
              fail("Unexpected tag value: " + tagValue);
          }
          groups.add(tagValue);
        }
        Assert.assertEquals(3, groups.size());
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlongWithSlidingWindow() {
    String query = "SELECT COUNT(t) from root.sg.** GROUP BY ([0, 20), 15ms, 5ms), TAGS(k1)";
    // Expected result set:
    // +-----------------------------+----+--------+
    // |                         Time|  k1|count(t)|
    // +-----------------------------+----+--------+
    // |1970-01-01T08:00:00.000+08:00|k1v2|       4|
    // |1970-01-01T08:00:00.000+08:00|k1v1|       6|
    // |1970-01-01T08:00:00.000+08:00|NULL|       4|
    // |1970-01-01T08:00:00.005+08:00|k1v2|       2|
    // |1970-01-01T08:00:00.005+08:00|k1v1|       3|
    // |1970-01-01T08:00:00.005+08:00|NULL|       2|
    // |1970-01-01T08:00:00.010+08:00|k1v2|       2|
    // |1970-01-01T08:00:00.010+08:00|k1v1|       3|
    // |1970-01-01T08:00:00.010+08:00|NULL|       2|
    // |1970-01-01T08:00:00.015+08:00|k1v2|       0|
    // |1970-01-01T08:00:00.015+08:00|k1v1|       0|
    // |1970-01-01T08:00:00.015+08:00|NULL|       0|
    // +-----------------------------+----+--------+
    long[][] expectedValue = new long[][] {{4L, 6L, 4L}, {2L, 3L, 2L}, {2L, 3L, 2L}, {0L, 0L, 0L}};
    long[] expectedTime = new long[] {0L, 5L, 10L, 15L};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        for (int i = 0; i < 4; i++) {
          for (int j = 0; j < 3; j++) {
            Assert.assertTrue(resultSet.next());
            String tagValue = resultSet.getString("k1");
            switch (tagValue) {
              case "k1v2":
                Assert.assertEquals(expectedTime[i], resultSet.getLong("Time"));
                Assert.assertEquals(expectedValue[i][0], resultSet.getLong("count(t)"));
                break;
              case "k1v1":
                Assert.assertEquals(expectedValue[i][1], resultSet.getLong("count(t)"));
                break;
              case "NULL":
                Assert.assertEquals(expectedValue[i][2], resultSet.getLong("count(t)"));
                break;
              default:
                fail("Unexpected tag value: " + tagValue);
            }
          }
        }
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlongWithTimeAggregationAndOrdering() {
    String query =
        "SELECT COUNT(t) from root.sg.** GROUP BY ([0, 20), 10ms), TAGS(k1) ORDER BY TIME DESC";
    // Expected result set:
    // +-----------------------------+----+--------+
    // |                         Time|  k1|count(t)|
    // +-----------------------------+----+--------+
    // |1970-01-01T08:00:00.010+08:00|k1v2|       2|
    // |1970-01-01T08:00:00.010+08:00|k1v1|       3|
    // |1970-01-01T08:00:00.010+08:00|NULL|       2|
    // |1970-01-01T08:00:00.000+08:00|k1v2|       2|
    // |1970-01-01T08:00:00.000+08:00|k1v1|       3|
    // |1970-01-01T08:00:00.000+08:00|NULL|       2|
    // +-----------------------------+----+--------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        Set<String> groups = new HashSet<>();
        for (int i = 0; i < 6; i++) {
          Assert.assertTrue(resultSet.next());
          if (i < 3) {
            Assert.assertEquals(10L, resultSet.getLong("Time"));
          } else {
            Assert.assertEquals(0L, resultSet.getLong("Time"));
          }
          String tagValue = resultSet.getString("k1");
          switch (tagValue) {
            case "k1v1":
              Assert.assertEquals(3L, resultSet.getLong("count(t)"));
              break;
            case "k1v2":
              Assert.assertEquals(2L, resultSet.getLong("count(t)"));
              break;
            case "NULL":
              Assert.assertEquals(2L, resultSet.getLong("count(t)"));
              break;
            default:
              fail("Unexpected tag value: " + tagValue);
          }
          groups.add(tagValue);
        }
        Assert.assertEquals(3, groups.size());
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlongWithTimeFiltering() {
    String query = "SELECT COUNT(t) FROM root.sg.** WHERE time > 1 GROUP BY TAGS(k1)";
    // Expected result set:
    // +----+--------+
    // |  k1|count(t)|
    // +----+--------+
    // |k1v2|       2|
    // |k1v1|       3|
    // |NULL|       2|
    // +----+--------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(2, resultSet.getMetaData().getColumnCount());
        Set<String> groups = new HashSet<>();
        for (int i = 0; i < 3; i++) {
          Assert.assertTrue(resultSet.next());
          String tagValue = resultSet.getString("k1");
          switch (tagValue) {
            case "k1v1":
              Assert.assertEquals(3L, resultSet.getLong("count(t)"));
              break;
            case "k1v2":
              Assert.assertEquals(2L, resultSet.getLong("count(t)"));
              break;
            case "NULL":
              Assert.assertEquals(2L, resultSet.getLong("count(t)"));
              break;
            default:
              fail("Unexpected tag value: " + tagValue);
          }
          groups.add(tagValue);
        }
        Assert.assertEquals(3, groups.size());
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testIncompatibleMixedDataTypes() {
    String query = "SELECT AVG(t) FROM root.** GROUP BY TAGS(k3)";
    // AVG() with numeric and text timeseries, an exception will be thrown
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet ignored = statement.executeQuery(query)) {
        Assert.fail();
      }
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("only support numeric data types"));
    }
  }

  @Test
  public void testWithValueFilters() {
    String query = "SELECT AVG(t) FROM root.sg.** WHERE t > 1.5 GROUP BY TAGS(k1)";
    // Value filter is not supported yet
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet ignored = statement.executeQuery(query)) {
        Assert.fail();
      }
    } catch (SQLException e) {
      Assert.assertTrue(
          e.getMessage().contains("Only time filters are supported in GROUP BY TAGS query"));
    }
  }

  @Test
  public void testWithHaving() {
    String query =
        "SELECT COUNT(t) from root.sg.** GROUP BY ([0, 20), 10ms), TAGS(k1) HAVING COUNT(t) > 3";
    // Having is not supported yet
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet ignored = statement.executeQuery(query)) {
        Assert.fail();
      }
    } catch (SQLException e) {
      Assert.assertTrue(
          e.getMessage().contains("Having clause is not supported yet in GROUP BY TAGS query"));
    }
  }

  @Test
  public void testWithEmptyGroupedTimeSeries() {
    String query = "SELECT avg(s1), avg(s2) FROM root.case2.** GROUP BY TAGS(k1)";
    // Expected result set:
    // +--+-----------------+-----------------+
    // |k1|          avg(s1)|          avg(s2)|
    // +--+-----------------+-----------------+
    // |v1|             8.25|             null|
    // |v2|9.899999618530273|6.599999904632568|
    // +--+-----------------+-----------------+
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        Set<String> groups = new HashSet<>();
        for (int i = 0; i < 2; i++) {
          Assert.assertTrue(resultSet.next());
          String tagValue = resultSet.getString("k1");
          switch (tagValue) {
            case "v1":
              Assert.assertEquals(8.25D, resultSet.getDouble("avg(s1)"), E);
              Assert.assertEquals(0.0D, resultSet.getDouble("avg(s2)"), E);
              break;
            case "v2":
              Assert.assertEquals(9.899999618530273D, resultSet.getDouble("avg(s1)"), E);
              Assert.assertEquals(6.599999904632568D, resultSet.getDouble("avg(s2)"), E);
              break;
            default:
              fail("Unexpected tag value: " + tagValue);
          }
          groups.add(tagValue);
        }
        Assert.assertEquals(2, groups.size());
        Assert.assertFalse(resultSet.next());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
