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
package org.apache.iotdb.db.it.aggregation;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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

import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.avg;
import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.apache.iotdb.itbase.constant.TestConstant.lastValue;
import static org.apache.iotdb.itbase.constant.TestConstant.maxTime;
import static org.apache.iotdb.itbase.constant.TestConstant.maxValue;
import static org.apache.iotdb.itbase.constant.TestConstant.minTime;
import static org.apache.iotdb.itbase.constant.TestConstant.sum;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationByLevelIT {

  protected static final String[] dataSet =
      new String[] {
        "CREATE DATABASE root.sg1",
        "CREATE DATABASE root.sg2",
        "CREATE TIMESERIES root.sg1.d1.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d1.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d2.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d2.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg2.d1.status WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg2.d1.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg2.d2.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(timestamp,status) values(150,true)",
        "INSERT INTO root.sg1.d1(timestamp,status,temperature) values(200,false,20.71)",
        "INSERT INTO root.sg1.d1(timestamp,status,temperature) values(600,false,71.12)",
        "INSERT INTO root.sg1.d2(timestamp,status,temperature) values(200,false,42.66)",
        "INSERT INTO root.sg1.d2(timestamp,status,temperature) values(300,false,46.77)",
        "INSERT INTO root.sg1.d2(timestamp,status,temperature) values(700,true,62.15)",
        "INSERT INTO root.sg2.d1(timestamp,status,temperature) values(100,3,88.24)",
        "INSERT INTO root.sg2.d1(timestamp,status,temperature) values(500,5,125.5)",
        "INSERT INTO root.sg2.d2(timestamp,temperature) values(200,105.5)",
        "INSERT INTO root.sg2.d2(timestamp,temperature) values(800,61.22)",
      };

  private static final double DOUBLE_PRECISION = 0.001d;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void sumFuncGroupByLevelTest() throws Exception {
    double[] retArray = new double[] {243.410d, 380.460d, 623.870d, 91.83d, 151.58d};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Here we duplicate the column to test the bug
      // https://issues.apache.org/jira/browse/IOTDB-2088

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select sum(temperature), sum(temperature) from root.sg1.* GROUP BY level=1")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(sum("root.sg1.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select sum(temperature) from root.sg2.* GROUP BY level=1")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(sum("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select sum(temperature) from root.*.* GROUP BY level=0")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(sum("root.*.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select sum(temperature) from root.sg1.* GROUP BY level=1,2")) {
        while (resultSet.next()) {
          String ans1 = resultSet.getString(sum("root.sg1.d1.temperature"));
          String ans2 = resultSet.getString(sum("root.sg1.d2.temperature"));
          Assert.assertEquals(retArray[cnt++], Double.parseDouble(ans1), DOUBLE_PRECISION);
          Assert.assertEquals(retArray[cnt++], Double.parseDouble(ans2), DOUBLE_PRECISION);
        }
      }
      Assert.assertEquals(retArray.length, cnt);
    }
  }

  @Test
  public void avgFuncGroupByLevelTest() throws Exception {
    double[] retArray = new double[] {48.682d, 95.115d, 69.319d, 45.915d, 50.527d};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("select avg(temperature) from root.sg1.* GROUP BY level=1")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(avg("root.sg1.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select avg(temperature) from root.sg2.* GROUP BY level=1")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(avg("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select avg(temperature) from root.*.* GROUP BY level=0")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(avg("root.*.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select avg(temperature) from root.sg1.* GROUP BY level=1, 2")) {
        while (resultSet.next()) {
          String ans1 = resultSet.getString(avg("root.sg1.d1.temperature"));
          String ans2 = resultSet.getString(avg("root.sg1.d2.temperature"));
          Assert.assertEquals(retArray[cnt++], Double.parseDouble(ans1), DOUBLE_PRECISION);
          Assert.assertEquals(retArray[cnt++], Double.parseDouble(ans2), DOUBLE_PRECISION);
        }
      }
      Assert.assertEquals(retArray.length, cnt);
    }
  }

  @Test
  public void timeFuncGroupByLevelTest() throws Exception {
    String[] retArray = new String[] {"5,3,100,200", "600,700,2,3", "600,700,500"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(status), min_time(temperature) from root.*.* GROUP BY level=2")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(count("root.*.d1.status"))
                  + ","
                  + resultSet.getString(count("root.*.d2.status"))
                  + ","
                  + resultSet.getString(minTime("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.*.d2.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_time(status), count(temperature) from root.sg1.* GROUP BY level=2")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(maxTime("root.*.d1.status"))
                  + ","
                  + resultSet.getString(maxTime("root.*.d2.status"))
                  + ","
                  + resultSet.getString(count("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(count("root.*.d2.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select max_time(status) from root.*.* GROUP BY level=1, 2")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(maxTime("root.sg1.d1.status"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d2.status"))
                  + ","
                  + resultSet.getString(maxTime("root.sg2.d1.status"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(retArray.length, cnt);
    }
  }

  @Test
  public void valueFuncGroupByLevelTest() throws Exception {
    String[] retArray =
        new String[] {
          "61.22,125.5", "71.12,62.15,71.12,62.15",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(temperature), max_value(temperature) from root.*.* GROUP BY level=0")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(lastValue("root.*.*.temperature"))
                  + ","
                  + resultSet.getString(maxValue("root.*.*.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(temperature), max_value(temperature) from root.sg1.* GROUP BY level=2")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(lastValue("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(lastValue("root.*.d2.temperature"))
                  + ","
                  + resultSet.getString(maxValue("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(maxValue("root.*.d2.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(retArray.length, cnt);
    }
  }

  @Test
  public void countStarGroupByLevelTest() throws Exception {
    String[] retArray = new String[] {"17", "17", "8"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("select count(*) from root.*.* GROUP BY level=0")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(count("root.*.*.*"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select count(**) from root GROUP BY level=0")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(count("root.*.*.*"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery("select count(status) from root.*.* GROUP BY level=0")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(count("root.*.*.status"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
    }
  }

  /**
   * [root.sg.d1.temperature, root.sg.d2.temperature] with level = 1
   *
   * <p>Result is [root.sg.*.temperature]
   */
  @Test
  public void groupByLevelWithAliasTest() throws Exception {
    String[] retArray = new String[] {"5", "5", "5"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature) as ct from root.sg1.d1, root.sg1.d2 GROUP BY level=1")) {
        while (resultSet.next()) {
          String ans = resultSet.getString("ct");
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature) as ct from root.sg1.* GROUP BY level=1")) {
        while (resultSet.next()) {
          String ans = resultSet.getString("ct");
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      // root.sg1.d1.* -> [root.sg1.d1.status, root.sg1.d1.temperature] -> root.*.*.* -> ct
      try (ResultSet resultSet =
          statement.executeQuery("select count(*) as ct from root.sg1.d1 GROUP BY level=0")) {
        while (resultSet.next()) {
          String ans = resultSet.getString("ct");
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
    }
  }

  /**
   * [root.sg.d1.temperature, root.sg.d2.temperature] with level = 2
   *
   * <p>Result is [root.*.d1.temperature, root.*.d2.temperature]
   */
  @Test
  public void groupByLevelWithAliasFailTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select count(temperature) as ct from root.sg1.* GROUP BY level=2");
      fail("No exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  // Different from above at: root.sg1.*.temperature is just one ResultColumn
  @Test
  public void groupByLevelWithAliasFailTest2() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "select count(temperature) as ct from root.sg1.d1, root.sg2.d2 GROUP BY level=2");
      fail("No exception thrown");
    } catch (Exception e) {
      System.out.println(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  /** One Result Column with more than one alias. */
  @Test
  public void groupByLevelWithAliasFailTest3() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "select count(temperature) as ct, count(temperature) as ct2 from root.sg1.d1 GROUP BY level=2");
      fail("No exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("more than one alias"));
    }
  }

  @Test
  public void groupByLevelWithAliasWithTimeIntervalTest() throws Exception {
    String[] retArray = new String[] {"0,0", "100,0", "200,2", "300,1", "400,0", "500,0"};
    String[] retArray2 = new String[] {"0,0", "100,1", "200,2", "300,0", "400,0", "500,0"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature) as ct from root.sg1.d1, root.sg1.d2 GROUP BY ([0, 600), 100ms), level=1")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME) + "," + resultSet.getString("ct");
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature) as ct from root.sg1.* GROUP BY ([0, 600), 100ms), level=1")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME) + "," + resultSet.getString("ct");
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      cnt = 0;
      // root.sg1.d1.* -> [root.sg1.d1.status, root.sg1.d1.temperature] -> root.*.*.* -> ct
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) as ct from root.sg1.d1 GROUP BY ([0, 600), 100ms), level=1")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME) + "," + resultSet.getString("ct");
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
      }
    }
  }

  /**
   * [root.sg.d1.temperature, root.sg.d2.temperature] with level = 2
   *
   * <p>Result is [root.*.d1.temperature, root.*.d2.temperature]
   */
  @Test
  public void groupByLevelWithAliasWithTimeIntervalFailTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "select count(temperature) as ct from root.sg1.* GROUP BY ([0, 600), 100ms), level=2");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  // Different from above at: root.sg1.*.temperature is just one ResultColumn
  @Test
  public void groupByLevelWithAliasWithTimeIntervalFailTest2() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "select count(temperature) as ct from root.sg1.d1, root.sg1.d2 GROUP BY ([0, 600), 100ms), level=2");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  @Test
  public void GroupByLevelSLimitTest() throws Exception {
    String[] retArray = new String[] {"5,4", "4,6", "3"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature), count(status) from root.*.* GROUP BY level=1 slimit 2")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(count("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(count("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature), count(status) from root.*.* GROUP BY level=1 slimit 2 soffset 1")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(count("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(count("root.sg1.*.status"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature), count(status) from root.*.* GROUP BY level=1,2 slimit 1 soffset 4")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(count("root.sg1.d1.status"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Test
  public void groupByLevelWithTimeIntervalTest() throws Exception {
    String[] retArray1 =
        new String[] {
          "null", "88.24", "105.5", "null", "null", "125.5",
        };
    String[] retArray2 =
        new String[] {
          "null,null,null,null",
          "null,100,null,88.24",
          "200,200,31.685,105.5",
          "300,null,46.77,null",
          "null,null,null,null",
          "null,500,null,125.5",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select sum(temperature) from root.sg2.* GROUP BY ([0, 600), 100ms), level=1")) {
        while (resultSet.next()) {
          String ans = "" + resultSet.getString(sum("root.sg2.*.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_time(temperature), avg(temperature) from root.*.* GROUP BY ([0, 600), 100ms), level=1")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(maxTime("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(avg("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(avg("root.sg2.*.temperature"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Test
  public void groupByMultiLevelWithTimeIntervalTest() throws Exception {
    String[] retArray1 =
        new String[] {
          "null", "88.24", "105.5", "null", "null", "125.5",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select sum(temperature) from root.sg2.* GROUP BY ([0, 600), 100ms), level=0,1")) {
        while (resultSet.next()) {
          String ans = "" + resultSet.getString(sum("root.sg2.*.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(6, cnt);
    }
  }

  @Test
  public void groupByMultiLevelWithTimeIntervalSLimitTest() throws Exception {
    String[] retArray =
        new String[] {"0,0,0", "100,0,1", "200,2,1", "300,1,0", "400,0,0", "500,0,1"};
    String[] retArray2 =
        new String[] {"0,0,0", "100,1,1", "200,1,2", "300,0,1", "400,0,0", "500,1,0"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature) from root.*.* GROUP BY ([0, 600), 100ms), level=1 slimit 2")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(count("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature), count(status) from root.*.* GROUP BY ([0, 600), 100ms), level=1 slimit 2 soffset 1")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(count("root.sg1.*.status"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Ignore
  @Test
  public void mismatchedFuncGroupByLevelTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select last_value(status) from root.*.* GROUP BY level=0");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("the data types of the same output column"));
    }
  }

  /**
   * Test group by level without aggregation function used in select clause. The expected situation
   * is throwing an exception.
   */
  @Test
  public void TestGroupByLevelWithoutAggregationFunc() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.executeQuery("select temperature from root.sg1.* group by level = 2");
      fail("No expected exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Common queries and aggregated queries are not allowed to appear at the same time"));
    }
  }

  @Test
  public void groupByLevelWithSameColumn() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(status),count(status) from root.** GROUP BY level=0")) {

        ResultSetMetaData metaData = resultSet.getMetaData();
        Assert.assertEquals(metaData.getColumnName(1), metaData.getColumnName(2));
        Assert.assertEquals(count("root.*.*.status"), metaData.getColumnName(1));
        Assert.assertEquals(2, metaData.getColumnCount());
      }
    }
  }

  protected static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
