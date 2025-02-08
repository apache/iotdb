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

package org.apache.iotdb.db.it.udaf;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.*;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFGroupByLevelIT {
  private static final double DELTA = 0.001d;

  private static final String[] dataset =
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

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(dataset);
    registerUDAF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void registerUDAF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION avg_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFAvg'");
      statement.execute(
          "CREATE FUNCTION count_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.execute(
          "CREATE FUNCTION sum_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFSum'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void UDAFGroupByLevelTest() throws Exception {
    double[] expected = new double[] {48.682d, 95.115d, 69.319d, 45.915d, 50.527d};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT avg_udaf(temperature) " + "FROM root.sg1.* " + "GROUP BY LEVEL=1")) {
        while (resultSet.next()) {
          String actual = resultSet.getString(avgUDAF("root.sg1.*.temperature"));
          Assert.assertEquals(expected[cnt], Double.parseDouble(actual), DELTA);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT avg_udaf(temperature) " + "FROM root.sg2.* " + "GROUP BY LEVEL=1")) {
        while (resultSet.next()) {
          String actual = resultSet.getString(avgUDAF("root.sg2.*.temperature"));
          Assert.assertEquals(expected[cnt], Double.parseDouble(actual), DELTA);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT avg_udaf(temperature) " + "FROM root.*.* " + "GROUP BY LEVEL=0")) {
        while (resultSet.next()) {
          String actual = resultSet.getString(avgUDAF("root.*.*.temperature"));
          Assert.assertEquals(expected[cnt], Double.parseDouble(actual), DELTA);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT avg_udaf(temperature) " + "FROM root.sg1.* " + "GROUP BY LEVEL=1, 2")) {
        while (resultSet.next()) {
          String actual1 = resultSet.getString(avgUDAF("root.sg1.d1.temperature"));
          String actual2 = resultSet.getString(avgUDAF("root.sg1.d2.temperature"));
          Assert.assertEquals(expected[cnt++], Double.parseDouble(actual1), DELTA);
          Assert.assertEquals(expected[cnt++], Double.parseDouble(actual2), DELTA);
        }
      }
      Assert.assertEquals(expected.length, cnt);
    }
  }

  /**
   * [root.sg.d1.temperature, root.sg.d2.temperature] with level = 1
   *
   * <p>Result is [root.sg.*.temperature]
   */
  @Test
  public void UDAFGroupByLevelWithAliasTest() throws Exception {
    String[] expected = new String[] {"5", "5", "5"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature) AS ct "
                  + "FROM root.sg1.d1, root.sg1.d2 "
                  + "GROUP BY LEVEL=1")) {
        while (resultSet.next()) {
          String actual = resultSet.getString("ct");
          Assert.assertEquals(expected[cnt], actual);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature) AS ct " + "FROM root.sg1.* " + "GROUP BY LEVEL=1")) {
        while (resultSet.next()) {
          String actual = resultSet.getString("ct");
          Assert.assertEquals(expected[cnt], actual);
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
  public void UDAFGroupByLevelWithAliasFailTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "SELECT count_udaf(temperature) AS ct " + "FROM root.sg1.* " + "GROUP BY LEVEL=2");
      fail("No exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  // Different from above at: root.sg1.*.temperature is just one ResultColumn
  @Test
  public void UDAFGroupByLevelWithAliasFailTest2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "SELECT count_udaf(temperature) AS ct "
              + "FROM root.sg1.d1, root.sg2.d2 "
              + "GROUP BY LEVEL=2");
      fail("No exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  /** One Result Column with more than one alias. */
  @Test
  public void UDAFGroupByLevelWithAliasFailTest3() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "SELECT count_udaf(temperature) AS ct, count_udaf(temperature) AS ct2 "
              + "FROM root.sg1.d1 "
              + "GROUP BY LEVEL=2");
      fail("No exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("more than one alias"));
    }
  }

  @Test
  public void UDAFGroupByLevelWithAliasWithTimeIntervalTest() throws Exception {
    String[] expected = new String[] {"0,0", "100,0", "200,2", "300,1", "400,0", "500,0"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature) AS ct "
                  + "FROM root.sg1.d1, root.sg1.d2 "
                  + "GROUP BY ([0, 600), 100ms), LEVEL=1")) {
        while (resultSet.next()) {
          String actual =
              resultSet.getString(ColumnHeaderConstant.TIME) + "," + resultSet.getString("ct");
          Assert.assertEquals(expected[cnt], actual);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature) AS ct FROM root.sg1.* "
                  + "GROUP BY ([0, 600), 100ms), LEVEL=1")) {
        while (resultSet.next()) {
          String actual =
              resultSet.getString(ColumnHeaderConstant.TIME) + "," + resultSet.getString("ct");
          Assert.assertEquals(expected[cnt], actual);
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
  public void UDAFGroupByLevelWithAliasWithTimeIntervalFailTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "SELECT count_udaf(temperature) AS ct "
              + "FROM root.sg1.* "
              + "GROUP BY ([0, 600), 100ms), LEVEL=2");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  // Different from above at: root.sg1.*.temperature is just one ResultColumn
  @Test
  public void UDAFGroupByLevelWithAliasWithTimeIntervalFailTest2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery(
          "SELECT count_udaf(temperature) AS ct "
              + "FROM root.sg1.d1, root.sg1.d2 "
              + "GROUP BY ([0, 600), 100ms), level=2");
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("can only be matched with one"));
    }
  }

  @Test
  public void UDAFGroupByLevelSLimitTest() throws Exception {
    String[] expected = new String[] {"5,4", "4,6", "3"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature), count_udaf(status) "
                  + "FROM root.*.* "
                  + "GROUP BY LEVEL=1 "
                  + "SLIMIT 2")) {
        while (resultSet.next()) {
          String actual =
              resultSet.getString(countUDAF("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(countUDAF("root.sg2.*.temperature"));
          Assert.assertEquals(expected[cnt], actual);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature), count_udaf(status) "
                  + "FROM root.*.* "
                  + "GROUP BY LEVEL=1 "
                  + "SLIMIT 2 SOFFSET 1")) {
        while (resultSet.next()) {
          String actual =
              resultSet.getString(countUDAF("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(countUDAF("root.sg1.*.status"));
          Assert.assertEquals(expected[cnt], actual);
          cnt++;
        }
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature), count_udaf(status) "
                  + "FROM root.*.* "
                  + "GROUP BY level=1,2 "
                  + "slimit 1 soffset 4")) {
        while (resultSet.next()) {
          String actual = resultSet.getString(countUDAF("root.sg1.d1.status"));
          Assert.assertEquals(expected[cnt], actual);
          cnt++;
        }
      }
    }
  }

  @Test
  public void UDAFGroupByLevelWithTimeIntervalTest() throws Exception {
    String[] expected1 =
        new String[] {
          "null", "88.24", "105.5", "null", "null", "125.5",
        };
    String[] expected2 =
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
              "SELECT sum_udaf(temperature) "
                  + "FROM root.sg2.* "
                  + "GROUP BY ([0, 600), 100ms), LEVEL=1")) {
        while (resultSet.next()) {
          String actual = "" + resultSet.getString(sumUDAF("root.sg2.*.temperature"));
          Assert.assertEquals(expected1[cnt], actual);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_time(temperature), avg_udaf(temperature) "
                  + "FROM root.*.* "
                  + "GROUP BY ([0, 600), 100ms), LEVEL=1")) {
        while (resultSet.next()) {
          String actual =
              resultSet.getString(maxTime("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(avgUDAF("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(avgUDAF("root.sg2.*.temperature"));
          Assert.assertEquals(expected2[cnt], actual);
          cnt++;
        }
      }
    }
  }

  @Test
  public void UDAFGroupByMultiLevelWithTimeIntervalTest() throws Exception {
    String[] expected =
        new String[] {
          "null", "88.24", "105.5", "null", "null", "125.5",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum_udaf(temperature) "
                  + "FROM root.sg2.* "
                  + "GROUP BY ([0, 600), 100ms), LEVEL=0,1")) {
        while (resultSet.next()) {
          String actual = "" + resultSet.getString(sumUDAF("root.sg2.*.temperature"));
          Assert.assertEquals(expected[cnt], actual);
          cnt++;
        }
      }
      Assert.assertEquals(6, cnt);
    }
  }

  @Test
  public void UDAFGroupByMultiLevelWithTimeIntervalSLimitTest() throws Exception {
    String[] expected =
        new String[] {"0,0,0", "100,0,1", "200,2,1", "300,1,0", "400,0,0", "500,0,1"};
    String[] expected2 =
        new String[] {"0,0,0", "100,1,1", "200,1,2", "300,0,1", "400,0,0", "500,1,0"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature) "
                  + "FROM root.*.* "
                  + "GROUP BY ([0, 600), 100ms), LEVEL=1 "
                  + "SLIMIT 2")) {
        while (resultSet.next()) {
          String actual =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(countUDAF("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(countUDAF("root.sg2.*.temperature"));
          Assert.assertEquals(expected[cnt], actual);
          cnt++;
        }
      }

      cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature), count_udaf(status) "
                  + "FROM root.*.* "
                  + "GROUP BY ([0, 600), 100ms), LEVEL=1 "
                  + "SLIMIT 2 SOFFSET 1")) {
        while (resultSet.next()) {
          String actual =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(countUDAF("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(countUDAF("root.sg1.*.status"));
          Assert.assertEquals(expected2[cnt], actual);
          cnt++;
        }
      }
    }
  }

  @Test
  public void groupByLevelWithSameColumn() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(status),count_udaf(status) "
                  + "FROM root.** "
                  + "GROUP BY LEVEL=0")) {

        ResultSetMetaData metaData = resultSet.getMetaData();
        Assert.assertEquals(metaData.getColumnName(1), metaData.getColumnName(2));
        Assert.assertEquals(countUDAF("root.*.*.status"), metaData.getColumnName(1));
        Assert.assertEquals(2, metaData.getColumnCount());
      }
    }
  }
}
