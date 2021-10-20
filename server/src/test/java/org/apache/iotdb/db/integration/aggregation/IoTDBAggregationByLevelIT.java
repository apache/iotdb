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
package org.apache.iotdb.db.integration.aggregation;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.logical.crud.AggregationQueryOperator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class IoTDBAggregationByLevelIT {

  private Planner planner = new Planner();
  private static final String[] dataSet =
      new String[] {
        "SET STORAGE GROUP TO root.sg1",
        "SET STORAGE GROUP TO root.sg2",
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

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void sumFuncGroupByLevelTest() throws Exception {
    double[] retArray = new double[] {243.410d, 380.460d, 623.870d, 91.83d, 151.58d};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select sum(temperature) from root.sg1.* GROUP BY level=1");

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.sum("root.sg1.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      statement.execute("select sum(temperature) from root.sg2.* GROUP BY level=1");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.sum("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      statement.execute("select sum(temperature) from root.*.* GROUP BY level=0");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.sum("root.*.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      statement.execute("select sum(temperature) from root.sg1.* GROUP BY level=1,2");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans1 = resultSet.getString(TestConstant.sum("root.sg1.d1.temperature"));
          String ans2 = resultSet.getString(TestConstant.sum("root.sg1.d2.temperature"));
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
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select avg(temperature) from root.sg1.* GROUP BY level=1");

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.avg("root.sg1.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      statement.execute("select avg(temperature) from root.sg2.* GROUP BY level=1");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.avg("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      statement.execute("select avg(temperature) from root.*.* GROUP BY level=0");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.avg("root.*.*.temperature"));
          Assert.assertEquals(retArray[cnt], Double.parseDouble(ans), DOUBLE_PRECISION);
          cnt++;
        }
      }

      statement.execute("select avg(temperature) from root.sg1.* GROUP BY level=1, 2");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans1 = resultSet.getString(TestConstant.avg("root.sg1.d1.temperature"));
          String ans2 = resultSet.getString(TestConstant.avg("root.sg1.d2.temperature"));
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
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select count(status), min_time(temperature) from root.*.* GROUP BY level=2");

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.count("root.*.d1.status"))
                  + ","
                  + resultSet.getString(TestConstant.count("root.*.d2.status"))
                  + ","
                  + resultSet.getString(TestConstant.min_time("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.min_time("root.*.d2.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "select max_time(status), count(temperature) from root.sg1.* GROUP BY level=2");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.max_time("root.*.d1.status"))
                  + ","
                  + resultSet.getString(TestConstant.max_time("root.*.d2.status"))
                  + ","
                  + resultSet.getString(TestConstant.count("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.count("root.*.d2.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute("select max_time(status) from root.*.* GROUP BY level=1, 2");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.max_time("root.sg1.d1.status"))
                  + ","
                  + resultSet.getString(TestConstant.max_time("root.sg1.d2.status"))
                  + ","
                  + resultSet.getString(TestConstant.max_time("root.sg2.d1.status"));
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
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select last_value(temperature), max_value(temperature) from root.*.* GROUP BY level=0");

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.last_value("root.*.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.max_value("root.*.*.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "select last_value(temperature), max_value(temperature) from root.sg1.* GROUP BY level=2");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.last_value("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.last_value("root.*.d2.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.max_value("root.*.d1.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.max_value("root.*.d2.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(retArray.length, cnt);
    }
  }

  @Test
  public void countStarGroupByLevelTest() throws Exception {
    String[] retArray = new String[] {"17", "8"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select count(*) from root.*.* GROUP BY level=0");

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.count("root.*.*.*"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute("select count(status) from root.*.* GROUP BY level=0");

      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.count("root.*.*.status"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Test
  public void GroupByLevelSLimitTest() throws Exception {
    String[] retArray = new String[] {"5,4", "4,6", "3"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select count(temperature), count(status) from root.*.* GROUP BY level=1 slimit 2");

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.count("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.count("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "select count(temperature), count(status) from root.*.* GROUP BY level=1 slimit 2 soffset 1");

      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.count("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.count("root.sg1.*.status"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "select count(temperature), count(status) from root.*.* GROUP BY level=1,2 slimit 1 soffset 4");

      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.count("root.sg1.d1.status"));
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
          "0.0", "88.24", "105.5", "0.0", "0.0", "125.5",
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
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "select sum(temperature) from root.sg2.* GROUP BY ([0, 600), 100ms), level=1");
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.sum("root.sg2.*.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
      }

      cnt = 0;
      statement.execute(
          "select max_time(temperature), avg(temperature) from root.*.* GROUP BY ([0, 600), 100ms), level=1");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.max_time("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.max_time("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.avg("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.avg("root.sg2.*.temperature"));
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
          "0.0", "88.24", "105.5", "0.0", "0.0", "125.5",
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "select sum(temperature) from root.sg2.* GROUP BY ([0, 600), 100ms), level=0,1");
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TestConstant.sum("root.sg2.*.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Test
  public void groupByMultiLevelWithTimeIntervalSLimitTest() throws Exception {
    String[] retArray =
        new String[] {"0,0,0", "100,0,1", "200,2,1", "300,1,0", "400,0,0", "500,0,1"};
    String[] retArray2 =
        new String[] {"0,0,0", "100,1,1", "200,1,2", "300,0,1", "400,0,0", "500,1,0"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "select count(temperature) from root.*.* GROUP BY ([0, 600), 100ms), level=1 slimit 2");
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TestConstant.count("root.sg1.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.count("root.sg2.*.temperature"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "select count(temperature), count(status) from root.*.* GROUP BY ([0, 600), 100ms), level=1 slimit 2 soffset 1");
      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TestConstant.count("root.sg2.*.temperature"))
                  + ","
                  + resultSet.getString(TestConstant.count("root.sg1.*.status"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Test
  public void mismatchedFuncGroupByLevelTest() throws Exception {
    String[] retArray =
        new String[] {
          "true", "3",
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select last_value(status) from root.*.* GROUP BY level=0");

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(1);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      try {
        planner.parseSQLToPhysicalPlan("select avg(status) from root.sg2.* GROUP BY level=1");
      } catch (Exception e) {
        Assert.assertEquals("Aggregate among unmatched data types", e.getMessage());
      }
    }
  }

  /**
   * Test group by level without aggregation function used in select clause. The expected situation
   * is throwing an exception.
   */
  @Test
  public void TestGroupByLevelWithoutAggregationFunc() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("select temperature from root.sg1.* group by level = 2");

      fail("No expected exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(AggregationQueryOperator.ERROR_MESSAGE1));
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
