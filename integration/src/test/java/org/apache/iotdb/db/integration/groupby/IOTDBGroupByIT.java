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

package org.apache.iotdb.db.integration.groupby;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.logical.crud.AggregationQueryOperator;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.constant.TestConstant.maxTime;
import static org.apache.iotdb.db.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.constant.TestConstant.minTime;
import static org.apache.iotdb.db.constant.TestConstant.minValue;
import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IOTDBGroupByIT {

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(10, 10.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(20, 20.2, true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(30, 30.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(40, 40.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(50, 50.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(100, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(150, 200.2, true, 220)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(200, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(250, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(300, 500.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(500, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(510, 200.2, true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(520, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(530, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(540, 500.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(580, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(590, 200.2, true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(600, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(610, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(620, 500.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1500, 23.3, false, 666)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1550, -23.3, true, 888)",
        "flush",
      };

  private static final String TIMESTAMP_STR = "Time";
  private long prevPartitionInterval;

  @Before
  public void setUp() throws Exception {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initBeforeTest();
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
  }

  @Test
  public void countSumAvgTest() {
    System.out.println("countSumAvgTest");
    String[] retArray1 =
        new String[] {
          "5,3,35.8,11.933333333333332",
          "25,2,70.7,35.35",
          "45,1,50.5,50.5",
          "65,0,null,null",
          "85,1,100.1,100.1",
          "105,0,null,null",
          "125,0,null,null",
          "145,1,200.2,200.2"
        };
    String[] retArray2 =
        new String[] {
          "50,1,50.5,50.5",
          "60,0,null,null",
          "70,0,null,null",
          "80,0,null,null",
          "90,0,null,null",
          "100,1,100.1,100.1",
          "110,0,null,null",
          "120,0,null,null",
          "130,0,null,null",
          "140,0,null,null",
          "150,1,200.2,200.2"
        };
    String[] retArray3 =
        new String[] {
          "25,2,70.7,35.35",
          "45,1,50.5,50.5",
          "65,0,null,null",
          "85,1,100.1,100.1",
          "105,0,null,null",
          "125,0,null,null",
          "145,1,200.2,200.2",
          "165,0,null,null",
          "185,1,300.3,300.3",
          "205,0,null,null",
          "225,0,null,null",
          "245,1,400.4,400.4",
          "265,0,null,null",
          "285,1,500.5,500.5",
          "305,0,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where time > 3 "
                  + "GROUP BY ([5, 160), 20ms)");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([50, 160), 10ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([25, 314), 20ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      // order by time desc
      hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where time > 3 "
                  + "GROUP BY ([5, 160), 20ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[retArray1.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([50, 160), 10ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[retArray2.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([25, 314), 20ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray3[retArray3.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void maxMinValueTimeTest() {
    String[] retArray1 =
        new String[] {
          "2,null,null,null,null",
          "4,5.5,4.4,5,4",
          "6,null,null,null,null",
          "8,null,null,null,null",
          "10,10.1,10.1,10,10",
          "12,null,null,null,null",
          "14,null,null,null,null",
          "16,null,null,null,null",
          "18,null,null,null,null",
          "20,20.2,20.2,20,20",
          "22,null,null,null,null",
          "24,null,null,null,null",
          "26,null,null,null,null",
          "28,null,null,null,null"
        };
    String[] retArray2 =
        new String[] {
          "2,20.2,3.3,20,3",
          "22,40.4,30.3,40,30",
          "42,50.5,50.5,50,50",
          "62,null,null,null,null",
          "82,100.1,100.1,100,100",
          "102,null,null,null,null",
          "122,null,null,null,null",
          "142,200.2,200.2,150,150",
          "162,null,null,null,null",
          "182,300.3,300.3,200,200",
          "202,null,null,null,null",
          "222,null,null,null,null",
          "242,400.4,400.4,250,250",
          "262,null,null,null,null",
          "282,null,null,null,null",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select max_value(temperature), min_value(temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where time > 3 "
                  + "GROUP BY ([2,30), 2ms)");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select max_value(temperature), min_value(temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([2,300), 20ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(retArray2.length, cnt);

      // order by time desc
      hasResultSet =
          statement.execute(
              "select max_value(temperature), min_value(temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where time > 3 "
                  + "GROUP BY ([2,30), 2ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[retArray1.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select max_value(temperature), min_value(temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([2,300), 20ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[retArray2.length - cnt - 1], ans);
          cnt++;
        }
      }
      Assert.assertEquals(retArray2.length, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void firstLastTest() {
    System.out.println("firstLastTest");
    String[] retArray1 =
        new String[] {
          "2,5.5,4.4",
          "6,null,null",
          "10,10.1,10.1",
          "14,null,null",
          "18,20.2,20.2",
          "22,null,null",
          "26,null,null"
        };
    String[] retArray2 =
        new String[] {
          "2,20.2,3.3",
          "22,40.4,30.3",
          "42,50.5,50.5",
          "62,null,null",
          "82,100.1,100.1",
          "102,null,null",
          "122,null,null",
          "142,200.2,200.2",
          "162,null,null",
          "182,300.3,300.3",
          "202,null,null",
          "222,null,null",
          "242,400.4,400.4",
          "262,null,null",
          "282,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature), first_value(temperature) from root.ln.wf01.wt01 where time > 3 "
                  + "GROUP BY ([2,30), 4ms)");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(firstValue("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select first_value(temperature), last_value(temperature) from root.ln.wf01.wt01 "
                  + "where temperature > 3 "
                  + "GROUP BY ([2,300), 20ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(firstValue("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      // order by time desc
      hasResultSet =
          statement.execute(
              "select last_value(temperature), first_value(temperature) from root.ln.wf01.wt01 where time > 3 "
                  + "GROUP BY ([2,30), 4ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(firstValue("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[retArray1.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select first_value(temperature), last_value(temperature) from root.ln.wf01.wt01 "
                  + "where temperature > 3 "
                  + "GROUP BY ([2,300), 20ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(firstValue("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[retArray2.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void largeIntervalTest() {
    System.out.println("largeIntervalTest");
    String[] retArray1 = new String[] {"0,4.4,12,300,4", "340,100.1,10,620,500"};
    String[] retArray2 = new String[] {"0,3.3,13,300,3", "340,100.1,10,620,500"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_value(temperature), count(temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where time > 3 GROUP BY "
                  + "([0, 680), 340ms)");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select min_value(temperature), count (temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where temperature > 3 GROUP BY "
                  + "([0, 680), 340ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      // order by time desc
      hasResultSet =
          statement.execute(
              "select min_value(temperature), count(temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where time > 3 GROUP BY "
                  + "([0, 680), 340ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[retArray1.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select min_value(temperature), count(temperature), max_time(temperature), "
                  + "min_time(temperature) from root.ln.wf01.wt01 where temperature > 3 GROUP BY "
                  + "([0, 680), 340ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(minValue("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(maxTime("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(minTime("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray2[retArray2.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countSumAvgInnerIntervalTest() {
    System.out.println("countSumAvgInnerIntervalTest");
    String[] retArray1 =
        new String[] {
          "0,2,7.7,3.85",
          "30,1,30.3,30.3",
          "60,0,null,null",
          "90,0,null,null",
          "120,0,null,null",
          "150,1,200.2,200.2",
          "180,0,null,null",
          "210,0,null,null",
          "240,0,null,null",
          "270,0,null,null",
          "300,1,500.5,500.5",
          "330,0,null,null",
          "360,0,null,null",
          "390,0,null,null",
          "420,0,null,null",
          "450,0,null,null",
          "480,0,null,null",
          "510,1,200.2,200.2",
          "540,1,500.5,500.5",
          "570,0,null,null"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([0, 600), 5ms, 30ms)");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([0, 600), 5ms, 30ms) order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[retArray1.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countSumAvgNoDataTest() {
    System.out.println("countSumAvgNoDataTest");
    String[] retArray1 =
        new String[] {
          "10000,0,null,null",
          "10005,0,null,null",
          "10010,0,null,null",
          "10015,0,null,null",
          "10020,0,null,null",
          "10025,0,null,null",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([10000, 10030), 5ms)");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString("Time")
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void usingLimit() {
    String[] retArray1 =
        new String[] {
          "90,0,null,null",
          "120,0,null,null",
          "150,1,200.2,200.2",
          "180,0,null,null",
          "210,0,null,null"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 where temperature > 3 "
                  + "GROUP BY ([0, 600), 5ms, 30ms) "
                  + "limit 5 offset 3");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void usingNowFunction() {
    System.out.println("usingNowFunction");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
              + "values(now(), 35.5, false, 650)");
      ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([now() - 1h, now() + 1h), 2h)");
      Assert.assertTrue(resultSet.next());
      // resultSet.getLong(1) is the timestamp
      Assert.assertEquals(1, Integer.valueOf(resultSet.getString(2)).intValue());
      Assert.assertEquals(35.5, Float.valueOf(resultSet.getString(3)).floatValue(), 0.01);
      Assert.assertEquals(35.5, Double.valueOf(resultSet.getString(4)).doubleValue(), 0.01);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countSumAvgZeroTest() {
    String ret = "1500,2,0.0,0.0";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([1500, 1600), 100ms) ");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(avg("root.ln.wf01.wt01.temperature"));
          Assert.assertEquals(ret, ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test group by without aggregation function used in select clause. The expected situation is
   * throwing an exception.
   */
  @Test
  public void TestGroupByWithoutAggregationFunc() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("select temperature from root.ln.wf01.wt01 group by ([0, 100), 5ms)");

      fail("No expected exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(AggregationQueryOperator.ERROR_MESSAGE1));
    }
  }

  private void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
