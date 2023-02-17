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

package org.apache.iotdb.db.it.groupby;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IOTDBGroupByIT {

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.noDataRegion.s1 WITH DATATYPE=INT32",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(2, 2.2,  true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(5, 5.5, false, 55)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(10, 10.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(20, 20.2,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(30, 30.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(40, 40.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(50, 50.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(100, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(150, 200.2,  true, 220)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(200, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(250, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(300, 500.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(500, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(510, 200.2,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(520, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(530, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(540, 500.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(580, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(590, 200.2,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(600, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(610, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(620, 500.5, false, 550)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1500, 23.3, false, 666)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1550, -23.3, true, 888)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void countSumAvgTest1() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          count("root.ln.wf01.wt01.temperature"),
          sum("root.ln.wf01.wt01.temperature"),
          avg("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "5,3,35.8,11.933333333333332,",
          "25,2,70.7,35.35,",
          "45,1,50.5,50.5,",
          "65,0,null,null,",
          "85,1,100.1,100.1,",
          "105,0,null,null,",
          "125,0,null,null,",
          "145,1,200.2,200.2,"
        };
    resultSetEqualWithDescOrderTest(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([5, 160), 20ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void countSumAvgTest2() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          count("root.ln.wf01.wt01.temperature"),
          sum("root.ln.wf01.wt01.temperature"),
          avg("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "50,1,50.5,50.5,",
          "60,0,null,null,",
          "70,0,null,null,",
          "80,0,null,null,",
          "90,0,null,null,",
          "100,1,100.1,100.1,",
          "110,0,null,null,",
          "120,0,null,null,",
          "130,0,null,null,",
          "140,0,null,null,",
          "150,1,200.2,200.2,"
        };
    resultSetEqualWithDescOrderTest(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where temperature > 3 "
            + "GROUP BY ([50, 160), 10ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void countSumAvgTest3() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          count("root.ln.wf01.wt01.temperature"),
          sum("root.ln.wf01.wt01.temperature"),
          avg("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "25,2,70.7,35.35,",
          "45,1,50.5,50.5,",
          "65,0,null,null,",
          "85,1,100.1,100.1,",
          "105,0,null,null,",
          "125,0,null,null,",
          "145,1,200.2,200.2,",
          "165,0,null,null,",
          "185,1,300.3,300.3,",
          "205,0,null,null,",
          "225,0,null,null,",
          "245,1,400.4,400.4,",
          "265,0,null,null,",
          "285,1,500.5,500.5,",
          "305,0,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where temperature > 3 "
            + "GROUP BY ([25, 314), 20ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void maxMinValueTimeTest1() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          maxValue("root.ln.wf01.wt01.temperature"),
          minValue("root.ln.wf01.wt01.temperature"),
          maxTime("root.ln.wf01.wt01.temperature"),
          minTime("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "2,null,null,null,null,",
          "4,5.5,4.4,5,4,",
          "6,null,null,null,null,",
          "8,null,null,null,null,",
          "10,10.1,10.1,10,10,",
          "12,null,null,null,null,",
          "14,null,null,null,null,",
          "16,null,null,null,null,",
          "18,null,null,null,null,",
          "20,20.2,20.2,20,20,",
          "22,null,null,null,null,",
          "24,null,null,null,null,",
          "26,null,null,null,null,",
          "28,null,null,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select max_value(temperature), min_value(temperature), max_time(temperature), "
            + "min_time(temperature) from root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([2,30), 2ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void maxMinValueTimeTest2() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          maxValue("root.ln.wf01.wt01.temperature"),
          minValue("root.ln.wf01.wt01.temperature"),
          maxTime("root.ln.wf01.wt01.temperature"),
          minTime("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "2,20.2,3.3,20,3,",
          "22,40.4,30.3,40,30,",
          "42,50.5,50.5,50,50,",
          "62,null,null,null,null,",
          "82,100.1,100.1,100,100,",
          "102,null,null,null,null,",
          "122,null,null,null,null,",
          "142,200.2,200.2,150,150,",
          "162,null,null,null,null,",
          "182,300.3,300.3,200,200,",
          "202,null,null,null,null,",
          "222,null,null,null,null,",
          "242,400.4,400.4,250,250,",
          "262,null,null,null,null,",
          "282,null,null,null,null,",
        };
    resultSetEqualWithDescOrderTest(
        "select max_value(temperature), min_value(temperature), max_time(temperature), "
            + "min_time(temperature) from root.ln.wf01.wt01 where temperature > 3 "
            + "GROUP BY ([2,300), 20ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void firstLastTest1() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          lastValue("root.ln.wf01.wt01.temperature"),
          firstValue("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "2,5.5,4.4,",
          "6,null,null,",
          "10,10.1,10.1,",
          "14,null,null,",
          "18,20.2,20.2,",
          "22,null,null,",
          "26,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select last_value(temperature), first_value(temperature) from root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([2,30), 4ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void firstLastTest2() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          firstValue("root.ln.wf01.wt01.temperature"),
          lastValue("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "2,3.3,20.2,",
          "22,30.3,40.4,",
          "42,50.5,50.5,",
          "62,null,null,",
          "82,100.1,100.1,",
          "102,null,null,",
          "122,null,null,",
          "142,200.2,200.2,",
          "162,null,null,",
          "182,300.3,300.3,",
          "202,null,null,",
          "222,null,null,",
          "242,400.4,400.4,",
          "262,null,null,",
          "282,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select first_value(temperature), last_value(temperature) from root.ln.wf01.wt01 "
            + "where temperature > 3 "
            + "GROUP BY ([2,300), 20ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void largeIntervalTest1() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          minValue("root.ln.wf01.wt01.temperature"),
          count("root.ln.wf01.wt01.temperature"),
          maxTime("root.ln.wf01.wt01.temperature"),
          minTime("root.ln.wf01.wt01.temperature")
        };
    String[] retArray = new String[] {"0,4.4,12,300,4,", "340,100.1,10,620,500,"};
    resultSetEqualWithDescOrderTest(
        "select min_value(temperature), count(temperature), max_time(temperature), "
            + "min_time(temperature) from root.ln.wf01.wt01 where time > 3 GROUP BY "
            + "([0, 680), 340ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void largeIntervalTest2() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          minValue("root.ln.wf01.wt01.temperature"),
          count("root.ln.wf01.wt01.temperature"),
          maxTime("root.ln.wf01.wt01.temperature"),
          minTime("root.ln.wf01.wt01.temperature")
        };
    String[] retArray = new String[] {"0,3.3,13,300,3,", "340,100.1,10,620,500,"};
    resultSetEqualWithDescOrderTest(
        "select min_value(temperature), count (temperature), max_time(temperature), "
            + "min_time(temperature) from root.ln.wf01.wt01 where temperature > 3 GROUP BY "
            + "([0, 680), 340ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void countSumAvgInnerIntervalTest() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          count("root.ln.wf01.wt01.temperature"),
          sum("root.ln.wf01.wt01.temperature"),
          avg("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "0,2,7.7,3.85,",
          "30,1,30.3,30.3,",
          "60,0,null,null,",
          "90,0,null,null,",
          "120,0,null,null,",
          "150,1,200.2,200.2,",
          "180,0,null,null,",
          "210,0,null,null,",
          "240,0,null,null,",
          "270,0,null,null,",
          "300,1,500.5,500.5,",
          "330,0,null,null,",
          "360,0,null,null,",
          "390,0,null,null,",
          "420,0,null,null,",
          "450,0,null,null,",
          "480,0,null,null,",
          "510,1,200.2,200.2,",
          "540,1,500.5,500.5,",
          "570,0,null,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where temperature > 3 "
            + "GROUP BY ([0, 600), 5ms, 30ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void countSumAvgNoDataTest() throws SQLException {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          count("root.ln.wf01.wt01.temperature"),
          sum("root.ln.wf01.wt01.temperature"),
          avg("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "10000,0,null,null,",
          "10005,0,null,null,",
          "10010,0,null,null,",
          "10015,0,null,null,",
          "10020,0,null,null,",
          "10025,0,null,null,",
        };
    resultSetEqualWithDescOrderTest(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where temperature > 3 "
            + "GROUP BY ([10000, 10030), 5ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void useLimitTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          count("root.ln.wf01.wt01.temperature"),
          sum("root.ln.wf01.wt01.temperature"),
          avg("root.ln.wf01.wt01.temperature")
        };
    String[] retArray =
        new String[] {
          "90,0,null,null,",
          "120,0,null,null,",
          "150,1,200.2,200.2,",
          "180,0,null,null,",
          "210,0,null,null,"
        };
    resultSetEqualTest(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where temperature > 3 "
            + "GROUP BY ([0, 600), 5ms, 30ms) "
            + "limit 5 offset 3",
        expectedHeader,
        retArray);
  }

  @Test
  public void countSumAvgZeroTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          count("root.ln.wf01.wt01.temperature"),
          sum("root.ln.wf01.wt01.temperature"),
          avg("root.ln.wf01.wt01.temperature")
        };
    String[] retArray = new String[] {"1500,2,0.0,0.0,"};
    resultSetEqualTest(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 "
            + "GROUP BY ([1500, 1600), 100ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void useNowFunctionTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
              + "values(now(), 35.5, false, 650)");
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(temperature), sum(temperature), avg(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ([now() - 1h, now() + 1h), 2h)")) {
        Assert.assertTrue(resultSet.next());
        // resultSet.getLong(1) is the timestamp
        Assert.assertEquals(1, Integer.valueOf(resultSet.getString(2)).intValue());
        Assert.assertEquals(35.5, Float.valueOf(resultSet.getString(3)).floatValue(), 0.01);
        Assert.assertEquals(35.5, Double.valueOf(resultSet.getString(4)).doubleValue(), 0.01);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void zeroTimeIntervalFailTest() {
    assertTestFail(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([1, 30), 0ms)",
        "The second parameter time interval should be a positive integer");
  }

  @Test
  public void negativeTimeIntervalFailTest() {
    assertTestFail(
        "select count(temperature), sum(temperature), avg(temperature) from "
            + "root.ln.wf01.wt01 where time > 3 "
            + "GROUP BY ([1, 30), -1ms)",
        "no viable alternative at input");
  }

  @Test
  public void noDataRegionTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, count("root.test.noDataRegion.s1"), sum("root.test.noDataRegion.s1")
        };
    String[] retArray = new String[] {"1,0,null,", "2,0,null,"};
    resultSetEqualWithDescOrderTest(
        "select count(s1), sum(s1) from root.test.noDataRegion GROUP BY ([1, 3), 1ms)",
        expectedHeader,
        retArray);
  }
}
