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

import static org.apache.iotdb.db.it.utils.TestUtils.*;
import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFGroupByTimeIT {
  protected static final String[] SQLs =
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
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(10, 10.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(20, 20.2,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(30, 30.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(40, 40.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(50, 50.5, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(100, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(150, 200.2,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(200, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(250, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(300, 500.5, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(500, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(510, 200.2,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(520, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(530, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(540, 500.5, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(580, 100.1, false, 110)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(590, 200.2,  true, 220)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(600, 300.3, false, 330 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(610, 400.4, false, 440)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(620, 500.5, false, 550)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1500, 23.3, false, 666)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(1550, -23.3, true, 888)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
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
  public void UDAFGroupByTimeTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          countUDAF("root.ln.wf01.wt01.temperature"),
          sumUDAF("root.ln.wf01.wt01.temperature"),
          avgUDAF("root.ln.wf01.wt01.temperature")
        };
    String[] expected =
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
        "SELECT count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
            + "FROM root.ln.wf01.wt01 "
            + "WHERE time > 3 "
            + "GROUP BY ([5, 160), 20ms)",
        expectedHeader,
        expected);
  }

  @Test
  public void UDAFGroupByTimeInnerIntervalTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          countUDAF("root.ln.wf01.wt01.temperature"),
          sumUDAF("root.ln.wf01.wt01.temperature"),
          avgUDAF("root.ln.wf01.wt01.temperature")
        };
    String[] expected =
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
        "SELECT count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
            + "FROM root.ln.wf01.wt01 "
            + "WHERE temperature > 3 "
            + "GROUP BY ([0, 600), 5ms, 30ms)",
        expectedHeader,
        expected);
  }

  @Test
  public void UDAFGroupByTimeWithOuterIntervalTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          countUDAF("root.ln.wf01.wt01.temperature"),
          sumUDAF("root.ln.wf01.wt01.temperature"),
          avgUDAF("root.ln.wf01.wt01.temperature")
        };
    String[] expected =
        new String[] {
          "15,3,90.9,30.3,",
          "20,3,90.9,30.3,",
          "25,3,121.2,40.4,",
          "30,3,121.2,40.4,",
          "35,2,90.9,45.45,"
        };
    resultSetEqualTest(
        "SELECT count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
            + "FROM root.ln.wf01.wt01 "
            + "GROUP BY ([0, 600), 30ms, 5ms) "
            + "LIMIT 5 OFFSET 3",
        expectedHeader,
        expected);
  }

  @Test
  public void UDAFGroupByTimeWithoutDataTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          countUDAF("root.ln.wf01.wt01.temperature"),
          sumUDAF("root.ln.wf01.wt01.temperature"),
          avgUDAF("root.ln.wf01.wt01.temperature")
        };
    String[] expected =
        new String[] {
          "10000,0,null,null,",
          "10005,0,null,null,",
          "10010,0,null,null,",
          "10015,0,null,null,",
          "10020,0,null,null,",
          "10025,0,null,null,",
        };
    resultSetEqualWithDescOrderTest(
        "SELECT count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
            + "FROM root.ln.wf01.wt01 "
            + "WHERE temperature > 3 "
            + "GROUP BY ([10000, 10030), 5ms)",
        expectedHeader,
        expected);
  }

  @Test
  public void UDAFGroupByTimeWithLimitTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          countUDAF("root.ln.wf01.wt01.temperature"),
          sumUDAF("root.ln.wf01.wt01.temperature"),
          avgUDAF("root.ln.wf01.wt01.temperature")
        };
    String[] expected =
        new String[] {
          "90,0,null,null,",
          "120,0,null,null,",
          "150,1,200.2,200.2,",
          "180,0,null,null,",
          "210,0,null,null,"
        };
    resultSetEqualTest(
        "SELECT count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
            + "FROM root.ln.wf01.wt01 "
            + "WHERE temperature > 3 "
            + "GROUP BY ([0, 600), 5ms, 30ms) "
            + "LIMIT 5 OFFSET 3",
        expectedHeader,
        expected);
  }

  @Test
  public void UDAFGroupByTimeWithEndTimeTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          END_TIMESTAMP_STR,
          countUDAF("root.ln.wf01.wt01.temperature"),
          sumUDAF("root.ln.wf01.wt01.temperature"),
          avgUDAF("root.ln.wf01.wt01.temperature")
        };
    String[] expected =
        new String[] {
          "15,19,0,null,null,",
          "20,24,1,20.2,20.2,",
          "25,29,0,null,null,",
          "30,34,1,30.3,30.3,",
          "35,39,0,null,null,"
        };
    resultSetEqualTest(
        "SELECT __endTime, count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
            + "FROM root.ln.wf01.wt01 "
            + "WHERE temperature > 0 "
            + "GROUP BY ([0, 600), 5ms) "
            + "LIMIT 5 OFFSET 3",
        expectedHeader,
        expected);
  }

  @Test
  public void UDAFGroupByTimeAlignByDeviceTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "Device",
          countUDAF("temperature"),
          sumUDAF("temperature"),
          avgUDAF("temperature")
        };
    String[] expected =
        new String[] {
          "15,root.ln.wf01.wt01,0,null,null,",
          "20,root.ln.wf01.wt01,1,20.2,20.2,",
          "25,root.ln.wf01.wt01,0,null,null,",
          "30,root.ln.wf01.wt01,1,30.3,30.3,",
          "35,root.ln.wf01.wt01,0,null,null,"
        };
    resultSetEqualTest(
        "SELECT count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
            + "FROM root.ln.wf01.wt01 "
            + "WHERE temperature > 0 "
            + "GROUP BY ([0, 600), 5ms) "
            + "LIMIT 5 OFFSET 3 "
            + "ALIGN BY DEVICE",
        expectedHeader,
        expected);
  }

  @Test
  public void UDAFGroupByTimeWithNowFunctionTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) "
              + "VALUES(now(), 35.5, false, 650)");
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count_udaf(temperature), sum_udaf(temperature), avg_udaf(temperature) "
                  + "FROM root.ln.wf01.wt01 "
                  + "GROUP BY ([now() - 1h, now() + 1h), 2h)")) {
        Assert.assertTrue(resultSet.next());
        // resultSet.getLong(1) is the timestamp
        Assert.assertEquals(1, Integer.parseInt(resultSet.getString(2)));
        Assert.assertEquals(35.5, Float.parseFloat(resultSet.getString(3)), 0.01);
        Assert.assertEquals(35.5, Double.parseDouble(resultSet.getString(4)), 0.01);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
