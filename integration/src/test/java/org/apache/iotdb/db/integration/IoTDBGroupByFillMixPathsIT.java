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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.*;

// TODO: @CRZbulabula
// rebuild test after IOTDB-1948 and IOTDB-1949

@Category({LocalStandaloneTest.class})
public class IoTDBGroupByFillMixPathsIT {

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(8, 23)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, status) values(10, true)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(11, 11.0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(23, 28)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(25, 23)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(27, 33.7)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(29, 35.3)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(30, 36.0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(32, 22, false, 40.7)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(33, 25, false, 42.5)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(34, 29, false, 43.6)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(35, 23, false, 41.8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(36, 27, true, 48.2)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(37, 36.8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(40, 38.2)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(41, 36.0)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, status) values(44, false)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, status) values(45, false)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(47, 35)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(48, 42)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(50, 36)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(51, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status) values(52, 15, false)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status) values(53, 13, true)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status) values(54, 24, false)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status) values(55, 38, false)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status) values(56, 20, true)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(58, 40.5)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(60, 27.5)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(61, 36.4)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(72, 33)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, status) values(74, true)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(75, 46.8)",
        "flush",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(110, 21, false, 11.1)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(112, 23, true, 22.3)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(114, 25, false, 33.5)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(123, 28, true, 34.9)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(125, 23, false, 31.7)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(133, 29, false, 44.6)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(136, 24, true, 44.8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(148, 28, false, 54.6)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(150, 30, true, 55.8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, status, hardware) values(166, 40, false, 33.0)",
        "flush"
      };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.cleanEnv();
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  //  @Test
  //  public void singlePathMixTest() {
  //    String[] retArray =
  //        new String[] {
  //          // "7,23.0,23,8"
  //          "17,41.66666666666667,23,8",
  //          "22,51.0,23,25",
  //          "27,88.5,25,25",
  //          "32,126.0,27,36",
  //          "37,129.0,26,36",
  //          "42,132.0,24,36",
  //          "47,135.0,22,51",
  //          "52,110.0,20,56",
  //          "57,null,23,null",
  //          "62,71.5,26,null"
  //          // "72,33.0,33,72"
  //        };
  //
  //    try (Connection connection =
  //            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
  //        Statement statement = connection.createStatement()) {
  //      boolean hasResultSet =
  //          statement.execute(
  //              "select sum(temperature), last_value(temperature), max_time(temperature) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "GROUP BY ([17, 65), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int32[linear, 10ms, 18ms],
  // int64[previousUntilLast, 17ms])");
  //      assertTrue(hasResultSet);
  //      int cnt;
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.lastValue("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.temperature"));
  //          assertEquals(retArray[cnt], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //      hasResultSet =
  //          statement.execute(
  //              "select sum(temperature), last_value(temperature), max_time(temperature) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "GROUP BY ([17, 65), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int32[linear, 10ms, 18ms],
  // int64[previousUntilLast, 17ms]) "
  //                  + "order by time desc");
  //      assertTrue(hasResultSet);
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.lastValue("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.temperature"));
  //          assertEquals(retArray[retArray.length - cnt - 1], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void MultiPathsMixTest() {
  //    String[] retArray =
  //        new String[] {
  //          "17,41.66666666666667,23.0,10,23,23.5,true",
  //          "22,51.0,null,10,23,null,true",
  //          "27,88.5,35.0,null,23,36.0,true",
  //          "32,126.0,43.36,36,22,48.2,false",
  //          "37,129.0,37.0,36,22,38.2,true",
  //          "42,132.0,null,45,22,null,false",
  //          "47,135.0,35.900000000000006,45,22,39.35,true",
  //          "52,110.0,null,56,13,null,false",
  //          "57,null,34.800000000000004,null,18,40.5,true",
  //          "62,71.5,38.800000000000004,null,23,42.6,true"
  //        };
  //
  //    /*  Format result
  //                  linear,      linear, preUntil,   linear,       linear,      value
  //          7,        23.0,        11.0,       10,       23,         11.0,       true
  //         17, 41.67(null),  23.0(null), 10(null), 23(null),   23.5(null), true(null)
  //         22,        51.0,        null, 10(null),       23,         null, true(null)
  //         27,  88.5(null),        35.0,     null, 23(null),         36.0, true(null)
  //         32,       126.0,       43.36,       36,       22,         48.2,      false
  //         37, 129.0(null),        37.0, 36(null), 22(null),         38.2, true(null)
  //         42, 132.0(null),        null,       45, 22(null),         null,      false
  //         47,       135.0,  35.9(null), 45(null),       22,  39.35(null), true(null)
  //         52,       110.0,        null,       56,       13,         null,      false
  //         57,        null,        34.8,     null, 18(null),         40.5, true(null)
  //         62,  71.5(null),  38.8(null),     null, 23(null),   42.6(null), true(null)
  //         72,        33.0,        46.8,       74,       33,         46.8,       true
  //    */
  //
  //    try (Connection connection =
  //            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
  //        Statement statement = connection.createStatement()) {
  //      boolean hasResultSet =
  //          statement.execute(
  //              "select sum(temperature), avg(hardware), max_time(status), "
  //                  + "min_value(temperature), max_value(hardware), first_value(status) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "GROUP BY ([17, 65), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int32[linear, 12ms, 18ms], "
  //                  + "int64[previousUntilLast, 17ms], boolean[true])");
  //      assertTrue(hasResultSet);
  //      int cnt;
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(avg("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.status"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.minValue("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxValue("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.firstValue("root.ln.wf01.wt01.status"));
  //          assertEquals(retArray[cnt], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //      hasResultSet =
  //          statement.execute(
  //              "select sum(temperature), avg(hardware), max_time(status), "
  //                  + "min_value(temperature), max_value(hardware), first_value(status) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "GROUP BY ([17, 65), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int32[linear, 12ms, 18ms], "
  //                  + "int64[previousUntilLast, 17ms], boolean[true]) "
  //                  + "order by time desc");
  //      assertTrue(hasResultSet);
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(avg("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.status"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.minValue("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxValue("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.firstValue("root.ln.wf01.wt01.status"));
  //          assertEquals(retArray[retArray.length - cnt - 1], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void singlePathMixWithValueFilterTest() {
  //    String[] retArray =
  //        new String[] {
  //          // "112,33.5,33.5,114"
  //          "117,null,null,114",
  //          "122,39.05,39.05,114",
  //          "127,null,null,114",
  //          "132,44.6,44.6,133",
  //          "137,47.93333333333334,47.93333333333334,133",
  //          "142,51.266666666666666,51.266666666666666,133",
  //          "147,54.6,54.6,148",
  //          "152,47.4,47.4,148"
  //          // "162,33.0,33.0,166"
  //        };
  //
  //    try (Connection connection =
  //            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
  //        Statement statement = connection.createStatement()) {
  //      boolean hasResultSet =
  //          statement.execute(
  //              "select sum(hardware), last_value(hardware), max_time(hardware) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "WHERE temperature >= 25 and status = false "
  //                  + "GROUP BY ([117, 155), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int64[previous, 17ms])");
  //      assertTrue(hasResultSet);
  //      int cnt;
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.lastValue("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.hardware"));
  //          assertEquals(retArray[cnt], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //      hasResultSet =
  //          statement.execute(
  //              "select sum(hardware), last_value(hardware), max_time(hardware) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "WHERE temperature >= 25 and status = false "
  //                  + "GROUP BY ([117, 155), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int64[previous, 17ms]) "
  //                  + "order by time desc");
  //      assertTrue(hasResultSet);
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.lastValue("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.hardware"));
  //          assertEquals(retArray[retArray.length - cnt - 1], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void MultiPathsMixWithValueFilterTest() {
  //    String[] retArray =
  //        new String[] {
  //          "117,null,null,114,26,null,true",
  //          "122,27.0,39.05,114,27,39.05,true",
  //          "127,null,null,114,null,null,true",
  //          "132,29.0,44.6,133,29,44.6,false",
  //          "137,28.666666666666668,47.93333333333334,133,29,47.93333333333334,true",
  //          "142,28.333333333333332,51.266666666666666,133,29,51.266666666666666,true",
  //          "147,28.0,54.6,148,28,54.6,false",
  //          "152,32.0,47.4,null,32,47.4,true"
  //        };
  //
  //    /*  Format result
  //                   linear,      linear,  preUntil,   linear,      linear,      value
  //          7,         25.0,        33.5,       114,       25,        33.5,      false
  //         117,        null,        null, 114(null), 26(null),        null, true(null)
  //         122,  27.0(null), 39.05(null), 114(null), 27(null), 39.05(null), true(null)
  //         127,        null,        null, 114(null),     null,        null, true(null)
  //         132,        29.0,        44.6,       133,       29,        44.6,      false
  //         137, 28.67(null), 47.93(null), 133(null), 29(null), 47.93(null), true(null)
  //         142, 28.33(null), 51.27(null), 133(null), 29(null), 51.27(null), true(null)
  //         147,        28.0,        54.6,       148,       28,        54.6,      false
  //         152,  32.0(null),  47.4(null),      null, 32(null),  47.4(null), true(null)
  //         162,        40.0,        33.0,      null,       40,        33.0,       null
  //    */
  //
  //    try (Connection connection =
  //            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
  //        Statement statement = connection.createStatement()) {
  //      boolean hasResultSet =
  //          statement.execute(
  //              "select sum(temperature), avg(hardware), max_time(status), "
  //                  + "min_value(temperature), max_value(hardware), first_value(status) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "WHERE temperature >= 25 and status = false "
  //                  + "GROUP BY ([117, 155), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int32[linear, 12ms, 18ms], "
  //                  + "int64[previousUntilLast, 17ms], boolean[true])");
  //      assertTrue(hasResultSet);
  //      int cnt;
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(avg("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.status"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.minValue("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxValue("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.firstValue("root.ln.wf01.wt01.status"));
  //          assertEquals(retArray[cnt], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //
  //      hasResultSet =
  //          statement.execute(
  //              "select sum(temperature), avg(hardware), max_time(status), "
  //                  + "min_value(temperature), max_value(hardware), first_value(status) "
  //                  + "from root.ln.wf01.wt01 "
  //                  + "WHERE temperature >= 25 and status = false "
  //                  + "GROUP BY ([117, 155), 5ms) "
  //                  + "FILL(double[linear, 12ms, 12ms], int32[linear, 12ms, 18ms], "
  //                  + "int64[previousUntilLast, 17ms], boolean[true]) "
  //                  + "order by time desc");
  //      assertTrue(hasResultSet);
  //      try (ResultSet resultSet = statement.getResultSet()) {
  //        cnt = 0;
  //        while (resultSet.next()) {
  //          String ans =
  //              resultSet.getString(TIMESTAMP_STR)
  //                  + ","
  //                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(avg("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxTime("root.ln.wf01.wt01.status"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.minValue("root.ln.wf01.wt01.temperature"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.maxValue("root.ln.wf01.wt01.hardware"))
  //                  + ","
  //                  + resultSet.getString(TestConstant.firstValue("root.ln.wf01.wt01.status"));
  //          assertEquals(retArray[retArray.length - cnt - 1], ans);
  //          cnt++;
  //        }
  //        assertEquals(retArray.length, cnt);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
}
