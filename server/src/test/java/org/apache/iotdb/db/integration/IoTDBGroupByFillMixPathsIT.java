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
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBGroupByFillMixPathsIT {

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(8, 23)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, status) values(10, false)",
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(11, 11.1)",
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
        "INSERT INTO root.ln.wf01.wt01(timestamp, hardware) values(41, 35.6)",
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

  @Test
  public void singlePathMixTest() {
    String[] retArray =
        new String[] {
          "17,41.66666666666667,23,8",
          "22,51.0,23,25",
          "27,88.5,25,25",
          "32,126.0,27,36",
          "37,129.0,26,36",
          "42,132.0,24,36",
          "47,135.0,22,51",
          "52,110.0,20,56",
          "57,90.75,23,null",
          "62,71.5,26,null"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select sum(temperature), last_value(temperature), max_time(temperature) "
                  + "from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 65), 5ms) "
                  + "FILL(double[linear, 12ms, 6ms], int32[linear, 5ms, 18ms], int64[previousUntilLast, 17ms])");
      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(max_time("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select sum(temperature), last_value(temperature), max_time(temperature) "
                  + "from root.ln.wf01.wt01 "
                  + "GROUP BY ([17, 65), 5ms) "
                  + "FILL(double[linear, 12ms, 6ms], int32[linear, 5ms, 18ms], int64[previousUntilLast, 17ms]) order by time desc");
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"))
                  + ","
                  + resultSet.getString(max_time("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
