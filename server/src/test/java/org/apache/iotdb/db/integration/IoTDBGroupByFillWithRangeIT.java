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

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.last_value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBGroupByFillWithRangeIT {

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) " + "values(1, 1)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) " + "values(6, 6)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) " + "values(11, 11)",
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

  @Test
  public void previousFillTestWithTimeRange() {
    String[] retArray =
        new String[] {
          "5,null", "7,6", "9,6", "11,11",
        };

    String[] retArray2 =
        new String[] {
          "5,null", "7,6", "9,null", "11,11",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ((3, 11], 2ms) FILL(int32[previous, 2ms])");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ((3, 11], 2ms) FILL(int32[previous, 1ms])");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ((3, 11], 2ms) FILL(ALL[previousUntilLast, 1ms])");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(temperature) from "
                  + "root.ln.wf01.wt01 "
                  + "GROUP BY ((3, 11], 2ms) FILL(ALL[previousUntilLast, 1ms]) order by time desc");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value("root.ln.wf01.wt01.temperature"));
          assertEquals(retArray2[retArray2.length - cnt - 1], ans);
          cnt++;
        }
        assertEquals(retArray2.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
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
}
