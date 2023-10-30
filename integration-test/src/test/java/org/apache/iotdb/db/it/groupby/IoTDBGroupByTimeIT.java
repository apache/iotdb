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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByTimeIT {

  private static final String[] SQLs =
          new String[] {
                  "CREATE DATABASE root.ln.wf02.wt02",
                  "CREATE TIMESERIES root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
                  "CREATE TIMESERIES root.ln.wf02.wt02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
                  "CREATE TIMESERIES root.ln.wf02.wt02.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(1, 35.7, false, 11)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(2, 35.8,  true, 22)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(3, 35.4, false, 33 )",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(4, 36.4, false, 44)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(5, 36.8, false, 55)",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(10, 36.8, false, 110)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(20, 37.8,  true, 220)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(30, 37.5, false, 330 )",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(40, 37.4, false, 440)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(50, 37.9, false, 550)",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(100, 38.0, false, 110)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(150, 38.8,  true, 220)",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(200, 38.6, false, 330 )",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(250, 38.4, false, 440)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(300, 38.3, false, 550)",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(400, null, null, 0)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(440, null, null, 0)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(480, null, null, 0)",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(500, 38.2, false, 110)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(510, 37.5,  true, 220)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(520, 37.4, false, 330 )",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(530, 36.8, false, 440)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(540, 37.4, false, 550)",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(580, 37.8, false, 110)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(590, 37.9,  true, 220)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(600, 36.9, true, 330 )",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(610, 38.2, true, 440)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(620, 39.2, true, 550)",
                  "flush",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(1500, 9.8, false, 666)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(1550, 10.2, true, 888)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(3550, 10.8, true, 999)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(5550, 10.6, false, 1888)",
                  "INSERT INTO root.ln.wf02.wt02(timestamp, temperature, status, hardware) values(7550, 10.2, true, 2888)",
                  "flush"
          };
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void checkHeader(ResultSetMetaData resultSetMetaData, String title) throws SQLException {
    String[] headers = title.split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  private void endTimeTest(String sql, String headers, String[][] res) {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, headers);
        int count = 0;
        while (resultSet.next()) {
          String actualTime = resultSet.getString(1);
          String actualEndTime = resultSet.getString(2);
          String actualCount = resultSet.getString(3);

          assertEquals(res[count][0], actualTime);
          assertEquals(res[count][1], actualEndTime);
          assertEquals(res[count][2], actualCount);
          count++;
        }
        assertEquals(res.length, count);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeEndTimeTest() {
    String sql =
        "select __endTime,count(temperature) from root.** group by ([100,700),100ms)";
    String headers = "Time,__endTime,count(root.ln.wf02.wt02.temperature)";
    String[][] res = {
      {"100", "200", "2"},
      {"200", "300", "2"},
      {"300", "400", "1"},
      {"400", "500", "0"},
      {"500", "600", "7"},
      {"600", "700", "3"}
    };
    endTimeTest(sql, headers, res);
  }

  @Test
  public void groupByTimeEndTimeTest2() {
    String sql =
            "select __endTime,count(temperature) from root.** group by ([100,700),100ms)";
    String headers = "Time,__endTime,count(root.ln.wf02.wt02.temperature)";
    String[][] res = {
            {"100", "200", "2"},
            {"200", "300", "2"},
            {"300", "400", "1"},
            {"400", "500", "0"},
            {"500", "600", "7"},
            {"600", "700", "3"}
    };
    endTimeTest(sql, headers, res);
  }
}
