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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class IoTDBExecuteBatchIT {
  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testJDBCExecuteBatch() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)");
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600001,25.957603)");
      statement.addBatch("delete timeseries root.ln.wf01.wt01");
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600002,25.957603)");
      statement.executeBatch();
      ResultSet resultSet = statement.executeQuery("select * from root.ln.wf01.wt01");
      int count = 0;

      String[] timestamps = {"1509465600002"};
      String[] values = {"25.957603"};

      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString("Time"));
        assertEquals(values[count], resultSet.getString("root.ln.wf01.wt01.temperature"));
        count++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
