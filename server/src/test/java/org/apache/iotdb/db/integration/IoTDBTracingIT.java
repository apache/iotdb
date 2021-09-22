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
import org.apache.iotdb.jdbc.IoTDBJDBCResultSet;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.fail;

public class IoTDBTracingIT {

  @BeforeClass
  public static void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void tracingTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    try (Statement statement = connection.createStatement()) {
      statement.execute("tracing on");
    } catch (Exception e) {
      Assert.assertEquals("411: TRACING ON/OFF hasn't been supported yet", e.getMessage());
    }
    try (Statement statement = connection.createStatement()) {
      statement.execute("tracing off");
    } catch (Exception e) {
      Assert.assertEquals("411: TRACING ON/OFF hasn't been supported yet", e.getMessage());
    }
    try (Statement statement = connection.createStatement()) {
      statement.execute("tracing select s1 from root.sg1.d1");
      ResultSet resultSet = statement.getResultSet();
      List<List<String>> tracingInfo = ((IoTDBJDBCResultSet) resultSet).getTracingInfo();
      Assert.assertEquals(2, tracingInfo.size());
      Assert.assertEquals(11, tracingInfo.get(0).size());
      Assert.assertEquals(11, tracingInfo.get(1).size());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
