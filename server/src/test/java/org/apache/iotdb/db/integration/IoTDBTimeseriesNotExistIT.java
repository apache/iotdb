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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IoTDBTimeseriesNotExistIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    initCreateSQLStatement();
    EnvironmentUtils.envSetUp();
    createUser();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvironmentUtils.cleanEnv();
  }

  private static void close() {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void initCreateSQLStatement() {
    sqls.add("CREATE USER test 'test'");
    sqls.add("CREATE ROLE test_role");
    sqls.add(
        "GRANT ROLE test_role PRIVILEGES SET_STORAGE_GROUP,CREATE_TIMESERIES,INSERT_TIMESERIES,READ_TIMESERIES,DELETE_TIMESERIES,DROP_FUNCTION,CREATE_FUNCTION,CREATE_TRIGGER,DROP_TRIGGER,START_TRIGGER,STOP_TRIGGER ON root.test");
    sqls.add("GRANT test_role to test");
  }

  private static void createUser() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }

    statement.close();
  }

  @Test
  public void testRootUser() throws SQLException {
    Connection connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "select sin(s1) from root.test where time >= 2021-01-01 and time<= 2021-01-31 23:59:59");
    rs.next();
    long countStatus = rs.getLong(1);
    Assert.assertEquals(countStatus, 0L);
  }

  @Test
  public void testOtherUser() throws SQLException {
    Connection connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "test", "test");
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "select sin(s1) from root.test where time >= 2021-01-01 and time<= 2021-01-31 23:59:59");
    rs.next();
    long countStatus = rs.getLong(1);
    Assert.assertEquals(countStatus, 0L);
  }
}
