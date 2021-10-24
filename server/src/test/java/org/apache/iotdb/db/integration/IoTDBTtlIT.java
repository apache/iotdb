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
 *
 */

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBTtlIT {

  @Before
  public void setUp() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testTTL() throws SQLException {
    try (IoTDBConnection connection =
            (IoTDBConnection)
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("SET TTL TO root.TTL_SG1 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }
      try {
        statement.execute("UNSET TTL TO root.TTL_SG1");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      statement.execute("SET STORAGE GROUP TO root.TTL_SG1");
      statement.execute("CREATE TIMESERIES root.TTL_SG1.s1 WITH DATATYPE=INT64,ENCODING=PLAIN");
      try {
        statement.execute("SET TTL TO root.TTL_SG1.s1 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      long now = System.currentTimeMillis();
      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.TTL_SG1(timestamp, s1) VALUES (%d, %d)", now - 100 + i, i));
      }
      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.TTL_SG1(timestamp, s1) VALUES (%d, %d)", now - 100000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.TTL_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }

      statement.execute("SET TTL TO root.TTL_SG1 10000");
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.TTL_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }
      for (int i = 0; i < 100; i++) {
        boolean caught = false;
        try {
          statement.execute(
              String.format(
                  "INSERT INTO root.TTL_SG1(timestamp, s1) VALUES (%d, %d)", now - 500000 + i, i));
        } catch (SQLException e) {
          if (TSStatusCode.OUT_OF_TTL_ERROR.getStatusCode() == e.getErrorCode()) {
            caught = true;
          }
        }
        assertTrue(caught);
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.TTL_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      statement.execute("UNSET TTL TO root.TTL_SG1");
      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.TTL_SG1(timestamp, s1) VALUES (%d, %d)", now - 30000 + i, i));
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.TTL_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertTrue(cnt >= 200);
      }
    }
  }

  @Test
  public void testShowTTL() throws SQLException {
    try (IoTDBConnection connection =
            (IoTDBConnection)
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.group1");
      statement.execute("SET STORAGE GROUP TO root.group2");
      String result = doQuery(statement, "SHOW ALL TTL");
      assertTrue(
          result.equals("root.group1,null\n" + "root.group2,null\n")
              || result.equals("root.group2,null\n" + "root.group1,null\n"));
      result = doQuery(statement, "SHOW TTL ON root.group1");
      assertEquals("root.group1,null\n", result);

      statement.execute("SET TTL TO root.group1 10000");
      result = doQuery(statement, "SHOW ALL TTL");
      assertTrue(
          result.equals("root.group1,10000\n" + "root.group2,null\n")
              || result.equals("root.group2,null\n" + "root.group1,10000\n"));
      result = doQuery(statement, "SHOW TTL ON root.group1");
      assertEquals("root.group1,10000\n", result);

      statement.execute("UNSET TTL TO root.group1");
      result = doQuery(statement, "SHOW ALL TTL");
      assertTrue(
          result.equals("root.group1,null\n" + "root.group2,null\n")
              || result.equals("root.group2,null\n" + "root.group1,null\n"));
      result = doQuery(statement, "SHOW TTL ON root.group1");
      assertEquals("root.group1,null\n", result);
    }
  }

  private String doQuery(Statement statement, String query) throws SQLException {
    StringBuilder ret;
    try (ResultSet resultSet = statement.executeQuery(query)) {
      ret = new StringBuilder();
      while (resultSet.next()) {
        ret.append(resultSet.getString(1));
        ret.append(",");
        ret.append(resultSet.getString(2));
        ret.append("\n");
      }
    }
    return ret.toString();
  }

  @Test
  public void testDefaultTTL() throws SQLException {
    IoTDBDescriptor.getInstance().getConfig().setDefaultTTL(10000);
    try (IoTDBConnection connection =
            (IoTDBConnection)
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.group1");
      statement.execute("SET STORAGE GROUP TO root.group2");

      String result = doQuery(statement, "SHOW ALL TTL");
      assertTrue(
          result.equals("root.group1,10000\n" + "root.group2,10000\n")
              || result.equals("root.group2,10000\n" + "root.group1,10000\n"));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setDefaultTTL(Long.MAX_VALUE);
    }
  }

  @Test
  public void testTTLOnAnyPath() throws SQLException {
    try (IoTDBConnection connection =
            (IoTDBConnection)
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.group1");
      statement.execute("SET STORAGE GROUP TO root.group2.sgroup1");
      statement.execute("SET TTL TO root.group2.** 10000");
      String result = doQuery(statement, "SHOW ALL TTL");
      assertTrue(
          result.equals("root.group1,null\n" + "root.group2.sgroup1,10000\n")
              || result.equals("root.group2.sgroup1 10000\n" + "root.group1,null\n"));
    }
  }
}
