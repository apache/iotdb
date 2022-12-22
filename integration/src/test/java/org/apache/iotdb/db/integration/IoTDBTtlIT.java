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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBTtlIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  @Category({ClusterTest.class})
  public void testTTL() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("SET TTL TO root.TTL_SG1 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.PATH_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }
      try {
        statement.execute("UNSET TTL TO root.TTL_SG1");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.PATH_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      statement.execute("CREATE DATABASE root.TTL_SG1");
      statement.execute("CREATE TIMESERIES root.TTL_SG1.s1 WITH DATATYPE=INT64,ENCODING=PLAIN");
      try {
        statement.execute("SET TTL TO root.TTL_SG1.s1 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      statement.execute("CREATE DATABASE root.TTL_SG2");
      statement.execute("CREATE TIMESERIES root.TTL_SG2.s1 WITH DATATYPE=INT64,ENCODING=PLAIN");
      try {
        statement.execute("SET TTL TO root.TTL_SG2.s1 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      try {
        statement.execute("SET TTL TO root.** 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.PATH_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }
      try {
        statement.execute("UNSET TTL TO root.**");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.PATH_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      try {
        statement.execute("SET TTL TO root.**.s1 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), e.getErrorCode());
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
          if (TSStatusCode.OUT_OF_TTL.getStatusCode() == e.getErrorCode()) {
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
      // make sure other nodes have applied UNSET TTL
      Thread.sleep(1000);
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
      statement.execute("CREATE DATABASE root.sg.TTL_SG3");
      statement.execute("CREATE DATABASE root.sg.TTL_SG4");
      // SG2
      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG3(timestamp, s1) VALUES (%d, %d)", now - 100 + i, i));
      }
      for (int i = 100; i < 200; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG3(timestamp, s1) VALUES (%d, %d)", now - 100000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg.TTL_SG3")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }
      // SG1
      for (int i = 200; i < 300; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG4(timestamp, s1) VALUES (%d, %d)", now - 100 + i, i));
      }
      for (int i = 300; i < 400; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG4(timestamp, s1) VALUES (%d, %d)", now - 100000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg.TTL_SG4")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }

      statement.execute("SET TTL TO root.sg.** 10000");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg.**")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }
      for (int i = 0; i < 100; i++) {
        boolean caught = false;
        try {
          statement.execute(
              String.format(
                  "INSERT INTO root.sg.TTL_SG3(timestamp, s1) VALUES (%d, %d)",
                  now - 500000 + i, i));
        } catch (SQLException e) {
          if (TSStatusCode.OUT_OF_TTL.getStatusCode() == e.getErrorCode()) {
            caught = true;
          }
        }
        assertTrue(caught);
      }
      for (int i = 100; i < 200; i++) {
        boolean caught = false;
        try {
          statement.execute(
              String.format(
                  "INSERT INTO root.sg.TTL_SG4(timestamp, s1) VALUES (%d, %d)",
                  now - 500000 + i, i));
        } catch (SQLException e) {
          if (TSStatusCode.OUT_OF_TTL.getStatusCode() == e.getErrorCode()) {
            caught = true;
          }
        }
        assertTrue(caught);
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg.**")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }

      statement.execute("UNSET TTL TO root.sg.**");
      // make sure other nodes have applied UNSET TTL
      Thread.sleep(1000);
      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG3(timestamp, s1) VALUES (%d, %d)", now - 30000 + i, i));
      }
      for (int i = 100; i < 200; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG4(timestamp, s1) VALUES (%d, %d)", now - 30000 + i, i));
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg.**")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertTrue(cnt >= 400);
      }
    }
  }

  @Test
  @Category({ClusterTest.class})
  public void testShowTTL() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.group1");
      statement.execute("CREATE DATABASE root.group2");
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
    CommonDescriptor.getInstance().getConfig().setDefaultTTLInMs(10000);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.group1");
      statement.execute("CREATE DATABASE root.group2");

      String result = doQuery(statement, "SHOW ALL TTL");
      assertTrue(
          result.equals("root.group1,10000\n" + "root.group2,10000\n")
              || result.equals("root.group2,10000\n" + "root.group1,10000\n"));
    } finally {
      CommonDescriptor.getInstance().getConfig().setDefaultTTLInMs(Long.MAX_VALUE);
    }
  }

  @Test
  @Category({ClusterTest.class})
  public void testTTLOnAnyPath() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.group1");
      statement.execute("CREATE DATABASE root.group2.sgroup1");
      statement.execute("SET TTL TO root.group2.** 10000");
      String result = doQuery(statement, "SHOW ALL TTL");
      assertTrue(
          result.equals("root.group1,null\n" + "root.group2.sgroup1,10000\n")
              || result.equals("root.group2.sgroup1,10000\n" + "root.group1,null\n"));
    }
  }
}
