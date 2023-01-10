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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBTtlIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTTL() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("SET TTL TO root.TTL_SG1 1000");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }
      try {
        statement.execute("UNSET TTL TO root.TTL_SG1");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      statement.execute("CREATE DATABASE root.TTL_SG1");
      statement.execute("CREATE TIMESERIES root.TTL_SG1.s1 WITH DATATYPE=INT64,ENCODING=PLAIN");
      try {
        statement.execute("SET TTL TO root.TTL_SG1.s1 1000");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      statement.execute("CREATE DATABASE root.TTL_SG2");
      statement.execute("CREATE TIMESERIES root.TTL_SG2.s1 WITH DATATYPE=INT64,ENCODING=PLAIN");
      try {
        statement.execute("SET TTL TO root.TTL_SG2.s1 1000");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try {
        statement.execute("SET TTL TO root.** 1000");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
      try {
        statement.execute("UNSET TTL TO root.**");
      } catch (SQLException e) {
        e.printStackTrace();
        fail(e.getMessage());
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
          assertEquals(TSStatusCode.OUT_OF_TTL.getStatusCode(), e.getErrorCode());
        }
      }

      Thread.sleep(1000);
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
      // SG3
      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG3(timestamp, s1) VALUES (%d, %d)", now - 100 + i, i));
      }
      for (int i = 100; i < 200; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG3(timestamp, s1) VALUES (%d, %d)",
                now - 10000000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg.TTL_SG3")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }
      // SG4
      for (int i = 200; i < 300; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG4(timestamp, s1) VALUES (%d, %d)", now - 100 + i, i));
      }
      for (int i = 300; i < 400; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg.TTL_SG4(timestamp, s1) VALUES (%d, %d)",
                now - 10000000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg.TTL_SG4")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }

      statement.execute("SET TTL TO root.sg.** 100000");
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
                  now - 5000000 + i, i));
        } catch (SQLException e) {
          assertEquals(TSStatusCode.OUT_OF_TTL.getStatusCode(), e.getErrorCode());
        }
      }
      for (int i = 100; i < 200; i++) {
        boolean caught = false;
        try {
          statement.execute(
              String.format(
                  "INSERT INTO root.sg.TTL_SG4(timestamp, s1) VALUES (%d, %d)",
                  now - 5000000 + i, i));
        } catch (SQLException e) {
          assertEquals(TSStatusCode.OUT_OF_TTL.getStatusCode(), e.getErrorCode());
        }
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
}
