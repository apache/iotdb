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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBFlushQueryMergeIT {

  private static String[] sqls = new String[]{
      "SET STORAGE GROUP TO root.vehicle.d0",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",

      "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
      "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
      "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
      "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
      "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
      "flush",
      "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
      "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
      "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",

  };

  @BeforeClass
  public static void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void selectAllSQLTest() throws ClassNotFoundException {

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("SELECT * FROM root");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
      }
      statement.execute("merge");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushGivenGroup() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String insertTemplate =
        "INSERT INTO root.group%d(timestamp, s1, s2, s3) VALUES (%d, %d, %f, %s)";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.group1");
      statement.execute("SET STORAGE GROUP TO root.group2");
      statement.execute("SET STORAGE GROUP TO root.group3");

      for (int i = 1; i <= 3; i++) {
        for (int j = 10; j < 20; j++) {
          statement.execute(String.format(insertTemplate, i, j, j, j * 0.1, String.valueOf(j)));
        }
      }
      statement.execute("FLUSH");

      for (int i = 1; i <= 3; i++) {
        for (int j = 0; j < 10; j++) {
          statement.execute(String.format(insertTemplate, i, j, j, j * 0.1, String.valueOf(j)));
        }
      }
      statement.execute("FLUSH root.group1");
      statement.execute("FLUSH root.group2,root.group3");

      for (int i = 1; i <= 3; i++) {
        for (int j = 0; j < 30; j++) {
          statement.execute(String.format(insertTemplate, i, j, j, j * 0.1, String.valueOf(j)));
        }
      }
      statement.execute("FLUSH root.group1 TRUE");
      statement.execute("FLUSH root.group2,root.group3 FALSE");

      int i = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.group1,root.group2,root" + ".group3")) {
        while (resultSet.next()) {
          i++;
        }
      }
      assertEquals(30, i);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
