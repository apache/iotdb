/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IoTDBDeletionTest {
  private static IoTDB daemon;

  private static String[] creationSqls = new String[]{
          "SET STORAGE GROUP TO root.vehicle.d0", "SET STORAGE GROUP TO root.vehicle.d1",

          "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
  };

  private static String intertTemplate = "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4"
          + ") VALUES(%d,%d,%d,%f,%s,%b)";
  private static String deleteAllTemplate = "DELETE FROM root.vehicle.d0 WHERE time <= 10000";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareSeries();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    daemon.stop();
    Thread.sleep(5000);

    EnvironmentUtils.cleanEnv();
  }

  @Before
  public void prepare() throws SQLException {
    prepareData();
  }

  @After
  public void cleanup() throws SQLException {
    cleanData();
  }

  @Test
  public void test() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                      "root");
      Statement statement = connection.createStatement();
      statement.execute("DELETE FROM root.vehicle.d0.s0  WHERE time <= 300");
      statement.execute("DELETE FROM root.vehicle.d0.s1,root.vehicle.d0.s2,root.vehicle.d0.s3"
              + " WHERE time <= 350");
      statement.execute("DELETE FROM root.vehicle.d0 WHERE time <= 150");

      ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0");
      int cnt = 0;
      while (set.next()) {
        cnt ++;
      }
      assertEquals(250, cnt);
      set.close();

      set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0");
      cnt = 0;
      while (set.next()) {
        cnt ++;
      }
      assertEquals(100, cnt);
      set.close();

      set = statement.executeQuery("SELECT s1,s2,s3 FROM root.vehicle.d0");
      cnt = 0;
      while (set.next()) {
        cnt ++;
      }
      assertEquals(50, cnt);
      set.close();

      statement.close();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  public static void prepareSeries() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                      "root");
      Statement statement = connection.createStatement();
      for (String sql : creationSqls) {
        statement.execute(sql);
      }
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  public void prepareData() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                      "root");
      Statement statement = connection.createStatement();
      // prepare BufferWrite file
      for (int i = 201; i <= 300; i++) {
        statement.execute(String.format(intertTemplate, i, i, i, (double) i, "\'" + i + "\'",
                i % 2 == 0));
      }
      statement.execute("merge");
      // prepare Overflow file
      for (int i = 1; i <= 100; i++) {
        statement.execute(String.format(intertTemplate, i, i, i, (double) i, "\'" + i + "\'",
                i % 2 == 0));
      }
      statement.execute("merge");
      // prepare BufferWrite cache
      for (int i = 301; i <= 400; i++) {
        statement.execute(String.format(intertTemplate, i, i, i, (double) i, "\'" + i + "\'",
                i % 2 == 0));
      }
      // prepare Overflow cache
      for (int i = 101; i <= 200; i++) {
        statement.execute(String.format(intertTemplate, i, i, i, (double) i, "\'" + i + "\'",
                i % 2 == 0));
      }

      statement.close();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  public void cleanData() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                      "root");
      Statement statement = connection.createStatement();
      statement.execute(deleteAllTemplate);

      statement.close();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
