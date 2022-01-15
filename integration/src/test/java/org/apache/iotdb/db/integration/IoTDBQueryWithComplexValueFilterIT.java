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
import java.sql.Statement;

import static org.junit.Assert.fail;

public class IoTDBQueryWithComplexValueFilterIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRawQuery1() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s1 from root.sg1.d1 where (time > 400 and s1 <= 600) or (s2 > 300 and time <= 500)");
      Assert.assertTrue(hasResultSet);

      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;

      while (resultSet.next()) {
        cnt++;
      }

      Assert.assertEquals(300, cnt);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRawQuery2() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s1 from root.sg1.d1 where (time > 400 and s1 <= 600) and (s2 > 300 and time <= 500)");
      Assert.assertTrue(hasResultSet);

      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;

      while (resultSet.next()) {
        cnt++;
      }

      Assert.assertEquals(100, cnt);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create storage group root.sg1");
      statement.execute("create timeseries root.sg1.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("create timeseries root.sg1.d1.s2 with datatype=DOUBLE,encoding=PLAIN");
      for (int i = 0; i < 1000; i++) {
        statement.execute(
            String.format(
                "insert into root.sg1.d1(time,s1,s2) values(%d,%d,%f)", i, i, (double) i));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
