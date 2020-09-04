/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.integration;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.Assert;
import org.junit.Test;

public class IoTDBRestartIT {

  @Test
  public void testRestart()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,1.0)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1.0)");

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[]{
          "1,1.0",
          "2,1.0",
          "3,1.0"
      };
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
    }

    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void testRestartDelete()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,3)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("delete from root.turbine.d1.s1 where time<=1");

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[]{
          "2,2.0",
          "3,3.0"
      };
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      try {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }

        statement.execute("flush");
        statement.execute("delete from root.turbine.d1.s1 where time<=2");

        hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
        assertTrue(hasResultSet);
        exp = new String[]{
                "3,3.0"
        };
        resultSet = statement.getResultSet();
        cnt = 0;
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      } finally {
        resultSet.close();
      }
    }

    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRestartQueryLargerThanEndTime()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(4,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1 where time > 3");
      assertTrue(hasResultSet);
      String[] exp = new String[]{
          "4,2.0",
      };
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(1, cnt);

    }

    EnvironmentUtils.cleanEnv();
  }



  @Test
  public void testRestartEndTime()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s2) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s2) values(2,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("SELECT s2 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[]{
          "1,1.0",
          "2,2.0"
      };
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(2, cnt);

    }

    EnvironmentUtils.cleanEnv();
  }
}
