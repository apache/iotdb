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
import org.junit.Test;

public class IoTDBRestartIT {

  @Test
  public void testRestart()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("flush");
    }

    EnvironmentUtils.restartDaemon();

    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,1)");
    }

    EnvironmentUtils.restartDaemon();

    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1)");

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[]{
          "1,1",
          "2,1",
          "3,1"
      };
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
        assertEquals(exp[cnt], result);
        cnt++;
      }
    }

    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void testRestartDelete()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,3)");
    }

    EnvironmentUtils.restartDaemon();

    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()){
      statement.execute("delete from root.turbine.d1.s1 where time<=1");
      statement.execute("flush");
      statement.execute("delete from root.turbine.d1.s1 where time<=2");

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[]{
          "3,3"
      };
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
        assertEquals(exp[cnt], result);
        cnt++;
      }
    }

    EnvironmentUtils.cleanEnv();
  }
}
