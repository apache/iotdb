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
package org.apache.iotdb.integration.env;

import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

public class RemoteServerEnv implements BaseEnv {
  private String ip_addr = System.getProperty("RemoteIp", "127.0.0.1");
  private String port = System.getProperty("RemotePort", "6667");
  private String user = System.getProperty("RemoteUser", "root");
  private String password = System.getProperty("RemotePassword", "root");

  @Override
  public void initBeforeClass() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.init;");
      statement.execute("DELETE DATABASE root;");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Override
  public void cleanAfterClass() {}

  @Override
  public void initBeforeTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.init;");
      statement.execute("DELETE DATABASE root;");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Override
  public void cleanAfterTest() {}

  @Override
  public Connection getConnection() throws SQLException {
    Connection connection = null;
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX + ip_addr + ":" + port, user, password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return connection;
  }

  @Override
  public Connection getConnection(Constant.Version version) throws SQLException {
    Connection connection = null;
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX
                  + ip_addr
                  + ":"
                  + port
                  + "?"
                  + VERSION
                  + "="
                  + version.toString(),
              user,
              password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return connection;
  }
}
