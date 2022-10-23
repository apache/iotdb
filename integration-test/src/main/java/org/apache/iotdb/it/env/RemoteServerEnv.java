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
package org.apache.iotdb.it.env;

import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.ISession;
import org.apache.iotdb.session.Session;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
      statement.execute("SET STORAGE GROUP TO root.init;");
      statement.execute("DELETE STORAGE GROUP root;");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    // Do nothing
  }

  @Override
  public void cleanAfterClass() {}

  @Override
  public void initBeforeTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.init;");
      statement.execute("DELETE STORAGE GROUP root;");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Override
  public void cleanAfterTest() {}

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    Connection connection = null;
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX + ip_addr + ":" + port, this.user, this.password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return connection;
  }

  @Override
  public Connection getConnection(Constant.Version version, String username, String password)
      throws SQLException {
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
              this.user,
              this.password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return connection;
  }

  public void setTestMethodName(String testCaseName) {
    // Do nothing
  }

  @Override
  public void dumpTestJVMSnapshot() {
    // Do nothing
  }

  @Override
  public List<ConfigNodeWrapper> getConfigNodeWrapperList() {
    return null;
  }

  @Override
  public void setConfigNodeWrapperList(List<ConfigNodeWrapper> configNodeWrapperList) {
    // Do nothing
  }

  @Override
  public List<DataNodeWrapper> getDataNodeWrapperList() {
    return null;
  }

  @Override
  public void setDataNodeWrapperList(List<DataNodeWrapper> dataNodeWrapperList) {
    // Do nothing
  }

  @Override
  public IConfigNodeRPCService.Iface getLeaderConfigNodeConnection() throws IOException {
    return null;
  }

  @Override
  public ISession getSessionConnection() throws IoTDBConnectionException {
    Session session = new Session(ip_addr, Integer.parseInt(port));
    session.open();
    return session;
  }

  @Override
  public int getLeaderConfigNodeIndex() throws IOException {
    return -1;
  }

  @Override
  public void startConfigNode(int index) {
    getConfigNodeWrapperList().get(index).start();
  }

  @Override
  public void shutdownConfigNode(int index) {
    getConfigNodeWrapperList().get(index).stop();
  }

  @Override
  public void startDataNode(int index) {
    getDataNodeWrapperList().get(index).start();
  }

  @Override
  public void shutdownDataNode(int index) {
    getDataNodeWrapperList().get(index).stop();
  }
}
