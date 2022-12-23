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
package org.apache.iotdb.db.it.env;

import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.ISession;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConfig;
import org.apache.iotdb.session.util.Version;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

/** This class is used by EnvFactory with using reflection. */
public class StandaloneEnv implements BaseEnv {

  @Override
  public void initBeforeClass() {
    EnvironmentUtils.envSetUp();
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    // Do nothing
  }

  @Override
  public void cleanAfterClass() {
    cleanAfterTest();
  }

  @Override
  public void initBeforeTest() {
    EnvironmentUtils.envSetUp();
  }

  @Override
  public void cleanAfterTest() {
    try {
      EnvironmentUtils.cleanEnv();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      return DriverManager.getConnection(
          Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", username, password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return null;
  }

  @Override
  public Connection getConnection(Constant.Version version, String username, String password)
      throws SQLException {
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      return DriverManager.getConnection(
          Config.IOTDB_URL_PREFIX + "127.0.0.1:6667" + "?" + VERSION + "=" + version.toString(),
          username,
          password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return null;
  }

  public void setTestMethodName(String testCaseName) {
    // Do nothing
  }

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
  public IConfigNodeRPCService.Iface getLeaderConfigNodeConnection() {
    return null;
  }

  @Override
  public ISession getSessionConnection() throws IoTDBConnectionException {
    Session session =
        new Session(
            SessionConfig.DEFAULT_HOST,
            SessionConfig.DEFAULT_PORT,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD);
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableRedirection,
      Version version)
      throws IoTDBConnectionException {
    Session session =
        new Session(
            host,
            rpcPort,
            username,
            password,
            fetchSize,
            zoneId,
            thriftDefaultBufferSize,
            thriftMaxFrameSize,
            enableRedirection,
            version);

    session.open();
    return session;
  }

  @Override
  public int getLeaderConfigNodeIndex() throws IOException {
    return -1;
  }

  @Override
  public void startConfigNode(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownConfigNode(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConfigNodeWrapper generateRandomConfigNodeWrapper() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataNodeWrapper generateRandomDataNodeWrapper() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConfigNodeWrapper getConfigNodeWrapper(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataNodeWrapper getDataNodeWrapper(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerNewDataNode(boolean isNeedVerify) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerNewConfigNode(boolean isNeedVerify) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerNewDataNode(DataNodeWrapper newDataNodeWrapper, boolean isNeedVerify) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerNewConfigNode(ConfigNodeWrapper newConfigNodeWrapper, boolean isNeedVerify) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startDataNode(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownDataNode(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMqttPort() {
    return 1883;
  }

  @Override
  public String getIP() {
    return "127.0.0.1";
  }

  @Override
  public String getPort() {
    return "6667";
  }

  @Override
  public String getSbinPath() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getToolsPath() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLibPath() {
    throw new UnsupportedOperationException();
  }
}
