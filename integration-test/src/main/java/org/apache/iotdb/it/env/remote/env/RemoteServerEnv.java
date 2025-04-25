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

package org.apache.iotdb.it.env.remote.env;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.env.remote.config.RemoteClusterConfig;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;
import org.apache.iotdb.itbase.env.ClusterConfig;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.jdbc.Config.VERSION;

public class RemoteServerEnv implements BaseEnv {

  private final String ip_addr = System.getProperty("RemoteIp", "127.0.0.1");
  private final String port = System.getProperty("RemotePort", "6667");
  private final String configNodeMetricPort =
      System.getProperty("RemoteConfigNodeMetricPort", "9091");

  private final String dataNodeMetricPort = System.getProperty("RemoteDataNodeMetricPort", "9093");
  private final String user = System.getProperty("RemoteUser", "root");
  private final String password = System.getProperty("RemotePassword", "root");
  private IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;
  private RemoteClusterConfig clusterConfig = new RemoteClusterConfig();

  @Override
  public void initClusterEnvironment() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.init;");
      statement.execute("DELETE DATABASE root.init;");
    } catch (Exception e) {
      e.printStackTrace();
      throw new AssertionError(e.getMessage());
    }
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    initClusterEnvironment();
  }

  @Override
  public void initClusterEnvironment(
      int configNodesNum, int dataNodesNum, int testWorkingRetryCount) {
    initClusterEnvironment();
  }

  @Override
  public void cleanClusterEnvironment() {
    if (clientManager != null) {
      clientManager.close();
    }
    clusterConfig = new RemoteClusterConfig();
  }

  @Override
  public ClusterConfig getConfig() {
    return clusterConfig;
  }

  @Override
  public List<String> getMetricPrometheusReporterContents(String authHeader) {
    List<String> result = new ArrayList<>();
    result.add(
        getUrlContent(
            Config.IOTDB_HTTP_URL_PREFIX + ip_addr + ":" + configNodeMetricPort + "/metrics",
            authHeader));
    result.add(
        getUrlContent(
            Config.IOTDB_HTTP_URL_PREFIX + ip_addr + ":" + dataNodeMetricPort + "/metrics",
            authHeader));
    return result;
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    Connection connection;
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX + ip_addr + ":" + port, this.user, this.password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new AssertionError();
    }
    return connection;
  }

  @Override
  public Connection getWriteOnlyConnectionWithSpecifiedDataNode(
      DataNodeWrapper dataNode, String username, String password) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getConnectionWithSpecifiedDataNode(
      DataNodeWrapper dataNode, String username, String password) throws SQLException {
    return getConnection(username, password);
  }

  @Override
  public Connection getConnection(Constant.Version version, String username, String password)
      throws SQLException {
    Connection connection;
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
      throw new AssertionError();
    }
    return connection;
  }

  @Override
  public Connection getConnection(DataNodeWrapper dataNodeWrapper, String username, String password)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setTestMethodName(String testCaseName) {
    // Do nothing
  }

  @Override
  public void dumpTestJVMSnapshot() {
    // Do nothing
  }

  @Override
  public List<AbstractNodeWrapper> getNodeWrapperList() {
    return null;
  }

  @Override
  public List<ConfigNodeWrapper> getConfigNodeWrapperList() {
    return null;
  }

  @Override
  public List<DataNodeWrapper> getDataNodeWrapperList() {
    return null;
  }

  @Override
  public IConfigNodeRPCService.Iface getLeaderConfigNodeConnection() throws ClientManagerException {
    return clientManager.borrowClient(new TEndPoint(ip_addr, 10710));
  }

  @Override
  public ISessionPool getSessionPool(int maxSize) {
    return new SessionPool(
        SessionConfig.DEFAULT_HOST,
        SessionConfig.DEFAULT_PORT,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        false,
        null,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  @Override
  public ISession getSessionConnection() throws IoTDBConnectionException {
    Session session = new Session(ip_addr, Integer.parseInt(port));
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(ZoneId zoneId) throws IoTDBConnectionException {
    Session session =
        new Session(
            ip_addr,
            Integer.parseInt(port),
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD,
            zoneId);
    session.open();
    return session;
  }

  public ISession getSessionConnection(String userName, String password)
      throws IoTDBConnectionException {
    Session session = new Session(ip_addr, Integer.parseInt(port), userName, password);
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(List<String> nodeUrls) throws IoTDBConnectionException {
    Session session =
        new Session(
            Collections.singletonList(ip_addr + ":" + port),
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD,
            SessionConfig.DEFAULT_FETCH_SIZE,
            null,
            SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
            SessionConfig.DEFAULT_MAX_FRAME_SIZE,
            SessionConfig.DEFAULT_REDIRECTION_MODE,
            SessionConfig.DEFAULT_VERSION);
    session.open();
    return session;
  }

  @Override
  public int getFirstLeaderSchemaRegionDataNodeIndex() {
    return -1;
  }

  @Override
  public int getLeaderConfigNodeIndex() {
    return -1;
  }

  @Override
  public void startConfigNode(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startAllConfigNodes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownConfigNode(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownAllConfigNodes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownForciblyAllConfigNodes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void ensureNodeStatus(List<BaseNodeWrapper> nodes, List<NodeStatus> targetStatus) {
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
  public void startAllDataNodes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownDataNode(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownAllDataNodes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdownForciblyAllDataNodes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMqttPort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getIP() {
    return ip_addr;
  }

  @Override
  public String getPort() {
    return port;
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

  @Override
  public Optional<DataNodeWrapper> dataNodeIdToWrapper(int nodeId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerConfigNodeKillPoints(List<String> killPoints) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerDataNodeKillPoints(List<String> killPoints) {
    throw new UnsupportedOperationException();
  }
}
