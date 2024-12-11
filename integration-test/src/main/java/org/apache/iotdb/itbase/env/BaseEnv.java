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

package org.apache.iotdb.itbase.env;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public interface BaseEnv {

  String TREE_SQL_DIALECT = "tree";

  String TABLE_SQL_DIALECT = "table";

  /** Init a cluster with default number of ConfigNodes and DataNodes. */
  void initClusterEnvironment();

  /**
   * Init a cluster with the specified number of ConfigNodes and DataNodes.
   *
   * @param configNodesNum the number of ConfigNodes.
   * @param dataNodesNum the number of DataNodes.
   */
  void initClusterEnvironment(int configNodesNum, int dataNodesNum);

  /**
   * Init a cluster with the specified number of ConfigNodes and DataNodes.
   *
   * @param configNodesNum the number of ConfigNodes.
   * @param dataNodesNum the number of DataNodes.
   * @param testWorkingRetryCount the retry count when testing the availability of cluster
   */
  void initClusterEnvironment(int configNodesNum, int dataNodesNum, int testWorkingRetryCount);

  /** Destroy the cluster and all the configurations. */
  void cleanClusterEnvironment();

  /** Return the {@link ClusterConfig} for developers to set values before test. */
  ClusterConfig getConfig();

  default String getUrlContent(String urlStr) {
    StringBuilder sb = new StringBuilder();
    try {
      URL url = new URL(urlStr);
      HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
      if (httpConnection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        InputStream in = httpConnection.getInputStream();
        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader bufr = new BufferedReader(isr);
        String str;
        while ((str = bufr.readLine()) != null) {
          sb.append(str);
          sb.append('\n');
        }
        bufr.close();
      } else {
        return null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
    return sb.toString();
  }

  /** Return the content of prometheus */
  List<String> getMetricPrometheusReporterContents();

  default Connection getConnection() throws SQLException {
    return getConnection(
        SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, TREE_SQL_DIALECT);
  }

  default Connection getTableConnection() throws SQLException {
    return getConnection(
        SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, TABLE_SQL_DIALECT);
  }

  default Connection getConnection(String sqlDialect) throws SQLException {
    return getConnection(SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
  }

  default Connection getConnection(Constant.Version version) throws SQLException {
    return getConnection(
        version, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, TREE_SQL_DIALECT);
  }

  default Connection getConnection(Constant.Version version, String sqlDialect)
      throws SQLException {
    return getConnection(
        version, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
  }

  default Connection getConnection(Constant.Version version, String username, String password)
      throws SQLException {
    return getConnection(version, username, password, TREE_SQL_DIALECT);
  }

  Connection getConnection(
      Constant.Version version, String username, String password, String sqlDialect)
      throws SQLException;

  default Connection getConnection(String username, String password) throws SQLException {
    return getConnection(username, password, TREE_SQL_DIALECT);
  }

  Connection getConnection(String username, String password, String sqlDialect) throws SQLException;

  default Connection getWriteOnlyConnectionWithSpecifiedDataNode(DataNodeWrapper dataNode)
      throws SQLException {
    return getWriteOnlyConnectionWithSpecifiedDataNode(
        dataNode, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, TREE_SQL_DIALECT);
  }

  default Connection getWriteOnlyConnectionWithSpecifiedDataNode(
      DataNodeWrapper dataNode, String sqlDialect) throws SQLException {
    return getWriteOnlyConnectionWithSpecifiedDataNode(
        dataNode, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
  }

  // This is useful when you shut down a dataNode.
  Connection getWriteOnlyConnectionWithSpecifiedDataNode(
      DataNodeWrapper dataNode, String username, String password, String sqlDialect)
      throws SQLException;

  default Connection getConnectionWithSpecifiedDataNode(DataNodeWrapper dataNode)
      throws SQLException {
    return getConnectionWithSpecifiedDataNode(
        dataNode, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  Connection getConnectionWithSpecifiedDataNode(
      DataNodeWrapper dataNode, String username, String password) throws SQLException;

  void setTestMethodName(String testCaseName);

  void dumpTestJVMSnapshot();

  List<AbstractNodeWrapper> getNodeWrapperList();

  List<ConfigNodeWrapper> getConfigNodeWrapperList();

  List<DataNodeWrapper> getDataNodeWrapperList();

  IConfigNodeRPCService.Iface getLeaderConfigNodeConnection()
      throws ClientManagerException, IOException, InterruptedException;

  ISessionPool getSessionPool(int maxSize);

  ITableSessionPool getTableSessionPool(int maxSize);

  ITableSessionPool getTableSessionPool(int maxSize, String database);

  ISession getSessionConnection() throws IoTDBConnectionException;

  ISession getSessionConnection(String userName, String password) throws IoTDBConnectionException;

  ISession getSessionConnection(List<String> nodeUrls) throws IoTDBConnectionException;

  ITableSession getTableSessionConnection() throws IoTDBConnectionException;

  ITableSession getTableSessionConnectionWithDB(String database) throws IoTDBConnectionException;

  ITableSession getTableSessionConnection(List<String> nodeUrls) throws IoTDBConnectionException;

  /**
   * Get the index of the first dataNode with a SchemaRegion leader.
   *
   * @return The index of DataNode with SchemaRegion-leader in dataNodeWrapperList
   */
  int getFirstLeaderSchemaRegionDataNodeIndex() throws IOException, InterruptedException;

  /**
   * Get the index of the ConfigNode leader.
   *
   * @return The index of ConfigNode-Leader in configNodeWrapperList
   */
  int getLeaderConfigNodeIndex() throws IOException, InterruptedException;

  default IConfigNodeRPCService.Iface getConfigNodeConnection(int index) throws Exception {
    throw new UnsupportedOperationException();
  }

  /** Start an existed ConfigNode. */
  void startConfigNode(int index);

  /** Start all existed ConfigNodes. */
  void startAllConfigNodes();

  /** Shutdown an existed ConfigNode. */
  void shutdownConfigNode(int index);

  /** Shutdown all existed ConfigNodes. */
  void shutdownAllConfigNodes();

  /**
   * Ensure all the nodes being in the corresponding status.
   *
   * @param nodes the nodes list to query.
   * @param targetStatus the target {@link NodeStatus} of each node. It should have the same length
   *     with nodes.
   * @throws IllegalStateException if there are some nodes not in the targetStatus after a period
   *     times of check.
   */
  void ensureNodeStatus(List<BaseNodeWrapper> nodes, List<NodeStatus> targetStatus)
      throws IllegalStateException;

  /**
   * Get the {@link ConfigNodeWrapper} of the specified index.
   *
   * @return The ConfigNodeWrapper of the specified index
   */
  ConfigNodeWrapper getConfigNodeWrapper(int index);

  /**
   * Get the {@link DataNodeWrapper} of the specified index.
   *
   * @return The DataNodeWrapper of the specified index
   */
  DataNodeWrapper getDataNodeWrapper(int index);

  /**
   * Get a {@link ConfigNodeWrapper} randomly.
   *
   * @return A random available ConfigNodeWrapper
   */
  ConfigNodeWrapper generateRandomConfigNodeWrapper();

  /**
   * Get a {@link DataNodeWrapper} randomly.
   *
   * @return A random available ConfigNodeWrapper
   */
  DataNodeWrapper generateRandomDataNodeWrapper();

  /** Register a new DataNode with random ports. */
  void registerNewDataNode(boolean isNeedVerify);

  /** Register a new DataNode with specified DataNodeWrapper. */
  void registerNewDataNode(DataNodeWrapper newDataNodeWrapper, boolean isNeedVerify);

  /** Register a new ConfigNode with random ports. */
  void registerNewConfigNode(boolean isNeedVerify);

  /** Register a new DataNode with specified ConfigNodeWrapper. */
  void registerNewConfigNode(ConfigNodeWrapper newConfigNodeWrapper, boolean isNeedVerify);

  /** Start an existed DataNode. */
  void startDataNode(int index);

  /** Start all existed DataNodes. */
  void startAllDataNodes();

  /** Shutdown an existed DataNode. */
  void shutdownDataNode(int index);

  /** Shutdown all existed DataNodes. */
  void shutdownAllDataNodes();

  int getMqttPort();

  String getIP();

  String getPort();

  String getSbinPath();

  String getToolsPath();

  String getLibPath();

  Optional<DataNodeWrapper> dataNodeIdToWrapper(int nodeId);

  void registerConfigNodeKillPoints(List<String> killPoints);

  void registerDataNodeKillPoints(List<String> killPoints);

  static Properties constructProperties(String username, String password, String sqlDialect) {
    Properties info = new Properties();

    if (username != null) {
      info.put("user", username);
    }
    if (password != null) {
      info.put("password", password);
    }
    if (sqlDialect != null) {
      info.put(Config.SQL_DIALECT, sqlDialect);
    }
    return info;
  }
}
