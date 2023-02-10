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
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.it.env.cluster.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.DataNodeWrapper;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface BaseEnv {

  /** Init a cluster with default number of ConfigNodes and DataNodes. */
  void initClusterEnvironment();

  /**
   * Init a cluster with the specified number of ConfigNodes and DataNodes.
   *
   * @param configNodesNum the number of ConfigNodes.
   * @param dataNodesNum the number of DataNodes.
   */
  void initClusterEnvironment(int configNodesNum, int dataNodesNum);

  /** Destroy the cluster and all the configurations. */
  void cleanClusterEnvironment();

  /** Return the {@link ClusterConfig} for developers to set values before test. */
  ClusterConfig getConfig();

  default Connection getConnection() throws SQLException {
    return getConnection("root", "root");
  }

  Connection getConnection(String username, String password) throws SQLException;

  default Connection getConnection(Constant.Version version) throws SQLException {
    return getConnection(version, "root", "root");
  }

  Connection getConnection(Constant.Version version, String username, String password)
      throws SQLException;

  void setTestMethodName(String testCaseName);

  void dumpTestJVMSnapshot();

  List<ConfigNodeWrapper> getConfigNodeWrapperList();

  List<DataNodeWrapper> getDataNodeWrapperList();

  IConfigNodeRPCService.Iface getLeaderConfigNodeConnection()
      throws ClientManagerException, IOException, InterruptedException;

  ISessionPool getSessionPool(int maxSize);

  ISession getSessionConnection() throws IoTDBConnectionException;

  ISession getSessionConnection(List<String> nodeUrls) throws IoTDBConnectionException;

  /**
   * Get the index of the ConfigNode leader.
   *
   * @return The index of ConfigNode-Leader in configNodeWrapperList
   */
  int getLeaderConfigNodeIndex() throws IOException, InterruptedException;

  /** Start an existed ConfigNode */
  void startConfigNode(int index);

  /** Shutdown an existed ConfigNode */
  void shutdownConfigNode(int index);

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
   * @return The DataNodeWrapper of the specified indexx
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

  /** Register a new DataNode with random ports */
  void registerNewDataNode(boolean isNeedVerify);

  /** Register a new ConfigNode with random ports */
  void registerNewConfigNode(boolean isNeedVerify);

  /** Register a new DataNode with specified DataNodeWrapper */
  void registerNewDataNode(DataNodeWrapper newDataNodeWrapper, boolean isNeedVerify);

  /** Register a new DataNode with specified ConfigNodeWrapper */
  void registerNewConfigNode(ConfigNodeWrapper newConfigNodeWrapper, boolean isNeedVerify);

  /** Start an existed DataNode */
  void startDataNode(int index);

  /** Shutdown an existed DataNode */
  void shutdownDataNode(int index);

  int getMqttPort();

  String getIP();

  String getPort();

  String getSbinPath();

  String getToolsPath();

  String getLibPath();
}
