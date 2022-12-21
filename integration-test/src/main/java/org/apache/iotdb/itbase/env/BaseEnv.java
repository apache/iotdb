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
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.ISession;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConfig;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.util.Version;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;

public interface BaseEnv {

  void initBeforeClass() throws InterruptedException;

  void initClusterEnvironment(int configNodesNum, int dataNodesNum);

  void cleanAfterClass();

  void initBeforeTest() throws InterruptedException;

  void cleanAfterTest();

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

  void setConfigNodeWrapperList(List<ConfigNodeWrapper> configNodeWrapperList);

  List<DataNodeWrapper> getDataNodeWrapperList();

  void setDataNodeWrapperList(List<DataNodeWrapper> dataNodeWrapperList);

  IConfigNodeRPCService.Iface getLeaderConfigNodeConnection()
      throws ClientManagerException, IOException, InterruptedException;

  default ISession getSessionConnection() throws IoTDBConnectionException {
    return getSessionConnection(
        SessionConfig.DEFAULT_HOST,
        SessionConfig.DEFAULT_PORT,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        SessionConfig.DEFAULT_FETCH_SIZE,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  default ISession getSessionConnection(
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

  default ISession getSessionConnection(List<String> nodeUrls) throws IoTDBConnectionException {
    Session session =
        new Session(
            nodeUrls,
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

  default SessionPool getSessionPool(int maxSize) {
    return getSessionPool(
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

  default SessionPool getSessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    SessionPool pool =
        new SessionPool(
            host,
            port,
            user,
            password,
            maxSize,
            fetchSize,
            waitToGetSessionTimeoutInMs,
            enableCompression,
            zoneId,
            enableRedirection,
            connectionTimeoutInMs,
            version,
            thriftDefaultBufferSize,
            thriftMaxFrameSize);
    return pool;
  }

  /** @return The index of ConfigNode-Leader in configNodeWrapperList */
  int getLeaderConfigNodeIndex() throws IOException, InterruptedException;

  /** Start an existed ConfigNode */
  void startConfigNode(int index);

  /** Shutdown an existed ConfigNode */
  void shutdownConfigNode(int index);

  /** @return The ConfigNodeWrapper of the specified index */
  ConfigNodeWrapper getConfigNodeWrapper(int index);

  /** @return The DataNodeWrapper of the specified indexx */
  DataNodeWrapper getDataNodeWrapper(int index);

  /** @return A random available ConfigNodeWrapper */
  ConfigNodeWrapper generateRandomConfigNodeWrapper();

  /** @return A random available ConfigNodeWrapper */
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

  String getLibPath();
}
