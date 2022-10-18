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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;
import org.apache.iotdb.itbase.runtime.NodeConnection;
import org.apache.iotdb.itbase.runtime.ParallelRequestDelegate;
import org.apache.iotdb.itbase.runtime.RequestDelegate;
import org.apache.iotdb.itbase.runtime.SerialRequestDelegate;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.ISession;
import org.apache.iotdb.session.Session;

import org.apache.thrift.TException;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

public abstract class AbstractEnv implements BaseEnv {
  private static final Logger logger = IoTDBTestLogger.logger;
  private final int NODE_START_TIMEOUT = 100;
  private final int PROBE_TIMEOUT_MS = 2000;
  private final int NODE_NETWORK_TIMEOUT_MS = 65_000;
  private final Random rand = new Random();
  protected List<ConfigNodeWrapper> configNodeWrapperList = Collections.emptyList();
  protected List<DataNodeWrapper> dataNodeWrapperList = Collections.emptyList();
  protected String testMethodName = null;

  protected void initEnvironment(int configNodesNum, int dataNodesNum) {
    this.configNodeWrapperList = new ArrayList<>();
    this.dataNodeWrapperList = new ArrayList<>();

    final String testClassName = getTestClassName();
    final String testMethodName = getTestMethodName();

    ConfigNodeWrapper seedConfigNodeWrapper =
        new ConfigNodeWrapper(
            true, "", testClassName, testMethodName, EnvUtils.searchAvailablePorts());
    seedConfigNodeWrapper.createDir();
    seedConfigNodeWrapper.changeConfig(ConfigFactory.getConfig().getConfignodeProperties());
    seedConfigNodeWrapper.start();
    String targetConfigNode = seedConfigNodeWrapper.getIpAndPortString();
    this.configNodeWrapperList.add(seedConfigNodeWrapper);

    List<String> configNodeEndpoints = new ArrayList<>();
    RequestDelegate<Void> configNodesDelegate = new SerialRequestDelegate<>(configNodeEndpoints);
    for (int i = 1; i < configNodesNum; i++) {
      ConfigNodeWrapper configNodeWrapper =
          new ConfigNodeWrapper(
              false,
              targetConfigNode,
              testClassName,
              testMethodName,
              EnvUtils.searchAvailablePorts());
      this.configNodeWrapperList.add(configNodeWrapper);
      configNodeEndpoints.add(configNodeWrapper.getIpAndPortString());
      configNodeWrapper.createDir();
      configNodeWrapper.changeConfig(ConfigFactory.getConfig().getConfignodeProperties());
      configNodesDelegate.addRequest(
          () -> {
            configNodeWrapper.start();
            return null;
          });
    }
    try {
      configNodesDelegate.requestAll();
    } catch (SQLException e) {
      logger.error("Start configNodes failed", e);
      fail();
    }

    List<String> dataNodeEndpoints = new ArrayList<>();
    RequestDelegate<Void> dataNodesDelegate =
        new ParallelRequestDelegate<>(dataNodeEndpoints, NODE_START_TIMEOUT);
    for (int i = 0; i < dataNodesNum; i++) {
      DataNodeWrapper dataNodeWrapper =
          new DataNodeWrapper(
              targetConfigNode, testClassName, testMethodName, EnvUtils.searchAvailablePorts());
      this.dataNodeWrapperList.add(dataNodeWrapper);
      dataNodeEndpoints.add(dataNodeWrapper.getIpAndPortString());
      dataNodeWrapper.createDir();
      dataNodeWrapper.changeConfig(ConfigFactory.getConfig().getEngineProperties());
      dataNodesDelegate.addRequest(
          () -> {
            dataNodeWrapper.start();
            return null;
          });
    }

    try {
      dataNodesDelegate.requestAll();
    } catch (SQLException e) {
      logger.error("Start dataNodes failed", e);
      fail();
    }

    testWorking();
  }

  private void cleanupEnvironment() {
    for (AbstractNodeWrapper nodeWrapper :
        Stream.concat(this.dataNodeWrapperList.stream(), this.configNodeWrapperList.stream())
            .collect(Collectors.toList())) {
      nodeWrapper.stop();
      nodeWrapper.waitingToShutDown();
      nodeWrapper.destroyDir();
      String lockPath = EnvUtils.getLockFilePath(nodeWrapper.getPort());
      if (!new File(lockPath).delete()) {
        logger.error("Delete lock file {} failed", lockPath);
      }
    }
    testMethodName = null;
  }

  public String getTestClassName() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement stackTraceElement : stack) {
      String className = stackTraceElement.getClassName();
      if (className.endsWith("IT")) {
        return className.substring(className.lastIndexOf(".") + 1);
      }
    }
    return "UNKNOWN-IT";
  }

  public void testWorking() {
    logger.info("Testing DataNode connection...");
    List<String> endpoints =
        dataNodeWrapperList.stream()
            .map(DataNodeWrapper::getIpAndPortString)
            .collect(Collectors.toList());
    RequestDelegate<Void> testDelegate =
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT);
    for (DataNodeWrapper dataNode : dataNodeWrapperList) {
      final String dataNodeEndpoint = dataNode.getIpAndPortString();
      testDelegate.addRequest(
          () -> {
            Exception lastException = null;
            for (int i = 0; i < 30; i++) {
              try (Connection ignored = getConnection(dataNodeEndpoint, PROBE_TIMEOUT_MS)) {
                logger.info("Successfully connecting to DataNode: {}.", dataNodeEndpoint);
                return null;
              } catch (Exception e) {
                lastException = e;
                TimeUnit.SECONDS.sleep(1L);
              }
            }
            throw lastException;
          });
    }
    try {
      long startTime = System.currentTimeMillis();
      testDelegate.requestAll();
      if (!configNodeWrapperList.isEmpty()) {
        checkNodeHeartbeat();
      }
      logger.info("Start cluster costs: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
    } catch (Exception e) {
      fail("After 30 times retry, the cluster can't work!");
    }
  }

  private void checkNodeHeartbeat() throws Exception {
    logger.info("Testing cluster environment...");
    TShowClusterResp showClusterResp;
    Exception lastException = null;
    boolean flag;
    for (int i = 0; i < 30; i++) {
      try (SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) getLeaderConfigNodeConnection()) {
        flag = true;
        showClusterResp = client.showCluster();

        // Check resp status
        if (showClusterResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          flag = false;
        }

        // Check the number of nodes
        if (showClusterResp.getNodeStatus().size()
            != configNodeWrapperList.size() + dataNodeWrapperList.size()) {
          flag = false;
        }

        // Check the status of nodes
        if (flag) {
          Map<Integer, String> nodeStatus = showClusterResp.getNodeStatus();
          for (String status : nodeStatus.values()) {
            if (!status.equals("Running")) {
              flag = false;
              break;
            }
          }
        }

        if (flag) {
          logger.info("The cluster is now ready for testing!");
          return;
        }
      } catch (Exception e) {
        lastException = e;
      }
      TimeUnit.SECONDS.sleep(1L);
    }
    throw lastException;
  }

  @Override
  public void cleanAfterClass() {
    cleanupEnvironment();
  }

  @Override
  public void cleanAfterTest() {
    cleanupEnvironment();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return new ClusterTestConnection(
        getWriteConnection(null, username, password), getReadConnections(null, username, password));
  }

  private Connection getConnection(String endpoint, int queryTimeout) throws SQLException {
    IoTDBConnection connection =
        (IoTDBConnection)
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + endpoint + getParam(null, queryTimeout),
                System.getProperty("User", "root"),
                System.getProperty("Password", "root"));
    connection.setQueryTimeout(queryTimeout);

    return connection;
  }

  @Override
  public Connection getConnection(Constant.Version version, String username, String password)
      throws SQLException {
    if (System.getProperty("ReadAndVerifyWithMultiNode", "true").equalsIgnoreCase("true")) {
      return new ClusterTestConnection(
          getWriteConnection(version, username, password),
          getReadConnections(version, username, password));
    } else {
      return getWriteConnection(version, username, password).getUnderlyingConnecton();
    }
  }

  @Override
  public ISession getSessionConnection() throws IoTDBConnectionException {
    DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    Session session = new Session(dataNode.getIp(), dataNode.getPort());
    session.open();
    return session;
  }

  protected NodeConnection getWriteConnection(
      Constant.Version version, String username, String password) throws SQLException {
    DataNodeWrapper dataNode;

    if (System.getProperty("RandomSelectWriteNode", "true").equalsIgnoreCase("true")) {
      // Randomly choose a node for handling write requests
      dataNode = this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    } else {
      dataNode = this.dataNodeWrapperList.get(0);
    }

    String endpoint = dataNode.getIp() + ":" + dataNode.getPort();
    Connection writeConnection =
        DriverManager.getConnection(
            Config.IOTDB_URL_PREFIX + endpoint + getParam(version, NODE_NETWORK_TIMEOUT_MS),
            System.getProperty("User", username),
            System.getProperty("Password", password));
    return new NodeConnection(
        endpoint,
        NodeConnection.NodeRole.DATA_NODE,
        NodeConnection.ConnectionRole.WRITE,
        writeConnection);
  }

  protected List<NodeConnection> getReadConnections(
      Constant.Version version, String username, String password) throws SQLException {
    List<String> endpoints = new ArrayList<>();
    ParallelRequestDelegate<NodeConnection> readConnRequestDelegate =
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT);
    for (DataNodeWrapper dataNodeWrapper : this.dataNodeWrapperList) {
      final String endpoint = dataNodeWrapper.getIpAndPortString();
      endpoints.add(endpoint);
      readConnRequestDelegate.addRequest(
          () -> {
            Connection readConnection =
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + endpoint + getParam(version, NODE_NETWORK_TIMEOUT_MS),
                    System.getProperty("User", username),
                    System.getProperty("Password", password));
            return new NodeConnection(
                endpoint,
                NodeConnection.NodeRole.DATA_NODE,
                NodeConnection.ConnectionRole.READ,
                readConnection);
          });
    }
    return readConnRequestDelegate.requestAll();
  }

  private String getParam(Constant.Version version, int timeout) {
    StringBuilder sb = new StringBuilder("?");
    sb.append(Config.NETWORK_TIMEOUT).append("=").append(timeout);
    if (version != null) {
      sb.append("&").append(VERSION).append("=").append(version);
    }
    return sb.toString();
  }

  public String getTestMethodName() {
    return testMethodName;
  }

  @Override
  public void setTestMethodName(String testMethodName) {
    this.testMethodName = testMethodName;
  }

  public void dumpTestJVMSnapshot() {
    for (ConfigNodeWrapper configNodeWrapper : configNodeWrapperList) {
      configNodeWrapper.dumpJVMSnapshot(testMethodName);
    }
    for (DataNodeWrapper dataNodeWrapper : dataNodeWrapperList) {
      dataNodeWrapper.dumpJVMSnapshot(testMethodName);
    }
  }

  @Override
  public List<ConfigNodeWrapper> getConfigNodeWrapperList() {
    return configNodeWrapperList;
  }

  @Override
  public void setConfigNodeWrapperList(List<ConfigNodeWrapper> configNodeWrapperList) {
    this.configNodeWrapperList = configNodeWrapperList;
  }

  @Override
  public List<DataNodeWrapper> getDataNodeWrapperList() {
    return dataNodeWrapperList;
  }

  @Override
  public void setDataNodeWrapperList(List<DataNodeWrapper> dataNodeWrapperList) {
    this.dataNodeWrapperList = dataNodeWrapperList;
  }

  @Override
  public IConfigNodeRPCService.Iface getLeaderConfigNodeConnection()
      throws IOException, InterruptedException {
    IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(
                new DataNodeClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    for (int i = 0; i < 30; i++) {
      for (ConfigNodeWrapper configNodeWrapper : configNodeWrapperList) {
        try (SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()))) {
          TShowClusterResp resp = client.showCluster();
          // Only the ConfigNodeClient who connects to the ConfigNode-leader
          // will respond the SUCCESS_STATUS
          if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            logger.info(
                "Successfully get connection to the leader ConfigNode: {}",
                configNodeWrapper.getIpAndPortString());
            return client;
          }
        } catch (Exception e) {
          logger.error(
              "Borrow ConfigNodeClient from ConfigNode: {} failed because: {}, retrying...",
              configNodeWrapper.getIpAndPortString(),
              e);
        }

        // Sleep 1s before next retry
        TimeUnit.SECONDS.sleep(1);
      }
    }
    throw new IOException("Failed to get connection to ConfigNode-Leader");
  }

  @Override
  public int getLeaderConfigNodeIndex() throws IOException, InterruptedException {
    IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(
                new DataNodeClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    for (int retry = 0; retry < 30; retry++) {
      for (int configNodeId = 0; configNodeId < configNodeWrapperList.size(); configNodeId++) {
        ConfigNodeWrapper configNodeWrapper = configNodeWrapperList.get(configNodeId);
        try (SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()))) {
          TShowClusterResp resp = client.showCluster();
          // Only the ConfigNodeClient who connects to the ConfigNode-leader
          // will respond the SUCCESS_STATUS
          if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return configNodeId;
          }
        } catch (TException | IOException e) {
          logger.error(
              "Borrow ConfigNodeClient from ConfigNode: {} failed because: {}, retrying...",
              configNodeWrapper.getIp(),
              e);
        }

        // Sleep 1s before next retry
        TimeUnit.SECONDS.sleep(1);
      }
    }

    throw new IOException("Failed to get the index of ConfigNode-Leader");
  }

  @Override
  public void startConfigNode(int index) {
    configNodeWrapperList.get(index).start();
  }

  @Override
  public void shutdownConfigNode(int index) {
    configNodeWrapperList.get(index).stop();
  }

  @Override
  public void startDataNode(int index) {
    dataNodeWrapperList.get(index).start();
  }

  public void shutdownDataNode(int index) {
    dataNodeWrapperList.get(index).stop();
  }
}
