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

package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;
import org.apache.iotdb.itbase.env.ClusterConfig;
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
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.thrift.TException;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.it.env.cluster.AbstractNodeWrapper.templateNodeLibPath;
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
  public static final String templateNodePath =
      System.getProperty("user.dir") + File.separator + "target" + File.separator + "template-node";

  private IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;

  /**
   * This config object stores the properties set by developers during the test. It will be cleared
   * after each call of cleanupEnvironment.
   */
  private MppClusterConfig clusterConfig;

  public AbstractEnv() {
    clusterConfig = new MppClusterConfig();
  }

  @Override
  public ClusterConfig getConfig() {
    return clusterConfig;
  }

  protected void initEnvironment(int configNodesNum, int dataNodesNum) {
    this.configNodeWrapperList = new ArrayList<>();
    this.dataNodeWrapperList = new ArrayList<>();

    clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());

    final String testClassName = getTestClassName();
    final String testMethodName = getTestMethodName();

    ConfigNodeWrapper seedConfigNodeWrapper =
        new ConfigNodeWrapper(
            true, "", testClassName, testMethodName, EnvUtils.searchAvailablePorts());
    seedConfigNodeWrapper.createDir();
    seedConfigNodeWrapper.changeConfig(
        (MppConfigNodeConfig) clusterConfig.getConfigNodeConfig(),
        (MppCommonConfig) clusterConfig.getConfigNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getConfigNodeJVMConfig());
    seedConfigNodeWrapper.start();
    String targetConfigNode = seedConfigNodeWrapper.getIpAndPortString();
    this.configNodeWrapperList.add(seedConfigNodeWrapper);

    // Check if the Seed-ConfigNode started successfully
    try (SyncConfigNodeIServiceClient ignored =
        (SyncConfigNodeIServiceClient) getLeaderConfigNodeConnection()) {
      // Do nothing
      logger.info("The Seed-ConfigNode started successfully!");
    } catch (Exception e) {
      logger.error("Failed to get connection to the Seed-ConfigNode", e);
    }

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
      configNodeWrapper.changeConfig(
          (MppConfigNodeConfig) clusterConfig.getConfigNodeConfig(),
          (MppCommonConfig) clusterConfig.getConfigNodeCommonConfig(),
          (MppJVMConfig) clusterConfig.getConfigNodeJVMConfig());
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
      dataNodeWrapper.changeConfig(
          (MppDataNodeConfig) clusterConfig.getDataNodeConfig(),
          (MppCommonConfig) clusterConfig.getDataNodeCommonConfig(),
          (MppJVMConfig) clusterConfig.getDataNodeJVMConfig());
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
      logger.error("exception in testWorking of ClusterID, message: {}", e.getMessage(), e);
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
            if (NodeStatus.Unknown.getStatus().equals(status)) {
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

    if (lastException != null) {
      throw lastException;
    }
  }

  @Override
  public void cleanClusterEnvironment() {
    for (AbstractNodeWrapper nodeWrapper :
        Stream.concat(this.dataNodeWrapperList.stream(), this.configNodeWrapperList.stream())
            .collect(Collectors.toList())) {
      nodeWrapper.stop();
      nodeWrapper.destroyDir();
      String lockPath = EnvUtils.getLockFilePath(nodeWrapper.getPort());
      if (!new File(lockPath).delete()) {
        logger.error("Delete lock file {} failed", lockPath);
      }
    }
    clientManager.close();
    testMethodName = null;
    clusterConfig = new MppClusterConfig();
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

  @Override
  public ISession getSessionConnection(List<String> nodeUrls) throws IoTDBConnectionException {
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

  @Override
  public ISessionPool getSessionPool(int maxSize) {
    DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return new SessionPool(
        dataNode.getIp(),
        dataNode.getPort(),
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        maxSize);
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

  @Override
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
  public List<DataNodeWrapper> getDataNodeWrapperList() {
    return dataNodeWrapperList;
  }

  /**
   * Get connection to ConfigNode-Leader in ClusterIT environment
   *
   * <p>Notice: The caller should always use try-with-resource to invoke this interface in order to
   * return client to ClientPool automatically
   *
   * @return SyncConfigNodeIServiceClient that connects to the ConfigNode-Leader
   */
  @Override
  public IConfigNodeRPCService.Iface getLeaderConfigNodeConnection()
      throws IOException, InterruptedException {
    Exception lastException = null;
    ConfigNodeWrapper lastErrorNode = null;
    for (int i = 0; i < 30; i++) {
      for (ConfigNodeWrapper configNodeWrapper : configNodeWrapperList) {
        try {
          lastErrorNode = configNodeWrapper;
          SyncConfigNodeIServiceClient client =
              clientManager.borrowClient(
                  new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()));
          TShowClusterResp resp = client.showCluster();

          if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            // Only the ConfigNodeClient who connects to the ConfigNode-leader
            // will respond the SUCCESS_STATUS
            return client;
          } else {
            // Return client otherwise
            client.close();
            throw new Exception(
                "Bad status: "
                    + resp.getStatus().getCode()
                    + " message: "
                    + resp.getStatus().getMessage());
          }
        } catch (Exception e) {
          lastException = e;
        }

        // Sleep 1s before next retry
        TimeUnit.SECONDS.sleep(1);
      }
    }
    if (lastErrorNode != null) {
      throw new IOException(
          "Failed to get connection to ConfigNode-Leader. Last error configNode: "
              + lastErrorNode.getIpAndPortString(),
          lastException);
    } else {
      throw new IOException("Empty configNode set");
    }
  }

  @Override
  public int getLeaderConfigNodeIndex() throws IOException, InterruptedException {
    Exception lastException = null;
    ConfigNodeWrapper lastErrorNode = null;
    for (int retry = 0; retry < 30; retry++) {
      for (int configNodeId = 0; configNodeId < configNodeWrapperList.size(); configNodeId++) {
        ConfigNodeWrapper configNodeWrapper = configNodeWrapperList.get(configNodeId);
        lastErrorNode = configNodeWrapper;
        try (SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()))) {
          TShowClusterResp resp = client.showCluster();
          // Only the ConfigNodeClient who connects to the ConfigNode-leader
          // will respond the SUCCESS_STATUS
          if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return configNodeId;
          } else {
            throw new Exception(
                "Bad status: "
                    + resp.getStatus().getCode()
                    + " message: "
                    + resp.getStatus().getMessage());
          }
        } catch (Exception e) {
          lastException = e;
        }

        // Sleep 1s before next retry
        TimeUnit.SECONDS.sleep(1);
      }
    }

    if (lastErrorNode != null) {
      throw new IOException(
          "Failed to get the index of ConfigNode-Leader. Last error configNode: "
              + lastErrorNode.getIpAndPortString(),
          lastException);
    } else {
      throw new IOException("Empty configNode set");
    }
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
  public ConfigNodeWrapper getConfigNodeWrapper(int index) {
    return configNodeWrapperList.get(index);
  }

  @Override
  public DataNodeWrapper getDataNodeWrapper(int index) {
    return dataNodeWrapperList.get(index);
  }

  @Override
  public ConfigNodeWrapper generateRandomConfigNodeWrapper() {
    ConfigNodeWrapper newConfigNodeWrapper =
        new ConfigNodeWrapper(
            false,
            configNodeWrapperList.get(0).getIpAndPortString(),
            getTestClassName(),
            getTestMethodName(),
            EnvUtils.searchAvailablePorts());
    configNodeWrapperList.add(newConfigNodeWrapper);
    newConfigNodeWrapper.createDir();
    newConfigNodeWrapper.changeConfig(
        (MppConfigNodeConfig) clusterConfig.getConfigNodeConfig(),
        (MppCommonConfig) clusterConfig.getConfigNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getConfigNodeJVMConfig());
    return newConfigNodeWrapper;
  }

  @Override
  public DataNodeWrapper generateRandomDataNodeWrapper() {
    DataNodeWrapper newDataNodeWrapper =
        new DataNodeWrapper(
            configNodeWrapperList.get(0).getIpAndPortString(),
            getTestClassName(),
            getTestMethodName(),
            EnvUtils.searchAvailablePorts());
    dataNodeWrapperList.add(newDataNodeWrapper);
    newDataNodeWrapper.createDir();
    newDataNodeWrapper.changeConfig(
        (MppDataNodeConfig) clusterConfig.getDataNodeConfig(),
        (MppCommonConfig) clusterConfig.getDataNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getDataNodeJVMConfig());
    return newDataNodeWrapper;
  }

  @Override
  public void registerNewDataNode(boolean isNeedVerify) {
    registerNewDataNode(generateRandomDataNodeWrapper(), isNeedVerify);
  }

  @Override
  public void registerNewConfigNode(boolean isNeedVerify) {
    registerNewConfigNode(generateRandomConfigNodeWrapper(), isNeedVerify);
  }

  @Override
  public void registerNewConfigNode(ConfigNodeWrapper newConfigNodeWrapper, boolean isNeedVerify) {
    // Start new ConfigNode
    RequestDelegate<Void> configNodeDelegate =
        new ParallelRequestDelegate<>(
            Collections.singletonList(newConfigNodeWrapper.getIpAndPortString()),
            NODE_START_TIMEOUT);
    configNodeDelegate.addRequest(
        () -> {
          newConfigNodeWrapper.start();
          return null;
        });

    try {
      configNodeDelegate.requestAll();
    } catch (SQLException e) {
      logger.error("Start configNode failed", e);
      fail();
    }

    if (isNeedVerify) {
      // Test whether register success
      testWorking();
    }
  }

  @Override
  public void registerNewDataNode(DataNodeWrapper newDataNodeWrapper, boolean isNeedVerify) {
    // Start new DataNode
    List<String> dataNodeEndpoints =
        Collections.singletonList(newDataNodeWrapper.getIpAndPortString());
    RequestDelegate<Void> dataNodesDelegate =
        new ParallelRequestDelegate<>(dataNodeEndpoints, NODE_START_TIMEOUT);
    dataNodesDelegate.addRequest(
        () -> {
          newDataNodeWrapper.start();
          return null;
        });
    try {
      dataNodesDelegate.requestAll();
    } catch (SQLException e) {
      logger.error("Start dataNodes failed", e);
      fail();
    }

    if (isNeedVerify) {
      // Test whether register success
      testWorking();
    }
  }

  @Override
  public void startDataNode(int index) {
    dataNodeWrapperList.get(index).start();
  }

  @Override
  public void shutdownDataNode(int index) {
    dataNodeWrapperList.get(index).stop();
  }

  @Override
  public void ensureNodeStatus(List<BaseNodeWrapper> nodes, List<NodeStatus> targetStatus)
      throws IllegalStateException {
    Throwable lastException = null;
    for (int i = 0; i < 30; i++) {
      try (SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        List<String> errorMessages = new ArrayList<>(nodes.size());
        Map<String, Integer> nodeIds = new HashMap<>(nodes.size());
        TShowClusterResp showClusterResp = client.showCluster();
        for (TConfigNodeLocation node : showClusterResp.getConfigNodeList()) {
          nodeIds.put(
              node.getInternalEndPoint().getIp() + ":" + node.getInternalEndPoint().getPort(),
              node.getConfigNodeId());
        }
        for (TDataNodeLocation node : showClusterResp.getDataNodeList()) {
          nodeIds.put(
              node.getClientRpcEndPoint().getIp() + ":" + node.getClientRpcEndPoint().getPort(),
              node.getDataNodeId());
        }
        for (int j = 0; j < nodes.size(); j++) {
          String endpoint = nodes.get(j).getIpAndPortString();
          if (!nodeIds.containsKey(endpoint)) {
            // Node not exist
            // Notice: Never modify this line, since the NodeLocation might be modified in IT
            errorMessages.add("The node " + nodes.get(j).getIpAndPortString() + " is not found!");
            continue;
          }
          String status = showClusterResp.getNodeStatus().get(nodeIds.get(endpoint));
          if (!targetStatus.get(j).getStatus().equals(status)) {
            // Error status
            errorMessages.add(
                String.format(
                    "Node %s is in status %s, but expected %s",
                    endpoint, status, targetStatus.get(j)));
          }
        }
        if (errorMessages.isEmpty()) {
          return;
        } else {
          lastException = new IllegalStateException(String.join(". ", errorMessages));
        }
      } catch (TException | ClientManagerException | IOException | InterruptedException e) {
        lastException = e;
      }
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IllegalStateException(lastException);
  }

  @Override
  public int getMqttPort() {
    int randomIndex = new Random(System.currentTimeMillis()).nextInt(dataNodeWrapperList.size());
    return dataNodeWrapperList.get(randomIndex).getMqttPort();
  }

  @Override
  public String getIP() {
    return dataNodeWrapperList.get(0).getIp();
  }

  @Override
  public String getPort() {
    return String.valueOf(dataNodeWrapperList.get(0).getPort());
  }

  @Override
  public String getSbinPath() {
    return templateNodePath + File.separator + "sbin";
  }

  @Override
  public String getToolsPath() {
    return templateNodePath + File.separator + "tools";
  }

  @Override
  public String getLibPath() {
    return templateNodeLibPath;
  }
}
