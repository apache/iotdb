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

package org.apache.iotdb.it.env.cluster.env;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.config.*;
import org.apache.iotdb.it.env.cluster.node.AINodeWrapper;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;
import org.apache.iotdb.itbase.env.ClusterConfig;
import org.apache.iotdb.itbase.runtime.*;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.*;
import static org.apache.iotdb.jdbc.Config.VERSION;

public abstract class AbstractEnv implements BaseEnv {
  private static final Logger logger = IoTDBTestLogger.logger;

  private final Random rand = new Random();
  protected List<ConfigNodeWrapper> configNodeWrapperList = Collections.emptyList();
  protected List<DataNodeWrapper> dataNodeWrapperList = Collections.emptyList();
  protected List<AINodeWrapper> aiNodeWrapperList = Collections.emptyList();
  protected String testMethodName = null;
  protected int index = 0;
  protected long startTime;
  protected int retryCount = 30;
  private IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;
  private List<String> configNodeKillPoints = new ArrayList<>();
  private List<String> dataNodeKillPoints = new ArrayList<>();

  /**
   * This config object stores the properties set by developers during the test. It will be cleared
   * after each call of cleanupEnvironment.
   */
  private MppClusterConfig clusterConfig;

  // For single environment ITs, time can be unified in this level.
  protected AbstractEnv() {
    this.startTime = System.currentTimeMillis();
    this.clusterConfig = new MppClusterConfig();
  }

  // For multiple environment ITs, time must be consistent across environments.
  protected AbstractEnv(final long startTime) {
    this.startTime = startTime;
    this.clusterConfig = new MppClusterConfig();
  }

  @Override
  public ClusterConfig getConfig() {
    return clusterConfig;
  }

  @Override
  public List<String> getMetricPrometheusReporterContents() {
    final List<String> result = new ArrayList<>();
    // get all report content of confignodes
    for (final ConfigNodeWrapper configNode : this.configNodeWrapperList) {
      final String configNodeMetricContent =
          getUrlContent(
              Config.IOTDB_HTTP_URL_PREFIX
                  + configNode.getIp()
                  + ":"
                  + configNode.getMetricPort()
                  + "/metrics");
      result.add(configNodeMetricContent);
    }
    // get all report content of datanodes
    for (final DataNodeWrapper dataNode : this.dataNodeWrapperList) {
      final String dataNodeMetricContent =
          getUrlContent(
              Config.IOTDB_HTTP_URL_PREFIX
                  + dataNode.getIp()
                  + ":"
                  + dataNode.getMetricPort()
                  + "/metrics");
      result.add(dataNodeMetricContent);
    }
    return result;
  }

  protected void initEnvironment(final int configNodesNum, final int dataNodesNum) {
    initEnvironment(configNodesNum, dataNodesNum, retryCount);
  }

  protected void initEnvironment(
      final int configNodesNum, final int dataNodesNum, final int testWorkingRetryCount) {
    initEnvironment(configNodesNum, dataNodesNum, testWorkingRetryCount, false);
  }

  protected void initEnvironment(
      final int configNodesNum,
      final int dataNodesNum,
      final int retryCount,
      final boolean addAINode) {
    this.retryCount = retryCount;
    this.configNodeWrapperList = new ArrayList<>();
    this.dataNodeWrapperList = new ArrayList<>();

    clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());

    final String testClassName = getTestClassName();

    final ConfigNodeWrapper seedConfigNodeWrapper =
        new ConfigNodeWrapper(
            true,
            "",
            testClassName,
            testMethodName,
            EnvUtils.searchAvailablePorts(),
            index,
            this instanceof MultiClusterEnv,
            startTime);
    seedConfigNodeWrapper.createNodeDir();
    seedConfigNodeWrapper.changeConfig(
        (MppConfigNodeConfig) clusterConfig.getConfigNodeConfig(),
        (MppCommonConfig) clusterConfig.getConfigNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getConfigNodeJVMConfig());
    seedConfigNodeWrapper.createLogDir();
    seedConfigNodeWrapper.setKillPoints(configNodeKillPoints);
    seedConfigNodeWrapper.start();
    final String seedConfigNode = seedConfigNodeWrapper.getIpAndPortString();
    this.configNodeWrapperList.add(seedConfigNodeWrapper);

    // Check if the Seed-ConfigNode started successfully
    try (final SyncConfigNodeIServiceClient ignored =
        (SyncConfigNodeIServiceClient) getLeaderConfigNodeConnection()) {
      // Do nothing
      logger.info("The Seed-ConfigNode started successfully!");
    } catch (final Exception e) {
      logger.error("Failed to get connection to the Seed-ConfigNode", e);
    }

    final List<String> configNodeEndpoints = new ArrayList<>();
    final RequestDelegate<Void> configNodesDelegate =
        new SerialRequestDelegate<>(configNodeEndpoints);
    for (int i = 1; i < configNodesNum; i++) {
      final ConfigNodeWrapper configNodeWrapper =
          new ConfigNodeWrapper(
              false,
              seedConfigNode,
              testClassName,
              testMethodName,
              EnvUtils.searchAvailablePorts(),
              index,
              this instanceof MultiClusterEnv,
              startTime);
      this.configNodeWrapperList.add(configNodeWrapper);
      configNodeEndpoints.add(configNodeWrapper.getIpAndPortString());
      configNodeWrapper.createNodeDir();
      configNodeWrapper.changeConfig(
          (MppConfigNodeConfig) clusterConfig.getConfigNodeConfig(),
          (MppCommonConfig) clusterConfig.getConfigNodeCommonConfig(),
          (MppJVMConfig) clusterConfig.getConfigNodeJVMConfig());
      configNodeWrapper.createLogDir();
      configNodeWrapper.setKillPoints(configNodeKillPoints);
      configNodesDelegate.addRequest(
          () -> {
            configNodeWrapper.start();
            return null;
          });
    }
    try {
      configNodesDelegate.requestAll();
    } catch (final SQLException e) {
      logger.error("Start configNodes failed", e);
      throw new AssertionError();
    }

    final List<String> dataNodeEndpoints = new ArrayList<>();
    final RequestDelegate<Void> dataNodesDelegate =
        new ParallelRequestDelegate<>(dataNodeEndpoints, NODE_START_TIMEOUT);
    for (int i = 0; i < dataNodesNum; i++) {
      final DataNodeWrapper dataNodeWrapper =
          new DataNodeWrapper(
              seedConfigNode,
              testClassName,
              testMethodName,
              EnvUtils.searchAvailablePorts(),
              index,
              this instanceof MultiClusterEnv,
              startTime);
      this.dataNodeWrapperList.add(dataNodeWrapper);
      dataNodeEndpoints.add(dataNodeWrapper.getIpAndPortString());
      dataNodeWrapper.createNodeDir();
      dataNodeWrapper.changeConfig(
          (MppDataNodeConfig) clusterConfig.getDataNodeConfig(),
          (MppCommonConfig) clusterConfig.getDataNodeCommonConfig(),
          (MppJVMConfig) clusterConfig.getDataNodeJVMConfig());
      dataNodeWrapper.createLogDir();
      dataNodeWrapper.setKillPoints(dataNodeKillPoints);
      dataNodesDelegate.addRequest(
          () -> {
            dataNodeWrapper.start();
            return null;
          });
    }

    try {
      dataNodesDelegate.requestAll();
    } catch (final SQLException e) {
      logger.error("Start dataNodes failed", e);
      throw new AssertionError();
    }

    if (addAINode) {
      this.aiNodeWrapperList = new ArrayList<>();
      startAINode(seedConfigNode, testClassName);
    }

    checkClusterStatusWithoutUnknown();
  }

  private void startAINode(final String seedConfigNode, final String testClassName) {
    final String aiNodeEndPoint;
    final AINodeWrapper aiNodeWrapper =
        new AINodeWrapper(
            seedConfigNode,
            testClassName,
            testMethodName,
            index,
            EnvUtils.searchAvailablePorts(),
            startTime);
    aiNodeWrapperList.add(aiNodeWrapper);
    aiNodeEndPoint = aiNodeWrapper.getIpAndPortString();
    aiNodeWrapper.createNodeDir();
    aiNodeWrapper.createLogDir();
    final RequestDelegate<Void> aiNodesDelegate =
        new ParallelRequestDelegate<>(
            Collections.singletonList(aiNodeEndPoint), NODE_START_TIMEOUT);

    aiNodesDelegate.addRequest(
        () -> {
          aiNodeWrapper.start();
          return null;
        });

    try {
      aiNodesDelegate.requestAll();
    } catch (final SQLException e) {
      logger.error("Start aiNodes failed", e);
    }
  }

  public String getTestClassName() {
    final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (final StackTraceElement stackTraceElement : stack) {
      final String className = stackTraceElement.getClassName();
      if (className.endsWith("IT")) {
        final String result = className.substring(className.lastIndexOf(".") + 1);
        if (!result.startsWith("Abstract")) {
          return result;
        }
      }
    }
    return "UNKNOWN-IT";
  }

  private Map<String, Integer> countNodeStatus(final Map<Integer, String> nodeStatus) {
    final Map<String, Integer> result = new HashMap<>();
    nodeStatus.values().forEach(status -> result.put(status, result.getOrDefault(status, 0) + 1));
    return result;
  }

  public void checkClusterStatusWithoutUnknown() {
    checkClusterStatus(
        nodeStatusMap -> nodeStatusMap.values().stream().noneMatch("Unknown"::equals));
    testJDBCConnection();
  }

  public void checkClusterStatusOneUnknownOtherRunning() {
    checkClusterStatus(
        nodeStatus -> {
          Map<String, Integer> count = countNodeStatus(nodeStatus);
          return count.getOrDefault("Unknown", 0) == 1
              && count.getOrDefault("Running", 0) == nodeStatus.size() - 1;
        });
    testJDBCConnection();
  }

  /**
   * check whether all nodes' status match the provided predicate with RPC. after retryCount times,
   * if the status of all nodes still not match the predicate, throw AssertionError.
   *
   * @param statusCheck the predicate to test the status of nodes
   */
  public void checkClusterStatus(final Predicate<Map<Integer, String>> statusCheck) {
    logger.info("Testing cluster environment...");
    TShowClusterResp showClusterResp;
    Exception lastException = null;
    boolean flag;
    for (int i = 0; i < retryCount; i++) {
      try (final SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) getLeaderConfigNodeConnection()) {
        flag = true;
        showClusterResp = client.showCluster();

        // Check resp status
        if (showClusterResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          flag = false;
        }

        // Check the number of nodes
        if (showClusterResp.getNodeStatus().size()
            != configNodeWrapperList.size()
                + dataNodeWrapperList.size()
                + aiNodeWrapperList.size()) {
          flag = false;
        }

        // Check the status of nodes
        if (flag) {
          flag = statusCheck.test(showClusterResp.getNodeStatus());
        }

        if (flag) {
          logger.info("The cluster is now ready for testing!");
          return;
        }
      } catch (final Exception e) {
        lastException = e;
      }
      try {
        TimeUnit.SECONDS.sleep(1L);
      } catch (final InterruptedException e) {
        lastException = e;
        Thread.currentThread().interrupt();
      }
    }
    if (lastException != null) {
      logger.error(
          "exception in test Cluster with RPC, message: {}",
          lastException.getMessage(),
          lastException);
    }
    throw new AssertionError(
        String.format("After %d times retry, the cluster can't work!", retryCount));
  }

  @Override
  public void cleanClusterEnvironment() {
    final List<AbstractNodeWrapper> allNodeWrappers =
        Stream.concat(
                dataNodeWrapperList.stream(),
                Stream.concat(configNodeWrapperList.stream(), aiNodeWrapperList.stream()))
            .collect(Collectors.toList());
    allNodeWrappers.stream()
        .findAny()
        .ifPresent(
            nodeWrapper -> logger.info("You can find logs at {}", nodeWrapper.getLogDirPath()));
    for (final AbstractNodeWrapper nodeWrapper : allNodeWrappers) {
      nodeWrapper.stopForcibly();
      nodeWrapper.destroyDir();
      final String lockPath = EnvUtils.getLockFilePath(nodeWrapper.getPort());
      if (!new File(lockPath).delete()) {
        logger.error("Delete lock file {} failed", lockPath);
      }
    }
    if (clientManager != null) {
      clientManager.close();
    }
    testMethodName = null;
    clusterConfig = new MppClusterConfig();
  }

  @Override
  public Connection getConnection(
      final String username, final String password, final String sqlDialect) throws SQLException {
    return new ClusterTestConnection(
        getWriteConnection(null, username, password, sqlDialect),
        getReadConnections(null, username, password, sqlDialect));
  }

  @Override
  public Connection getWriteOnlyConnectionWithSpecifiedDataNode(
      final DataNodeWrapper dataNode, final String username, final String password)
      throws SQLException {
    return new ClusterTestConnection(
        getWriteConnectionWithSpecifiedDataNode(
            dataNode, null, username, password, TREE_SQL_DIALECT),
        Collections.emptyList());
  }

  @Override
  public Connection getConnectionWithSpecifiedDataNode(
      final DataNodeWrapper dataNode, final String username, final String password)
      throws SQLException {
    return new ClusterTestConnection(
        getWriteConnectionWithSpecifiedDataNode(
            dataNode, null, username, password, TREE_SQL_DIALECT),
        getReadConnections(null, username, password, TREE_SQL_DIALECT));
  }

  @Override
  public Connection getConnection(
      final Constant.Version version,
      final String username,
      final String password,
      final String sqlDialect)
      throws SQLException {
    return System.getProperty("ReadAndVerifyWithMultiNode", "true").equalsIgnoreCase("true")
        ? new ClusterTestConnection(
            getWriteConnection(version, username, password, sqlDialect),
            getReadConnections(version, username, password, sqlDialect))
        : getWriteConnection(version, username, password, sqlDialect).getUnderlyingConnecton();
  }

  @Override
  public ISession getSessionConnection(final String sqlDialect) throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    final Session session =
        new Session.Builder()
            .host(dataNode.getIp())
            .port(dataNode.getPort())
            .sqlDialect(sqlDialect)
            .build();
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnectionWithDB(final String sqlDialect, final String database)
      throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    final Session session =
        new Session.Builder()
            .host(dataNode.getIp())
            .port(dataNode.getPort())
            .sqlDialect(sqlDialect)
            .database(database)
            .build();
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(
      final String userName, final String password, final String sqlDialect)
      throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    final Session session =
        new Session.Builder()
            .host(dataNode.getIp())
            .port(dataNode.getPort())
            .username(userName)
            .password(password)
            .sqlDialect(sqlDialect)
            .build();
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(final List<String> nodeUrls, final String sqlDialect)
      throws IoTDBConnectionException {
    final Session session =
        new Session.Builder()
            .nodeUrls(nodeUrls)
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .fetchSize(SessionConfig.DEFAULT_FETCH_SIZE)
            .zoneId(null)
            .thriftDefaultBufferSize(SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY)
            .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
            .enableRedirection(SessionConfig.DEFAULT_REDIRECTION_MODE)
            .version(SessionConfig.DEFAULT_VERSION)
            .sqlDialect(sqlDialect)
            .build();
    session.open();
    return session;
  }

  @Override
  public ISessionPool getSessionPool(final int maxSize, final String sqlDialect) {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return new SessionPool.Builder()
        .host(dataNode.getIp())
        .port(dataNode.getPort())
        .user(SessionConfig.DEFAULT_USER)
        .password(SessionConfig.DEFAULT_PASSWORD)
        .maxSize(maxSize)
        .sqlDialect(sqlDialect)
        .build();
  }

  @Override
  public ISessionPool getSessionPool(
      final int maxSize, final String sqlDialect, final String database) {
    DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return new SessionPool.Builder()
        .host(dataNode.getIp())
        .port(dataNode.getPort())
        .user(SessionConfig.DEFAULT_USER)
        .password(SessionConfig.DEFAULT_PASSWORD)
        .maxSize(maxSize)
        .sqlDialect(sqlDialect)
        .database(database)
        .build();
  }

  protected NodeConnection getWriteConnection(
      Constant.Version version, String username, String password, String sqlDialect)
      throws SQLException {
    DataNodeWrapper dataNode;

    if (System.getProperty("RandomSelectWriteNode", "true").equalsIgnoreCase("true")) {
      // Randomly choose a node for handling write requests
      dataNode = this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    } else {
      dataNode = this.dataNodeWrapperList.get(0);
    }

    return getWriteConnectionFromDataNodeList(
        this.dataNodeWrapperList, version, username, password, sqlDialect);
  }

  protected NodeConnection getWriteConnectionWithSpecifiedDataNode(
      final DataNodeWrapper dataNode,
      final Constant.Version version,
      final String username,
      final String password,
      final String sqlDialect)
      throws SQLException {
    final String endpoint = dataNode.getIp() + ":" + dataNode.getPort();
    final Connection writeConnection =
        DriverManager.getConnection(
            Config.IOTDB_URL_PREFIX
                + endpoint
                + getParam(version, NODE_NETWORK_TIMEOUT_MS, ZERO_TIME_ZONE),
            BaseEnv.constructProperties(username, password, sqlDialect));
    return new NodeConnection(
        endpoint,
        NodeConnection.NodeRole.DATA_NODE,
        NodeConnection.ConnectionRole.WRITE,
        writeConnection);
  }

  protected NodeConnection getWriteConnectionFromDataNodeList(
      final List<DataNodeWrapper> dataNodeList,
      final Constant.Version version,
      final String username,
      final String password,
      final String sqlDialect)
      throws SQLException {
    final List<DataNodeWrapper> dataNodeWrapperListCopy = new ArrayList<>(dataNodeList);
    Collections.shuffle(dataNodeWrapperListCopy);
    SQLException lastException = null;
    for (final DataNodeWrapper dataNode : dataNodeWrapperListCopy) {
      try {
        return getWriteConnectionWithSpecifiedDataNode(
            dataNode, version, username, password, sqlDialect);
      } catch (final SQLException e) {
        lastException = e;
      }
    }
    if (!(lastException.getCause() instanceof TTransportException)) {
      logger.error("Failed to get connection from any DataNode, last exception is ", lastException);
    }
    throw lastException;
  }

  protected List<NodeConnection> getReadConnections(
      final Constant.Version version,
      final String username,
      final String password,
      final String sqlDialect)
      throws SQLException {
    final List<String> endpoints = new ArrayList<>();
    final ParallelRequestDelegate<NodeConnection> readConnRequestDelegate =
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT);

    dataNodeWrapperList.stream()
        .map(AbstractNodeWrapper::getIpAndPortString)
        .forEach(
            endpoint -> {
              endpoints.add(endpoint);
              readConnRequestDelegate.addRequest(
                  () ->
                      new NodeConnection(
                          endpoint,
                          NodeConnection.NodeRole.DATA_NODE,
                          NodeConnection.ConnectionRole.READ,
                          DriverManager.getConnection(
                              Config.IOTDB_URL_PREFIX
                                  + endpoint
                                  + getParam(version, NODE_NETWORK_TIMEOUT_MS, ZERO_TIME_ZONE),
                              BaseEnv.constructProperties(username, password, sqlDialect))));
            });
    return readConnRequestDelegate.requestAll();
  }

  // use this to avoid some runtimeExceptions when try to get jdbc connections.
  // because it is hard to add retry and handle exception when getting jdbc connections in
  // getWriteConnectionWithSpecifiedDataNode and getReadConnections.
  // so use this function to add retry when cluster is ready.
  // after retryCount times, if the jdbc can't connect, throw
  // AssertionError.
  protected void testJDBCConnection() {
    logger.info("Testing JDBC connection...");
    final List<String> endpoints =
        dataNodeWrapperList.stream()
            .map(DataNodeWrapper::getIpAndPortString)
            .collect(Collectors.toList());
    final RequestDelegate<Void> testDelegate =
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT);
    for (final DataNodeWrapper dataNode : dataNodeWrapperList) {
      final String dataNodeEndpoint = dataNode.getIpAndPortString();
      testDelegate.addRequest(
          () -> {
            Exception lastException = null;
            for (int i = 0; i < retryCount; i++) {
              try (final IoTDBConnection ignored =
                  (IoTDBConnection)
                      DriverManager.getConnection(
                          Config.IOTDB_URL_PREFIX
                              + dataNodeEndpoint
                              + getParam(null, NODE_NETWORK_TIMEOUT_MS, ZERO_TIME_ZONE),
                          System.getProperty("User", "root"),
                          System.getProperty("Password", "root"))) {
                logger.info("Successfully connecting to DataNode: {}.", dataNodeEndpoint);
                return null;
              } catch (final Exception e) {
                lastException = e;
                TimeUnit.SECONDS.sleep(1L);
              }
            }
            if (lastException != null) {
              throw lastException;
            }
            return null;
          });
    }
    try {
      testDelegate.requestAll();
    } catch (final Exception e) {
      logger.error("exception in test Cluster with RPC, message: {}", e.getMessage(), e);
      throw new AssertionError(
          String.format("After %d times retry, the cluster can't work!", retryCount));
    }
  }

  private String getParam(
      final Constant.Version version, final int timeout, final String timeZone) {
    final StringBuilder sb = new StringBuilder("?");
    sb.append(Config.NETWORK_TIMEOUT).append("=").append(timeout);
    if (version != null) {
      sb.append("&").append(VERSION).append("=").append(version);
    }
    if (timeZone != null) {
      sb.append("&").append(Config.TIME_ZONE).append("=").append(timeZone);
    }
    return sb.toString();
  }

  public String getTestMethodName() {
    return testMethodName;
  }

  @Override
  public void setTestMethodName(final String testMethodName) {
    this.testMethodName = testMethodName;
  }

  @Override
  public void dumpTestJVMSnapshot() {
    configNodeWrapperList.forEach(
        configNodeWrapper -> configNodeWrapper.executeJstack(testMethodName));
    dataNodeWrapperList.forEach(dataNodeWrapper -> dataNodeWrapper.executeJstack(testMethodName));
  }

  @Override
  public List<AbstractNodeWrapper> getNodeWrapperList() {
    final List<AbstractNodeWrapper> result = new ArrayList<>(configNodeWrapperList);
    result.addAll(dataNodeWrapperList);
    return result;
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
   * @return {@link SyncConfigNodeIServiceClient} that connects to the ConfigNode-Leader
   */
  @Override
  public IConfigNodeRPCService.Iface getLeaderConfigNodeConnection()
      throws IOException, InterruptedException {
    Exception lastException = null;
    ConfigNodeWrapper lastErrorNode = null;
    for (int i = 0; i < retryCount; i++) {
      for (final ConfigNodeWrapper configNodeWrapper : configNodeWrapperList) {
        try {
          lastErrorNode = configNodeWrapper;
          final SyncConfigNodeIServiceClient client =
              clientManager.borrowClient(
                  new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()));
          final TShowClusterResp resp = client.showCluster();

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
        } catch (final Exception e) {
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
  public IConfigNodeRPCService.Iface getConfigNodeConnection(int index) throws Exception {
    Exception lastException = null;
    final ConfigNodeWrapper configNodeWrapper = configNodeWrapperList.get(index);
    for (int i = 0; i < 30; i++) {
      try {
        return clientManager.borrowClient(
            new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()));
      } catch (final Exception e) {
        lastException = e;
      }
      // Sleep 1s before next retry
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IOException(
        "Failed to get connection to this ConfigNode. Last error: " + lastException);
  }

  @Override
  public int getFirstLeaderSchemaRegionDataNodeIndex() throws IOException, InterruptedException {
    Exception lastException = null;
    ConfigNodeWrapper lastErrorNode = null;
    for (int retry = 0; retry < 30; retry++) {
      for (int configNodeId = 0; configNodeId < configNodeWrapperList.size(); configNodeId++) {
        final ConfigNodeWrapper configNodeWrapper = configNodeWrapperList.get(configNodeId);
        lastErrorNode = configNodeWrapper;
        try (final SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()))) {
          TShowRegionResp resp =
              client.showRegion(
                  new TShowRegionReq().setConsensusGroupType(TConsensusGroupType.SchemaRegion));
          // Only the ConfigNodeClient who connects to the ConfigNode-leader
          // will respond the SUCCESS_STATUS

          String ip;
          int port;

          if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            for (final TRegionInfo tRegionInfo : resp.getRegionInfoList()) {
              if (tRegionInfo.getRoleType().equals("Leader")) {
                ip = tRegionInfo.getClientRpcIp();
                port = tRegionInfo.getClientRpcPort();
                for (int dataNodeId = 0; dataNodeId < dataNodeWrapperList.size(); ++dataNodeId) {
                  final DataNodeWrapper dataNodeWrapper = dataNodeWrapperList.get(dataNodeId);
                  if (dataNodeWrapper.getIp().equals(ip) && dataNodeWrapper.getPort() == port) {
                    return dataNodeId;
                  }
                }
              }
            }
            logger.error("No leaders in all schemaRegions.");
            return -1;
          } else {
            throw new Exception(
                "Bad status: "
                    + resp.getStatus().getCode()
                    + " message: "
                    + resp.getStatus().getMessage());
          }
        } catch (final Exception e) {
          lastException = e;
        }

        // Sleep 1s before next retry
        TimeUnit.SECONDS.sleep(1);
      }
    }
    if (lastErrorNode != null) {
      throw new IOException(
          "Failed to get the index of SchemaRegion-Leader from configNode. Last error configNode: "
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
    for (int retry = 0; retry < retryCount; retry++) {
      for (int configNodeId = 0; configNodeId < configNodeWrapperList.size(); configNodeId++) {
        final ConfigNodeWrapper configNodeWrapper = configNodeWrapperList.get(configNodeId);
        lastErrorNode = configNodeWrapper;
        try (final SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()))) {
          final TShowClusterResp resp = client.showCluster();
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
        } catch (final Exception e) {
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
  public void startConfigNode(final int index) {
    configNodeWrapperList.get(index).start();
  }

  @Override
  public void startAllConfigNodes() {
    configNodeWrapperList.forEach(AbstractNodeWrapper::start);
  }

  @Override
  public void shutdownConfigNode(int index) {
    configNodeWrapperList.get(index).stop();
  }

  @Override
  public void shutdownAllConfigNodes() {
    configNodeWrapperList.forEach(AbstractNodeWrapper::stop);
  }

  @Override
  public ConfigNodeWrapper getConfigNodeWrapper(final int index) {
    return configNodeWrapperList.get(index);
  }

  @Override
  public DataNodeWrapper getDataNodeWrapper(final int index) {
    return dataNodeWrapperList.get(index);
  }

  @Override
  public ConfigNodeWrapper generateRandomConfigNodeWrapper() {
    final ConfigNodeWrapper newConfigNodeWrapper =
        new ConfigNodeWrapper(
            false,
            configNodeWrapperList.get(0).getIpAndPortString(),
            getTestClassName(),
            getTestMethodName(),
            EnvUtils.searchAvailablePorts(),
            index,
            this instanceof MultiClusterEnv,
            startTime);
    configNodeWrapperList.add(newConfigNodeWrapper);
    newConfigNodeWrapper.createNodeDir();
    newConfigNodeWrapper.changeConfig(
        (MppConfigNodeConfig) clusterConfig.getConfigNodeConfig(),
        (MppCommonConfig) clusterConfig.getConfigNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getConfigNodeJVMConfig());
    newConfigNodeWrapper.createLogDir();
    return newConfigNodeWrapper;
  }

  @Override
  public DataNodeWrapper generateRandomDataNodeWrapper() {
    final DataNodeWrapper newDataNodeWrapper =
        new DataNodeWrapper(
            configNodeWrapperList.get(0).getIpAndPortString(),
            getTestClassName(),
            getTestMethodName(),
            EnvUtils.searchAvailablePorts(),
            index,
            this instanceof MultiClusterEnv,
            startTime);
    dataNodeWrapperList.add(newDataNodeWrapper);
    newDataNodeWrapper.createNodeDir();
    newDataNodeWrapper.changeConfig(
        (MppDataNodeConfig) clusterConfig.getDataNodeConfig(),
        (MppCommonConfig) clusterConfig.getDataNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getDataNodeJVMConfig());
    newDataNodeWrapper.createLogDir();
    return newDataNodeWrapper;
  }

  @Override
  public void registerNewDataNode(final boolean isNeedVerify) {
    registerNewDataNode(generateRandomDataNodeWrapper(), isNeedVerify);
  }

  @Override
  public void registerNewConfigNode(final boolean isNeedVerify) {
    registerNewConfigNode(generateRandomConfigNodeWrapper(), isNeedVerify);
  }

  @Override
  public void registerNewConfigNode(
      final ConfigNodeWrapper newConfigNodeWrapper, final boolean isNeedVerify) {
    // Start new ConfigNode
    final RequestDelegate<Void> configNodeDelegate =
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
    } catch (final SQLException e) {
      logger.error("Start configNode failed", e);
      throw new AssertionError();
    }

    if (isNeedVerify) {
      // Test whether register success
      checkClusterStatusWithoutUnknown();
    }
  }

  @Override
  public void registerNewDataNode(
      final DataNodeWrapper newDataNodeWrapper, final boolean isNeedVerify) {
    // Start new DataNode
    final List<String> dataNodeEndpoints =
        Collections.singletonList(newDataNodeWrapper.getIpAndPortString());
    final RequestDelegate<Void> dataNodesDelegate =
        new ParallelRequestDelegate<>(dataNodeEndpoints, NODE_START_TIMEOUT);
    dataNodesDelegate.addRequest(
        () -> {
          newDataNodeWrapper.start();
          return null;
        });
    try {
      dataNodesDelegate.requestAll();
    } catch (final SQLException e) {
      logger.error("Start dataNodes failed", e);
      throw new AssertionError();
    }

    if (isNeedVerify) {
      // Test whether register success
      checkClusterStatusWithoutUnknown();
    }
  }

  @Override
  public void startDataNode(final int index) {
    dataNodeWrapperList.get(index).start();
  }

  @Override
  public void startAllDataNodes() {
    dataNodeWrapperList.forEach(AbstractNodeWrapper::start);
  }

  @Override
  public void shutdownDataNode(final int index) {
    dataNodeWrapperList.get(index).stop();
  }

  @Override
  public void shutdownAllDataNodes() {
    dataNodeWrapperList.forEach(AbstractNodeWrapper::stop);
  }

  @Override
  public void ensureNodeStatus(
      final List<BaseNodeWrapper> nodes, final List<NodeStatus> targetStatus)
      throws IllegalStateException {
    Throwable lastException = null;
    for (int i = 0; i < retryCount; i++) {
      try (final SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
        final List<String> errorMessages = new ArrayList<>(nodes.size());
        final Map<String, Integer> nodeIds = new HashMap<>(nodes.size());
        final TShowClusterResp showClusterResp = client.showCluster();
        showClusterResp
            .getConfigNodeList()
            .forEach(
                node ->
                    nodeIds.put(
                        node.getInternalEndPoint().getIp()
                            + ":"
                            + node.getInternalEndPoint().getPort(),
                        node.getConfigNodeId()));
        showClusterResp
            .getDataNodeList()
            .forEach(
                node ->
                    nodeIds.put(
                        node.getClientRpcEndPoint().getIp()
                            + ":"
                            + node.getClientRpcEndPoint().getPort(),
                        node.getDataNodeId()));
        for (int j = 0; j < nodes.size(); j++) {
          final String endpoint = nodes.get(j).getIpAndPortString();
          if (!nodeIds.containsKey(endpoint)) {
            // Node not exist
            // Notice: Never modify this line, since the NodeLocation might be modified in IT
            errorMessages.add("The node " + nodes.get(j).getIpAndPortString() + " is not found!");
            continue;
          }
          final String status = showClusterResp.getNodeStatus().get(nodeIds.get(endpoint));
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
      } catch (final TException | ClientManagerException | IOException | InterruptedException e) {
        lastException = e;
      }
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IllegalStateException(lastException);
  }

  @Override
  public int getMqttPort() {
    return dataNodeWrapperList
        .get(new Random(System.currentTimeMillis()).nextInt(dataNodeWrapperList.size()))
        .getMqttPort();
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
    return TEMPLATE_NODE_PATH + File.separator + "sbin";
  }

  @Override
  public String getToolsPath() {
    return TEMPLATE_NODE_PATH + File.separator + "tools";
  }

  @Override
  public String getLibPath() {
    return TEMPLATE_NODE_LIB_PATH;
  }

  @Override
  public Optional<DataNodeWrapper> dataNodeIdToWrapper(final int nodeId) {
    try (final SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) getLeaderConfigNodeConnection()) {
      final TShowDataNodesResp resp = leaderClient.showDataNodes();
      for (final TDataNodeInfo dataNodeInfo : resp.getDataNodesInfoList()) {
        if (dataNodeInfo.getDataNodeId() == nodeId) {
          return dataNodeWrapperList.stream()
              .filter(dataNodeWrapper -> dataNodeWrapper.getPort() == dataNodeInfo.getRpcPort())
              .findAny();
        }
      }
      return Optional.empty();
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  @Override
  public void registerConfigNodeKillPoints(final List<String> killPoints) {
    this.configNodeKillPoints = killPoints;
  }

  @Override
  public void registerDataNodeKillPoints(final List<String> killPoints) {
    this.dataNodeKillPoints = killPoints;
  }
}
