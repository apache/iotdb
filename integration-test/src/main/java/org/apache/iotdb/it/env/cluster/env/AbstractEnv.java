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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.PortOccupiedException;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.config.MppClusterConfig;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.config.MppConfigNodeConfig;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
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
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.NODE_NETWORK_TIMEOUT_MS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.NODE_START_TIMEOUT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TEMPLATE_NODE_LIB_PATH;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TEMPLATE_NODE_PATH;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.ZERO_TIME_ZONE;
import static org.apache.iotdb.jdbc.Config.VERSION;

public abstract class AbstractEnv implements BaseEnv {
  private static final Logger logger = IoTDBTestLogger.logger;
  private static final int DEFAULT_CLUSTER_READY_RETRY_COUNT = 30;
  private static final String CLUSTER_READY_RETRY_COUNT_PROPERTY =
      "integrationTest.clusterReadyRetryCount";

  private final Random rand = new Random();
  protected List<ConfigNodeWrapper> configNodeWrapperList = Collections.emptyList();
  protected List<DataNodeWrapper> dataNodeWrapperList = Collections.emptyList();
  protected List<AbstractNodeWrapper> extraNodeWrappers = Collections.emptyList();
  protected String testClassName = null;
  protected String testMethodName = null;
  protected int index = 0;
  protected long startTime;
  protected int retryCount = getDefaultClusterReadyRetryCount();
  private IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;
  private List<String> configNodeKillPoints = new ArrayList<>();
  private List<String> dataNodeKillPoints = new ArrayList<>();
  protected List<String> extraNodeKillPoints = new ArrayList<>();

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

  private static int getDefaultClusterReadyRetryCount() {
    final int configuredRetryCount =
        Integer.getInteger(CLUSTER_READY_RETRY_COUNT_PROPERTY, DEFAULT_CLUSTER_READY_RETRY_COUNT);
    return configuredRetryCount > 0 ? configuredRetryCount : DEFAULT_CLUSTER_READY_RETRY_COUNT;
  }

  @Override
  public ClusterConfig getConfig() {
    return clusterConfig;
  }

  @Override
  public List<String> getMetricPrometheusReporterContents(String authHeader) {
    final List<String> result = new ArrayList<>();
    // get all report content of confignodes
    for (final ConfigNodeWrapper configNode : this.configNodeWrapperList) {
      final String configNodeMetricContent =
          getUrlContent(
              Config.IOTDB_HTTP_URL_PREFIX
                  + UrlUtils.formatTEndPointIpv4AndIpv6Url(
                      configNode.getIp(), configNode.getMetricPort())
                  + "/metrics",
              authHeader);
      result.add(configNodeMetricContent);
    }
    // get all report content of datanodes
    for (final DataNodeWrapper dataNode : this.dataNodeWrapperList) {
      final String dataNodeMetricContent =
          getUrlContent(
              Config.IOTDB_HTTP_URL_PREFIX
                  + UrlUtils.formatTEndPointIpv4AndIpv6Url(
                      dataNode.getIp(), dataNode.getMetricPort())
                  + "/metrics",
              authHeader);
      result.add(dataNodeMetricContent);
    }
    return result;
  }

  protected void initEnvironment(final int configNodesNum, final int dataNodesNum) {
    initEnvironment(configNodesNum, dataNodesNum, retryCount);
  }

  protected void initEnvironment(
      final int configNodesNum, final int dataNodesNum, final int testWorkingRetryCount) {
    this.retryCount = testWorkingRetryCount;
    this.configNodeWrapperList = new ArrayList<>();
    this.dataNodeWrapperList = new ArrayList<>();
    this.extraNodeWrappers = new ArrayList<>();

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
      ConfigNodeWrapper configNodeWrapper = newConfigNode();
      this.configNodeWrapperList.add(configNodeWrapper);
      configNodeEndpoints.add(configNodeWrapper.getIpAndPortString());
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
        new ParallelRequestDelegate<>(dataNodeEndpoints, NODE_START_TIMEOUT, this);
    for (int i = 0; i < dataNodesNum; i++) {
      DataNodeWrapper dataNodeWrapper = newDataNode();
      dataNodeEndpoints.add(dataNodeWrapper.getIpAndPortString());
      this.dataNodeWrapperList.add(dataNodeWrapper);
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

    initExtraNodes(configNodeWrapperList, dataNodeWrapperList, testClassName);

    checkClusterStatusWithoutUnknown();
  }

  /**
   * Hook method for subclasses to initialize and start extra node types beyond the core ConfigNode
   * and DataNode (e.g., AINode, StreamNode, ProxyNode).
   *
   * <p>Subclasses should create node wrappers, add them to {@link #extraNodeWrappers}, configure
   * kill points via {@link #extraNodeKillPoints}, and start the nodes. Subclasses have direct
   * access to protected fields: {@code testMethodName}, {@code index}, {@code startTime}.
   *
   * @param configNodeWrappers list of all ConfigNode wrappers in the cluster (unmodifiable)
   * @param dataNodeWrappers list of all DataNode wrappers in the cluster (unmodifiable)
   * @param testClassName the test class name for logging and identification purposes
   */
  protected void initExtraNodes(
      final List<ConfigNodeWrapper> configNodeWrappers,
      final List<DataNodeWrapper> dataNodeWrappers,
      final String testClassName) {
    // Default: no extra nodes. Subclasses override to add nodes.
  }

  protected void registerExtraNode(final AbstractNodeWrapper nodeWrapper) {
    extraNodeWrappers.add(nodeWrapper);
  }

  private ConfigNodeWrapper newConfigNode() {
    final ConfigNodeWrapper configNodeWrapper =
        new ConfigNodeWrapper(
            false,
            configNodeWrapperList.get(0).getIpAndPortString(),
            getTestClassName(),
            testMethodName,
            EnvUtils.searchAvailablePorts(),
            index,
            this instanceof MultiClusterEnv,
            startTime);

    configNodeWrapper.createNodeDir();
    configNodeWrapper.changeConfig(
        (MppConfigNodeConfig) clusterConfig.getConfigNodeConfig(),
        (MppCommonConfig) clusterConfig.getConfigNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getConfigNodeJVMConfig());
    configNodeWrapper.createLogDir();
    configNodeWrapper.setKillPoints(configNodeKillPoints);
    return configNodeWrapper;
  }

  private DataNodeWrapper newDataNode() {
    final DataNodeWrapper dataNodeWrapper =
        new DataNodeWrapper(
            configNodeWrapperList.get(0).getIpAndPortString(),
            getTestClassName(),
            testMethodName,
            EnvUtils.searchAvailablePorts(),
            index,
            this instanceof MultiClusterEnv,
            startTime);

    dataNodeWrapper.createNodeDir();
    dataNodeWrapper.changeConfig(
        (MppDataNodeConfig) clusterConfig.getDataNodeConfig(),
        (MppCommonConfig) clusterConfig.getDataNodeCommonConfig(),
        (MppJVMConfig) clusterConfig.getDataNodeJVMConfig());
    dataNodeWrapper.createLogDir();
    dataNodeWrapper.setKillPoints(dataNodeKillPoints);
    return dataNodeWrapper;
  }

  public String getTestClassName() {
    if (testClassName != null) {
      return testClassName;
    }
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

  public void checkNodeInStatus(int nodeId, NodeStatus expectation) {
    checkClusterStatus(
        nodeStatusMap -> expectation.getStatus().equals(nodeStatusMap.get(nodeId)), m -> true);
  }

  public void checkClusterStatusWithoutUnknown() {
    checkClusterStatus(
        nodeStatusMap -> nodeStatusMap.values().stream().noneMatch("Unknown"::equals),
        processStatus -> processStatus.values().stream().noneMatch(i -> i != 0));
    testJDBCConnection();
  }

  public void checkClusterStatusOneUnknownOtherRunning() {
    checkClusterStatus(
        nodeStatus -> {
          Map<String, Integer> count = countNodeStatus(nodeStatus);
          return count.getOrDefault("Unknown", 0) == 1
              && count.getOrDefault("Running", 0) == nodeStatus.size() - 1;
        },
        processStatus -> {
          long aliveProcessCount = processStatus.values().stream().filter(i -> i == 0).count();
          return aliveProcessCount == processStatus.size() - 1;
        });
    testJDBCConnection();
  }

  /**
   * check whether all nodes' status match the provided predicate with RPC. after retryCount times,
   * if the status of all nodes still not match the predicate, throw AssertionError.
   *
   * @param nodeStatusCheck the predicate to test the status of nodes
   */
  public void checkClusterStatus(
      final Predicate<Map<Integer, String>> nodeStatusCheck,
      final Predicate<Map<AbstractNodeWrapper, Integer>> processStatusCheck) {
    logger.info("Testing cluster environment...");
    TShowClusterResp showClusterResp;
    Exception lastException = null;
    boolean passed;
    boolean showClusterPassed = true;
    boolean nodeSizePassed = true;
    boolean nodeStatusPassed = true;
    boolean processStatusPassed = true;
    TSStatus showClusterStatus = null;
    int actualNodeSize = 0;
    Map<Integer, String> lastNodeStatus = null;
    Map<AbstractNodeWrapper, Integer> processStatusMap = new HashMap<>();

    for (int i = 0; i < retryCount; i++) {
      try (final SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) getLeaderConfigNodeConnection()) {
        passed = true;
        showClusterPassed = true;
        nodeSizePassed = true;
        nodeStatusPassed = true;
        processStatusPassed = true;
        processStatusMap.clear();

        showClusterResp = client.showCluster();
        showClusterStatus = showClusterResp.getStatus();
        actualNodeSize = showClusterResp.getNodeStatusSize();
        lastNodeStatus = showClusterResp.getNodeStatus();

        // Check resp status
        if (showClusterResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          passed = false;
          showClusterPassed = false;
        }

        // Check the number of nodes
        if (showClusterResp.getNodeStatus().size()
            != configNodeWrapperList.size()
                + dataNodeWrapperList.size()
                + extraNodeWrappers.size()) {
          passed = false;
          nodeSizePassed = false;
        }

        // Check the status of nodes
        if (passed) {
          passed = nodeStatusCheck.test(showClusterResp.getNodeStatus());
          if (!passed) {
            nodeStatusPassed = false;
          }
        }

        // check the status of processes
        for (DataNodeWrapper dataNodeWrapper : dataNodeWrapperList) {
          boolean alive = dataNodeWrapper.getInstance().isAlive();
          if (!alive) {
            processStatusMap.put(dataNodeWrapper, dataNodeWrapper.getInstance().waitFor());
          } else {
            processStatusMap.put(dataNodeWrapper, 0);
          }
        }
        for (ConfigNodeWrapper nodeWrapper : configNodeWrapperList) {
          boolean alive = nodeWrapper.getInstance().isAlive();
          if (!alive) {
            processStatusMap.put(nodeWrapper, nodeWrapper.getInstance().waitFor());
          } else {
            processStatusMap.put(nodeWrapper, 0);
          }
        }
        for (AbstractNodeWrapper nodeWrapper : extraNodeWrappers) {
          boolean alive = nodeWrapper.getInstance().isAlive();
          if (!alive) {
            processStatusMap.put(nodeWrapper, nodeWrapper.getInstance().waitFor());
          } else {
            processStatusMap.put(nodeWrapper, 0);
          }
        }

        processStatusPassed = processStatusCheck.test(processStatusMap);
        if (!processStatusPassed) {
          passed = false;
        }

        if (!processStatusPassed) {
          handleProcessStatus(processStatusMap);
        }

        if (passed) {
          logger.info("The cluster is now ready for testing!");
          return;
        }
        logger.info(
            "Retry {}: showClusterPassed={}, nodeSizePassed={}, nodeStatusPassed={}, processStatusPassed={}",
            i,
            showClusterPassed,
            nodeSizePassed,
            nodeStatusPassed,
            processStatusPassed);
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
    if (!showClusterPassed) {
      logger.error("Show cluster failed: {}", showClusterStatus);
    }
    if (!nodeSizePassed) {
      logger.error("Only {} nodes detected", actualNodeSize);
    }
    if (!nodeStatusPassed) {
      logger.error("Some node status incorrect: {}", lastNodeStatus);
    }
    if (!processStatusPassed) {
      logger.error(
          "Some process status incorrect: {}",
          processStatusMap.entrySet().stream()
              .collect(Collectors.toMap(e -> e.getKey().getId(), Map.Entry::getValue)));

      if (processStatusMap.containsValue(TSStatusCode.PORT_OCCUPIED.getStatusCode())) {
        throw new PortOccupiedException();
      }
    }

    dumpTestJVMSnapshotQuietly("cluster status check failed");
    throw new AssertionError(
        buildClusterStatusFailureMessage(
            showClusterPassed,
            nodeSizePassed,
            nodeStatusPassed,
            processStatusPassed,
            showClusterStatus,
            actualNodeSize,
            lastNodeStatus,
            processStatusMap,
            lastException));
  }

  private String buildClusterStatusFailureMessage(
      final boolean showClusterPassed,
      final boolean nodeSizePassed,
      final boolean nodeStatusPassed,
      final boolean processStatusPassed,
      final TSStatus showClusterStatus,
      final int actualNodeSize,
      final Map<Integer, String> lastNodeStatus,
      final Map<AbstractNodeWrapper, Integer> processStatusMap,
      final Exception lastException) {
    final StringBuilder builder =
        new StringBuilder(
            String.format("After %d times retry, the cluster status check failed", retryCount));
    builder
        .append(": showClusterPassed=")
        .append(showClusterPassed)
        .append(", nodeSizePassed=")
        .append(nodeSizePassed)
        .append(", nodeStatusPassed=")
        .append(nodeStatusPassed)
        .append(", processStatusPassed=")
        .append(processStatusPassed)
        .append(", expectedNodeSize=")
        .append(
            configNodeWrapperList.size() + dataNodeWrapperList.size() + extraNodeWrappers.size())
        .append(", actualNodeSize=")
        .append(actualNodeSize);
    if (showClusterStatus != null) {
      builder.append(", showClusterStatus=").append(showClusterStatus);
    }
    if (lastNodeStatus != null) {
      builder.append(", lastNodeStatus=").append(lastNodeStatus);
    }
    if (!processStatusMap.isEmpty()) {
      builder.append(", processStatus=").append(formatProcessStatus(processStatusMap));
    }
    if (lastException != null) {
      builder
          .append(", lastException=")
          .append(lastException.getClass().getName())
          .append(": ")
          .append(lastException.getMessage());
    }
    builder.append(", logDirs=").append(getClusterLogDirs());
    return builder.toString();
  }

  private Map<String, Integer> formatProcessStatus(
      final Map<AbstractNodeWrapper, Integer> processStatusMap) {
    final Map<String, Integer> result = new LinkedHashMap<>();
    processStatusMap.forEach(
        (nodeWrapper, statusCode) -> result.put(nodeWrapper.getId(), statusCode));
    return result;
  }

  private List<String> getClusterLogDirs() {
    final List<AbstractNodeWrapper> allNodeWrappers = new ArrayList<>();
    allNodeWrappers.addAll(configNodeWrapperList);
    allNodeWrappers.addAll(dataNodeWrapperList);
    allNodeWrappers.addAll(extraNodeWrappers);
    return allNodeWrappers.stream()
        .map(AbstractNodeWrapper::getLogDirPath)
        .distinct()
        .collect(Collectors.toList());
  }

  private void dumpTestJVMSnapshotQuietly(final String reason) {
    try {
      logger.info("Dumping test JVM snapshots because {}.", reason);
      dumpTestJVMSnapshot();
    } catch (final Exception e) {
      logger.warn("Failed to dump test JVM snapshots after {}", reason, e);
    }
  }

  private void handleProcessStatus(Map<AbstractNodeWrapper, Integer> processStatusMap) {
    for (Map.Entry<AbstractNodeWrapper, Integer> entry : processStatusMap.entrySet()) {
      Integer statusCode = entry.getValue();
      AbstractNodeWrapper nodeWrapper = entry.getKey();
      if (statusCode != 0) {
        logger.info("Node {} is not running due to {}", nodeWrapper.getId(), statusCode);
      }
      if (statusCode == TSStatusCode.PORT_OCCUPIED.getStatusCode() || statusCode == 1) {
        // the occupation of jmx port may return 1
        try {
          Map<Integer, Long> portOccupationMap =
              EnvUtils.listPortOccupation(
                  Arrays.stream(nodeWrapper.getPortList()).boxed().collect(Collectors.toList()));
          logger.info("Check port result: {}", portOccupationMap);
          for (DataNodeWrapper dataNodeWrapper : dataNodeWrapperList) {
            if (portOccupationMap.containsValue(dataNodeWrapper.getPid())) {
              logger.info(
                  "A port is occupied by another DataNode {}-{}, restart it",
                  dataNodeWrapper.getIpAndPortString(),
                  dataNodeWrapper.getPid());
              dataNodeWrapper.stop();
              dataNodeWrapper.start();
            }
          }
          for (ConfigNodeWrapper configNodeWrapper : configNodeWrapperList) {
            if (portOccupationMap.containsValue(configNodeWrapper.getPid())) {
              logger.info(
                  "A port is occupied by another ConfigNode {}-{}, restart it",
                  configNodeWrapper.getIpAndPortString(),
                  configNodeWrapper.getPid());
              configNodeWrapper.stop();
              configNodeWrapper.start();
            }
          }
          for (AbstractNodeWrapper extraNodeWrapper : extraNodeWrappers) {
            if (portOccupationMap.containsValue(extraNodeWrapper.getPid())) {
              logger.info(
                  "A port is occupied by another node {}-{}, restart it",
                  extraNodeWrapper.getIpAndPortString(),
                  extraNodeWrapper.getPid());
              extraNodeWrapper.stop();
              extraNodeWrapper.start();
            }
          }
        } catch (IOException e) {
          logger.error("Cannot check port occupation", e);
        }
        logger.info("Restarting it");
        nodeWrapper.stop();
        nodeWrapper.start();
      }
    }
  }

  @Override
  public void cleanClusterEnvironment() {
    final List<AbstractNodeWrapper> allNodeWrappers =
        Stream.concat(
                Stream.concat(configNodeWrapperList.stream(), dataNodeWrapperList.stream()),
                extraNodeWrappers.stream())
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

  private boolean isThriftClientSSLEnabled() {
    return Boolean.parseBoolean(getDataNodeCommonConfigProperty("enable_thrift_ssl", "false"));
  }

  private boolean isThriftSSLClientAuthEnabled() {
    return Boolean.parseBoolean(getDataNodeCommonConfigProperty("thrift_ssl_client_auth", "false"));
  }

  private String getDataNodeCommonConfigProperty(final String key, final String defaultValue) {
    return ((MppCommonConfig) clusterConfig.getDataNodeCommonConfig())
        .getProperty(key, defaultValue);
  }

  private String getClientSSLProtocol() {
    return getDataNodeCommonConfigProperty("ssl_protocol", SessionConfig.DEFAULT_SSL_PROTOCOL);
  }

  private Properties constructConnectionProperties(
      final String username, final String password, final String sqlDialect) {
    final Properties info = BaseEnv.constructProperties(username, password, sqlDialect);
    if (isThriftClientSSLEnabled()) {
      info.put(Config.USE_SSL, Boolean.TRUE.toString());
      putIfPresent(
          info, Config.TRUST_STORE, getDataNodeCommonConfigProperty("trust_store_path", ""));
      putIfPresent(
          info, Config.TRUST_STORE_PWD, getDataNodeCommonConfigProperty("trust_store_pwd", ""));
      putIfPresent(info, Config.SSL_PROTOCOL, getClientSSLProtocol());
      if (isThriftSSLClientAuthEnabled()) {
        putIfPresent(info, Config.KEY_STORE, getDataNodeCommonConfigProperty("key_store_path", ""));
        putIfPresent(
            info, Config.KEY_STORE_PWD, getDataNodeCommonConfigProperty("key_store_pwd", ""));
      }
    }
    return info;
  }

  private void putIfPresent(final Properties properties, final String key, final String value) {
    if (value != null && !value.isEmpty()) {
      properties.put(key, value);
    }
  }

  private Session.Builder configureClientSSL(final Session.Builder builder) {
    if (isThriftClientSSLEnabled()) {
      builder
          .useSSL(true)
          .trustStore(getDataNodeCommonConfigProperty("trust_store_path", ""))
          .trustStorePwd(getDataNodeCommonConfigProperty("trust_store_pwd", ""))
          .sslProtocol(getClientSSLProtocol());
      if (isThriftSSLClientAuthEnabled()) {
        builder
            .keyStore(getDataNodeCommonConfigProperty("key_store_path", ""))
            .keyStorePwd(getDataNodeCommonConfigProperty("key_store_pwd", ""));
      }
    }
    return builder;
  }

  private TableSessionBuilder configureClientSSL(final TableSessionBuilder builder) {
    if (isThriftClientSSLEnabled()) {
      builder
          .useSSL(true)
          .trustStore(getDataNodeCommonConfigProperty("trust_store_path", ""))
          .trustStorePwd(getDataNodeCommonConfigProperty("trust_store_pwd", ""))
          .sslProtocol(getClientSSLProtocol());
      if (isThriftSSLClientAuthEnabled()) {
        builder
            .keyStore(getDataNodeCommonConfigProperty("key_store_path", ""))
            .keyStorePwd(getDataNodeCommonConfigProperty("key_store_pwd", ""));
      }
    }
    return builder;
  }

  private SessionPool.Builder configureClientSSL(final SessionPool.Builder builder) {
    if (isThriftClientSSLEnabled()) {
      builder
          .useSSL(true)
          .trustStore(getDataNodeCommonConfigProperty("trust_store_path", ""))
          .trustStorePwd(getDataNodeCommonConfigProperty("trust_store_pwd", ""))
          .sslProtocol(getClientSSLProtocol());
      if (isThriftSSLClientAuthEnabled()) {
        builder
            .keyStore(getDataNodeCommonConfigProperty("key_store_path", ""))
            .keyStorePwd(getDataNodeCommonConfigProperty("key_store_pwd", ""));
      }
    }
    return builder;
  }

  private TableSessionPoolBuilder configureClientSSL(final TableSessionPoolBuilder builder) {
    if (isThriftClientSSLEnabled()) {
      builder
          .useSSL(true)
          .trustStore(getDataNodeCommonConfigProperty("trust_store_path", ""))
          .trustStorePwd(getDataNodeCommonConfigProperty("trust_store_pwd", ""))
          .sslProtocol(getClientSSLProtocol());
      if (isThriftSSLClientAuthEnabled()) {
        builder
            .keyStore(getDataNodeCommonConfigProperty("key_store_path", ""))
            .keyStorePwd(getDataNodeCommonConfigProperty("key_store_pwd", ""));
      }
    }
    return builder;
  }

  @Override
  public Connection getConnection(
      final String username, final String password, final String sqlDialect) throws SQLException {
    return new ClusterTestConnection(
        getWriteConnection(null, username, password, sqlDialect),
        getReadConnections(null, username, password, sqlDialect),
        this);
  }

  @Override
  public Connection getAvailableConnection(String username, String password, String sqlDialect)
      throws SQLException {
    return new ClusterTestConnection(
        getWriteConnection(null, username, password, sqlDialect),
        getOneAvailableReadConnection(null, username, password, sqlDialect),
        this);
  }

  @Override
  public Connection getConnection(
      final DataNodeWrapper dataNodeWrapper,
      final String username,
      final String password,
      final String sqlDialect)
      throws SQLException {
    return new ClusterTestConnection(
        getWriteConnectionWithSpecifiedDataNode(
            dataNodeWrapper, null, username, password, sqlDialect),
        getReadConnections(null, dataNodeWrapper, username, password, sqlDialect),
        this);
  }

  @Override
  public Connection getWriteOnlyConnectionWithSpecifiedDataNode(
      final DataNodeWrapper dataNode,
      final String username,
      final String password,
      String sqlDialect)
      throws SQLException {
    return new ClusterTestConnection(
        getWriteConnectionWithSpecifiedDataNode(
            dataNode,
            null,
            username,
            password,
            TABLE_SQL_DIALECT.equals(sqlDialect) ? TABLE_SQL_DIALECT : TREE_SQL_DIALECT),
        Collections.emptyList(),
        this);
  }

  @Override
  public Connection getConnectionWithSpecifiedDataNode(
      final DataNodeWrapper dataNode, final String username, final String password)
      throws SQLException {
    return new ClusterTestConnection(
        getWriteConnectionWithSpecifiedDataNode(
            dataNode, null, username, password, TREE_SQL_DIALECT),
        getReadConnections(null, username, password, TREE_SQL_DIALECT),
        this);
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
            getReadConnections(version, username, password, sqlDialect),
            this)
        : getWriteConnection(version, username, password, sqlDialect).getUnderlyingConnection();
  }

  @Override
  public ISession getSessionConnection() throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    final Session session =
        configureClientSSL(new Session.Builder().host(dataNode.getIp()).port(dataNode.getPort()))
            .build();
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(ZoneId zoneId) throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    final Session session =
        configureClientSSL(
                new Session.Builder()
                    .host(dataNode.getIp())
                    .port(dataNode.getPort())
                    .zoneId(zoneId))
            .build();
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(final String userName, final String password)
      throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    final Session session =
        configureClientSSL(
                new Session.Builder()
                    .host(dataNode.getIp())
                    .port(dataNode.getPort())
                    .username(userName)
                    .password(password))
            .build();
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(final List<String> nodeUrls)
      throws IoTDBConnectionException {
    final Session session =
        configureClientSSL(
                new Session.Builder()
                    .nodeUrls(nodeUrls)
                    .username(SessionConfig.DEFAULT_USER)
                    .password(SessionConfig.DEFAULT_PASSWORD)
                    .fetchSize(SessionConfig.DEFAULT_FETCH_SIZE)
                    .zoneId(null)
                    .thriftDefaultBufferSize(SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY)
                    .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
                    .enableRedirection(SessionConfig.DEFAULT_REDIRECTION_MODE)
                    .version(SessionConfig.DEFAULT_VERSION))
            .build();
    session.open();
    return session;
  }

  @Override
  public ITableSession getTableSessionConnection() throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return configureClientSSL(
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString())))
        .build();
  }

  @Override
  public ITableSession getTableSessionConnection(String userName, String password)
      throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return configureClientSSL(
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString()))
                .username(userName)
                .password(password))
        .build();
  }

  @Override
  public ITableSession getTableSessionConnectionWithDB(final String database)
      throws IoTDBConnectionException {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return configureClientSSL(
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString()))
                .database(database))
        .build();
  }

  public ITableSession getTableSessionConnection(List<String> nodeUrls)
      throws IoTDBConnectionException {
    return configureClientSSL(
            new TableSessionBuilder()
                .nodeUrls(nodeUrls)
                .username(SessionConfig.DEFAULT_USER)
                .password(SessionConfig.DEFAULT_PASSWORD)
                .fetchSize(SessionConfig.DEFAULT_FETCH_SIZE)
                .zoneId(null)
                .thriftDefaultBufferSize(SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY)
                .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
                .enableRedirection(SessionConfig.DEFAULT_REDIRECTION_MODE))
        .build();
  }

  @Override
  public ISessionPool getSessionPool(final int maxSize) {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return configureClientSSL(
            new SessionPool.Builder()
                .host(dataNode.getIp())
                .port(dataNode.getPort())
                .user(SessionConfig.DEFAULT_USER)
                .password(SessionConfig.DEFAULT_PASSWORD)
                .maxSize(maxSize))
        .build();
  }

  @Override
  public ITableSessionPool getTableSessionPool(final int maxSize) {
    final DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return configureClientSSL(
            new TableSessionPoolBuilder()
                .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString()))
                .user(SessionConfig.DEFAULT_USER)
                .password(SessionConfig.DEFAULT_PASSWORD)
                .maxSize(maxSize))
        .build();
  }

  @Override
  public ITableSessionPool getTableSessionPool(final int maxSize, final String database) {
    DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return configureClientSSL(
            new TableSessionPoolBuilder()
                .nodeUrls(Collections.singletonList(dataNode.getIpAndPortString()))
                .user(SessionConfig.DEFAULT_USER)
                .password(SessionConfig.DEFAULT_PASSWORD)
                .database(database)
                .maxSize(maxSize))
        .build();
  }

  protected NodeConnection getWriteConnection(
      Constant.Version version, String username, String password, String sqlDialect)
      throws SQLException {
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
    final String endpoint =
        UrlUtils.formatTEndPointIpv4AndIpv6Url(dataNode.getIp(), dataNode.getPort());
    final Connection writeConnection =
        DriverManager.getConnection(
            Config.IOTDB_URL_PREFIX
                + endpoint
                + getParam(version, NODE_NETWORK_TIMEOUT_MS, ZERO_TIME_ZONE),
            constructConnectionProperties(username, password, sqlDialect));
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
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT, this);

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
                              constructConnectionProperties(username, password, sqlDialect))));
            });
    return readConnRequestDelegate.requestAll();
  }

  protected List<NodeConnection> getOneAvailableReadConnection(
      final Constant.Version version,
      final String username,
      final String password,
      final String sqlDialect)
      throws SQLException {
    final List<DataNodeWrapper> dataNodeWrapperListCopy = new ArrayList<>(dataNodeWrapperList);
    Collections.shuffle(dataNodeWrapperListCopy);
    SQLException lastException = null;
    for (final DataNodeWrapper dataNode : dataNodeWrapperListCopy) {
      try {
        return getReadConnections(version, dataNode, username, password, sqlDialect);
      } catch (final SQLException e) {
        lastException = e;
      }
    }
    logger.error("Failed to get connection from any DataNode, last exception is ", lastException);
    throw lastException;
  }

  protected List<NodeConnection> getReadConnections(
      final Constant.Version version,
      final DataNodeWrapper dataNode,
      final String username,
      final String password,
      final String sqlDialect)
      throws SQLException {
    final List<String> endpoints = new ArrayList<>();
    final ParallelRequestDelegate<NodeConnection> readConnRequestDelegate =
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT, this);

    endpoints.add(dataNode.getIpAndPortString());
    readConnRequestDelegate.addRequest(
        () ->
            new NodeConnection(
                dataNode.getIpAndPortString(),
                NodeConnection.NodeRole.DATA_NODE,
                NodeConnection.ConnectionRole.READ,
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX
                        + dataNode.getIpAndPortString()
                        + getParam(version, NODE_NETWORK_TIMEOUT_MS, ZERO_TIME_ZONE),
                    constructConnectionProperties(username, password, sqlDialect))));

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
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT, this);
    final Map<String, String> lastConnectionFailures = Collections.synchronizedMap(new HashMap<>());
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
                          constructConnectionProperties(
                              System.getProperty("User", "root"),
                              System.getProperty("Password", "root"),
                              TREE_SQL_DIALECT))) {
                logger.info("Successfully connecting to DataNode: {}.", dataNodeEndpoint);
                return null;
              } catch (final Exception e) {
                lastException = e;
                lastConnectionFailures.put(
                    dataNodeEndpoint, e.getClass().getName() + ": " + e.getMessage());
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
      dumpTestJVMSnapshotQuietly("JDBC connection check failed");
      throw new AssertionError(
          String.format(
              "After %d times retry, JDBC connections to DataNodes are not ready. endpoints=%s, lastConnectionFailures=%s, logDirs=%s",
              retryCount, endpoints, lastConnectionFailures, getClusterLogDirs()));
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
  public void setTestClassName(final String testClassName) {
    if (testClassName == null || testClassName.isEmpty()) {
      this.testClassName = null;
      return;
    }
    final int lastDotIndex = testClassName.lastIndexOf(".");
    this.testClassName =
        lastDotIndex >= 0 ? testClassName.substring(lastDotIndex + 1) : testClassName;
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
    result.addAll(extraNodeWrappers);
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
          if (!configNodeWrapper.getInstance().isAlive()) {
            throw new IOException("ConfigNode " + configNodeWrapper.getId() + " is not alive");
          }

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
  public void shutdownForciblyAllConfigNodes() {
    configNodeWrapperList.forEach(AbstractNodeWrapper::stopForcibly);
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
            NODE_START_TIMEOUT,
            this);
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
        new ParallelRequestDelegate<>(dataNodeEndpoints, NODE_START_TIMEOUT, this);
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
  public void shutdownForciblyAllDataNodes() {
    dataNodeWrapperList.forEach(AbstractNodeWrapper::stopForcibly);
  }

  @Override
  public void ensureNodeStatus(
      final List<BaseNodeWrapper> nodes, final List<NodeStatus> targetStatusList)
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
                        UrlUtils.convertTEndPointIpv4AndIpv6Url(node.getInternalEndPoint()),
                        node.getConfigNodeId()));
        showClusterResp
            .getDataNodeList()
            .forEach(
                node ->
                    nodeIds.put(
                        UrlUtils.convertTEndPointIpv4AndIpv6Url(node.getClientRpcEndPoint()),
                        node.getDataNodeId()));
        for (int j = 0; j < nodes.size(); j++) {
          BaseNodeWrapper nodeWrapper = nodes.get(j);
          String ipAndPortString = nodeWrapper.getIpAndPortString();
          final String endpoint = ipAndPortString;
          if (!nodeIds.containsKey(endpoint)) {
            // Node not exist
            // Notice: Never modify this line, since the NodeLocation might be modified in IT
            errorMessages.add("The node " + nodes.get(j).getIpAndPortString() + " is not found!");
            continue;
          }
          final String status = showClusterResp.getNodeStatus().get(nodeIds.get(endpoint));
          final NodeStatus targetStatus = targetStatusList.get(j);
          if (!targetStatus.getStatus().equals(status)) {
            // Error status
            errorMessages.add(
                String.format(
                    "Node %s is in status %s, but expected %s",
                    endpoint, status, targetStatusList.get(j)));
            continue;
          }
          if (nodeWrapper instanceof DataNodeWrapper && targetStatus.equals(NodeStatus.Running)) {
            try (TSocket socket =
                new TSocket(
                    new TConfiguration(), nodeWrapper.getIp(), nodeWrapper.getPort(), 1000)) {
              socket.open();
            } catch (final TTransportException e) {
              errorMessages.add(
                  String.format(
                      "DataNode %s is not reachable: %s",
                      nodeWrapper.getIpAndPortString(), e.getMessage()));
            }
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

  public void clearClientManager() {
    clientManager.clearAll();
  }
}
