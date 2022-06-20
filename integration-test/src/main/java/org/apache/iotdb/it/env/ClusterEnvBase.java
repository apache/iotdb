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

import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;
import org.apache.iotdb.itbase.runtime.NodeConnection;
import org.apache.iotdb.itbase.runtime.ParallelRequestDelegate;
import org.apache.iotdb.itbase.runtime.RequestDelegate;
import org.apache.iotdb.itbase.runtime.SerialRequestDelegate;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

public abstract class ClusterEnvBase implements BaseEnv {
  private static final Logger logger = LoggerFactory.getLogger(ClusterEnvBase.class);
  private final int NODE_START_TIMEOUT = 10;
  private List<ConfigNode> configNodes;
  private List<DataNode> dataNodes;
  private final Random rand = new Random();
  private String nextTestCase = null;

  protected void initEnvironment(int configNodesNum, int dataNodesNum) throws InterruptedException {
    this.configNodes = new ArrayList<>();
    this.dataNodes = new ArrayList<>();

    final String testName = getTestClassName() + getNextTestCaseString();

    ConfigNode seedConfigNode = new ConfigNode(true, "", testName);
    seedConfigNode.createDir();
    seedConfigNode.changeConfig(null);
    seedConfigNode.start();
    String targetConfignode = seedConfigNode.getIpAndPortString();
    this.configNodes.add(seedConfigNode);
    logger.info("In test " + testName + " SeedConfigNode " + seedConfigNode.getId() + " started.");

    List<String> configNodeEndpoints = new ArrayList<>();
    RequestDelegate<Void> configNodesDelegate = new SerialRequestDelegate<>(configNodeEndpoints);
    for (int i = 1; i < configNodesNum; i++) {
      ConfigNode configNode = new ConfigNode(false, targetConfignode, testName);
      this.configNodes.add(configNode);
      configNodeEndpoints.add(configNode.getIpAndPortString());
      configNode.createDir();
      configNode.changeConfig(null);
      configNodesDelegate.addRequest(
          () -> {
            configNode.start();
            logger.info("In test " + testName + " ConfigNode " + configNode.getId() + " started.");
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
      DataNode dataNode = new DataNode(targetConfignode, testName);
      this.dataNodes.add(dataNode);
      dataNodeEndpoints.add(dataNode.getIpAndPortString());
      dataNode.createDir();
      dataNode.changeConfig(ConfigFactory.getConfig().getEngineProperties());
      dataNodesDelegate.addRequest(
          () -> {
            dataNode.start();
            logger.info("In test " + testName + " DataNode " + dataNode.getId() + " started.");
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
    for (DataNode dataNode : this.dataNodes) {
      dataNode.stop();
      dataNode.waitingToShutDown();
      dataNode.destroyDir();
    }
    for (ConfigNode configNode : this.configNodes) {
      configNode.stop();
      configNode.waitingToShutDown();
      configNode.destroyDir();
    }
    nextTestCase = null;
  }

  public String getTestClassName() {
    StackTraceElement stack[] = Thread.currentThread().getStackTrace();
    for (int i = 0; i < stack.length; i++) {
      String className = stack[i].getClassName();
      if (className.endsWith("IT")) {
        return className.substring(className.lastIndexOf(".") + 1);
      }
    }
    return "UNKNOWN-IT";
  }

  public String getTestClassNameAndDate() {
    Date date = new Date();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement stackTraceElement : stack) {
      String className = stackTraceElement.getClassName();
      if (className.endsWith("IT")) {
        return className.substring(className.lastIndexOf(".") + 1) + "-" + formatter.format(date);
      }
    }
    return "IT";
  }

  public void testWorking() throws InterruptedException {
    List<String> endpoints =
        dataNodes.stream().map(ClusterNodeBase::getIpAndPortString).collect(Collectors.toList());
    boolean[] success = new boolean[dataNodes.size()];
    Exception[] exceptions = new Exception[dataNodes.size()];
    final int probeTimeout = 5;
    AtomicInteger successCount = new AtomicInteger(0);
    for (int counter = 0; counter < 30; counter++) {
      RequestDelegate<Void> testDelegate = new ParallelRequestDelegate<>(endpoints, probeTimeout);
      for (int i = 0; i < dataNodes.size(); i++) {
        final int idx = i;
        final String dataNodeEndpoint = dataNodes.get(i).getIpAndPortString();
        testDelegate.addRequest(
            () -> {
              if (!success[idx]) {
                try (Connection ignored = getConnection(dataNodeEndpoint, probeTimeout)) {
                  success[idx] = true;
                  successCount.incrementAndGet();
                } catch (Exception e) {
                  exceptions[idx] = e;
                  logger.debug("Open connection of {} failed", dataNodeEndpoint, e);
                }
              }
              return null;
            });
      }
      try {
        testDelegate.requestAll();
      } catch (SQLException e) {
        // It will never be thrown as it has already caught in the request.
      }
      if (successCount.get() == dataNodes.size()) {
        logger.info("The whole cluster is ready.");
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    // The cluster is not ready after 30 times to try
    for (int i = 0; i < dataNodes.size(); i++) {
      if (!success[i] && exceptions[i] != null) {
        logger.error("Connect to {} failed", dataNodes.get(i).getIpAndPortString(), exceptions[i]);
      }
    }
    fail("After 30 times retry, the cluster can't work!");
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
  public Connection getConnection() throws SQLException {
    return new ClusterTestConnection(getWriteConnection(null), getReadConnections(null));
  }

  @Override
  public void setNextTestCaseName(String testCaseName) {
    nextTestCase = testCaseName;
  }

  private Connection getConnection(String endpoint, int queryTimeout) throws SQLException {
    IoTDBConnection connection =
        (IoTDBConnection)
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + endpoint,
                System.getProperty("User", "root"),
                System.getProperty("Password", "root"));
    connection.setQueryTimeout(queryTimeout);
    return connection;
  }

  @Override
  public Connection getConnection(Constant.Version version) throws SQLException {
    return new ClusterTestConnection(getWriteConnection(version), getReadConnections(version));
  }

  protected NodeConnection getWriteConnection(Constant.Version version) throws SQLException {
    // Randomly choose a node for handling write requests
    DataNode dataNode = this.dataNodes.get(rand.nextInt(this.dataNodes.size()));
    String endpoint = dataNode.getIp() + ":" + dataNode.getPort();
    Connection writeConnection =
        DriverManager.getConnection(
            Config.IOTDB_URL_PREFIX + endpoint + getVersionParam(version),
            System.getProperty("User", "root"),
            System.getProperty("Password", "root"));
    return new NodeConnection(
        endpoint,
        NodeConnection.NodeRole.DATA_NODE,
        NodeConnection.ConnectionRole.WRITE,
        writeConnection);
  }

  protected List<NodeConnection> getReadConnections(Constant.Version version) throws SQLException {
    List<String> endpoints = new ArrayList<>();
    ParallelRequestDelegate<NodeConnection> readConnRequestDelegate =
        new ParallelRequestDelegate<>(endpoints, NODE_START_TIMEOUT);
    for (DataNode dataNode : this.dataNodes) {
      final String endpoint = dataNode.getIpAndPortString();
      endpoints.add(endpoint);
      readConnRequestDelegate.addRequest(
          () -> {
            Connection readConnection =
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + endpoint + getVersionParam(version),
                    System.getProperty("User", "root"),
                    System.getProperty("Password", "root"));
            return new NodeConnection(
                endpoint,
                NodeConnection.NodeRole.DATA_NODE,
                NodeConnection.ConnectionRole.READ,
                readConnection);
          });
    }
    return readConnRequestDelegate.requestAll();
  }

  private String getVersionParam(Constant.Version version) {
    if (version == null) {
      return "";
    }
    return "?" + VERSION + "=" + version;
  }

  private String getNextTestCaseString() {
    if (nextTestCase != null) {
      return "_" + nextTestCase;
    }
    return "";
  }
}
