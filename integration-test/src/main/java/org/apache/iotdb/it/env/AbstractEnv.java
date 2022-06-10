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

public abstract class AbstractEnv implements BaseEnv {
  private static final Logger logger = LoggerFactory.getLogger(AbstractEnv.class);
  private final int NODE_START_TIMEOUT = 10;
  protected List<ConfigNodeWrapper> configNodeWrapperList;
  protected List<DataNodeWrapper> dataNodeWrapperList;
  private final Random rand = new Random();
  protected String nextTestCase = null;

  protected void initEnvironment(int configNodesNum, int dataNodesNum) throws InterruptedException {
    this.configNodeWrapperList = new ArrayList<>();
    this.dataNodeWrapperList = new ArrayList<>();

    final String testName = getTestClassName() + getNextTestCaseString();

    ConfigNodeWrapper seedConfigNodeWrapper = new ConfigNodeWrapper(true, "", testName);
    seedConfigNodeWrapper.createDir();
    seedConfigNodeWrapper.changeConfig(ConfigFactory.getConfig().getConfignodeProperties());
    seedConfigNodeWrapper.start();
    String targetConfigNode = seedConfigNodeWrapper.getIpAndPortString();
    this.configNodeWrapperList.add(seedConfigNodeWrapper);
    logger.info(
        "In test " + testName + " SeedConfigNode " + seedConfigNodeWrapper.getId() + " started.");

    List<String> configNodeEndpoints = new ArrayList<>();
    RequestDelegate<Void> configNodesDelegate = new SerialRequestDelegate<>(configNodeEndpoints);
    for (int i = 1; i < configNodesNum; i++) {
      ConfigNodeWrapper configNodeWrapper =
          new ConfigNodeWrapper(false, targetConfigNode, testName);
      this.configNodeWrapperList.add(configNodeWrapper);
      configNodeEndpoints.add(configNodeWrapper.getIpAndPortString());
      configNodeWrapper.createDir();
      configNodeWrapper.changeConfig(null);
      configNodesDelegate.addRequest(
          () -> {
            configNodeWrapper.start();
            logger.info(
                "In test " + testName + " ConfigNode " + configNodeWrapper.getId() + " started.");
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
      DataNodeWrapper dataNodeWrapper = new DataNodeWrapper(targetConfigNode, testName);
      this.dataNodeWrapperList.add(dataNodeWrapper);
      dataNodeEndpoints.add(dataNodeWrapper.getIpAndPortString());
      dataNodeWrapper.createDir();
      dataNodeWrapper.changeConfig(ConfigFactory.getConfig().getEngineProperties());
      dataNodesDelegate.addRequest(
          () -> {
            dataNodeWrapper.start();
            logger.info(
                "In test " + testName + " DataNode " + dataNodeWrapper.getId() + " started.");
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
    for (DataNodeWrapper dataNodeWrapper : this.dataNodeWrapperList) {
      dataNodeWrapper.stop();
      dataNodeWrapper.waitingToShutDown();
      dataNodeWrapper.destroyDir();
    }
    for (ConfigNodeWrapper configNodeWrapper : this.configNodeWrapperList) {
      configNodeWrapper.stop();
      configNodeWrapper.waitingToShutDown();
      configNodeWrapper.destroyDir();
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
        dataNodeWrapperList.stream()
            .map(AbstractNodeWrapper::getIpAndPortString)
            .collect(Collectors.toList());
    boolean[] success = new boolean[dataNodeWrapperList.size()];
    Exception[] exceptions = new Exception[dataNodeWrapperList.size()];
    final int probeTimeout = 5;
    AtomicInteger successCount = new AtomicInteger(0);
    for (int counter = 0; counter < 30; counter++) {
      RequestDelegate<Void> testDelegate = new ParallelRequestDelegate<>(endpoints, probeTimeout);
      for (int i = 0; i < dataNodeWrapperList.size(); i++) {
        final int idx = i;
        final String dataNodeEndpoint = dataNodeWrapperList.get(i).getIpAndPortString();
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
      if (successCount.get() == dataNodeWrapperList.size()) {
        logger.info("The whole cluster is ready.");
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    // The cluster is not ready after 30 times to try
    for (int i = 0; i < dataNodeWrapperList.size(); i++) {
      if (!success[i] && exceptions[i] != null) {
        logger.error(
            "Connect to {} failed", dataNodeWrapperList.get(i).getIpAndPortString(), exceptions[i]);
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
    if (System.getProperty("ReadAndVerifyWithMultiNode", "true").equalsIgnoreCase("true")) {
      return new ClusterTestConnection(getWriteConnection(version), getReadConnections(version));
    } else {
      return getWriteConnection(version).getUnderlyingConnecton();
    }
  }

  protected NodeConnection getWriteConnection(Constant.Version version) throws SQLException {
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
    for (DataNodeWrapper dataNodeWarpper : this.dataNodeWrapperList) {
      final String endpoint = dataNodeWarpper.getIpAndPortString();
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

  public String getNextTestCaseString() {
    if (nextTestCase != null) {
      return "_" + nextTestCase;
    }
    return "";
  }
}
