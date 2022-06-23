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
import java.util.stream.Collectors;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

public abstract class AbstractEnv implements BaseEnv {
  private static final Logger logger = LoggerFactory.getLogger(AbstractEnv.class);
  private final int NODE_START_TIMEOUT = 100;
  private final int PROBE_TIMEOUT_MS = 2000;
  private final int NODE_NETWORK_TIMEOUT_MS = 65_000;
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
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement stackTraceElement : stack) {
      String className = stackTraceElement.getClassName();
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

  public void testWorking() {
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
      testDelegate.requestAll();
    } catch (Exception e) {
      fail("After 30 times retry, the cluster can't work!");
    }
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
                Config.IOTDB_URL_PREFIX + endpoint + getParam(null, queryTimeout),
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
            Config.IOTDB_URL_PREFIX + endpoint + getParam(version, NODE_NETWORK_TIMEOUT_MS),
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
    for (DataNodeWrapper dataNodeWrapper : this.dataNodeWrapperList) {
      final String endpoint = dataNodeWrapper.getIpAndPortString();
      endpoints.add(endpoint);
      readConnRequestDelegate.addRequest(
          () -> {
            Connection readConnection =
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + endpoint + getParam(version, NODE_NETWORK_TIMEOUT_MS),
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

  private String getParam(Constant.Version version, int timeout) {
    StringBuilder sb = new StringBuilder("?");
    sb.append(Config.NETWORK_TIMEOUT).append("=").append(timeout);
    if (version != null) {
      sb.append("&").append(VERSION).append("=").append(version);
    }
    return sb.toString();
  }

  public String getNextTestCaseString() {
    if (nextTestCase != null) {
      return "_" + nextTestCase;
    }
    return "";
  }
}
