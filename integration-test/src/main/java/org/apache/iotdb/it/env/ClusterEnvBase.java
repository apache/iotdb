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
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

public abstract class ClusterEnvBase implements BaseEnv {
  private static final Logger logger = LoggerFactory.getLogger(ClusterEnvBase.class);
  private List<ConfigNode> configNodes;
  private List<DataNode> dataNodes;

  protected void initEnvironment(int configNodesNum, int dataNodesNum) throws InterruptedException {
    this.configNodes = new ArrayList<>();
    this.dataNodes = new ArrayList<>();

    String testName = getTestClassName();

    ConfigNode seedConfigNode = new ConfigNode(true, "", testName);
    seedConfigNode.createDir();
    seedConfigNode.changeConfig(null);
    seedConfigNode.start();
    String targetConfignode = seedConfigNode.getIpAndPortString();
    this.configNodes.add(seedConfigNode);
    logger.info("In test " + testName + " SeedConfigNode " + seedConfigNode.getId() + " started.");

    for (int i = 1; i < configNodesNum; i++) {
      ConfigNode configNode = new ConfigNode(false, targetConfignode, testName);
      configNode.createDir();
      configNode.changeConfig(null);
      configNode.start();
      this.configNodes.add(configNode);
      logger.info("In test " + testName + " ConfigNode " + configNode.getId() + " started.");
    }

    for (int i = 0; i < dataNodesNum; i++) {
      DataNode dataNode = new DataNode(targetConfignode, testName);
      dataNode.createDir();
      dataNode.changeConfig(ConfigFactory.getConfig().getEngineProperties());
      dataNode.start();
      this.dataNodes.add(dataNode);
      logger.info("In test " + testName + " DataNode " + dataNode.getId() + " started.");
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
    StackTraceElement stack[] = Thread.currentThread().getStackTrace();
    for (int i = 0; i < stack.length; i++) {
      String className = stack[i].getClassName();
      if (className.endsWith("IT")) {
        return className.substring(className.lastIndexOf(".") + 1) + "-" + formatter.format(date);
      }
    }
    return "IT";
  }

  public void testWorking() throws InterruptedException {
    int counter = 0;
    Thread.sleep(2000);

    do {
      Thread.sleep(1000);

      counter++;
      if (counter > 30) {
        fail("After 30 times retry, the cluster can't work!");
      }

      try (IoTDBConnection connection = getConnection(60);
          Statement statement = connection.createStatement()) {
        statement.execute("SET STORAGE GROUP TO root.test" + counter);
        statement.execute(
            "CREATE TIMESERIES root.test" + counter + ".d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
        if (statement.execute("SHOW TIMESERIES")) {
          ResultSet resultSet = statement.getResultSet();
          if (resultSet.next()) {
            statement.execute("DELETE STORAGE GROUP root.*");
            logger.info("init done");
            break;
          }
        }

      } catch (SQLException e) {
        logger.debug(counter + " time(s) connect to cluster failed!", e);
      }
    } while (true);
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
    Connection connection = null;

    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX
                  + this.dataNodes.get(0).getIp()
                  + ":"
                  + this.dataNodes.get(0).getPort(),
              System.getProperty("User", "root"),
              System.getProperty("Password", "root"));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return connection;
  }

  public IoTDBConnection getConnection(int queryTimeout) throws SQLException {
    IoTDBConnection connection = null;
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          (IoTDBConnection)
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX
                      + this.dataNodes.get(0).getIp()
                      + ":"
                      + this.dataNodes.get(0).getPort(),
                  System.getProperty("User", "root"),
                  System.getProperty("Password", "root"));
      connection.setQueryTimeout(queryTimeout);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return connection;
  }

  @Override
  public Connection getConnection(Constant.Version version) throws SQLException {
    Connection connection = null;
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX
                  + this.dataNodes.get(0).getIp()
                  + ":"
                  + this.dataNodes.get(0).getPort()
                  + "?"
                  + VERSION
                  + "="
                  + version.toString(),
              System.getProperty("User", "root"),
              System.getProperty("Password", "root"));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      fail();
    }
    return connection;
  }
}
