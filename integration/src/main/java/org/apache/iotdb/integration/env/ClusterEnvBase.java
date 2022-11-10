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
package org.apache.iotdb.integration.env;

import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

public abstract class ClusterEnvBase implements BaseEnv {

  private static final Logger logger = LoggerFactory.getLogger(ClusterEnvBase.class);
  protected List<ClusterNode> nodes;

  public List<Integer> searchAvailablePort(int nodeNum) {
    // To search available ports and to prepare for concurrent cluster testing, so we search port in
    // batches. For example, when there are 5 nodes, the port range of the first search is
    // 6671-6680, 10001-10010, 11001-21010. If any one of these 30 ports is occupied, it will be
    // added up as a whole (add 10 to each port) to look for the next batch of ports.

    String cmd = "lsof -iTCP -sTCP:LISTEN -P -n | grep -E ";
    int rpcPortStart = 6671;
    int metaPortStart = 10001;
    int dataPortStart = 11001;
    boolean flag = true;
    int counter = 0;
    do {
      StringBuilder port =
          new StringBuilder("" + rpcPortStart++)
              .append("|")
              .append(metaPortStart++)
              .append("|")
              .append(dataPortStart++)
              .append("|")
              .append(rpcPortStart++)
              .append("|")
              .append(metaPortStart++)
              .append("|")
              .append(dataPortStart++);
      for (int i = 1; i < nodeNum; i++) {
        port.append("|")
            .append(rpcPortStart++)
            .append("|")
            .append(metaPortStart++)
            .append("|")
            .append(dataPortStart++)
            .append("|")
            .append(rpcPortStart++)
            .append("|")
            .append(metaPortStart++)
            .append("|")
            .append(dataPortStart++);
      }
      try {
        Process proc = Runtime.getRuntime().exec(cmd + "\"" + port + "\"");
        BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line;
        while ((line = br.readLine()) != null) {
          System.out.println(line);
        }
        System.out.println();
        if (proc.waitFor() == 1) {
          flag = false;
        }
      } catch (IOException | InterruptedException ex) {
        // ignore
      }

      counter++;
      if (counter >= 100) {
        fail("No more available port to test cluster.");
      }
    } while (flag);

    List<Integer> portList = new ArrayList<>();
    for (int i = 0; i < nodeNum; i++) {
      portList.add(rpcPortStart - 2 * (nodeNum - i));
      portList.add(metaPortStart - 2 * (nodeNum - i));
      portList.add(dataPortStart - 2 * (nodeNum - i));
    }

    return portList;
  }

  public void testWorking() throws InterruptedException {
    int counter = 0;
    Thread.sleep(2000);

    do {
      Thread.sleep(1000);

      try (IoTDBConnection connection = getConnection(60);
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE DATABASE root.test" + counter);
        statement.execute(
            "CREATE TIMESERIES root.test" + counter + ".d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
        if (statement.execute("SHOW TIMESERIES")) {
          ResultSet resultSet = statement.getResultSet();
          if (resultSet.next()) {
            statement.execute("DELETE DATABASE root.*");
            break;
          }
        }

      } catch (SQLException e) {
        logger.debug(++counter + " time(s) connect to cluster failed!");
        logger.debug(e.getMessage());
      }

      if (counter > 30) {
        fail("After 30 times retry, the cluster can't work!");
      }
    } while (true);
  }

  public void startCluster() throws InterruptedException {
    try {
      for (ClusterNode node : this.nodes) {
        node.start();
      }
    } catch (IOException ex) {
      fail(ex.getMessage());
    }

    testWorking();
  }

  public void stopCluster() {
    for (ClusterNode node : this.nodes) {
      node.stop();
    }
    for (ClusterNode node : this.nodes) {
      node.waitingToShutDown();
    }
  }

  public void createNodeDir() {
    for (ClusterNode node : this.nodes) {
      node.createDir();
    }
  }

  public void destroyNodeDir() {
    for (ClusterNode node : this.nodes) {
      node.destroyDir();
    }
  }

  public void changeNodesConfig() {
    try {
      for (ClusterNode node : this.nodes) {
        node.changeConfig(
            ConfigFactory.getConfig().getEngineProperties(),
            ConfigFactory.getConfig().getClusterProperties());
      }
      ConfigFactory.getConfig().clearAllProperties();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Override
  public void initBeforeClass() throws InterruptedException {}

  @Override
  public void cleanAfterClass() {
    stopCluster();
    destroyNodeDir();
  }

  @Override
  public void initBeforeTest() throws InterruptedException {}

  @Override
  public void cleanAfterTest() {
    stopCluster();
    destroyNodeDir();
  }

  @Override
  public Connection getConnection() throws SQLException {
    Connection connection = null;

    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX
                  + this.nodes.get(0).getIp()
                  + ":"
                  + this.nodes.get(0).getPort(),
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
                      + this.nodes.get(0).getIp()
                      + ":"
                      + this.nodes.get(0).getPort(),
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
                  + this.nodes.get(0).getIp()
                  + ":"
                  + this.nodes.get(0).getPort()
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
