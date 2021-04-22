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
package org.apache.iotdb.db.sql;

import org.apache.iotdb.jdbc.Config;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class ClusterIT {
  private static Logger logger = LoggerFactory.getLogger(ClusterIT.class);
  private static Logger node1Logger = LoggerFactory.getLogger("iotdb-server_1");
  private static Logger node2Logger = LoggerFactory.getLogger("iotdb-server_2");
  private static Logger node3Logger = LoggerFactory.getLogger("iotdb-server_3");

  private Statement statement;
  private Connection connection;

  // in TestContainer's document, it is @ClassRule, and the environment is `public static`
  // I am not sure the difference now.
  @Rule
  public DockerComposeContainer environment =
      new NoProjectNameDockerComposeContainer(
              "3nodes", new File("src/test/resources/3nodes/docker-compose.yaml"))
          .withExposedService("iotdb-server_1", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_1", new Slf4jLogConsumer(node1Logger))
          .withExposedService("iotdb-server_2", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_2", new Slf4jLogConsumer(node2Logger))
          .withExposedService("iotdb-server_3", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_3", new Slf4jLogConsumer(node3Logger))
          .withLocalCompose(true);

  int rpcPort = 6667;

  @Before
  public void setUp() throws Exception {

    String ip = environment.getServiceHost("iotdb-server_1", 6667);
    rpcPort = environment.getServicePort("iotdb-server_1", 6667);

    Class.forName(Config.JDBC_DRIVER_NAME);
    connection = DriverManager.getConnection("jdbc:iotdb://" + ip + ":" + rpcPort, "root", "root");
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
  }

  @Test
  public void testSimplePutAndGet() throws SQLException {

    String[] timeSeriesArray = {"root.sg1.aa.bb", "root.sg1.aa.bb.cc", "root.sg1.aa"};

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              timeSeries));
    }
    ResultSet resultSet = null;
    resultSet = statement.executeQuery("show timeseries");
    Set<String> result = new HashSet<>();
    while (resultSet.next()) {
      result.add(resultSet.getString(1));
    }
    Assert.assertEquals(3, result.size());
    for (String timeseries : timeSeriesArray) {
      Assert.assertTrue(result.contains(timeseries));
    }
  }
}
