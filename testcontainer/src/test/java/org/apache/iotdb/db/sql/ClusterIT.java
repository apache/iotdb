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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ClusterIT {
  private Statement statement;
  private Connection connection;

  @Rule
  public DockerComposeContainer environment =
      new NoProjectNameDockerComposeContainer(
              "3nodes", new File("src/test/resources/3nodes/docker-compose.yaml"))
          .withExposedService("iotdb-server_1", 6667, Wait.forListeningPort())
          .withExposedService("iotdb-server_2", 6667, Wait.forListeningPort())
          .withExposedService("iotdb-server_3", 6667, Wait.forListeningPort())
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
  }
}
