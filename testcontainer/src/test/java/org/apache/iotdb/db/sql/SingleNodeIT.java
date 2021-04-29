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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class SingleNodeIT {
  private static Logger logger = LoggerFactory.getLogger(SingleNodeIT.class);
  private Statement statement;
  private Connection connection;

  @Rule
  public GenericContainer dslContainer =
      new GenericContainer(DockerImageName.parse("apache/iotdb:maven-development"))
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          // mount another properties for changing parameters, e.g., open 5555 port (sync module)
          .withFileSystemBind(
              new File("src/test/resources/iotdb-engine.properties").getAbsolutePath(),
              "/iotdb/conf/iotdb-engine.properties",
              BindMode.READ_ONLY)
          .withFileSystemBind(
              new File("src/test/resources/logback-container.xml").getAbsolutePath(),
              "/iotdb/conf/logback.xml",
              BindMode.READ_ONLY)
          .withLogConsumer(new Slf4jLogConsumer(logger))
          .withExposedPorts(6667)
          .waitingFor(Wait.forListeningPort());

  int rpcPort = 6667;
  int syncPort = 5555;

  @Before
  public void setUp() throws Exception {
    rpcPort = dslContainer.getMappedPort(6667);

    syncPort = dslContainer.getMappedPort(5555);
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:" + rpcPort, "root", "root");
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
