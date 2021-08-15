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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ClusterSessionSimpleIT {

  private static Logger node1Logger = LoggerFactory.getLogger("iotdb-server_1");
  private static Logger node2Logger = LoggerFactory.getLogger("iotdb-server_2");
  private static Logger node3Logger = LoggerFactory.getLogger("iotdb-server_3");

  private Session session;

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

  protected DockerComposeContainer getContainer() {
    return environment;
  }

  @Test
  public void testSessionCluster() throws IoTDBConnectionException, StatementExecutionException {
    List<String> stringList = new ArrayList<>();
    Integer service1Port = getContainer().getServicePort("iotdb-server_1", 6667);
    Integer service2Port = getContainer().getServicePort("iotdb-server_2", 6667);
    Integer service3Port = getContainer().getServicePort("iotdb-server_3", 6667);
    stringList.add("localhost:" + service1Port);
    stringList.add("localhost:" + service2Port);
    stringList.add("localhost:" + service3Port);
    session = new Session(stringList, "root", "root");
    session.open();
    session.setStorageGroup("root.sg1");
    session.createTimeseries(
        "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    session.createTimeseries(
        "root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.close();
  }
}
