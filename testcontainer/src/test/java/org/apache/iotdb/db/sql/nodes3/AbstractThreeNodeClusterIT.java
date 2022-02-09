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

package org.apache.iotdb.db.sql.nodes3;

import org.apache.iotdb.db.sql.ClusterIT;

import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;

// just add real ITs into AbstractClusterIT.
// in this case, the data must be on all nodes.
// we just simulate write data on node A and read data on either node A or B.
public abstract class AbstractThreeNodeClusterIT extends ClusterIT {

  private static Logger node1Logger = LoggerFactory.getLogger("iotdb-server_1");
  private static Logger node2Logger = LoggerFactory.getLogger("iotdb-server_2");
  private static Logger node3Logger = LoggerFactory.getLogger("iotdb-server_3");

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

  @Override
  protected DockerComposeContainer getContainer() {
    return environment;
  }
}
