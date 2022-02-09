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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FiveNodeCluster1Env extends ClusterEnvBase {
  private static final Logger logger = LoggerFactory.getLogger(FiveNodeCluster1Env.class);

  private void initEnvironment() throws InterruptedException {
    List<Integer> portList = searchAvailablePort(5);
    int[] metaPortArray =
        new int[] {
          portList.get(1), portList.get(4), portList.get(7), portList.get(10), portList.get(13)
        };

    this.nodes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      this.nodes.add(
          new ClusterNode(
              "node" + portList.get(i * 3),
              "127.0.0.1",
              portList.get(i * 3),
              portList.get(i * 3 + 1),
              portList.get(i * 3 + 2),
              metaPortArray));
    }
    createNodeDir();
    changeNodesConfig();
    startCluster();
  }

  @Override
  public void initBeforeClass() throws InterruptedException {
    logger.debug("=======start init class=======");
    initEnvironment();
  }

  @Override
  public void initBeforeTest() throws InterruptedException {
    logger.debug("=======start init test=======");
    initEnvironment();
  }
}
