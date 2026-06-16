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

package org.apache.iotdb.it.env.cluster.env;

import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.node.AINodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.runtime.ParallelRequestDelegate;
import org.apache.iotdb.itbase.runtime.RequestDelegate;

import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Collections;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.NODE_START_TIMEOUT;

public class AIEnv extends AbstractEnv {
  private static final Logger logger = IoTDBTestLogger.logger;

  @Override
  public void initClusterEnvironment() {
    initClusterEnvironment(1, 1);
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    super.initEnvironment(configNodesNum, dataNodesNum, 600);
  }

  @Override
  public void initClusterEnvironment(
      int configNodesNum, int dataNodesNum, int testWorkingRetryCount) {
    super.initEnvironment(configNodesNum, dataNodesNum, testWorkingRetryCount);
  }

  @Override
  protected void initExtraNodes(String seedConfigNode, int dataNodePort) {
    startAINode(seedConfigNode, dataNodePort, getTestClassName());
  }

  private void startAINode(
      final String seedConfigNode, final int clusterIngressPort, final String testClassName) {
    final AINodeWrapper aiNodeWrapper =
        new AINodeWrapper(
            seedConfigNode,
            clusterIngressPort,
            testClassName,
            testMethodName,
            index,
            EnvUtils.searchAvailablePorts(),
            startTime);
    extraNodeWrappers.add(aiNodeWrapper);
    aiNodeWrapper.setKillPoints(extraNodeKillPoints);
    final String aiNodeEndPoint = aiNodeWrapper.getIpAndPortString();
    aiNodeWrapper.createNodeDir();
    aiNodeWrapper.createLogDir();
    final RequestDelegate<Void> aiNodesDelegate =
        new ParallelRequestDelegate<>(
            Collections.singletonList(aiNodeEndPoint), NODE_START_TIMEOUT, this);

    aiNodesDelegate.addRequest(
        () -> {
          aiNodeWrapper.start();
          return null;
        });

    try {
      aiNodesDelegate.requestAll();
    } catch (final SQLException e) {
      logger.error("Start aiNodes failed", e);
    }
  }
}
