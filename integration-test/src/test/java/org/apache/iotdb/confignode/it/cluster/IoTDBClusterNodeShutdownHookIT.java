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

package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterNodeShutdownHookIT {

  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  private static final int testConfigNodeNum = 3;
  private static final int testDataNodeNum = 1;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass);

    // Init 2C1D environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNodeShutdownReporter()
      throws ClientManagerException, IOException, InterruptedException, TException {
    EnvFactory.getEnv().shutdownConfigNode(1);
    EnvFactory.getEnv().shutdownDataNode(0);

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // The stopped Nodes should be detected immediately with the help of shutdown hook. A
      // ConfigNode whose report can not reach the newly elected leader remains Unknown instead.
      TShowClusterResp showClusterResp = client.showCluster();
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showClusterResp.getStatus().getCode());

      int stoppedDataNodeId = -1;
      for (TDataNodeLocation dataNodeLocation : showClusterResp.getDataNodeList()) {
        if (dataNodeLocation.getInternalEndPoint().getPort()
            == EnvFactory.getEnv().getDataNodeWrapper(0).getInternalPort()) {
          stoppedDataNodeId = dataNodeLocation.getDataNodeId();
        }
      }
      Assert.assertNotEquals(-1, stoppedDataNodeId);

      int stoppedConfigNodeId = -1;
      for (TConfigNodeLocation configNodeLocation : showClusterResp.getConfigNodeList()) {
        if (configNodeLocation.getConsensusEndPoint().getPort()
            == EnvFactory.getEnv().getConfigNodeWrapper(1).getConsensusPort()) {
          stoppedConfigNodeId = configNodeLocation.getConfigNodeId();
        }
      }
      Assert.assertNotEquals(-1, stoppedConfigNodeId);

      boolean isDetected = false;
      for (int retry = 0; retry < 5; retry++) {
        showClusterResp = client.showCluster();
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), showClusterResp.getStatus().getCode());

        // The stopped DataNode must be observable as Stopped
        final String dataNodeStatus = showClusterResp.getNodeStatus().get(stoppedDataNodeId);
        final boolean isDataNodeStopped = NodeStatus.Stopped.getStatus().equals(dataNodeStatus);

        // The stopped ConfigNode is Stopped when its report reached a leader, and may otherwise
        // remain Unknown until heartbeat timeout
        final String configNodeStatus = showClusterResp.getNodeStatus().get(stoppedConfigNodeId);
        final boolean isConfigNodeDetected =
            NodeStatus.Stopped.getStatus().equals(configNodeStatus)
                || NodeStatus.Unknown.getStatus().equals(configNodeStatus);

        if (isDataNodeStopped && isConfigNodeDetected) {
          isDetected = true;
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      Assert.assertTrue(
          "Timed out waiting for the stopped DataNode and ConfigNode to be detected", isDetected);
    }
  }
}
