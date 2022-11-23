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
package org.apache.iotdb.confignode.it.partition;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.ConfigFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionDurableTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPartitionDurableTest.class);

  private static String originalConfigNodeConsensusProtocolClass;
  private static String originalSchemaRegionConsensusProtocolClass;
  private static String originalDataRegionConsensusProtocolClass;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  private static int originalSchemaReplicationFactor;
  private static int originalDataReplicationFactor;
  private static final int testReplicationFactor = 3;

  private static long originalTimePartitionInterval;
  private static final long testTimePartitionInterval = 604800000;

  private static final String sg = "root.sg";

  @BeforeClass
  public static void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(testConsensusProtocolClass);
    ConfigFactory.getConfig().setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass);
    ConfigFactory.getConfig().setDataRegionConsensusProtocolClass(testConsensusProtocolClass);

    originalSchemaReplicationFactor = ConfigFactory.getConfig().getSchemaReplicationFactor();
    originalDataReplicationFactor = ConfigFactory.getConfig().getDataReplicationFactor();
    ConfigFactory.getConfig().setSchemaReplicationFactor(testReplicationFactor);
    ConfigFactory.getConfig().setDataReplicationFactor(testReplicationFactor);

    originalTimePartitionInterval = ConfigFactory.getConfig().getTimePartitionInterval();
    ConfigFactory.getConfig().setTimePartitionInterval(testTimePartitionInterval);

    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();

    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setSchemaRegionConsensusProtocolClass(originalSchemaRegionConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);

    ConfigFactory.getConfig().setSchemaReplicationFactor(originalSchemaReplicationFactor);
    ConfigFactory.getConfig().setDataReplicationFactor(originalDataReplicationFactor);

    ConfigFactory.getConfig().setTimePartitionInterval(originalTimePartitionInterval);
  }

  // TODO: Optimize this in IOTDB-4334
  @Test
  public void testPartitionDurable() throws IOException, TException, InterruptedException {
    final int testDataNodeId = 0;
    final int seriesPartitionBatchSize = 10;
    final int timePartitionBatchSize = 10;

    // Shutdown the first DataNode
    EnvFactory.getEnv().shutdownDataNode(testDataNodeId);

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final String sg0 = sg + 0;
      final String sg1 = sg + 1;

      // Set StorageGroup, the result should be success
      TSetStorageGroupReq setStorageGroupReq =
          new TSetStorageGroupReq(new TStorageGroupSchema(sg0));
      TSStatus status = client.setStorageGroup(setStorageGroupReq);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      setStorageGroupReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg1));
      status = client.setStorageGroup(setStorageGroupReq);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Test getOrCreateDataPartition, ConfigNode should create DataPartition and return
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg0,
              0,
              seriesPartitionBatchSize,
              0,
              timePartitionBatchSize,
              testTimePartitionInterval);
      TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      TDataPartitionTableResp dataPartitionTableResp = null;
      for (int retry = 0; retry < 5; retry++) {
        // Build new Client since it's unstable in Win8 environment
        try (SyncConfigNodeIServiceClient configNodeClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
          dataPartitionTableResp = configNodeClient.getOrCreateDataPartitionTable(dataPartitionReq);
          if (dataPartitionTableResp != null) {
            break;
          }
        } catch (Exception e) {
          // Retry sometimes in order to avoid request timeout
          LOGGER.error(e.getMessage());
          TimeUnit.SECONDS.sleep(1);
        }
      }
      Assert.assertNotNull(dataPartitionTableResp);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      ConfigNodeTestUtils.checkDataPartitionTable(
          sg0,
          0,
          seriesPartitionBatchSize,
          0,
          timePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // Check Region count
      int runningCnt = 0;
      int unknownCnt = 0;
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        if (RegionStatus.Running.getStatus().equals(regionInfo.getStatus())) {
          runningCnt += 1;
        } else if (RegionStatus.Unknown.getStatus().equals(regionInfo.getStatus())) {
          unknownCnt += 1;
        }
      }
      // The runningCnt should be exactly twice as the unknownCnt
      // since there exists one DataNode is shutdown
      Assert.assertEquals(unknownCnt * 2, runningCnt);

      // Wait for shutdown check
      TShowClusterResp showClusterResp;
      while (true) {
        boolean containUnknown = false;
        showClusterResp = client.showCluster();
        for (TDataNodeLocation dataNodeLocation : showClusterResp.getDataNodeList()) {
          if (NodeStatus.Unknown.getStatus()
              .equals(showClusterResp.getNodeStatus().get(dataNodeLocation.getDataNodeId()))) {
            containUnknown = true;
            break;
          }
        }
        if (containUnknown) {
          break;
        }
      }
      runningCnt = 0;
      unknownCnt = 0;
      showClusterResp = client.showCluster();
      for (TDataNodeLocation dataNodeLocation : showClusterResp.getDataNodeList()) {
        if (NodeStatus.Running.getStatus()
            .equals(showClusterResp.getNodeStatus().get(dataNodeLocation.getDataNodeId()))) {
          runningCnt += 1;
        } else if (NodeStatus.Unknown.getStatus()
            .equals(showClusterResp.getNodeStatus().get(dataNodeLocation.getDataNodeId()))) {
          unknownCnt += 1;
        }
      }
      Assert.assertEquals(2, runningCnt);
      Assert.assertEquals(1, unknownCnt);

      // Test getOrCreateDataPartition, ConfigNode should create DataPartition and return
      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg1,
              0,
              seriesPartitionBatchSize,
              0,
              timePartitionBatchSize,
              testTimePartitionInterval);
      dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      for (int retry = 0; retry < 5; retry++) {
        // Build new Client since it's unstable in Win8 environment
        try (SyncConfigNodeIServiceClient configNodeClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
          dataPartitionTableResp = configNodeClient.getOrCreateDataPartitionTable(dataPartitionReq);
          if (dataPartitionTableResp != null) {
            break;
          }
        } catch (Exception e) {
          // Retry sometimes in order to avoid request timeout
          LOGGER.error(e.getMessage());
          TimeUnit.SECONDS.sleep(1);
        }
      }
      Assert.assertNotNull(dataPartitionTableResp);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      ConfigNodeTestUtils.checkDataPartitionTable(
          sg1,
          0,
          seriesPartitionBatchSize,
          0,
          timePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // Check Region count and status
      runningCnt = 0;
      unknownCnt = 0;
      showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        if (RegionStatus.Running.getStatus().equals(regionInfo.getStatus())) {
          runningCnt += 1;
        } else if (RegionStatus.Unknown.getStatus().equals(regionInfo.getStatus())) {
          unknownCnt += 1;
        }
      }
      // The runningCnt should be exactly twice as the unknownCnt
      // since there exists one DataNode is shutdown
      Assert.assertEquals(unknownCnt * 2, runningCnt);

      EnvFactory.getEnv().startDataNode(testDataNodeId);
      // Wait for heartbeat check
      while (true) {
        boolean containUnknown = false;
        showClusterResp = client.showCluster();
        for (TDataNodeLocation dataNodeLocation : showClusterResp.getDataNodeList()) {
          if (NodeStatus.Unknown.getStatus()
              .equals(showClusterResp.getNodeStatus().get(dataNodeLocation.getDataNodeId()))) {
            containUnknown = true;
            break;
          }
        }
        if (!containUnknown) {
          break;
        }
      }

      // All Regions should alive after the testDataNode is restarted
      boolean allRunning = true;
      for (int retry = 0; retry < 30; retry++) {
        allRunning = true;
        showRegionResp = client.showRegion(new TShowRegionReq());
        for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
          if (!RegionStatus.Running.getStatus().equals(regionInfo.getStatus())) {
            allRunning = false;
            break;
          }
        }
        if (allRunning) {
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      Assert.assertTrue(allRunning);
    }
  }
}
