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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionDurableIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPartitionDurableIT.class);
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;
  private static final int testReplicationFactor = 3;
  private static final long testTimePartitionInterval = 604800000;
  private static final int testDataNodeId = 0;
  private static final String sg = "root.sg";
  final String d0 = sg + ".d0.s";
  final String d1 = sg + ".d1.s";
  private static final int testSeriesPartitionBatchSize = 1;
  private static final int testTimePartitionBatchSize = 1;

  private static final int testSchemaRegionPerDataNode = 3;
  private static final TEndPoint defaultEndPoint = new TEndPoint("-1", -1);
  private static final TDataNodeLocation defaultDataNode =
      new TDataNodeLocation(
          -1,
          new TEndPoint(defaultEndPoint),
          new TEndPoint(defaultEndPoint),
          new TEndPoint(defaultEndPoint),
          new TEndPoint(defaultEndPoint),
          new TEndPoint(defaultEndPoint));

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval)
        .setSchemaRegionPerDataNode(testSchemaRegionPerDataNode);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);

    setStorageGroup();
  }

  private void setStorageGroup() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status = client.setDatabase(new TDatabaseSchema(sg));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // TODO: Fix this when replica completion is supported
  @Test
  public void testReadOnlyDataNode() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      /* Test getOrCreateDataPartition, ConfigNode should create DataPartition and return */
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              0,
              testSeriesPartitionBatchSize,
              0,
              testTimePartitionBatchSize,
              testTimePartitionInterval);
      TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      ConfigNodeTestUtils.checkDataPartitionTable(
          sg,
          0,
          testSeriesPartitionBatchSize,
          0,
          testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      /* Check Region distribution */
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      // Create exactly one RegionGroup
      Assert.assertEquals(3, showRegionResp.getRegionInfoListSize());
      // Each DataNode has exactly one Region
      Set<Integer> dataNodeIdSet = new HashSet<>();
      showRegionResp
          .getRegionInfoList()
          .forEach(regionInfo -> dataNodeIdSet.add(regionInfo.getDataNodeId()));
      Assert.assertEquals(3, dataNodeIdSet.size());

      /* Change the NodeStatus of the test DataNode to ReadOnly */
      TSetDataNodeStatusReq setDataNodeStatusReq = new TSetDataNodeStatusReq();
      DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(testDataNodeId);
      setDataNodeStatusReq.setTargetDataNode(
          new TDataNodeLocation(defaultDataNode)
              .setInternalEndPoint(
                  new TEndPoint()
                      .setIp(dataNodeWrapper.getInternalAddress())
                      .setPort(dataNodeWrapper.getInternalPort())));
      setDataNodeStatusReq.setStatus(NodeStatus.ReadOnly.getStatus());
      client.setDataNodeStatus(setDataNodeStatusReq);
      // Waiting for heartbeat update
      while (true) {
        AtomicBoolean containReadOnly = new AtomicBoolean(false);
        TShowDataNodesResp showDataNodesResp = client.showDataNodes();
        showDataNodesResp
            .getDataNodesInfoList()
            .forEach(
                dataNodeInfo -> {
                  if (NodeStatus.ReadOnly.getStatus().equals(dataNodeInfo.getStatus())) {
                    containReadOnly.set(true);
                  }
                });

        if (containReadOnly.get()) {
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }

      /* Test getOrCreateDataPartition, the result should be NO_ENOUGH_DATANODE */
      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              1,
              1 + testSeriesPartitionBatchSize,
              1,
              1 + testTimePartitionBatchSize,
              testTimePartitionInterval);
      dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      dataPartitionTableResp = client.getOrCreateDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());

      /* Register a new DataNode */
      EnvFactory.getEnv().registerNewDataNode(true);

      /* Test getOrCreateDataPartition, ConfigNode should create DataPartition and return */
      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              1,
              1 + testSeriesPartitionBatchSize,
              1,
              1 + testTimePartitionBatchSize,
              testTimePartitionInterval);
      dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      dataPartitionTableResp = client.getOrCreateDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      ConfigNodeTestUtils.checkDataPartitionTable(
          sg,
          1,
          1 + testSeriesPartitionBatchSize,
          1,
          1 + testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      /* Check Region distribution */
      showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      // There should be 2 RegionGroups
      Assert.assertEquals(6, showRegionResp.getRegionInfoListSize());
      // The new RegionGroup should keep away from the ReadOnly DataNode
      Map<Integer, AtomicInteger> regionCounter = new ConcurrentHashMap<>();
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo ->
                  regionCounter
                      .computeIfAbsent(regionInfo.getDataNodeId(), empty -> new AtomicInteger(0))
                      .getAndIncrement());
      dataNodeIdSet.forEach(dataNodeId -> regionCounter.get(dataNodeId).getAndDecrement());
      TShowDataNodesResp showDataNodesResp = client.showDataNodes();
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showDataNodesResp.getStatus().getCode());
      regionCounter.forEach(
          (dataNodeId, regionCount) -> {
            String nodeStatus =
                showDataNodesResp.getDataNodesInfoList().stream()
                    .filter(dataNodeInfo -> dataNodeInfo.getDataNodeId() == dataNodeId)
                    .findFirst()
                    .orElse(new TDataNodeInfo().setStatus("ERROR"))
                    .getStatus();
            if (NodeStatus.ReadOnly.getStatus().equals(nodeStatus)) {
              Assert.assertEquals(0, regionCount.get());
            } else if (NodeStatus.Running.getStatus().equals(nodeStatus)) {
              Assert.assertEquals(1, regionCount.get());
            } else {
              Assert.fail();
            }
          });
    }
  }

  @Test
  public void testUnknownDataNode() throws Exception {
    // Shutdown a DataNode, the ConfigNode should still be able to create RegionGroup
    EnvFactory.getEnv().shutdownDataNode(testDataNodeId);
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(testDataNodeId)),
            Collections.singletonList(NodeStatus.Unknown));

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Wait for shutdown check
      TShowClusterResp showClusterResp;
      while (true) {
        AtomicBoolean containUnknown = new AtomicBoolean(false);
        TShowDataNodesResp showDataNodesResp = client.showDataNodes();
        showDataNodesResp
            .getDataNodesInfoList()
            .forEach(
                dataNodeInfo -> {
                  if (NodeStatus.Unknown.getStatus().equals(dataNodeInfo.getStatus())) {
                    containUnknown.set(true);
                  }
                });

        if (containUnknown.get()) {
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }
      int runningCnt = 0;
      int unknownCnt = 0;
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
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              0,
              testSeriesPartitionBatchSize,
              0,
              testTimePartitionBatchSize,
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
          sg,
          0,
          testSeriesPartitionBatchSize,
          0,
          testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // Check Region count
      runningCnt = 0;
      unknownCnt = 0;
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

      // Test getOrCreateDataPartition, ConfigNode should create DataPartition and return
      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              1,
              1 + testSeriesPartitionBatchSize,
              1,
              1 + testTimePartitionBatchSize,
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
          sg,
          1,
          1 + testSeriesPartitionBatchSize,
          1,
          1 + testTimePartitionBatchSize,
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
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(testDataNodeId)),
              Collections.singletonList(NodeStatus.Running));

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
