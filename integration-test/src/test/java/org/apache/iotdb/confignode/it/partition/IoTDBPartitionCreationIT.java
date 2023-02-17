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
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.DataNodeWrapper;
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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionCreationIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPartitionDurableIT.class);
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;
  private static final String IoTConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;
  private static final int testReplicationFactor = 3;
  private static final long testTimePartitionInterval = 604800000;
  private static final String sg = "root.sg";
  private static final int testSeriesPartitionBatchSize = 1;
  private static final int testTimePartitionBatchSize = 1;
  private static final int testDataRegionGroupPerDatabase = 4;
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
        .setDataRegionConsensusProtocolClass(IoTConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval)
        .setDefaultDataRegionGroupNumPerDatabase(testDataRegionGroupPerDatabase);

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

  @Test
  public void testPartitionAllocation() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Status: Running, Running, Running, Region: [0], [0], [0]
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

      // Status: Running, Running, Removing, Region: [0], [0], [0]
      TSetDataNodeStatusReq setDataNodeStatusReq = new TSetDataNodeStatusReq();
      DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(2);
      setDataNodeStatusReq.setTargetDataNode(
          new TDataNodeLocation(defaultDataNode)
              .setInternalEndPoint(
                  new TEndPoint()
                      .setIp(dataNodeWrapper.getInternalAddress())
                      .setPort(dataNodeWrapper.getInternalPort())));
      setDataNodeStatusReq.setStatus(NodeStatus.Removing.getStatus());
      client.setDataNodeStatus(setDataNodeStatusReq);
      // Waiting for heartbeat update
      while (true) {
        AtomicBoolean containRemoving = new AtomicBoolean(false);
        TShowDataNodesResp showDataNodesResp = client.showDataNodes();
        showDataNodesResp
            .getDataNodesInfoList()
            .forEach(
                dataNodeInfo -> {
                  if (NodeStatus.Removing.getStatus().equals(dataNodeInfo.getStatus())) {
                    containRemoving.set(true);
                  }
                });

        if (containRemoving.get()) {
          break;
        }
        TimeUnit.SECONDS.sleep(1);
      }

      // Status: Running, Running, Removing, Running, RegionGroup: [0, 1], [0, 1], [0], [1]
      EnvFactory.getEnv().registerNewDataNode(true);
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

      // Status: Running, Running, Removing, ReadOnly, RegionGroup: [0, 1], [0, 1],
      // [0], [1]
      setDataNodeStatusReq = new TSetDataNodeStatusReq();
      dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(3);
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

      // Status: Running, Running, Removing, ReadOnly, Running, RegionGroup: [0, 1, 2], [0, 1, 2],
      // [0], [1], [2]
      EnvFactory.getEnv().registerNewDataNode(true);
      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              2,
              2 + testSeriesPartitionBatchSize,
              2,
              2 + testTimePartitionBatchSize,
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
          2,
          2 + testSeriesPartitionBatchSize,
          2,
          2 + testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // Status: Running, Running, Removing, ReadOnly, Unknown, RegionGroup:[0, 1, 2], [0, 1, 2],
      // [0], [1], [2]
      EnvFactory.getEnv().shutdownDataNode(4);
      // Wait for shutdown check
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

      // Status: Running, Running, Removing, ReadOnly, Unknown, Running,
      // RegionGroup: [0, 1, 2, 3], [0, 1, 2, 3], [0], [1], [2], [3]
      EnvFactory.getEnv().registerNewDataNode(false);

      // Use thread sleep to replace verifying because the Unknown DataNode can not pass the
      // connection check
      TimeUnit.SECONDS.sleep(25);
      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              3,
              3 + testSeriesPartitionBatchSize,
              3,
              3 + testTimePartitionBatchSize,
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
          3,
          3 + testSeriesPartitionBatchSize,
          3,
          3 + testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // Check Region count and status
      int runningCnt = 0;
      int unknownCnt = 0;
      int readOnlyCnt = 0;
      int removingCnt = 0;
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        if (RegionStatus.Running.getStatus().equals(regionInfo.getStatus())) {
          runningCnt += 1;
        } else if (RegionStatus.Unknown.getStatus().equals(regionInfo.getStatus())) {
          unknownCnt += 1;
        } else if (RegionStatus.Removing.getStatus().equals(regionInfo.getStatus())) {
          removingCnt += 1;
        } else if (RegionStatus.ReadOnly.getStatus().equals(regionInfo.getStatus())) {
          readOnlyCnt += 1;
        }
      }
      Assert.assertEquals(9, runningCnt);
      Assert.assertEquals(1, removingCnt);
      Assert.assertEquals(1, readOnlyCnt);
      Assert.assertEquals(1, unknownCnt);

      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              4,
              4 + testSeriesPartitionBatchSize,
              4,
              4 + testTimePartitionBatchSize,
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
          4,
          4 + testSeriesPartitionBatchSize,
          4,
          4 + testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // RegionGroup statistics:
      // 0: 1 Removing, 1 partition
      // 1: 1 ReadOnly, 1 partition
      // 2: 1 Unknown, 1 partition
      // 3: All Running, 1 partition
      // Least Region Group number per storageGroup = 4, match the current Region Group number
      // Will allocate the new partition to Running RegionGroup 3, DataNodes: [1, 2, 6]
      showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        if (regionInfo.getDataNodeId() == 6) {
          Assert.assertEquals(regionInfo.getTimeSlots(), 2);
        }
      }

      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              5,
              5 + testSeriesPartitionBatchSize,
              5,
              5 + testTimePartitionBatchSize,
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
          5,
          5 + testSeriesPartitionBatchSize,
          5,
          5 + testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // RegionGroup statistics:
      // 0: 1 Removing, 1 partition
      // 1: 1 ReadOnly, 1 partition
      // 2: 1 Unknown, 1 partition
      // 3: All Running, 2 partition
      // Least Region Group number per storageGroup = 4, match the current Region Group number
      // Will allocate the new partition to available RegionGroup 2, DataNodes: [1, 2, 5]
      showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        if (regionInfo.getDataNodeId() == 5) {
          Assert.assertEquals(regionInfo.getTimeSlots(), 2);
        }
      }

      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              6,
              6 + testSeriesPartitionBatchSize,
              6,
              6 + testTimePartitionBatchSize,
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
          6,
          6 + testSeriesPartitionBatchSize,
          6,
          6 + testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // RegionGroup statistics:
      // 0: 1 Removing, 1 partition
      // 1: 1 ReadOnly, 1 partition
      // 2: 1 Unknown, 2 partition
      // 3: All Running, 2 partition
      // Least Region Group number per storageGroup = 4, match the current Region Group number
      // Will allocate the new partition to Discouraged RegionGroup 2, DataNodes: [1, 2, 4]
      showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        if (regionInfo.getDataNodeId() == 4) {
          Assert.assertEquals(regionInfo.getTimeSlots(), 2);
        }
      }

      partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg,
              7,
              7 + testSeriesPartitionBatchSize,
              7,
              7 + testTimePartitionBatchSize,
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
          7,
          7 + testSeriesPartitionBatchSize,
          7,
          7 + testTimePartitionBatchSize,
          testTimePartitionInterval,
          dataPartitionTableResp.getDataPartitionTable());

      // RegionGroup statistics:
      // 0: 1 Removing, 1 partition
      // 1: 1 ReadOnly, 2 partition
      // 2: 1 Unknown, 2 partition
      // 3: All Running, 2 partition
      // Least Region Group number per storageGroup = 4, match the current Region Group number
      // Will allocate the new partition to Running RegionGroup 3, DataNodes: [0, 1, 5]
      // Because RegionGroup 1 is Disabled
      showRegionResp = client.showRegion(new TShowRegionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        if (regionInfo.getDataNodeId() == 6) {
          Assert.assertEquals(regionInfo.getTimeSlots(), 3);
        }
      }
    }
  }
}
