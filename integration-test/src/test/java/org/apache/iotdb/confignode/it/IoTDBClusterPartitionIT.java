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
package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterPartitionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClusterPartitionIT.class);

  protected static String originalConfigNodeConsensusProtocolClass;
  protected static String originalSchemaRegionConsensusProtocolClass;
  protected static String originalDataRegionConsensusProtocolClass;

  protected static int originalSchemaReplicationFactor;
  protected static int originalDataReplicationFactor;
  private static final int testReplicationFactor = 3;

  protected static long originalTimePartitionInterval;
  private static final long testTimePartitionInterval = 604800000;

  private static final String sg = "root.sg";
  private static final int storageGroupNum = 5;
  private static final int seriesPartitionSlotsNum = 10000;
  private static final int timePartitionSlotsNum = 10;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    ConfigFactory.getConfig()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    ConfigFactory.getConfig().setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);

    originalSchemaReplicationFactor = ConfigFactory.getConfig().getSchemaReplicationFactor();
    originalDataReplicationFactor = ConfigFactory.getConfig().getDataReplicationFactor();
    ConfigFactory.getConfig().setSchemaReplicationFactor(testReplicationFactor);
    ConfigFactory.getConfig().setDataReplicationFactor(testReplicationFactor);

    originalTimePartitionInterval = ConfigFactory.getConfig().getTimePartitionInterval();
    ConfigFactory.getConfig().setTimePartitionInterval(testTimePartitionInterval);

    EnvFactory.getEnv().initBeforeClass();
  }

  @After
  public void tearDown() {
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

  @Test
  public void testGetAndCreateSchemaPartition()
      throws TException, IOException, IllegalPathException, InterruptedException {
    final String sg = "root.sg";
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    final String d00 = sg0 + ".d0.s";
    final String d01 = sg0 + ".d1.s";
    final String d10 = sg1 + ".d0.s";
    final String d11 = sg1 + ".d1.s";

    final String allPaths = "root.**";
    final String allSg0 = "root.sg0.**";
    final String allSg1 = "root.sg1.**";

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status;
      ByteBuffer buffer;
      TSchemaPartitionReq schemaPartitionReq;
      TSchemaPartitionTableResp schemaPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;

      // Set StorageGroups
      status = client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg0)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      status = client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg1)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Test getSchemaPartition, the result should be empty
      buffer = generatePatternTreeBuffer(new String[] {d00, d01, allSg1});
      schemaPartitionReq = new TSchemaPartitionReq(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(0, schemaPartitionTableResp.getSchemaPartitionTableSize());

      // Test getOrCreateSchemaPartition, ConfigNode should create SchemaPartitions and return
      buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(2, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      for (int i = 0; i < 2; i++) {
        Assert.assertTrue(schemaPartitionTable.containsKey(sg + i));
        Assert.assertEquals(2, schemaPartitionTable.get(sg + i).size());
      }

      // Test getSchemaPartition, when a device path doesn't match any StorageGroup and including
      // "**", ConfigNode will return all the SchemaPartitions
      buffer = generatePatternTreeBuffer(new String[] {allPaths});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(2, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      for (int i = 0; i < 2; i++) {
        Assert.assertTrue(schemaPartitionTable.containsKey(sg + i));
        Assert.assertEquals(2, schemaPartitionTable.get(sg + i).size());
      }

      // Test getSchemaPartition, when a device path matches with a StorageGroup and end with "*",
      // ConfigNode will return all the SchemaPartitions in this StorageGroup
      buffer = generatePatternTreeBuffer(new String[] {allSg0, d11});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(2, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      // Check "root.sg0"
      Assert.assertTrue(schemaPartitionTable.containsKey(sg0));
      Assert.assertEquals(2, schemaPartitionTable.get(sg0).size());
      // Check "root.sg1"
      Assert.assertTrue(schemaPartitionTable.containsKey(sg1));
      Assert.assertEquals(1, schemaPartitionTable.get(sg1).size());
    }
  }

  private Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
      constructPartitionSlotsMap(
          String storageGroup,
          int seriesSlotStart,
          int seriesSlotEnd,
          long timeSlotStart,
          long timeSlotEnd) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> result = new HashMap<>();
    result.put(storageGroup, new HashMap<>());

    for (int i = seriesSlotStart; i < seriesSlotEnd; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      result.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
      for (long j = timeSlotStart; j < timeSlotEnd; j++) {
        TTimePartitionSlot timePartitionSlot =
            new TTimePartitionSlot(j * testTimePartitionInterval);
        result.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);
      }
    }

    return result;
  }

  private void checkDataPartitionMap(
      String storageGroup,
      int seriesSlotStart,
      int seriesSlotEnd,
      long timeSlotStart,
      long timeSlotEnd,
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          dataPartitionTable) {

    Assert.assertTrue(dataPartitionTable.containsKey(storageGroup));
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
        seriesPartitionTable = dataPartitionTable.get(storageGroup);
    Assert.assertEquals(seriesSlotEnd - seriesSlotStart, seriesPartitionTable.size());

    for (int i = seriesSlotStart; i < seriesSlotEnd; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      Assert.assertTrue(seriesPartitionTable.containsKey(seriesPartitionSlot));
      Map<TTimePartitionSlot, List<TConsensusGroupId>> timePartitionTable =
          seriesPartitionTable.get(seriesPartitionSlot);
      Assert.assertEquals(timeSlotEnd - timeSlotStart, timePartitionTable.size());

      for (long j = timeSlotStart; j < timeSlotEnd; j++) {
        TTimePartitionSlot timePartitionSlot =
            new TTimePartitionSlot(j * testTimePartitionInterval);
        Assert.assertTrue(timePartitionTable.containsKey(timePartitionSlot));
        if (j > timeSlotStart) {
          // Check consistency
          Assert.assertEquals(
              timePartitionTable.get(
                  new TTimePartitionSlot(timeSlotStart * testTimePartitionInterval)),
              timePartitionTable.get(timePartitionSlot));
        }
      }
    }
  }

  @Test
  public void testGetAndCreateDataPartition() throws TException, IOException, InterruptedException {
    final int seriesPartitionBatchSize = 100;
    final int timePartitionBatchSize = 10;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status;
      TDataPartitionReq dataPartitionReq;
      TDataPartitionTableResp dataPartitionTableResp;

      // Prepare partitionSlotsMap
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
          constructPartitionSlotsMap(sg + 0, 0, 10, 0, 10);

      // Set StorageGroups
      for (int i = 0; i < storageGroupNum; i++) {
        TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
        status = client.setStorageGroup(setReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      // Test getDataPartitionTable, the result should be empty
      dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      dataPartitionTableResp = client.getDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      Assert.assertEquals(0, dataPartitionTableResp.getDataPartitionTableSize());

      for (int i = 0; i < storageGroupNum; i++) {
        String storageGroup = sg + i;
        for (int j = 0; j < seriesPartitionSlotsNum; j += seriesPartitionBatchSize) {
          for (long k = 0; k < timePartitionSlotsNum; k += timePartitionBatchSize) {
            partitionSlotsMap =
                constructPartitionSlotsMap(
                    storageGroup, j, j + seriesPartitionBatchSize, k, k + timePartitionBatchSize);

            // Test getOrCreateDataPartition, ConfigNode should create DataPartition and return
            dataPartitionReq.setPartitionSlotsMap(partitionSlotsMap);
            for (int retry = 0; retry < 5; retry++) {
              // Build new Client since it's unstable
              try (SyncConfigNodeIServiceClient configNodeClient =
                  (SyncConfigNodeIServiceClient)
                      EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
                dataPartitionTableResp =
                    configNodeClient.getOrCreateDataPartitionTable(dataPartitionReq);
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
            checkDataPartitionMap(
                storageGroup,
                j,
                j + seriesPartitionBatchSize,
                k,
                k + timePartitionBatchSize,
                dataPartitionTableResp.getDataPartitionTable());

            // Test getDataPartition, the result should only contain DataPartition created before
            dataPartitionReq.setPartitionSlotsMap(partitionSlotsMap);
            dataPartitionTableResp = client.getDataPartitionTable(dataPartitionReq);
            Assert.assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                dataPartitionTableResp.getStatus().getCode());
            Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
            checkDataPartitionMap(
                storageGroup,
                j,
                j + seriesPartitionBatchSize,
                k,
                k + timePartitionBatchSize,
                dataPartitionTableResp.getDataPartitionTable());
          }
        }
      }

      // Test DataPartition inherit policy
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                // Normally, all Timeslots belonging to the same SeriesSlot are allocated to the
                // same DataRegionGroup
                Assert.assertEquals(
                    regionInfo.getSeriesSlots() * timePartitionSlotsNum, regionInfo.getTimeSlots());
              });
    }
  }

  // TODO: Optimize in IOTDB-4334
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
          constructPartitionSlotsMap(sg0, 0, seriesPartitionBatchSize, 0, timePartitionBatchSize);
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
      checkDataPartitionMap(
          sg0,
          0,
          seriesPartitionBatchSize,
          0,
          timePartitionBatchSize,
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
          constructPartitionSlotsMap(sg1, 0, seriesPartitionBatchSize, 0, timePartitionBatchSize);
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
      checkDataPartitionMap(
          sg1,
          0,
          seriesPartitionBatchSize,
          0,
          timePartitionBatchSize,
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

  @Test
  public void testGetSlots()
      throws TException, IOException, IllegalPathException, InterruptedException {
    final String sg = "root.sg";
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    final String d00 = sg0 + ".d0.s";
    final String d01 = sg0 + ".d1.s";
    final String d10 = sg1 + ".d0.s";
    final String d11 = sg1 + ".d1.s";

    final int seriesPartitionBatchSize = 100;
    final int timePartitionBatchSize = 10;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      ByteBuffer buffer;
      TSchemaPartitionReq schemaPartitionReq;

      // We assert the correctness of setting storageGroups, dataPartitions, schemaPartitions

      // Set StorageGroups
      client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg0)));
      client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg1)));

      // Create SchemaPartitions
      buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
      schemaPartitionReq = new TSchemaPartitionReq(buffer);
      TSchemaPartitionTableResp schemaPartitionTableResp =
          client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
      TSeriesPartitionSlot schemaSlot =
          new ArrayList<>(schemaPartitionTableResp.getSchemaPartitionTable().get(sg0).keySet())
              .get(0);

      TDataPartitionReq dataPartitionReq;
      TDataPartitionTableResp dataPartitionTableResp;

      // Prepare partitionSlotsMap
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap;

      // Create DataPartitions
      for (int i = 0; i < 2; i++) {
        String storageGroup = sg + i;
        partitionSlotsMap =
            constructPartitionSlotsMap(
                storageGroup, 0, seriesPartitionBatchSize, 0, timePartitionBatchSize);

        dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
        for (int retry = 0; retry < 5; retry++) {
          // Build new Client since it's unstable
          try (SyncConfigNodeIServiceClient configNodeClient =
              (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
            dataPartitionTableResp =
                configNodeClient.getOrCreateDataPartitionTable(dataPartitionReq);
            if (dataPartitionTableResp != null) {
              break;
            }
          } catch (Exception e) {
            // Retry sometimes in order to avoid request timeout
            LOGGER.error(e.getMessage());
            TimeUnit.SECONDS.sleep(1);
          }
        }
      }

      // Test getRouting api
      TGetRegionIdReq getRegionIdReq;
      TGetRegionIdResp getRegionIdResp;

      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(0);
      TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(0L);

      getRegionIdReq =
          new TGetRegionIdReq(sg0, TConsensusGroupType.DataRegion, seriesPartitionSlot);
      getRegionIdReq.setTimeSlotId(timePartitionSlot);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());
      Assert.assertEquals(1, getRegionIdResp.getDataRegionIdListSize());

      getRegionIdReq.setType(TConsensusGroupType.SchemaRegion);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.ILLEGAL_PARAMETER.getStatusCode(), getRegionIdResp.status.getCode());

      getRegionIdReq.setType(TConsensusGroupType.ConfigNodeRegion);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.ILLEGAL_PARAMETER.getStatusCode(), getRegionIdResp.status.getCode());

      getRegionIdReq.unsetTimeSlotId();
      getRegionIdReq.setType(TConsensusGroupType.DataRegion);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());
      Assert.assertEquals(1, getRegionIdResp.getDataRegionIdListSize());

      getRegionIdReq.setSeriesSlotId(schemaSlot);
      getRegionIdReq.setType(TConsensusGroupType.SchemaRegion);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());
      Assert.assertEquals(1, getRegionIdResp.getDataRegionIdListSize());

      getRegionIdReq.setType(TConsensusGroupType.ConfigNodeRegion);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());
      Assert.assertEquals(0, getRegionIdResp.getDataRegionIdListSize());

      // Test GetTimeSlotList api
      TGetTimeSlotListReq getTimeSlotListReq;
      TGetTimeSlotListResp getTimeSlotListResp;

      seriesPartitionSlot.setSlotId(0);

      getTimeSlotListReq = new TGetTimeSlotListReq(sg0, seriesPartitionSlot);
      getTimeSlotListResp = client.getTimeSlotList(getTimeSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getTimeSlotListResp.status.getCode());
      Assert.assertEquals(timePartitionBatchSize, getTimeSlotListResp.getTimeSlotListSize());

      long startTime = 5;
      getTimeSlotListReq.setStartTime(startTime * testTimePartitionInterval);

      getTimeSlotListResp = client.getTimeSlotList(getTimeSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getTimeSlotListResp.status.getCode());
      Assert.assertEquals(
          timePartitionBatchSize - startTime, getTimeSlotListResp.getTimeSlotListSize());

      long endTime = 6;
      getTimeSlotListReq.setEndTime(endTime * testTimePartitionInterval);

      getTimeSlotListResp = client.getTimeSlotList(getTimeSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getTimeSlotListResp.status.getCode());
      Assert.assertEquals(endTime - startTime, getTimeSlotListResp.getTimeSlotListSize());

      // Test GetSeriesSlotList api
      TGetSeriesSlotListReq getSeriesSlotListReq;
      TGetSeriesSlotListResp getSeriesSlotListResp;

      getSeriesSlotListReq = new TGetSeriesSlotListReq(sg0);
      getSeriesSlotListResp = client.getSeriesSlotList(getSeriesSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getSeriesSlotListResp.status.getCode());
      Assert.assertEquals(102, getSeriesSlotListResp.getSeriesSlotListSize());

      getSeriesSlotListReq.setType(TConsensusGroupType.ConfigNodeRegion);

      getSeriesSlotListResp = client.getSeriesSlotList(getSeriesSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getSeriesSlotListResp.status.getCode());
      Assert.assertEquals(
          seriesPartitionBatchSize + 2, getSeriesSlotListResp.getSeriesSlotListSize());

      getSeriesSlotListReq.setType(TConsensusGroupType.SchemaRegion);

      getSeriesSlotListResp = client.getSeriesSlotList(getSeriesSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getSeriesSlotListResp.status.getCode());
      Assert.assertEquals(2, getSeriesSlotListResp.getSeriesSlotListSize());

      getSeriesSlotListReq.setType(TConsensusGroupType.DataRegion);

      getSeriesSlotListResp = client.getSeriesSlotList(getSeriesSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getSeriesSlotListResp.status.getCode());
      Assert.assertEquals(seriesPartitionBatchSize, getSeriesSlotListResp.getSeriesSlotListSize());
    }
  }

  @Test
  public void testGetSchemaNodeManagementPartition()
      throws IOException, TException, IllegalPathException, InterruptedException {
    final String sg = "root.sg";
    final int storageGroupNum = 2;

    TSStatus status;
    TSchemaNodeManagementReq nodeManagementReq;
    TSchemaNodeManagementResp nodeManagementResp;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // set StorageGroups
      for (int i = 0; i < storageGroupNum; i++) {
        TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
        status = client.setStorageGroup(setReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      ByteBuffer byteBuffer = generatePatternTreeBuffer(new String[] {"root"});
      nodeManagementReq = new TSchemaNodeManagementReq(byteBuffer);
      nodeManagementReq.setLevel(-1);
      nodeManagementResp = client.getSchemaNodeManagementPartition(nodeManagementReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), nodeManagementResp.getStatus().getCode());
      Assert.assertEquals(2, nodeManagementResp.getMatchedNodeSize());
      Assert.assertNotNull(nodeManagementResp.getSchemaRegionMap());
      Assert.assertEquals(0, nodeManagementResp.getSchemaRegionMapSize());
    }
  }
}
