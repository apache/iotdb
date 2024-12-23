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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionGetterIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPartitionGetterIT.class);
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;
  private static final int testReplicationFactor = 3;
  private static final long testTimePartitionInterval = 604800000;
  private static final int testDataRegionGroupPerDatabase = 5;

  private static final String sg = "root.sg";
  private static final int storageGroupNum = 2;
  private static final int testSeriesPartitionSlotNum = 1000;
  private static final int seriesPartitionBatchSize = 10;
  private static final int testTimePartitionSlotsNum = 10;
  private static final int timePartitionBatchSize = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval)
        .setDefaultDataRegionGroupNumPerDatabase(testDataRegionGroupPerDatabase);
    // .setSeriesSlotNum(testSeriesPartitionSlotNum);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
    prepareData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      /* Set StorageGroups */
      for (int i = 0; i < storageGroupNum; i++) {
        TSStatus status = client.setDatabase(new TDatabaseSchema(sg + i));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      /* Create SchemaPartitions */
      final String sg0 = "root.sg0";
      final String sg1 = "root.sg1";

      final String d00 = sg0 + ".d0.s";
      final String d01 = sg0 + ".d1.s";
      final String d10 = sg1 + ".d0.s";
      final String d11 = sg1 + ".d1.s";

      TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq();
      TSchemaPartitionTableResp schemaPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;

      ByteBuffer buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
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

      /* Create DataPartitions */
      for (int i = 0; i < storageGroupNum; i++) {
        String storageGroup = sg + i;
        for (int j = 0; j < testSeriesPartitionSlotNum; j += seriesPartitionBatchSize) {
          for (long k = 0; k < testTimePartitionSlotsNum; k += timePartitionBatchSize) {
            Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
                ConfigNodeTestUtils.constructPartitionSlotsMap(
                    storageGroup,
                    j,
                    j + seriesPartitionBatchSize,
                    k,
                    k + timePartitionBatchSize,
                    testTimePartitionInterval);

            // Test getOrCreateDataPartition, ConfigNode should create DataPartition and return
            TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
            TDataPartitionTableResp dataPartitionTableResp = null;
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
            ConfigNodeTestUtils.checkDataPartitionTable(
                storageGroup,
                j,
                j + seriesPartitionBatchSize,
                k,
                k + timePartitionBatchSize,
                testTimePartitionInterval,
                dataPartitionTableResp.getDataPartitionTable());
          }
        }
      }
    }
  }

  @Test
  public void testGetSchemaPartition() throws Exception {
    final String sg = "root.sg";
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    final String d11 = sg1 + ".d1.s";

    final String allPaths = "root.**";
    final String allSg0 = "root.sg0.**";

    final String notExistsSg = "root.sg10.**";

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      ByteBuffer buffer;
      TSchemaPartitionReq schemaPartitionReq;
      TSchemaPartitionTableResp schemaPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;

      // Test getSchemaPartition, the result should be empty
      buffer = generatePatternTreeBuffer(new String[] {notExistsSg});
      schemaPartitionReq = new TSchemaPartitionReq(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(0, schemaPartitionTableResp.getSchemaPartitionTableSize());

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

  @Test
  public void testGetDataPartition() throws Exception {
    final int seriesPartitionBatchSize = 100;
    final int timePartitionBatchSize = 10;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TDataPartitionReq dataPartitionReq;
      TDataPartitionTableResp dataPartitionTableResp;

      // Prepare partitionSlotsMap
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              sg + 10, 0, 10, 0, 10, testTimePartitionInterval);

      // Test getDataPartitionTable, the result should be empty
      dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      dataPartitionTableResp = client.getDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      Assert.assertEquals(0, dataPartitionTableResp.getDataPartitionTableSize());

      // Re-calculate the least DataRegionGroup num based on the test resource
      int totalCpuCoreNum = 0;
      TShowDataNodesResp showDataNodesResp = client.showDataNodes();
      for (TDataNodeInfo dataNodeInfo : showDataNodesResp.getDataNodesInfoList()) {
        totalCpuCoreNum += dataNodeInfo.getCpuCoreNum();
      }
      int leastDataRegionGroupNum =
          (int)
              Math.ceil(
                  (double) totalCpuCoreNum / (double) (storageGroupNum * testReplicationFactor));
      leastDataRegionGroupNum = Math.min(leastDataRegionGroupNum, testDataRegionGroupPerDatabase);

      for (int i = 0; i < storageGroupNum; i++) {
        String storageGroup = sg + i;
        for (int j = 0; j < testSeriesPartitionSlotNum; j += seriesPartitionBatchSize) {
          for (long k = 0; k < testTimePartitionSlotsNum; k += timePartitionBatchSize) {
            partitionSlotsMap =
                ConfigNodeTestUtils.constructPartitionSlotsMap(
                    storageGroup,
                    j,
                    j + seriesPartitionBatchSize,
                    k,
                    k + timePartitionBatchSize,
                    testTimePartitionInterval);

            // Test getDataPartition, the result should only contain DataPartition created before
            dataPartitionReq.setPartitionSlotsMap(partitionSlotsMap);
            dataPartitionTableResp = client.getDataPartitionTable(dataPartitionReq);
            Assert.assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                dataPartitionTableResp.getStatus().getCode());
            Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
            ConfigNodeTestUtils.checkDataPartitionTable(
                storageGroup,
                j,
                j + seriesPartitionBatchSize,
                k,
                k + timePartitionBatchSize,
                testTimePartitionInterval,
                dataPartitionTableResp.getDataPartitionTable());
          }
        }

        // Check the number of DataRegionGroup.
        // And this number should be greater than or equal to leastDataRegionGroupNum
        TGetDatabaseReq req =
            new TGetDatabaseReq(
                Arrays.asList(storageGroup.split("\\.")), SchemaConstant.ALL_MATCH_SCOPE_BINARY);
        TShowDatabaseResp showStorageGroupResp = client.showDatabase(req);
        Assert.assertTrue(
            showStorageGroupResp.getDatabaseInfoMap().get(storageGroup).getDataRegionNum()
                >= leastDataRegionGroupNum);
      }
    }
  }

  @Test
  public void testGetSlots() throws Exception {
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    final IDeviceID d00 = Factory.DEFAULT_FACTORY.create(sg0 + ".d0.s");
    final IDeviceID d01 = Factory.DEFAULT_FACTORY.create(sg0 + ".d1.s");
    final IDeviceID d10 = Factory.DEFAULT_FACTORY.create(sg1 + ".d0.s");
    final IDeviceID d11 = Factory.DEFAULT_FACTORY.create(sg1 + ".d1.s");

    IDeviceID[] devices = new IDeviceID[] {d00, d01, d10, d11};

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      /* Test getRegionId */
      TGetRegionIdReq getRegionIdReq;
      TGetRegionIdResp getRegionIdResp;

      // Get RegionIds of specified database and timestamp
      getRegionIdReq = new TGetRegionIdReq(TConsensusGroupType.DataRegion);
      getRegionIdReq.setDatabase(sg0);
      getRegionIdReq.setStartTimeSlot(new TTimePartitionSlot(0));
      getRegionIdReq.setEndTimeSlot(new TTimePartitionSlot(0));
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());

      // Get RegionId with negative timestamp
      getRegionIdReq.setStartTimeSlot(new TTimePartitionSlot(-100));
      getRegionIdReq.setEndTimeSlot(new TTimePartitionSlot(-1));
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());
      Assert.assertTrue(getRegionIdResp.getDataRegionIdList().isEmpty());

      // Get RegionId with Disjoint timestampRange
      getRegionIdReq.setStartTimeSlot(new TTimePartitionSlot(Long.MAX_VALUE));
      getRegionIdReq.setEndTimeSlot(new TTimePartitionSlot(0));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());
      Assert.assertTrue(getRegionIdResp.getDataRegionIdList().isEmpty());

      //   Get schema RegionIds with specified database and timestamp will ignore timestamp and
      // return success.
      getRegionIdReq.setType(TConsensusGroupType.SchemaRegion);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());

      // Get all RegionIds within database
      for (int i = 0; i < storageGroupNum; i++) {
        String curSg = sg + i;
        getRegionIdReq = new TGetRegionIdReq(TConsensusGroupType.DataRegion);
        getRegionIdReq.setDatabase(curSg);
        getRegionIdResp = client.getRegionId(getRegionIdReq);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());
        Set<TConsensusGroupId> idSet = new HashSet<>(getRegionIdResp.getDataRegionIdList());
        Set<TConsensusGroupId> subSets = new HashSet<>();

        // Get RegionId with an equivalent query。
        for (long j = 0; j < testTimePartitionSlotsNum; j++) {
          TGetRegionIdReq subReq = new TGetRegionIdReq(TConsensusGroupType.DataRegion);
          subReq.setDatabase(curSg);
          subReq.setStartTimeSlot(new TTimePartitionSlot(j * testTimePartitionInterval));
          subReq.setEndTimeSlot(new TTimePartitionSlot(j * testTimePartitionInterval));
          TGetRegionIdResp subResp = client.getRegionId(subReq);
          Assert.assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(), subResp.getStatus().getCode());
          subSets.addAll(subResp.getDataRegionIdList());
        }
        Assert.assertEquals(idSet, subSets);

        // Get RegionId with time range query
        TGetRegionIdReq rangeReq = new TGetRegionIdReq(TConsensusGroupType.DataRegion);
        rangeReq.setDatabase(curSg);
        rangeReq.setStartTimeSlot(new TTimePartitionSlot(0L));
        rangeReq.setEndTimeSlot(
            new TTimePartitionSlot(testTimePartitionSlotsNum * testTimePartitionInterval));
        TGetRegionIdResp rangeResp = client.getRegionId(rangeReq);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), rangeResp.getStatus().getCode());
        Set<TConsensusGroupId> rangeSets = new HashSet<>(getRegionIdResp.getDataRegionIdList());
        Assert.assertEquals(idSet, rangeSets);
      }

      // Get RegionId with device

      TGetRegionIdReq deviceReq = new TGetRegionIdReq(TConsensusGroupType.DataRegion);
      for (IDeviceID device : devices) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        device.serialize(baos);
        deviceReq.setDevice(baos.toByteArray());
        TGetRegionIdResp resp = client.getRegionId(deviceReq);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
        Assert.assertFalse(resp.getDataRegionIdList().isEmpty());
      }

      // Get RegionId of SchemaPartition
      getRegionIdReq.setDatabase(sg0);
      getRegionIdReq.setType(TConsensusGroupType.SchemaRegion);
      getRegionIdResp = client.getRegionId(getRegionIdReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getRegionIdResp.status.getCode());

      // Test GetTimeSlotList api
      TGetTimeSlotListReq getTimeSlotListReq;
      TGetTimeSlotListResp getTimeSlotListResp;
      getTimeSlotListReq = new TGetTimeSlotListReq();
      getTimeSlotListReq.setDatabase(sg0);
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

      // Test CountTimeSlotList api
      TCountTimeSlotListReq countTimeSlotListReq;
      TCountTimeSlotListResp countTimeSlotListResp;
      countTimeSlotListReq = new TCountTimeSlotListReq();
      countTimeSlotListReq.setDatabase(sg0);
      countTimeSlotListResp = client.countTimeSlotList(countTimeSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), countTimeSlotListResp.status.getCode());
      Assert.assertEquals(timePartitionBatchSize, countTimeSlotListResp.getCount());

      countTimeSlotListReq.setStartTime(startTime * testTimePartitionInterval);

      countTimeSlotListResp = client.countTimeSlotList(countTimeSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), countTimeSlotListResp.status.getCode());
      Assert.assertEquals(timePartitionBatchSize - startTime, countTimeSlotListResp.getCount());

      countTimeSlotListReq.setEndTime(endTime * testTimePartitionInterval);

      countTimeSlotListResp = client.countTimeSlotList(countTimeSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), countTimeSlotListResp.status.getCode());
      Assert.assertEquals(endTime - startTime, countTimeSlotListResp.getCount());

      // Test GetSeriesSlotList api
      TGetSeriesSlotListReq getSeriesSlotListReq;
      TGetSeriesSlotListResp getSeriesSlotListResp;

      getSeriesSlotListReq = new TGetSeriesSlotListReq(sg0, TConsensusGroupType.DataRegion);
      getSeriesSlotListResp = client.getSeriesSlotList(getSeriesSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getSeriesSlotListResp.status.getCode());
      Assert.assertEquals(
          testSeriesPartitionSlotNum, getSeriesSlotListResp.getSeriesSlotListSize());

      getSeriesSlotListReq.setType(TConsensusGroupType.SchemaRegion);
      getSeriesSlotListResp = client.getSeriesSlotList(getSeriesSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getSeriesSlotListResp.status.getCode());
      Assert.assertEquals(2, getSeriesSlotListResp.getSeriesSlotListSize());
    }
  }

  @Test
  public void testGetSchemaNodeManagementPartition() throws Exception {

    TSchemaNodeManagementReq nodeManagementReq;
    TSchemaNodeManagementResp nodeManagementResp;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      ByteBuffer byteBuffer = generatePatternTreeBuffer(new String[] {"root"});
      nodeManagementReq = new TSchemaNodeManagementReq(byteBuffer);
      nodeManagementReq.setLevel(-1);
      nodeManagementResp = client.getSchemaNodeManagementPartition(nodeManagementReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), nodeManagementResp.getStatus().getCode());
      Assert.assertEquals(storageGroupNum, nodeManagementResp.getMatchedNodeSize());
      Assert.assertNotNull(nodeManagementResp.getSchemaRegionMap());
      Assert.assertEquals(0, nodeManagementResp.getSchemaRegionMapSize());
    }
  }
}
