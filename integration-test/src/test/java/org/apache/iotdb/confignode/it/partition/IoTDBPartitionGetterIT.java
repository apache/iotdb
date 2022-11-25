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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseConfig;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionGetterIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPartitionGetterIT.class);

  private static final BaseConfig CONF = ConfigFactory.getConfig();

  private static String originalConfigNodeConsensusProtocolClass;
  private static String originalSchemaRegionConsensusProtocolClass;
  private static String originalDataRegionConsensusProtocolClass;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  private static int originalSchemaReplicationFactor;
  private static int originalDataReplicationFactor;
  private static final int testReplicationFactor = 3;

  private static int originalSeriesPartitionSlotNum;

  private static long originalTimePartitionInterval;
  private static final long testTimePartitionInterval = 604800000;

  protected static int originalLeastDataRegionGroupNum;
  private static final int testLeastDataRegionGroupNum = 10;

  private static final String sg = "root.sg";
  private static final int storageGroupNum = 5;
  private static final int testSeriesPartitionSlotNum = 100;
  private static final int seriesPartitionBatchSize = 10;
  private static final int testTimePartitionSlotsNum = 10;
  private static final int timePartitionBatchSize = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    CONF.setConfigNodeConsesusProtocolClass(testConsensusProtocolClass);
    CONF.setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass);
    CONF.setDataRegionConsensusProtocolClass(testConsensusProtocolClass);

    originalSchemaReplicationFactor = CONF.getSchemaReplicationFactor();
    originalDataReplicationFactor = CONF.getDataReplicationFactor();
    CONF.setSchemaReplicationFactor(testReplicationFactor);
    CONF.setDataReplicationFactor(testReplicationFactor);

    originalSeriesPartitionSlotNum = CONF.getSeriesPartitionSlotNum();
    CONF.setSeriesPartitionSlotNum(testSeriesPartitionSlotNum);

    originalTimePartitionInterval = CONF.getTimePartitionInterval();
    CONF.setTimePartitionInterval(testTimePartitionInterval);

    originalLeastDataRegionGroupNum = CONF.getLeastDataRegionGroupNum();
    CONF.setLeastDataRegionGroupNum(testLeastDataRegionGroupNum);

    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
    prepareData();
  }

  private static void prepareData()
      throws IOException, InterruptedException, TException, IllegalPathException {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      /* Set StorageGroups */
      for (int i = 0; i < storageGroupNum; i++) {
        TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
        TSStatus status = client.setStorageGroup(setReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      /* Create SchemaPartitions */
      final String sg = "root.sg";
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
            Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
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

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();

    CONF.setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
    CONF.setSchemaRegionConsensusProtocolClass(originalSchemaRegionConsensusProtocolClass);
    CONF.setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);

    CONF.setSchemaReplicationFactor(originalSchemaReplicationFactor);
    CONF.setDataReplicationFactor(originalDataReplicationFactor);
    CONF.setSeriesPartitionSlotNum(originalSeriesPartitionSlotNum);
    CONF.setTimePartitionInterval(originalTimePartitionInterval);
  }

  @Test
  public void testGetSchemaPartition()
      throws TException, IOException, IllegalPathException, InterruptedException {
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
  public void testGetDataPartition() throws TException, IOException, InterruptedException {
    final int seriesPartitionBatchSize = 100;
    final int timePartitionBatchSize = 10;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TDataPartitionReq dataPartitionReq;
      TDataPartitionTableResp dataPartitionTableResp;

      // Prepare partitionSlotsMap
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
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
        // And this number should be greater than or equal to testLeastDataRegionGroupNum
        TShowStorageGroupResp showStorageGroupResp =
            client.showStorageGroup(Arrays.asList(storageGroup.split("\\.")));
        Assert.assertTrue(
            showStorageGroupResp.getStorageGroupInfoMap().get(storageGroup).getDataRegionNum()
                >= testLeastDataRegionGroupNum);
      }
    }
  }

  @Test
  public void testGetSlots()
      throws TException, IOException, IllegalPathException, InterruptedException {
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Test getSlots api
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
      Assert.assertEquals(10, getRegionIdResp.getDataRegionIdListSize());

      final String d00 = sg0 + ".d0.s";
      final String d01 = sg0 + ".d1.s";
      final String d10 = sg1 + ".d0.s";
      final String d11 = sg1 + ".d1.s";
      ByteBuffer buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
      TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq(buffer);
      TSchemaPartitionTableResp schemaPartitionTableResp =
          client.getSchemaPartitionTable(schemaPartitionReq);
      getRegionIdReq.setSeriesSlotId(
          new ArrayList<>(schemaPartitionTableResp.getSchemaPartitionTable().get(sg0).keySet())
              .get(0));
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
      Assert.assertEquals(
          testSeriesPartitionSlotNum, getSeriesSlotListResp.getSeriesSlotListSize());

      getSeriesSlotListReq.setType(TConsensusGroupType.ConfigNodeRegion);

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

      getSeriesSlotListReq.setType(TConsensusGroupType.DataRegion);

      getSeriesSlotListResp = client.getSeriesSlotList(getSeriesSlotListReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getSeriesSlotListResp.status.getCode());
      Assert.assertEquals(
          testSeriesPartitionSlotNum, getSeriesSlotListResp.getSeriesSlotListSize());
    }
  }

  @Test
  public void testGetSchemaNodeManagementPartition()
      throws IOException, TException, IllegalPathException, InterruptedException {

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
      Assert.assertEquals(5, nodeManagementResp.getMatchedNodeSize());
      Assert.assertNotNull(nodeManagementResp.getSchemaRegionMap());
      Assert.assertEquals(2, nodeManagementResp.getSchemaRegionMapSize());
    }
  }
}
