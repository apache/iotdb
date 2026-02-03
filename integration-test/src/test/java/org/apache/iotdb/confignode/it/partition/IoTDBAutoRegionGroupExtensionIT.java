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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBAutoRegionGroupExtensionIT {

  private static final int TEST_DATA_NODE_NUM = 3;
  private static final String TEST_DATA_REGION_GROUP_EXTENSION_POLICY = "AUTO";
  private static final String TEST_CONSENSUS_PROTOCOL_CLASS = ConsensusFactory.RATIS_CONSENSUS;
  private static final int TEST_REPLICATION_FACTOR = 2;

  private static final String DATABASE = "root.db";
  private static final int TEST_DATABASE_NUM = 2;
  private static final long TEST_TIME_PARTITION_INTERVAL = 604800000;
  private static final int TEST_MIN_SCHEMA_REGION_GROUP_NUM = 2;
  private static final int TEST_MIN_DATA_REGION_GROUP_NUM = 2;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(TEST_CONSENSUS_PROTOCOL_CLASS)
        .setDataRegionConsensusProtocolClass(TEST_CONSENSUS_PROTOCOL_CLASS)
        .setSchemaReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataRegionGroupExtensionPolicy(TEST_DATA_REGION_GROUP_EXTENSION_POLICY)
        .setTimePartitionInterval(TEST_TIME_PARTITION_INTERVAL);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, TEST_DATA_NODE_NUM);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAutoRegionGroupExtensionPolicy() throws Exception {

    final int retryNum = 100;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      setDatabaseAndCheckRegionGroupDistribution(client);

      // Delete all Databases
      for (int i = 0; i < TEST_DATABASE_NUM; i++) {
        String curSg = DATABASE + i;
        client.deleteDatabase(new TDeleteDatabaseReq(curSg));
      }
      boolean isAllRegionGroupDeleted = false;
      for (int retry = 0; retry < retryNum; retry++) {
        TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.equals(SystemConstant.SYSTEM_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.equals(SystemConstant.AUDIT_DATABASE));
        if (showRegionResp.getRegionInfoListSize() == 0) {
          isAllRegionGroupDeleted = true;
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      Assert.assertTrue(isAllRegionGroupDeleted);

      // Re-test for safety
      setDatabaseAndCheckRegionGroupDistribution(client);
    }
  }

  private void setDatabaseAndCheckRegionGroupDistribution(SyncConfigNodeIServiceClient client)
      throws TException, IllegalPathException, IOException {

    for (int i = 0; i < TEST_DATABASE_NUM; i++) {
      String curSg = DATABASE + i;
      TSStatus status =
          client.setDatabase(
              new TDatabaseSchema(curSg)
                  .setMinSchemaRegionGroupNum(TEST_MIN_SCHEMA_REGION_GROUP_NUM)
                  .setMinDataRegionGroupNum(TEST_MIN_DATA_REGION_GROUP_NUM));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Insert SchemaPartitions to create SchemaRegionGroups
      String d0 = curSg + ".d0.s";
      String d1 = curSg + ".d1.s";
      String d2 = curSg + ".d2.s";
      String d3 = curSg + ".d3.s";
      TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq();
      TSchemaPartitionTableResp schemaPartitionTableResp;
      ByteBuffer buffer = generatePatternTreeBuffer(new String[] {d0, d1, d2, d3});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());

      // Insert DataPartitions to create DataRegionGroups
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              curSg, 0, 10, 0, 10, TEST_TIME_PARTITION_INTERVAL);
      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(new TDataPartitionReq(partitionSlotsMap));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
    }

    /* Check Region distribution */
    checkRegionDistribution(TConsensusGroupType.SchemaRegion, client);
    checkRegionDistribution(TConsensusGroupType.DataRegion, client);
  }

  private void checkRegionDistribution(
      TConsensusGroupType type, SyncConfigNodeIServiceClient client) throws TException {
    TShowRegionResp resp = client.showRegion(new TShowRegionReq().setConsensusGroupType(type));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
    Map<String, Integer> databaseRegionCounter = new TreeMap<>();
    Map<Integer, Integer> dataNodeRegionCounter = new TreeMap<>();
    Map<String, Map<Integer, Integer>> databaseDataNodeRegionCounter = new TreeMap<>();
    resp.getRegionInfoList()
        .forEach(
            regionInfo -> {
              databaseRegionCounter.merge(regionInfo.getDatabase(), 1, Integer::sum);
              dataNodeRegionCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
              databaseDataNodeRegionCounter
                  .computeIfAbsent(regionInfo.getDatabase(), empty -> new TreeMap<>())
                  .merge(regionInfo.getDataNodeId(), 1, Integer::sum);
            });
    // The number of RegionGroups should not less than the testMinRegionGroupNum for each database
    // +1 for AUDIT database
    Assert.assertEquals(TEST_DATABASE_NUM + 1, databaseRegionCounter.size());
    databaseRegionCounter.forEach(
        (database, regionCount) ->
            Assert.assertTrue(
                regionCount
                    >= (type == TConsensusGroupType.SchemaRegion
                        ? TEST_MIN_SCHEMA_REGION_GROUP_NUM
                        : TEST_MIN_DATA_REGION_GROUP_NUM)));
    // The maximal Region count - minimal Region count should be less than or equal to 1 for each
    // DataNode
    Assert.assertEquals(TEST_DATA_NODE_NUM, dataNodeRegionCounter.size());
    System.out.println(databaseRegionCounter);
    System.out.println(dataNodeRegionCounter);
    Assert.assertTrue(
        dataNodeRegionCounter.values().stream().max(Integer::compareTo).orElse(0)
                - dataNodeRegionCounter.values().stream().min(Integer::compareTo).orElse(0)
            <= 1);
    // The maximal Region count - minimal Region count should be less than or equal to 1 for each
    // Database
    // +1 for system database
    Assert.assertEquals(TEST_DATABASE_NUM + 1, databaseDataNodeRegionCounter.size());
    databaseDataNodeRegionCounter.forEach(
        (database, dataNodeRegionCount) ->
            Assert.assertTrue(
                dataNodeRegionCount.values().stream().max(Integer::compareTo).orElse(0)
                        - dataNodeRegionCount.values().stream().min(Integer::compareTo).orElse(0)
                    <= 1));
  }
}
