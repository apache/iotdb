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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBAutoRegionGroupExtensionIT {
  private static final String testDataRegionGroupExtensionPolicy = "AUTO";
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;
  private static final int testReplicationFactor = 1;

  private static final String sg = "root.sg";
  private static final int testSgNum = 2;
  private static final long testTimePartitionInterval = 604800000;
  private static final int testMinSchemaRegionGroupNum = 2;
  private static final int testMinDataRegionGroupNum = 2;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor)
        .setDataRegionGroupExtensionPolicy(testDataRegionGroupExtensionPolicy)
        .setTimePartitionInterval(testTimePartitionInterval);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
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

      setStorageGroupAndCheckRegionGroupDistribution(client);

      // Delete all StorageGroups
      for (int i = 0; i < testSgNum; i++) {
        String curSg = sg + i;
        client.deleteDatabase(new TDeleteDatabaseReq(curSg));
      }
      boolean isAllRegionGroupDeleted = false;
      for (int retry = 0; retry < retryNum; retry++) {
        TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
        if (showRegionResp.getRegionInfoListSize() == 0) {
          isAllRegionGroupDeleted = true;
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      Assert.assertTrue(isAllRegionGroupDeleted);

      // Re-test for safety
      setStorageGroupAndCheckRegionGroupDistribution(client);
    }
  }

  private void setStorageGroupAndCheckRegionGroupDistribution(SyncConfigNodeIServiceClient client)
      throws TException, IllegalPathException, IOException {

    for (int i = 0; i < testSgNum; i++) {
      String curSg = sg + i;
      TSStatus status =
          client.setDatabase(
              new TDatabaseSchema(curSg)
                  .setMinSchemaRegionGroupNum(testMinSchemaRegionGroupNum)
                  .setMinDataRegionGroupNum(testMinDataRegionGroupNum));
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
              curSg, 0, 10, 0, 10, testTimePartitionInterval);
      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(new TDataPartitionReq(partitionSlotsMap));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
    }

    // The number of SchemaRegionGroups should not less than the testMinSchemaRegionGroupNum
    TShowRegionResp showRegionReq =
        client.showRegion(
            new TShowRegionReq().setConsensusGroupType(TConsensusGroupType.SchemaRegion));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionReq.getStatus().getCode());
    Map<String, AtomicInteger> regionCounter = new ConcurrentHashMap<>();
    showRegionReq
        .getRegionInfoList()
        .forEach(
            regionInfo ->
                regionCounter
                    .computeIfAbsent(regionInfo.getDatabase(), empty -> new AtomicInteger(0))
                    .getAndIncrement());
    Assert.assertEquals(testSgNum, regionCounter.size());
    regionCounter.forEach(
        (sg, regionCount) -> Assert.assertTrue(regionCount.get() >= testMinSchemaRegionGroupNum));

    // The number of DataRegionGroups should not less than the testMinDataRegionGroupNum
    showRegionReq =
        client.showRegion(
            new TShowRegionReq().setConsensusGroupType(TConsensusGroupType.DataRegion));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionReq.getStatus().getCode());
    regionCounter.clear();
    showRegionReq
        .getRegionInfoList()
        .forEach(
            regionInfo ->
                regionCounter
                    .computeIfAbsent(regionInfo.getDatabase(), empty -> new AtomicInteger(0))
                    .getAndIncrement());
    Assert.assertEquals(testSgNum, regionCounter.size());
    regionCounter.forEach(
        (sg, regionCount) -> Assert.assertTrue(regionCount.get() >= testMinDataRegionGroupNum));
  }
}
