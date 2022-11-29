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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseConfig;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBAutoRegionGroupExtensionIT {

  private static final BaseConfig CONF = ConfigFactory.getConfig();

  private static String originalDataRegionGroupExtensionPolicy;
  private static final String testDataRegionGroupExtensionPolicy = "AUTO";

  private static String originalSchemaRegionConsensusProtocolClass;
  private static String originalDataRegionConsensusProtocolClass;
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  private static int originalSchemaReplicationFactor;
  private static int originalDataReplicationFactor;
  private static final int testReplicationFactor = 1;

  private static long originalTimePartitionInterval;

  private static int originalLeastDataRegionGroupNum;

  private static final String sg = "root.sg";
  private static final int testSgNum = 2;

  @Before
  public void setUp() throws Exception {
    originalSchemaRegionConsensusProtocolClass = CONF.getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass = CONF.getDataRegionConsensusProtocolClass();
    CONF.setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass);
    CONF.setDataRegionConsensusProtocolClass(testConsensusProtocolClass);

    originalSchemaReplicationFactor = CONF.getSchemaReplicationFactor();
    originalDataReplicationFactor = CONF.getDataReplicationFactor();
    CONF.setSchemaReplicationFactor(testReplicationFactor);
    CONF.setDataReplicationFactor(testReplicationFactor);

    originalTimePartitionInterval = CONF.getTimePartitionInterval();

    originalLeastDataRegionGroupNum = CONF.getLeastDataRegionGroupNum();

    originalDataRegionGroupExtensionPolicy = CONF.getDataRegionGroupExtensionPolicy();
    CONF.setDataRegionGroupExtensionPolicy(testDataRegionGroupExtensionPolicy);

    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();

    CONF.setSchemaRegionConsensusProtocolClass(originalSchemaRegionConsensusProtocolClass);
    CONF.setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);
    CONF.setSchemaReplicationFactor(originalSchemaReplicationFactor);
    CONF.setDataReplicationFactor(originalDataReplicationFactor);
    CONF.setDataRegionGroupExtensionPolicy(originalDataRegionGroupExtensionPolicy);
  }

  @Test
  public void testAutoRegionGroupExtensionPolicy()
      throws IOException, InterruptedException, TException {

    final int retryNum = 100;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      setStorageGroupAndCheckRegionGroupDistribution(client);

      // Delete all StorageGroups
      for (int i = 0; i < testSgNum; i++) {
        String curSg = sg + i;
        client.deleteStorageGroup(new TDeleteStorageGroupReq(curSg));
      }
      boolean isAllRegionGroupDeleted = false;
      for (int retry = 0; retry < 100; retry++) {
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
      throws TException {
    for (int i = 0; i < testSgNum; i++) {
      String curSg = sg + i;
      TSStatus status =
          client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(curSg)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    for (int i = 0; i < testSgNum; i++) {
      String curSg = sg + i;

      /* Insert a DataPartition to create DataRegionGroups */
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              curSg, 0, 10, 0, 10, originalTimePartitionInterval);
      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(new TDataPartitionReq(partitionSlotsMap));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
    }

    // Re-calculate the least DataRegionGroup num based on the test resource
    int totalCpuCoreNum = 0;
    TShowDataNodesResp showDataNodesResp = client.showDataNodes();
    for (TDataNodeInfo dataNodeInfo : showDataNodesResp.getDataNodesInfoList()) {
      totalCpuCoreNum += dataNodeInfo.getCpuCoreNum();
    }
    final int leastDataRegionGroupNum =
        Math.min(
            originalLeastDataRegionGroupNum,
            (int)
                Math.ceil((double) totalCpuCoreNum / (double) (testSgNum * testReplicationFactor)));

    /* Check the number of DataRegionGroups */
    TShowRegionResp showRegionReq = client.showRegion(new TShowRegionReq());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionReq.getStatus().getCode());
    Map<String, AtomicInteger> regionCounter = new ConcurrentHashMap<>();
    showRegionReq
        .getRegionInfoList()
        .forEach(
            regionInfo ->
                regionCounter
                    .computeIfAbsent(regionInfo.getStorageGroup(), empty -> new AtomicInteger(0))
                    .getAndIncrement());
    Assert.assertEquals(testSgNum, regionCounter.size());
    regionCounter.forEach(
        (sg, regionCount) -> Assert.assertEquals(leastDataRegionGroupNum, regionCount.get()));
  }
}
