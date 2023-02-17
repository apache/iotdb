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
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBCustomRegionGroupExtensionIT {

  private static final String testSchemaRegionGroupExtensionPolicy = "CUSTOM";
  private static final String testDataRegionGroupExtensionPolicy = "CUSTOM";
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;
  private static final int testSchemaRegionGroupPerDatabase = 2;
  private static final int testDataRegionGroupPerDatabase = 3;
  private static final int testReplicationFactor = 3;
  private static final long testTimePartitionInterval = 604800000;

  private static final String sg = "root.sg";
  private static final int testSgNum = 2;

  @Before
  public void setUp() throws Exception {

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor)
        .setSchemaRegionGroupExtensionPolicy(testSchemaRegionGroupExtensionPolicy)
        .setDefaultSchemaRegionGroupNumPerDatabase(testSchemaRegionGroupPerDatabase)
        .setDataRegionGroupExtensionPolicy(testDataRegionGroupExtensionPolicy)
        .setDefaultDataRegionGroupNumPerDatabase(testDataRegionGroupPerDatabase)
        .setTimePartitionInterval(testTimePartitionInterval);

    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCustomRegionGroupExtensionPolicy() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      for (int i = 0; i < testSgNum; i++) {
        String curSg = sg + i;

        /* Set StorageGroup */
        TSStatus status = client.setDatabase(new TDatabaseSchema(curSg));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        /* Insert a DataPartition to create DataRegionGroups */
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
            ConfigNodeTestUtils.constructPartitionSlotsMap(
                curSg, 0, 10, 0, 10, testTimePartitionInterval);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(partitionSlotsMap));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());

        ByteBuffer patternTree = generatePatternTreeBuffer(new String[] {curSg + ".d1.s1"});
        TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq(patternTree);
        TSchemaPartitionTableResp schemaPartitionTableResp =
            client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            schemaPartitionTableResp.getStatus().getCode());

        /* Check the number of DataRegionGroups */
        TShowRegionResp showRegionReq = client.showRegion(new TShowRegionReq());
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionReq.getStatus().getCode());
        AtomicInteger dataRegionCount = new AtomicInteger(0);
        AtomicInteger schemaRegionCount = new AtomicInteger(0);
        showRegionReq
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (regionInfo.getDatabase().equals(curSg)
                      && TConsensusGroupType.DataRegion.equals(
                          regionInfo.getConsensusGroupId().getType())) {
                    dataRegionCount.getAndIncrement();
                  }
                  if (regionInfo.getDatabase().equals(curSg)
                      && TConsensusGroupType.SchemaRegion.equals(
                          regionInfo.getConsensusGroupId().getType())) {
                    schemaRegionCount.getAndIncrement();
                  }
                });
        Assert.assertEquals(
            testDataRegionGroupPerDatabase * testReplicationFactor, dataRegionCount.get());
        Assert.assertEquals(
            testSchemaRegionGroupPerDatabase * testReplicationFactor, schemaRegionCount.get());
      }
    }
  }
}
