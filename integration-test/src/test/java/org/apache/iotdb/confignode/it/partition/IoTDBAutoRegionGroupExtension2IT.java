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
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBAutoRegionGroupExtension2IT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBAutoRegionGroupExtension2IT.class);

  private static final String testDataRegionGroupExtensionPolicy = "AUTO";
  private static final String testConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;
  private static final int testReplicationFactor = 3;

  private static final String database = "root.db";
  private static final long testTimePartitionInterval = 604800000;
  private static final int testMinDataRegionGroupNum = 3;
  private static final int testDataNodeNum = 3;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(testConsensusProtocolClass)
        .setDataReplicationFactor(testReplicationFactor)
        .setDataRegionGroupExtensionPolicy(testDataRegionGroupExtensionPolicy)
        .setTimePartitionInterval(testTimePartitionInterval);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAutoRegionGroupExtensionPolicy2()
      throws ClientManagerException, IOException, InterruptedException, TException {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status =
          client.setDatabase(
              new TDatabaseSchema(database).setMinDataRegionGroupNum(testMinDataRegionGroupNum));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Shutdown 1 DataNode
      EnvFactory.getEnv().shutdownDataNode(1);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(1)),
              Collections.singletonList(NodeStatus.Unknown));

      // Create 3 DataPartitions to extend 3 DataRegionGroups
      for (int i = 0; i < testMinDataRegionGroupNum; i++) {
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
            ConfigNodeTestUtils.constructPartitionSlotsMap(
                database, i, i + 1, i, i + 1, testTimePartitionInterval);
        TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
        TDataPartitionTableResp dataPartitionTableResp = null;
        for (int retry = 0; retry < 5; retry++) {
          // Build new Client since it's unstable in Win8 environment
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
        Assert.assertNotNull(dataPartitionTableResp);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
        Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
        ConfigNodeTestUtils.checkDataPartitionTable(
            database,
            i,
            i + 1,
            i,
            i + 1,
            testTimePartitionInterval,
            dataPartitionTableResp.getDataPartitionTable());
      }

      // Restart DataNode
      EnvFactory.getEnv().startDataNode(1);

      // Check DataRegionGroups
      TShowRegionResp resp = client.showRegion(new TShowRegionReq());
      resp.getRegionInfoList()
          .removeIf(r -> r.getDatabase().startsWith(SystemConstant.SYSTEM_DATABASE));
      resp.getRegionInfoList()
          .removeIf(r -> r.getDatabase().startsWith(SystemConstant.AUDIT_DATABASE));
      Map<Integer, AtomicInteger> counter = new HashMap<>();
      resp.getRegionInfoList()
          .forEach(
              regionInfo ->
                  counter
                      .computeIfAbsent(regionInfo.getDataNodeId(), empty -> new AtomicInteger(0))
                      .incrementAndGet());
      counter.forEach((dataNodeId, regionCount) -> Assert.assertEquals(3, regionCount.get()));
    }
  }
}
