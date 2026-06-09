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

package org.apache.iotdb.confignode.it.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
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

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMap;
import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBLoadBalanceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBLoadBalanceIT.class);

  private static final int INITIAL_DATA_NODE_NUM = 3;
  private static final int FINAL_DATA_NODE_NUM = 4;
  private static final int TEST_REPLICATION_FACTOR = 2;
  private static final String DATABASE = "root.db";
  private static final int TEST_DATABASE_NUM = 2;
  private static final long TEST_TIME_PARTITION_INTERVAL = 604800000;
  private static final int TEST_MIN_DATA_REGION_GROUP_NUM = 3;
  private static final String LOAD_BALANCE_SQL = "LOAD BALANCE";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(TEST_MIN_DATA_REGION_GROUP_NUM)
        .setTimePartitionInterval(TEST_TIME_PARTITION_INTERVAL);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, INITIAL_DATA_NODE_NUM);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRegionLoadBalanceAfterAddingDataNode() throws Exception {
    final int retryNum = 100;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Create databases and partitions to generate multiple regions
      createDatabasesAndPartitions(client);

      // Check initial region distribution
      Map<Integer, Integer> beforeRegionCounter = getRegionDistribution(client);
      LOGGER.info("Region distribution before migration: {}", beforeRegionCounter);

      // Verify initial distribution is not balanced (should have variance)
      int maxBefore = beforeRegionCounter.values().stream().max(Integer::compareTo).orElse(0);
      int minBefore = beforeRegionCounter.values().stream().min(Integer::compareTo).orElse(0);
      LOGGER.info(
          "Max Region count before migration: {}, Min Region count: {}", maxBefore, minBefore);

      // Add a new data node to trigger load balance
      // Let DataNode register itself during startup instead of pre-registering via RPC
      // Pre-registration causes endpoint conflict when DataNode tries to register during startup
      EnvFactory.getEnv().registerNewDataNode(true);
      LOGGER.info("New data node started successfully");

      // Manually trigger load balance using SQL statement
      try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
          Statement statement = makeItCloseQuietly(connection.createStatement())) {
        statement.execute(LOAD_BALANCE_SQL);
        LOGGER.info("Load balance triggered successfully");
      }

      // Wait for migration to complete
      try {
        awaitMigrationComplete();
      } catch (ConditionTimeoutException e) {
        LOGGER.error("Region migration did not complete in time", e);
        Assert.fail();
      }

      // Wait for load balance to complete
      Map<Integer, Integer> afterRegionCounter = null;
      boolean isBalanced = false;
      for (int retry = 0; retry < retryNum; retry++) {
        afterRegionCounter = getRegionDistribution(client);
        int maxAfter = afterRegionCounter.values().stream().max(Integer::compareTo).orElse(0);
        int minAfter = afterRegionCounter.values().stream().min(Integer::compareTo).orElse(0);
        int variance = maxAfter - minAfter;

        LOGGER.info(
            "Retry {}: Region distribution after migration: {}, Max: {}, Min: {}, Variance: {}",
            retry,
            afterRegionCounter,
            maxAfter,
            minAfter,
            variance);

        // Check if distribution is balanced (variance should be <= 1)
        // Also verify that the new node has some regions
        if (variance <= 1 && afterRegionCounter.size() == FINAL_DATA_NODE_NUM) {
          isBalanced = true;
          break;
        }

        TimeUnit.SECONDS.sleep(2);
      }

      Assert.assertTrue("Load balance not completed", isBalanced);
      Assert.assertNotNull("Cannot get Region distribution after migration", afterRegionCounter);
      LOGGER.info("Region distribution after migration: {}", afterRegionCounter);

      // Verify the new data node has regions
      // The new node should have DataNodeId = 3 (since we started with 3 nodes: 0, 1, 2)
      // DataNodeId starts from 0, so with INITIAL_DATA_NODE_NUM=3, IDs are 0,1,2, and new one is 3
      int newDataNodeId = INITIAL_DATA_NODE_NUM;
      Assert.assertTrue(
          "New data node should contain Regions",
          afterRegionCounter.containsKey(newDataNodeId)
              && afterRegionCounter.get(newDataNodeId) > 0);

      // Verify distribution is more balanced
      int maxAfter = afterRegionCounter.values().stream().max(Integer::compareTo).orElse(0);
      int minAfter = afterRegionCounter.values().stream().min(Integer::compareTo).orElse(0);
      Assert.assertTrue(
          "After load balance, the difference between max and min Region count should <= 1",
          maxAfter - minAfter <= 1);
    }

    // Also verify using SQL query
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {

      Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
      Map<Integer, Integer> sqlRegionCounter = new TreeMap<>();
      regionMap.forEach(
          (regionId, dataNodeIds) -> {
            for (Integer dataNodeId : dataNodeIds) {
              sqlRegionCounter.merge(dataNodeId, 1, Integer::sum);
            }
          });

      LOGGER.info("Region distribution queried via SQL: {}", sqlRegionCounter);

      // Verify each region has correct replication factor
      regionMap.forEach(
          (regionId, dataNodeIds) -> {
            Assert.assertEquals(
                "Each Region should have correct replication factor",
                TEST_REPLICATION_FACTOR,
                dataNodeIds.size());
          });

      // Verify distribution is balanced
      int maxSql = sqlRegionCounter.values().stream().max(Integer::compareTo).orElse(0);
      int minSql = sqlRegionCounter.values().stream().min(Integer::compareTo).orElse(0);
      Assert.assertTrue(
          "Via SQL query, after load balance, the difference between max and min Region count should <= 1",
          maxSql - minSql <= 1);
    }
  }

  private void createDatabasesAndPartitions(SyncConfigNodeIServiceClient client) throws Exception {
    for (int i = 0; i < TEST_DATABASE_NUM; i++) {
      String curSg = DATABASE + i;

      // Set Database
      TSStatus status = client.setDatabase(new TDatabaseSchema(curSg));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Create DataPartitions to create DataRegionGroups
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
          ConfigNodeTestUtils.constructPartitionSlotsMap(
              curSg, 0, 10, 0, 10, TEST_TIME_PARTITION_INTERVAL);
      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(new TDataPartitionReq(partitionSlotsMap));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());

      LOGGER.info("Partitions created successfully for database {}", curSg);
    }
  }

  private Map<Integer, Integer> getRegionDistribution(SyncConfigNodeIServiceClient client)
      throws Exception {
    TShowRegionResp resp = client.showRegion(new TShowRegionReq());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());

    Map<Integer, Integer> dataNodeRegionCounter = new TreeMap<>();
    resp.getRegionInfoList()
        .forEach(
            regionInfo -> {
              // Filter out system and audit databases
              if (!regionInfo.getDatabase().equals(SystemConstant.SYSTEM_DATABASE)
                  && !regionInfo.getDatabase().equals(SystemConstant.AUDIT_DATABASE)
                  && TConsensusGroupType.DataRegion.equals(
                      regionInfo.getConsensusGroupId().getType())) {
                dataNodeRegionCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
              }
            });

    return dataNodeRegionCounter;
  }

  /**
   * Wait for region migration to complete. All regions should have the correct replication factor.
   */
  private void awaitMigrationComplete() {
    AtomicReference<Exception> lastException = new AtomicReference<>();
    AtomicReference<Map<Integer, Set<Integer>>> lastRegionMap = new AtomicReference<>();

    try {
      Awaitility.await()
          .atMost(5, TimeUnit.MINUTES)
          .pollDelay(2, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .until(
              () -> {
                try (Connection connection =
                        makeItCloseQuietly(EnvFactory.getEnv().getConnection());
                    Statement statement = makeItCloseQuietly(connection.createStatement())) {
                  Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
                  lastRegionMap.set(regionMap);

                  // Check if all regions have correct replication factor
                  for (Map.Entry<Integer, Set<Integer>> entry : regionMap.entrySet()) {
                    int regionId = entry.getKey();
                    Set<Integer> dataNodeIds = entry.getValue();
                    if (dataNodeIds.size() != TEST_REPLICATION_FACTOR) {
                      LOGGER.info(
                          "Region {} has {} replicas, expected {}, migration not finished yet",
                          regionId,
                          dataNodeIds.size(),
                          TEST_REPLICATION_FACTOR);
                      return false;
                    }
                  }
                  return true;
                } catch (Exception e) {
                  lastException.set(e);
                  LOGGER.warn("Exception while checking migration status: {}", e.getMessage());
                  return false;
                }
              });
      LOGGER.info("Region migration completed successfully");
    } catch (ConditionTimeoutException e) {
      if (lastRegionMap.get() == null) {
        LOGGER.error(
            "Migration check failed, lastRegionMap is null, last Exception:", lastException.get());
        throw e;
      }
      StringBuilder errorMsg = new StringBuilder();
      errorMsg.append("Region migration timeout in 5 minutes. Regions with incorrect replication:");
      for (Map.Entry<Integer, Set<Integer>> entry : lastRegionMap.get().entrySet()) {
        int regionId = entry.getKey();
        Set<Integer> dataNodeIds = entry.getValue();
        if (dataNodeIds.size() != TEST_REPLICATION_FACTOR) {
          errorMsg.append(
              String.format(
                  " Region %d has %d replicas (expected %d);",
                  regionId, dataNodeIds.size(), TEST_REPLICATION_FACTOR));
        }
      }
      LOGGER.error(errorMsg.toString());
      if (lastException.get() != null) {
        LOGGER.error("Last exception during awaiting:", lastException.get());
      }
      throw e;
    }
  }
}
