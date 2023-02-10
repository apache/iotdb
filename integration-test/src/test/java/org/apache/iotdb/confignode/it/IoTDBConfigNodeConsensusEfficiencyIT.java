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

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
/*
 * The IoTDBConfigNodeConsensusEfficiencyIT will create and serialize a number of
 * DataPartitions to test the persistence efficiency of ConfigNode's specified ConsensusProtocolClass
 */
public class IoTDBConfigNodeConsensusEfficiencyIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConfigNodeConsensusEfficiencyIT.class);

  // Approximate size for each DataPartition
  // A DataPartition consist of a map:
  // (TSeriesPartitionSlot, TTimePartitionSlot, TConsensusGroupId)
  // 4 Bytes for TSeriesPartitionSlot
  // 8 Bytes for TTimePartitionSlot
  // 8 Bytes for TConsensusGroupId
  private static final int approximateDataPartitionSizeInBytes = 4 + 8 + 8;

  /* Don't modify any of the following default configuration */
  private static final String sg = "root.db";
  private static final String defaultSchemaRegionGroupExtensionPolicy = "CUSTOM";
  private static final int defaultSchemaRegionGroupNumPerDatabase = 1;
  private static final String defaultDataRegionGroupExtensionPolicy = "CUSTOM";
  private static final int defaultDataRegionGroupNumPerDatabase = 1;
  private static final long defaultTimePartitionInterval = 604800000;

  // The total number of partitions is equals to
  // testSeriesPartitionNum * testTimePartitionNum
  private static final int testSeriesPartitionNum = 100;
  private static final int testTimePartitionNum = 10;

  // The total number of consensus logs is equals to
  // (testSeriesPartitionNum / testSeriesPartitionBatchSize) *
  // (testTimePartitionNum / testTimePartitionBatchSize)
  private static final int testSeriesPartitionBatchSize = 10;
  private static final int testTimePartitionBatchSize = 10;

  // Modify testConsensusProtocolClass to indicate the test consensusProtocolClass
  private static final String testConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  @BeforeClass
  public static void setUp() throws SQLException {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy(defaultSchemaRegionGroupExtensionPolicy)
        .setDefaultSchemaRegionGroupNumPerDatabase(defaultSchemaRegionGroupNumPerDatabase)
        .setDataRegionGroupExtensionPolicy(defaultDataRegionGroupExtensionPolicy)
        .setDefaultDataRegionGroupNumPerDatabase(defaultDataRegionGroupNumPerDatabase)
        .setTimePartitionInterval(defaultTimePartitionInterval)
        .setSeriesSlotNum(testSeriesPartitionNum)
        .setConfigNodeConsensusProtocolClass(testConsensusProtocolClass);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Create a Database, a SchemaRegionGroup and a DataRegionGroup
      // in order to initialize test environment
      String initSql = String.format("INSERT INTO %s.d(timestamp, s) values(0, 0.0)", sg);
      statement.execute(initSql);
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void consensusEfficiencyIT() throws InterruptedException {
    long totalTime = 0;
    int logsCount =
        testSeriesPartitionNum
            / testSeriesPartitionBatchSize
            * testTimePartitionNum
            / testTimePartitionBatchSize;

    for (int sid = 0; sid < testSeriesPartitionNum; sid += testSeriesPartitionBatchSize) {
      for (int tid = 0; tid < testTimePartitionNum; tid += testTimePartitionBatchSize) {
        long startTime = System.currentTimeMillis();

        // Prepare PartitionSlotsMap
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
            ConfigNodeTestUtils.constructPartitionSlotsMap(
                sg,
                sid,
                sid + testSeriesPartitionBatchSize,
                tid,
                tid + testTimePartitionBatchSize,
                defaultTimePartitionInterval);

        // Create DataPartitions
        TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
        TDataPartitionTableResp dataPartitionTableResp = null;
        for (int retry = 0; retry < 5; retry++) {
          try (SyncConfigNodeIServiceClient client =
              (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
            dataPartitionTableResp = client.getOrCreateDataPartitionTable(dataPartitionReq);
            if (dataPartitionTableResp != null) {
              break;
            }
          } catch (Exception e) {
            // Retry sometimes in order to avoid request timeout
            LOGGER.error(e.getMessage());
            TimeUnit.SECONDS.sleep(1);
          }
        }

        // Validate DataPartitions
        Assert.assertNotNull(dataPartitionTableResp);
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
        Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
        ConfigNodeTestUtils.checkDataPartitionTable(
            sg,
            sid,
            sid + testSeriesPartitionBatchSize,
            tid,
            tid + testTimePartitionBatchSize,
            defaultTimePartitionInterval,
            dataPartitionTableResp.getDataPartitionTable());

        long endTime = System.currentTimeMillis();
        totalTime += endTime - startTime;
      }
    }

    String result =
        "IoTDBConfigNodeConsensusEfficiencyIT Finished."
            + "\n\t"
            + String.format("Test consensus protocol class: %s", testConsensusProtocolClass)
            + "\n\t"
            + String.format("The number of consensus logs: %d", logsCount)
            + "\n\t"
            + String.format(
                "The number of DataPartitions per log: %d",
                testSeriesPartitionBatchSize * testTimePartitionBatchSize)
            + "\n\t"
            + String.format(
                "The approximate size of each log: %dBytes",
                testSeriesPartitionBatchSize
                    * testTimePartitionBatchSize
                    * approximateDataPartitionSizeInBytes)
            + "\n\t"
            + String.format("Total time consuming: %.2fs", (double) totalTime / 1000)
            + "\n\t"
            + String.format("Average time for each log: %.2fms", (double) totalTime / logsCount);
    LOGGER.info(result);
  }
}
