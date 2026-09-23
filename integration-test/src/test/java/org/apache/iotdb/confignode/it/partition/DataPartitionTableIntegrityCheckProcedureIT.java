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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.enums.RepairDataPartitionTableProgressState;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class DataPartitionTableIntegrityCheckProcedureIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataPartitionTableIntegrityCheckProcedureIT.class);

  private static final String TABLE_DATABASE = "repair_table_db";
  private static final String TABLE_NAME = "table1";
  private static final long TIME_PARTITION_INTERVAL = 604_800_000L;
  private static final long TABLE_TTL = 7 * TIME_PARTITION_INTERVAL;
  private static final long CURRENT_TIME_PARTITION_START =
      System.currentTimeMillis() / TIME_PARTITION_INTERVAL * TIME_PARTITION_INTERVAL;
  private static final long EXPIRED_TIME_PARTITION_START =
      CURRENT_TIME_PARTITION_START - TABLE_TTL * 2;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setTimePartitionInterval(TIME_PARTITION_INTERVAL);
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeCommonConfig()
        .setTTLCheckInterval(TimeUnit.MILLISECONDS.toMillis(500));
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeCommonConfig()
        .setTTLCheckInterval(TimeUnit.MINUTES.toMillis(10));
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testConcurrentSubmitDataPartitionTableIntegrityCheckProcedure()
      throws InterruptedException {
    final int threadCount = 10;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishLatch = new CountDownLatch(threadCount);
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicInteger failCount = new AtomicInteger(0);
    final List<String> failureMessages = Collections.synchronizedList(new ArrayList<>());

    // Concurrently submit the DataPartitionTableIntegrityCheckProcedure
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();

              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement stmt = connection.createStatement()) {
                stmt.execute("REPAIR DATA PARTITION TABLE");
                successCount.incrementAndGet();
                LOGGER.info("Thread {} submitted integrity check successfully", threadId);
              }
            } catch (final SQLException e) {
              failCount.incrementAndGet();
              failureMessages.add("Thread " + threadId + " failed: " + e.getMessage());
              LOGGER.info(
                  "Thread {} failed to submit integrity check: {}", threadId, e.getMessage());
            } catch (final Exception e) {
              failCount.incrementAndGet();
              failureMessages.add("Thread " + threadId + " failed unexpectedly: " + e.getMessage());
              LOGGER.error("Thread {} unexpected error: {}", threadId, e.getMessage(), e);
            } finally {
              finishLatch.countDown();
            }
          });
    }

    startLatch.countDown();

    final boolean completed = finishLatch.await(60, TimeUnit.SECONDS);
    Assert.assertTrue("Not all threads completed within timeout", completed);

    executor.shutdown();
    Assert.assertTrue(
        "Executor did not terminate", executor.awaitTermination(10, TimeUnit.SECONDS));

    LOGGER.info("Success count: {}, Fail count: {}", successCount.get(), failCount.get());
    LOGGER.info("Failure messages: {}", failureMessages);

    Assert.assertEquals(
        "Only one procedure should be submitted successfully", 1, successCount.get());
    Assert.assertEquals(
        "The other concurrent submissions should be rejected", threadCount - 1, failCount.get());
  }

  @Test
  public void testShowRepairDataPartitionTableProgress() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      assertRepairProgress(statement, RepairDataPartitionTableProgressState.IDLE.name(), 0.0, 0.0);

      statement.execute("REPAIR DATA PARTITION TABLE");
      assertRepairProgress(statement, null, 0.0, 100.0);
    }
  }

  @Test
  public void testRepairDataPartitionTableIgnoresTableModelDatabase() throws Exception {
    final TDataPartitionReq dataPartitionReq = new TDataPartitionReq();
    dataPartitionReq.putToPartitionSlotsMap(TABLE_DATABASE, new TreeMap<>());

    try (final Connection tableConnection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement tableStatement = tableConnection.createStatement()) {
      tableStatement.execute(String.format("CREATE DATABASE %s", TABLE_DATABASE));
      tableStatement.execute(String.format("USE %s", TABLE_DATABASE));
      tableStatement.execute(
          String.format("CREATE TABLE %s (device_id STRING TAG, value INT64 FIELD)", TABLE_NAME));
      tableStatement.execute(
          String.format(
              "INSERT INTO %s(time, device_id, value) VALUES (%d, 'd1', 1)",
              TABLE_NAME, EXPIRED_TIME_PARTITION_START));
      tableStatement.execute(
          String.format(
              "INSERT INTO %s(time, device_id, value) VALUES (%d, 'd1', 2)",
              TABLE_NAME, CURRENT_TIME_PARTITION_START));
      tableStatement.execute("FLUSH");

      Assert.assertTrue(
          "The expired time partition must exist before TTL cleanup",
          containsTimePartition(dataPartitionReq, EXPIRED_TIME_PARTITION_START));

      tableStatement.execute(
          String.format("ALTER TABLE %s SET PROPERTIES TTL=%d", TABLE_NAME, TABLE_TTL));
      waitUntilTimePartitionRemoved(dataPartitionReq, EXPIRED_TIME_PARTITION_START);
      Assert.assertTrue(
          "The current table-model time partition must survive TTL cleanup",
          containsTimePartition(dataPartitionReq, CURRENT_TIME_PARTITION_START));

      // Stop periodic cleanup from hiding an incorrectly restored partition after repair.
      tableStatement.execute(String.format("ALTER TABLE %s SET PROPERTIES TTL='INF'", TABLE_NAME));
    }

    try (final Connection treeConnection =
            EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement treeStatement = treeConnection.createStatement()) {
      treeStatement.execute("REPAIR DATA PARTITION TABLE");
      waitForRepairCompletion(treeStatement);
    }

    Assert.assertFalse(
        "Repair must not restore a table-model time partition removed by TTL cleanup",
        containsTimePartition(dataPartitionReq, EXPIRED_TIME_PARTITION_START));
    Assert.assertTrue(
        "Repair must preserve the current table-model time partition",
        containsTimePartition(dataPartitionReq, CURRENT_TIME_PARTITION_START));
  }

  private static boolean containsTimePartition(
      final TDataPartitionReq request, final long timePartitionStart) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TDataPartitionTableResp response = client.getDataPartitionTable(request);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      final Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          seriesPartitionTable = response.getDataPartitionTable().get(TABLE_DATABASE);
      return seriesPartitionTable != null
          && seriesPartitionTable.values().stream()
              .anyMatch(
                  timePartitionTable ->
                      timePartitionTable.containsKey(new TTimePartitionSlot(timePartitionStart)));
    }
  }

  private static void waitUntilTimePartitionRemoved(
      final TDataPartitionReq request, final long timePartitionStart) throws Exception {
    for (int retry = 0; retry < 120; retry++) {
      if (!containsTimePartition(request, timePartitionStart)) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.fail("The expired table-model time partition was not cleaned within the timeout");
  }

  private static void waitForRepairCompletion(final Statement statement) throws Exception {
    TimeUnit.SECONDS.sleep(1);
    for (int retry = 0; retry < 120; retry++) {
      try (final ResultSet resultSet =
          statement.executeQuery("SHOW REPAIR DATA PARTITION TABLE PROGRESS")) {
        Assert.assertTrue(resultSet.next());
        if (RepairDataPartitionTableProgressState.IDLE
            .name()
            .equals(resultSet.getString("Status"))) {
          return;
        }
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.fail("The data partition table repair did not complete within the timeout");
  }

  private static void assertRepairProgress(
      final Statement statement,
      final String expectedStatus,
      final double minProgress,
      final double maxProgress)
      throws SQLException {
    try (final ResultSet resultSet =
        statement.executeQuery("SHOW REPAIR DATA PARTITION TABLE PROGRESS")) {
      Assert.assertTrue(resultSet.next());
      if (expectedStatus != null) {
        Assert.assertEquals(expectedStatus, resultSet.getString("Status"));
      } else {
        Assert.assertNotEquals(
            RepairDataPartitionTableProgressState.UNKNOWN.name(), resultSet.getString("Status"));
      }
      final double progress = resultSet.getDouble("Progress(%)");
      Assert.assertTrue(progress >= minProgress);
      Assert.assertTrue(progress <= maxProgress);
      Assert.assertNotNull(resultSet.getString("Message"));
      Assert.assertFalse(resultSet.next());
    }
  }
}
