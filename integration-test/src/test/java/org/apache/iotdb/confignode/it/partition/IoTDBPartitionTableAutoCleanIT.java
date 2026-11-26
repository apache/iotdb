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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionTableAutoCleanIT {

  private static final String TREE_DATABASE_PREFIX = "root.db.auto_clean_";
  private static final String TREE_DEVICE_TTL_DATABASE_PREFIX = "root.db.device_ttl_";
  private static final String TABLE_DATABASE_NAME = "database_auto_clean";

  private static final int TEST_REPLICATION_FACTOR = 1;
  private static final long TEST_TIME_PARTITION_INTERVAL = 604800000;
  private static final long TEST_TTL_CHECK_INTERVAL_IN_MS = 5_00;
  private static final long AUTO_CLEAN_POLL_INTERVAL_IN_MS = 100;

  private static final TTimePartitionSlot TEST_CURRENT_TIME_SLOT =
      new TTimePartitionSlot()
          .setStartTime(
              System.currentTimeMillis()
                  / TEST_TIME_PARTITION_INTERVAL
                  * TEST_TIME_PARTITION_INTERVAL);
  private static final long TEST_TTL = 7 * TEST_TIME_PARTITION_INTERVAL;

  // All scenarios use isolated database names so one initialized 1C1D cluster can serve the class.
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setTimePartitionInterval(TEST_TIME_PARTITION_INTERVAL)
        .setTTLCheckInterval(TEST_TTL_CHECK_INTERVAL_IN_MS);

    // Init 1C1D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // Expired partitions are eventually removed for database and table TTL rules.
  @Test
  public void testAutoCleanPartitionTableForTreeModel() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // Create databases and insert test data
      for (int i = 0; i < 3; i++) {
        String databaseName = String.format("%s%d", TREE_DATABASE_PREFIX, i);
        statement.execute(String.format("CREATE DATABASE %s", databaseName));
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s.s WITH DATATYPE=INT64,ENCODING=PLAIN", databaseName));
        // Insert expired data
        statement.execute(
            String.format(
                "INSERT INTO %s(timestamp, s) VALUES (%d, %d)",
                databaseName, TEST_CURRENT_TIME_SLOT.getStartTime() - TEST_TTL * 2, -1));
        // Insert existed data
        statement.execute(
            String.format(
                "INSERT INTO %s(timestamp, s) VALUES (%d, %d)",
                databaseName, TEST_CURRENT_TIME_SLOT.getStartTime(), 1));
      }
      // Let db0.TTL > device.TTL, the valid TTL should be the bigger one
      statement.execute(String.format("SET TTL TO %s0 %d", TREE_DATABASE_PREFIX, TEST_TTL));
      statement.execute(String.format("SET TTL TO %s0.s %d", TREE_DATABASE_PREFIX, 10));
      // Let db1.TTL < device.TTL, the valid TTL should be the bigger one
      statement.execute(String.format("SET TTL TO %s1 %d", TREE_DATABASE_PREFIX, 10));
      statement.execute(String.format("SET TTL TO %s1.s %d", TREE_DATABASE_PREFIX, TEST_TTL));
      // Set TTL to path db2.**
      statement.execute(String.format("SET TTL TO %s2.** %d", TREE_DATABASE_PREFIX, TEST_TTL));
    }

    TDataPartitionReq req = new TDataPartitionReq();
    for (int i = 0; i < 3; i++) {
      req.putToPartitionSlotsMap(String.format("%s%d", TREE_DATABASE_PREFIX, i), new TreeMap<>());
    }
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      awaitPartitionTableAutoCleaned(client, req);
    }
  }

  // Device-level TTL alone must not clean expired partitions when the database has no TTL.
  @Test
  public void testAutoCleanTakesNoEffectsForTreeDeviceTTL() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // Create databases and insert test data
      for (int i = 0; i < 3; i++) {
        String databaseName = String.format("%s%d", TREE_DEVICE_TTL_DATABASE_PREFIX, i);
        statement.execute(String.format("CREATE DATABASE %s", databaseName));
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s.s WITH DATATYPE=INT64,ENCODING=PLAIN", databaseName));
        // Insert expired data
        statement.execute(
            String.format(
                "INSERT INTO %s(timestamp, s) VALUES (%d, %d)",
                databaseName, TEST_CURRENT_TIME_SLOT.getStartTime() - TEST_TTL * 2, -1));
        // Insert existed data
        statement.execute(
            String.format(
                "INSERT INTO %s(timestamp, s) VALUES (%d, %d)",
                databaseName, TEST_CURRENT_TIME_SLOT.getStartTime(), 1));
        // Create an empty device and set a TTL.
        // This TTL should not trigger the auto cleaner,
        // since the database does not have a TTL.
        statement.execute(
            String.format(
                "CREATE TIMESERIES %s.m.empty WITH DATATYPE=INT64,ENCODING=PLAIN", databaseName));
        statement.execute(String.format("SET TTL TO %s.m.** %d", databaseName, TEST_TTL));
      }
    }

    TDataPartitionReq req = new TDataPartitionReq();
    for (int i = 0; i < 3; i++) {
      req.putToPartitionSlotsMap(
          String.format("%s%d", TREE_DEVICE_TTL_DATABASE_PREFIX, i), new TreeMap<>());
    }
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      for (int retry = 0; retry < 10; retry++) {
        // Ensure the partitions are not cleaned
        boolean partitionTableAutoCleaned = false;
        TDataPartitionTableResp resp = client.getDataPartitionTable(req);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
          partitionTableAutoCleaned =
              resp.getDataPartitionTable().entrySet().stream()
                  .flatMap(e1 -> e1.getValue().entrySet().stream())
                  .allMatch(e2 -> e2.getValue().size() == 1);
        }
        Assert.assertFalse(partitionTableAutoCleaned);
        TimeUnit.SECONDS.sleep(1);
      }
    }
  }

  // Expired partitions are eventually removed when a table-model database has a TTL.
  @Test
  public void testAutoCleanPartitionTableForTableModel() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      // Create databases and insert test data
      String databaseName = TABLE_DATABASE_NAME;
      statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName));
      statement.execute(String.format("USE %s", databaseName));
      statement.execute("CREATE TABLE tb (time TIMESTAMP TIME, s int64 FIELD)");
      // Insert expired data
      statement.execute(
          String.format(
              "INSERT INTO tb(time, s) VALUES (%d, %d)",
              TEST_CURRENT_TIME_SLOT.getStartTime() - TEST_TTL * 2, -1));
      // Insert existed data
      statement.execute(
          String.format(
              "INSERT INTO tb(time, s) VALUES (%d, %d)", TEST_CURRENT_TIME_SLOT.getStartTime(), 1));
      statement.execute(String.format("USE %s", databaseName));
      statement.execute(String.format("ALTER TABLE tb SET PROPERTIES TTL=%d", TEST_TTL));
    }

    TDataPartitionReq req = new TDataPartitionReq();
    req.putToPartitionSlotsMap(TABLE_DATABASE_NAME, new TreeMap<>());
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      awaitPartitionTableAutoCleaned(client, req);
    }
  }

  private static void awaitPartitionTableAutoCleaned(
      SyncConfigNodeIServiceClient client, TDataPartitionReq req) {
    // TTL checks run asynchronously; poll promptly while retaining the existing two-minute bound.
    Awaitility.await("partition table auto cleanup")
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(AUTO_CLEAN_POLL_INTERVAL_IN_MS, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              TDataPartitionTableResp resp = client.getDataPartitionTable(req);
              return TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()
                  && resp.getDataPartitionTable().entrySet().stream()
                      .flatMap(e1 -> e1.getValue().entrySet().stream())
                      .allMatch(e2 -> e2.getValue().size() == 1);
            });
  }
}
