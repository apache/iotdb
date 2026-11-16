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
public class IoTDBPartitionTableAutoCleanUSIT {

  private static final String TREE_DATABASE_PREFIX = "root.db.g_";
  private static final String TABLE_DATABASE_PREFIX = "database_";

  private static final int TEST_REPLICATION_FACTOR = 1;
  private static final long TEST_TIME_PARTITION_INTERVAL_IN_MS = 604800_000;
  private static final long TEST_TIME_PARTITION_INTERVAL_IN_US =
      TimeUnit.MILLISECONDS.toMicros(TEST_TIME_PARTITION_INTERVAL_IN_MS);
  // Exercise TTL cleanup promptly without changing the microsecond timestamps or TTL rules.
  private static final long TEST_TTL_CHECK_INTERVAL = 500;
  private static final long AUTO_CLEAN_POLL_INTERVAL_IN_MS = 100;

  private static final TTimePartitionSlot TEST_CURRENT_TIME_SLOT =
      new TTimePartitionSlot()
          .setStartTime(
              System.currentTimeMillis()
                  * 1000L
                  / TEST_TIME_PARTITION_INTERVAL_IN_MS
                  * TEST_TIME_PARTITION_INTERVAL_IN_MS);
  private static final long TEST_TTL_IN_MS = 7 * TEST_TIME_PARTITION_INTERVAL_IN_MS;

  // Inserts use microseconds, while the configured partition interval is converted from
  // milliseconds.
  private static final TTimePartitionSlot EXPECTED_CURRENT_TIME_SLOT =
      new TTimePartitionSlot(
          TEST_CURRENT_TIME_SLOT.getStartTime()
              / TEST_TIME_PARTITION_INTERVAL_IN_US
              * TEST_TIME_PARTITION_INTERVAL_IN_US);

  // Tree and table scenarios use distinct database names and can share the same 1C1D cluster.
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setTimePartitionInterval(TEST_TIME_PARTITION_INTERVAL_IN_MS)
        .setTTLCheckInterval(TEST_TTL_CHECK_INTERVAL)
        // Note that the time precision of IoTDB is us in this IT
        .setTimestampPrecision("us");

    // Init 1C1D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // With microsecond timestamps, database/device TTL precedence must retain only current
  // partitions.
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
                databaseName, TEST_CURRENT_TIME_SLOT.getStartTime() - TEST_TTL_IN_MS * 2000, -1));
        // Insert existed data
        statement.execute(
            String.format(
                "INSERT INTO %s(timestamp, s) VALUES (%d, %d)",
                databaseName, TEST_CURRENT_TIME_SLOT.getStartTime(), 1));
      }
      // Let db0.TTL > device.TTL, the valid TTL should be the bigger one
      statement.execute(String.format("SET TTL TO %s0 %d", TREE_DATABASE_PREFIX, TEST_TTL_IN_MS));
      statement.execute(String.format("SET TTL TO %s0.s %d", TREE_DATABASE_PREFIX, 10));
      // Let db1.TTL < device.TTL, the valid TTL should be the bigger one
      statement.execute(String.format("SET TTL TO %s1 %d", TREE_DATABASE_PREFIX, 10));
      statement.execute(String.format("SET TTL TO %s1.s %d", TREE_DATABASE_PREFIX, TEST_TTL_IN_MS));
      // Set TTL to path db2.**
      statement.execute(
          String.format("SET TTL TO %s2.** %d", TREE_DATABASE_PREFIX, TEST_TTL_IN_MS));
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

  // With microsecond timestamps, table TTL must remove expired partitions and retain the current
  // one.
  @Test
  public void testAutoCleanPartitionTableForTableModel() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      // Create databases and insert test data
      String databaseName = TABLE_DATABASE_PREFIX;
      statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName));
      statement.execute(String.format("USE %s", databaseName));
      statement.execute("CREATE TABLE tb (time TIMESTAMP TIME, s int64 FIELD)");
      // Insert expired data
      statement.execute(
          String.format(
              "INSERT INTO tb(time, s) VALUES (%d, %d)",
              TEST_CURRENT_TIME_SLOT.getStartTime() - TEST_TTL_IN_MS * 2000, -1));
      // Insert existed data
      statement.execute(
          String.format(
              "INSERT INTO tb(time, s) VALUES (%d, %d)", TEST_CURRENT_TIME_SLOT.getStartTime(), 1));
      statement.execute(String.format("USE %s", TABLE_DATABASE_PREFIX));
      statement.execute(String.format("ALTER TABLE tb SET PROPERTIES TTL=%d", TEST_TTL_IN_MS));
    }

    TDataPartitionReq req = new TDataPartitionReq();
    req.putToPartitionSlotsMap(TABLE_DATABASE_PREFIX, new TreeMap<>());
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      awaitPartitionTableAutoCleaned(client, req);
    }
  }

  private static void awaitPartitionTableAutoCleaned(
      SyncConfigNodeIServiceClient client, TDataPartitionReq req) {
    // Keep the two-minute deadline and require real, successful results rather than an empty match.
    Awaitility.await("microsecond partition table auto cleanup")
        .atMost(120, TimeUnit.SECONDS)
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(AUTO_CLEAN_POLL_INTERVAL_IN_MS, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              TDataPartitionTableResp resp = client.getDataPartitionTable(req);
              return TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()
                  && resp.getDataPartitionTable()
                      .keySet()
                      .equals(req.getPartitionSlotsMap().keySet())
                  && resp.getDataPartitionTable().values().stream()
                      .allMatch(
                          seriesPartitions ->
                              !seriesPartitions.isEmpty()
                                  && seriesPartitions.values().stream()
                                      .allMatch(
                                          timePartitions ->
                                              timePartitions.size() == 1
                                                  && timePartitions.containsKey(
                                                      EXPECTED_CURRENT_TIME_SLOT)));
            });
  }
}
