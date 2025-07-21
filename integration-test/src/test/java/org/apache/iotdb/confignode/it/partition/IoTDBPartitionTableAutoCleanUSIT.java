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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
  private static final long TEST_TTL_CHECK_INTERVAL = 5_000;

  private static final TTimePartitionSlot TEST_CURRENT_TIME_SLOT =
      new TTimePartitionSlot()
          .setStartTime(
              System.currentTimeMillis()
                  * 1000L
                  / TEST_TIME_PARTITION_INTERVAL_IN_MS
                  * TEST_TIME_PARTITION_INTERVAL_IN_MS);
  private static final long TEST_TTL_IN_MS = 7 * TEST_TIME_PARTITION_INTERVAL_IN_MS;

  @Before
  public void setUp() throws Exception {
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

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

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
      for (int retry = 0; retry < 120; retry++) {
        boolean partitionTableAutoCleaned = true;
        TDataPartitionTableResp resp = client.getDataPartitionTable(req);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
          partitionTableAutoCleaned =
              resp.getDataPartitionTable().entrySet().stream()
                  .flatMap(e1 -> e1.getValue().entrySet().stream())
                  .allMatch(e2 -> e2.getValue().size() == 1);
        }
        if (partitionTableAutoCleaned) {
          return;
        }
        TimeUnit.SECONDS.sleep(1);
      }
    }
    Assert.fail("The PartitionTable in the ConfigNode is not auto cleaned!");
  }

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
      for (int retry = 0; retry < 120; retry++) {
        boolean partitionTableAutoCleaned = true;
        TDataPartitionTableResp resp = client.getDataPartitionTable(req);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
          partitionTableAutoCleaned =
              resp.getDataPartitionTable().entrySet().stream()
                  .flatMap(e1 -> e1.getValue().entrySet().stream())
                  .allMatch(e2 -> e2.getValue().size() == 1);
        }
        if (partitionTableAutoCleaned) {
          return;
        }
        TimeUnit.SECONDS.sleep(1);
      }
    }
    Assert.fail("The PartitionTable in the ConfigNode is not auto cleaned!");
  }
}
