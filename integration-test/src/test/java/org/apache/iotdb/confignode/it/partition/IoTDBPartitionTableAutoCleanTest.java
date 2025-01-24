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
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
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

import java.sql.Connection;
import java.sql.Statement;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionTableAutoCleanTest {

  private static final int TEST_REPLICATION_FACTOR = 1;
  private static final long TEST_TIME_PARTITION_INTERVAL = 604800000;
  private static final long TEST_TTL_CHECK_INTERVAL = 5_000;

  private static final TTimePartitionSlot TEST_CURRENT_TIME_SLOT =
      TimePartitionUtils.getCurrentTimePartitionSlot();
  private static final long TEST_TTL = 7 * TEST_TIME_PARTITION_INTERVAL;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setTimePartitionInterval(TEST_TIME_PARTITION_INTERVAL)
        .setTTLCheckInterval(TEST_TTL_CHECK_INTERVAL);

    // Init 1C1D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAutoCleanPartitionTable() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Create db1
      statement.execute("CREATE DATABASE root.db1");
      statement.execute("CREATE TIMESERIES root.db1.s WITH DATATYPE=INT64,ENCODING=PLAIN");
      // Insert expired data
      statement.execute(
          String.format(
              "INSERT INTO root.db1(timestamp, s) VALUES (%d, %d)",
              TEST_CURRENT_TIME_SLOT.getStartTime() - TEST_TTL * 2, -1));
      // Insert existed data
      statement.execute(
          String.format(
              "INSERT INTO root.db1(timestamp, s) VALUES (%d, %d)",
              TEST_CURRENT_TIME_SLOT.getStartTime(), 1));
      // Let db.TTL > device.TTL, the valid TTL should be the bigger one
      statement.execute("SET TTL TO root.db1 " + TEST_TTL);
      statement.execute("SET TTL TO root.db1.s " + 10);
      // Create db2
      statement.execute("CREATE DATABASE root.db2");
      statement.execute("CREATE TIMESERIES root.db2.s WITH DATATYPE=INT64,ENCODING=PLAIN");
      // Insert expired data
      statement.execute(
          String.format(
              "INSERT INTO root.db2(timestamp, s) VALUES (%d, %d)",
              TEST_CURRENT_TIME_SLOT.getStartTime() - TEST_TTL * 2, -1));
      // Insert existed data
      statement.execute(
          String.format(
              "INSERT INTO root.db2(timestamp, s) VALUES (%d, %d)",
              TEST_CURRENT_TIME_SLOT.getStartTime(), 1));
      // Let db.TTL < device.TTL, the valid TTL should be the bigger one
      statement.execute("SET TTL TO root.db2 " + 10);
      statement.execute("SET TTL TO root.db2.s " + TEST_TTL);
    }

    TDataPartitionReq req = new TDataPartitionReq();
    req.putToPartitionSlotsMap("root.db1", new TreeMap<>());
    req.putToPartitionSlotsMap("root.db2", new TreeMap<>());
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
