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

import org.apache.iotdb.calc.utils.constant.SqlConstant;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBTimePartitionIT {

  private static final String TEST_CONSENSUS_PROTOCOL_CLASS = ConsensusFactory.RATIS_CONSENSUS;
  private static final int TEST_REPLICATION_FACTOR = 3;
  private static final long TEST_TIME_PARTITION_ORIGIN = 1000L;
  private static final long TEST_TIME_PARTITION_INTERVAL = 3600000L;

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBTimePartitionIT.class);
  private static final String INSERTION_1 =
      "INSERT INTO root.sg1.d1(timestamp,speed,temperature) values(0, 1, 2)";
  private static final String INSERTION_2 =
      "INSERT INTO root.sg1.d1(timestamp,speed,temperature) values(1000, 1, 2)";
  private static final String INSERTION_3 =
      "INSERT INTO root.sg1.d1(timestamp,speed,temperature) values(3601000, 1, 2)";

  private List<Long> timestatmps = Arrays.asList(0L, 1000L, 3601000L);

  private static final String SHOW_TIME_PARTITION = "show timePartition where database = root.sg1";

  private static final TGetDatabaseReq showAllDatabasesReq;

  static {
    try {
      showAllDatabasesReq =
          new TGetDatabaseReq(
              Arrays.asList(
                  new ShowDatabaseStatement(new PartialPath(SqlConstant.getSingleRootArray()))
                      .getPathPattern()
                      .getNodes()),
              SchemaConstant.ALL_MATCH_SCOPE.serialize());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(TEST_CONSENSUS_PROTOCOL_CLASS)
        .setDataRegionConsensusProtocolClass(TEST_CONSENSUS_PROTOCOL_CLASS)
        .setSchemaReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setTimePartitionOrigin(TEST_TIME_PARTITION_ORIGIN)
        .setTimePartitionInterval(TEST_TIME_PARTITION_INTERVAL);
    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTimePartition() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // create three databases
      statement.execute(INSERTION_1);
      statement.execute(INSERTION_2);
      statement.execute(INSERTION_3);

      // check database info
      TShowDatabaseResp resp = client.showDatabase(showAllDatabasesReq);
      resp.databaseInfoMap.forEach(
          (k, v) -> {
            assertEquals(TEST_TIME_PARTITION_ORIGIN, v.getTimePartitionOrigin());
            assertEquals(TEST_TIME_PARTITION_INTERVAL, v.getTimePartitionInterval());
          });

      // check time partition
      ResultSet result = statement.executeQuery(SHOW_TIME_PARTITION);
      List<Long> timePartitions = new ArrayList<>();

      while (result.next()) {
        LOGGER.info(
            "timePartition: {}, startTime: {}",
            result.getLong(ColumnHeaderConstant.TIME_PARTITION),
            result.getString(ColumnHeaderConstant.START_TIME));
        timePartitions.add(result.getLong(ColumnHeaderConstant.TIME_PARTITION));
      }
      timestatmps.forEach(
          t -> {
            long timePartitionId = TimePartitionUtils.getTimePartitionId(t, "root.sg1");
            assertTrue(timePartitions.contains(timePartitionId));
          });
    }
  }

  @Test
  public void testDefaultTimePartitionConfigAfterClusterRestart() throws Exception {
    final String database = "root.default_time_partition";

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);

      final SQLException originException =
          assertThrows(
              SQLException.class,
              () ->
                  statement.execute(
                      "ALTER DATABASE " + database + " WITH TIME_PARTITION_ORIGIN=0"));
      assertTrue(
          originException.getMessage(),
          originException.getMessage().contains("Doesn't support ALTER TimePartitionOrigin yet."));

      final SQLException intervalException =
          assertThrows(
              SQLException.class,
              () ->
                  statement.execute(
                      "ALTER DATABASE " + database + " WITH TIME_PARTITION_INTERVAL=1"));
      assertTrue(
          intervalException.getMessage(),
          intervalException
              .getMessage()
              .contains("Doesn't support ALTER TimePartitionInterval yet."));
    }

    assertDefaultTimePartitionConfig(database);
    TestUtils.restartCluster(EnvFactory.getEnv());
    assertDefaultTimePartitionConfig(database);

    final long[] timestamps = {0L, 3601000L, 7201000L};
    for (int i = 0; i < timestamps.length; i++) {
      try (final Connection connection =
              EnvFactory.getEnv().getConnection(EnvFactory.getEnv().getDataNodeWrapper(i));
          final Statement statement = connection.createStatement()) {
        statement.execute(
            "INSERT INTO " + database + ".d1(timestamp, s1) values(" + timestamps[i] + ", 1)");
      }
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      final List<Long> timePartitions = new ArrayList<>();
      try (final ResultSet result =
          statement.executeQuery("SHOW TIMEPARTITION WHERE DATABASE = " + database)) {
        while (result.next()) {
          timePartitions.add(result.getLong(ColumnHeaderConstant.TIME_PARTITION));
        }
      }
      assertTrue(timePartitions.contains(-1L));
      assertTrue(timePartitions.contains(1L));
      assertTrue(timePartitions.contains(2L));
    }
  }

  private void assertDefaultTimePartitionConfig(final String database) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TShowDatabaseResp response = client.showDatabase(showAllDatabasesReq);
      assertTrue(response.databaseInfoMap.containsKey(database));
      assertEquals(
          TEST_TIME_PARTITION_ORIGIN,
          response.databaseInfoMap.get(database).getTimePartitionOrigin());
      assertEquals(
          TEST_TIME_PARTITION_INTERVAL,
          response.databaseInfoMap.get(database).getTimePartitionInterval());
    }
  }
}
