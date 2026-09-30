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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.NumericRow;
import org.apache.iotdb.isession.NumericRowPreparedStatement;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.TableSessionBuilder;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(MultiClusterIT2DualTableManualBasic.class)
public class IoTDBPipeNumericRowIT extends AbstractPipeTableModelDualManualIT {
  private static final String DATABASE = "numeric_sync";
  private static final List<String> NAMES =
      Arrays.asList("d", "device", "f", "l", "region", "i", "b", "event_date", "event_time");
  private static final List<TSDataType> TYPES =
      Arrays.asList(
          TSDataType.DOUBLE,
          TSDataType.STRING,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.STRING,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.DATE,
          TSDataType.TIMESTAMP);
  private static final List<ColumnCategory> CATEGORIES =
      Arrays.asList(
          ColumnCategory.FIELD,
          ColumnCategory.TAG,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.TAG,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD);

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataReplicationFactor(3)
        .setSchemaReplicationFactor(3);
    senderEnv
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    senderEnv.initClusterEnvironment(1, 3);
    receiverEnv.initClusterEnvironment(1, 1);
  }

  @Test
  public void testRealtimeWithoutBatch() throws Exception {
    // Unflushed NumericRow and prepared rows must arrive through individual InsertNode requests.
    testRealtime(false);
  }

  @Test
  public void testRealtimeWithBatch() throws Exception {
    // Batch transfer must preserve all numeric types, FIELD null bits, and null/empty STRING TAGs.
    testRealtime(true);
  }

  private void testRealtime(boolean batch) throws Exception {
    createTable();
    createPipe(batch, false);
    writeRows(0, 32);
    assertRowsEventually(receiverEnv, 0, 32);
    // The destination must remain readable after its own MemTable has been flushed.
    try (ITableSession session = receiverEnv.getTableSessionConnection()) {
      session.executeNonQueryStatement("FLUSH");
    }
    assertRowsEventually(receiverEnv, 0, 32);
  }

  @Test
  public void testHistoricalTsFile() throws Exception {
    // Start the pipe after flushing so the historical TsFile path also carries numeric-row data.
    createTable();
    writeRows(0, 32);
    try (ITableSession session = senderEnv.getTableSessionConnection()) {
      session.executeNonQueryStatement("FLUSH");
    }
    createPipe(true, true);
    assertRowsEventually(receiverEnv, 0, 32);
  }

  @Test
  public void testIoTConsensusReplicaAndRestart() throws Exception {
    // Pin writes to the actual DataRegion leader, then stop that leader before querying followers.
    // Merely stopping a client entry node can still leave all reads on the original leader.
    createTable();
    writeRows(0, 16);
    int leader = findDataRegionLeader();
    awaitReplication(leader, 16);
    int offlineFollower = (leader + 1) % 3;
    senderEnv.shutdownDataNode(offlineFollower);
    try {
      awaitNodeStatus(offlineFollower, NodeStatus.Unknown);
      writeRows(16, 32, leader);
    } finally {
      senderEnv.startDataNode(offlineFollower);
    }
    awaitNodeStatus(offlineFollower, NodeStatus.Running);
    // Running only confirms node health; IoTConsensus catch-up must finish before losing its
    // sender.
    awaitReplication(leader, 32);
    // Resolve the leader again after restart instead of assuming its assignment is unchanged.
    int stoppedLeader = findDataRegionLeader();
    assertEquals(
        "The primary that accepted the second write batch must be stopped", leader, stoppedLeader);
    senderEnv.shutdownDataNode(stoppedLeader);
    try {
      assertFalse(senderEnv.getDataNodeWrapper(stoppedLeader).isAlive());
      awaitNodeStatus(stoppedLeader, NodeStatus.Unknown);
      for (int follower = 0; follower < 3; follower++) {
        if (follower == stoppedLeader) {
          continue;
        }
        assertFalse(senderEnv.getDataNodeWrapper(stoppedLeader).isAlive());
        assertRowsEventually(senderEnv, follower, 32);
        assertFalse(senderEnv.getDataNodeWrapper(stoppedLeader).isAlive());
        System.out.printf(
            "NumericRow IoTConsensus: stopped primary port=%d, query follower port=%d, verified rows=32%n",
            senderEnv.getDataNodeWrapper(stoppedLeader).getPort(),
            senderEnv.getDataNodeWrapper(follower).getPort());
      }
    } finally {
      senderEnv.startDataNode(stoppedLeader);
    }
  }

  private void awaitReplication(int leader, int expectedWrites) {
    String metricsUrl =
        "http://"
            + senderEnv.getDataNodeWrapper(leader).getIp()
            + ":"
            + senderEnv.getDataNodeWrapper(leader).getMetricPort()
            + "/metrics";
    await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              String metrics = senderEnv.getUrlContent(metricsUrl, null);
              assertNotNull(metrics);
              // Require the latest write index too: an old zero-lag sample is not catch-up proof.
              // This database has exactly one DataRegion and each numeric row adds one write.
              double searchIndex = -1;
              double safeIndex = -1;
              for (String line : metrics.split("\\R")) {
                if (!line.startsWith("iot_consensus{")) {
                  continue;
                }
                double value = Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
                if (line.contains("type=\"searchIndex\"")) {
                  searchIndex = value;
                } else if (line.contains("type=\"safeIndex\"")) {
                  safeIndex = value;
                }
              }
              assertTrue(
                  "Missing or stale searchIndex at " + metricsUrl, searchIndex >= expectedWrites);
              assertEquals("Replication is incomplete at " + metricsUrl, searchIndex, safeIndex, 0);
            });
  }

  private void awaitNodeStatus(int node, NodeStatus expected) {
    // BaseEnv.ensureNodeStatus uses EnvFactory, which cannot be used with MultiCluster
    // environments.
    await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (SyncConfigNodeIServiceClient client =
                  (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
                TShowClusterResp response = client.showCluster();
                assertEquals(
                    TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
                List<TDataNodeLocation> matches =
                    response.getDataNodeList().stream()
                        .filter(
                            location ->
                                location.getClientRpcEndPoint().getPort()
                                    == senderEnv.getDataNodeWrapper(node).getPort())
                        .toList();
                assertEquals(1, matches.size());
                assertEquals(
                    expected.getStatus(),
                    response.getNodeStatus().get(matches.get(0).getDataNodeId()));
              }
            });
  }

  private int findDataRegionLeader() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TShowRegionResp response =
          client.showRegion(
              new TShowRegionReq()
                  .setConsensusGroupType(TConsensusGroupType.DataRegion)
                  .setDatabases(Collections.singletonList(DATABASE))
                  .setIsTableModel(true));
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      // Exactly one three-replica region ensures stopping this leader covers all inserted rows.
      List<TRegionInfo> replicas = response.getRegionInfoList();
      assertEquals(3, replicas.size());
      for (TRegionInfo replica : replicas) {
        assertEquals(replicas.get(0).getConsensusGroupId(), replica.getConsensusGroupId());
      }
      List<TRegionInfo> leaders =
          replicas.stream()
              .filter(replica -> RegionRoleType.Leader.getRoleType().equals(replica.getRoleType()))
              .toList();
      assertEquals(1, leaders.size());
      TRegionInfo leader = leaders.get(0);
      for (int node = 0; node < 3; node++) {
        if (senderEnv.getDataNodeWrapper(node).getPort() == leader.getClientRpcPort()) {
          System.out.printf(
              "NumericRow IoTConsensus: region=%d, primary DataNodeId=%d, port=%d%n",
              leader.getConsensusGroupId().getId(),
              leader.getDataNodeId(),
              leader.getClientRpcPort());
          return node;
        }
      }
      throw new AssertionError("No wrapper for DataRegion leader " + leader);
    }
  }

  private void createTable() throws Exception {
    try (ITableSession session = senderEnv.getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      session.executeNonQueryStatement("USE " + DATABASE);
      session.executeNonQueryStatement(
          "CREATE TABLE readings(region STRING TAG, device STRING TAG, missing STRING TAG, "
              + "d DOUBLE FIELD, f FLOAT FIELD, l INT64 FIELD, i INT32 FIELD, b BOOLEAN FIELD, "
              + "event_date DATE FIELD, event_time TIMESTAMP FIELD)");
    }
  }

  private void createPipe(boolean batch, boolean history) throws Exception {
    Map<String, String> source = new HashMap<>();
    source.put("capture.table", "true");
    source.put("capture.tree", "false");
    source.put("__system.sql-dialect", "table");
    source.put("user", "root");
    source.put("database-name", DATABASE);
    source.put("table-name", "readings");
    source.put("source.realtime.mode", "log");
    source.put("source.history.enable", Boolean.toString(history));
    Map<String, String> sink = new HashMap<>();
    sink.put("sink", "iotdb-thrift-sink");
    sink.put("sink.ip", receiverEnv.getDataNodeWrapper(0).getIp());
    sink.put("sink.port", Integer.toString(receiverEnv.getDataNodeWrapper(0).getPort()));
    sink.put("sink.batch.enable", Boolean.toString(batch));
    sink.put("sink.batch.max-delay-seconds", "1");
    sink.put("sink.format", history ? "tsfile" : "tablet");
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("numeric_pipe", sink)
                      .setExtractorAttributes(source)
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("numeric_pipe").getCode());
    }
  }

  private void writeRows(int start, int end) throws Exception {
    writeRows(start, end, 0);
  }

  private void writeRows(int start, int end, int node) throws Exception {
    // Pin the client to the selected endpoint; its index is not necessarily the DataRegion leader.
    try (ITableSession session =
            new TableSessionBuilder()
                .nodeUrls(
                    Collections.singletonList(
                        senderEnv.getDataNodeWrapper(node).getIpAndPortString()))
                .username(SessionConfig.DEFAULT_USER)
                .password(SessionConfig.DEFAULT_PASSWORD)
                .database(DATABASE)
                .enableRedirection(false)
                .build();
        NumericRowPreparedStatement prepared =
            session.prepareInsertNumericRow("readings", NAMES, TYPES, CATEGORIES)) {
      for (int row = start; row < end; row++) {
        long[] values = {
          Double.doubleToRawLongBits(row + 0.25),
          Float.floatToRawIntBits(-row - 0.5f),
          Long.MIN_VALUE + row,
          Integer.MIN_VALUE + row,
          row % 2,
          20260924,
          1000L + row
        };
        String[] tags = {device(row), "region"};
        BitMap nulls = new BitMap(7);
        if (row % 3 == 0) {
          nulls.mark(row % 7);
        }
        if (row % 2 == 0) {
          session.executePreparedInsertNumericRow(prepared, row, values, tags, nulls);
        } else {
          session.insert(
              new NumericRow("readings", row, NAMES, TYPES, CATEGORIES, values, tags, nulls));
        }
      }
    }
  }

  private static String device(int row) {
    return row % 3 == 0 ? null : row % 3 == 1 ? "" : "device-\u8bbe\u5907";
  }

  private void assertRowsEventually(BaseEnv env, int node, int count) {
    await()
        // Restart and leader failover can temporarily reject JDBC connections or query RPCs.
        .ignoreExceptionsInstanceOf(SQLException.class)
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (Connection connection =
                      env.getConnection(
                          env.getDataNodeWrapper(node),
                          SessionConfig.DEFAULT_USER,
                          SessionConfig.DEFAULT_PASSWORD,
                          "table");
                  Statement statement = connection.createStatement()) {
                statement.execute("USE " + DATABASE);
                try (ResultSet result =
                    statement.executeQuery(
                        "SELECT time,region,device,missing,d,f,l,i,b,event_date,event_time FROM readings ORDER BY time")) {
                  for (int row = 0; row < count; row++) {
                    assertTrue("Missing row " + row, result.next());
                    assertEquals(row, result.getLong(1));
                    assertEquals("region", result.getString(2));
                    assertEquals(device(row), result.getString(3));
                    assertNull(result.getObject(4));
                    for (int field = 0; field < 7; field++) {
                      int column = field + 5;
                      if (row % 3 == 0 && field == row % 7) {
                        assertNull(result.getObject(column));
                        continue;
                      }
                      assertNotNull(result.getObject(column));
                      switch (field) {
                        case 0 -> assertEquals(row + 0.25, result.getDouble(column), 0);
                        case 1 -> assertEquals(-row - 0.5f, result.getFloat(column), 0);
                        case 2 -> assertEquals(Long.MIN_VALUE + row, result.getLong(column));
                        case 3 -> assertEquals(Integer.MIN_VALUE + row, result.getInt(column));
                        case 4 -> assertEquals(row % 2 != 0, result.getBoolean(column));
                        case 5 ->
                            assertEquals(
                                LocalDate.of(2026, 9, 24), result.getDate(column).toLocalDate());
                        case 6 -> assertEquals(1000L + row, result.getLong(column));
                        default -> throw new AssertionError(field);
                      }
                    }
                  }
                  assertFalse(result.next());
                }
              }
            });
  }
}
