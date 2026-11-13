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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;
import org.apache.iotdb.itbase.runtime.NodeConnection;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.pipe.it.PipeTransferGate;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.gson.JsonParser;
import org.apache.thrift.transport.TTransportException;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tree and Table cases deliberately share the same two clusters and deterministic RPC gate. */
@RunWith(IoTDBTestRunner.class)
@Category(MultiClusterIT2DualTreeAutoEnhanced.class)
public class IoTDBPipeRuntimeRecoveryIT {
  private static BaseEnv sender;
  private static BaseEnv receiver;
  private static final int SOCKET_TIMEOUT_MS = 20000;
  private static final int QUERY_TIMEOUT_SECONDS = 15;

  @BeforeClass
  public static void setUp() {
    MultiEnvFactory.createEnv(2);
    sender = MultiEnvFactory.getEnv(0);
    receiver = MultiEnvFactory.getEnv(1);
    for (final BaseEnv env : Arrays.asList(sender, receiver)) {
      env.getConfig()
          .getCommonConfig()
          .setEnforceStrongPassword(false)
          .setAutoCreateSchemaEnabled(true)
          .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
          .setSchemaReplicationFactor(1)
          .setDataReplicationFactor(1)
          .setPipeMemoryManagementEnabled(true)
          .setIsPipeEnableMemoryCheck(false)
          .setPipeAutoSplitFullEnabled(false)
          .setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(1)
          .setDnConnectionTimeoutMs(10000);
      if (env == sender) {
        env.getConfig().getCommonConfig().setDefaultDataRegionGroupNumPerDatabase(2);
      }
      env.getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:9:0");
      env.initClusterEnvironment(1, 1);
    }
  }

  @AfterClass
  public static void tearDown() {
    try {
      if (sender != null) {
        sender.cleanClusterEnvironment();
      }
    } finally {
      if (receiver != null) {
        receiver.cleanClusterEnvironment();
      }
    }
  }

  @Test
  public void testStopStartWithUnacknowledgedBatchKeepsBacklogAndDropReleasesReceiver()
      throws Exception {
    // Unlike a status-switch test, the receiver has applied a batch whose ACK is still in flight.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("stop_ack", gate)) {
      f.create(gate, "", "");
      f.write(0, 1);
      f.assertRows(0, 1);
      final long generation = pipe(f.name).getCreationTime();
      try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
        f.write(1, 2);
        ack.awaitReached();
        f.assertRows(0, 2);
        f.write(2, 8);
        f.assertBacklog(0, 8, 0, 2);
        operateWhileHeld(
            "stop pipe " + f.name, ack, () -> assertEquals("STOPPED", pipe(f.name).getState()));
        assertEquals("STOPPED", pipe(f.name).getState());
        // STOP suspends the existing sink; its idle clients remain reusable until DROP.
        f.assertBacklog(0, 8, 0, 2);
        sql(sender, "start pipe " + f.name);
      }
      f.assertRows(0, 8);
      assertEquals(generation, pipe(f.name).getCreationTime());
      awaitDrained(f.name);
      f.assertSchema();
      sql(sender, "drop pipe " + f.name);
      awaitNoPipeResources(f.name);
    }
  }

  @Test
  public void testFailedAlterIsAtomicWithCommittedInflightAndPendingData() throws Exception {
    // Validation failure must preserve all three plugins while a real receiver ACK is outstanding.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("failed_alter", gate)) {
      f.create(gate, "", "");
      f.write(0, 1);
      f.assertRows(0, 1);
      final TShowPipeInfo before = pipe(f.name);
      try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
        f.write(1, 2);
        ack.awaitReached();
        f.write(2, 8);
        f.assertBacklog(0, 8, 0, 2);
        final SQLException validation =
            assertThrows(
                SQLException.class,
                () ->
                    sql(
                        sender,
                        "alter pipe "
                            + f.name
                            + " modify source ('source.path'='root.must_not_apply.**')"
                            + " replace processor ('processor'='p1_missing_processor')"
                            + " modify sink ('sink.batch.enable'='true')"));
        assertTrue(
            validation.getMessage(),
            validation.getMessage().toLowerCase(Locale.ROOT).contains("p1_missing_processor"));
        final TShowPipeInfo after = pipe(f.name);
        assertEquals(before.getCreationTime(), after.getCreationTime());
        assertEquals(before.getPipeExtractor(), after.getPipeExtractor());
        assertEquals(before.getPipeProcessor(), after.getPipeProcessor());
        assertEquals(before.getPipeConnector(), after.getPipeConnector());
      }
      f.assertRows(0, 8);
      awaitDrained(f.name);
      restartConfigNode();
      assertEquals(before.getPipeExtractor(), pipe(f.name).getPipeExtractor());
      assertEquals(before.getPipeProcessor(), pipe(f.name).getPipeProcessor());
      assertEquals(before.getPipeConnector(), pipe(f.name).getPipeConnector());
      f.assertRows(0, 8);
    }
  }

  @Test
  public void testSinkAlterAfterLostAckDrainsOriginalBacklogWithNewFormat() throws Exception {
    // A sink replacement may retry an applied batch; assert final values, not exactly-once RPCs.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("sink_alter", gate)) {
      f.create(gate, "", "");
      f.write(0, 1);
      f.assertRows(0, 1);
      final TShowPipeInfo before = pipe(f.name);
      try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
        f.write(1, 2);
        ack.awaitReached();
        f.write(2, 8);
        f.assertBacklog(0, 8, 0, 2);
        operateWhileHeld(
            "alter pipe "
                + f.name
                + " modify sink ('sink.batch.enable'='true', 'sink.batch.max-delay-seconds'='1',"
                + " 'sink.format'='tsfile')",
            ack,
            () -> assertTrue(pipe(f.name).getPipeConnector().contains("sink.format=tsfile")));
      }
      assertTrue(pipe(f.name).getPipeConnector().contains("sink.format=tsfile"));
      assertEquals(before.getPipeExtractor(), pipe(f.name).getPipeExtractor());
      assertEquals(before.getPipeProcessor(), pipe(f.name).getPipeProcessor());
      f.assertRows(0, 8);
      await().until(() -> gate.completedSeals() > 0);
      awaitDrained(f.name);
      f.assertSchema();
    }
  }

  @Test
  public void testDropWithLostAckReleasesResourcesAndSameNameStartsNewGeneration()
      throws Exception {
    // DROP must release a batch waiting for ACK; same-name realtime-only recreation skips old WAL.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("drop_ack", gate)) {
      f.create(gate, "", "");
      f.write(0, 1);
      f.assertRows(0, 1);
      final long oldGeneration = pipe(f.name).getCreationTime();
      try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
        f.write(1, 2);
        ack.awaitReached();
        f.assertRows(0, 2);
        f.write(2, 8);
        f.assertBacklog(0, 8, 0, 2);
        operateWhileHeld(
            "drop pipe " + f.name, ack, () -> assertEquals("PRE_DELETE", pipe(f.name).getState()));
      }
      awaitNoPipeResources(f.name);
      await().until(() -> gate.openConnections() == 0);
      f.write(10, 12);
      f.create(gate, ", 'source.history.enable'='false'", "");
      assertNotEquals(oldGeneration, pipe(f.name).getCreationTime());
      f.write(20, 24);
      f.assertRows(0, 2, 20, 24);
      awaitDrained(f.name);
      restartConfigNode();
      f.assertRows(0, 2, 20, 24);
    }
  }

  @Test
  public void testReceiverRoleRevocationRetriesPendingRowsAfterGrant() throws Exception {
    // Static permission tests miss revocation on a connector that has already authenticated.
    final String user = "p1_role_user";
    final String password = "P1-test-pass1234";
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("role_recovery", gate)) {
      sql(receiver, "create user " + user + " '" + password + "'");
      try (Cleanup cleanup =
          new Cleanup(
              () -> sql(sender, "drop pipe if exists " + f.name),
              () -> awaitNoPipeResources(f.name),
              () -> sql(receiver, "drop user " + user))) {
        sql(receiver, "grant WRITE on " + f.database + ".** to user " + user);
        sql(receiver, "grant SYSTEM on root.** to user " + user);
        f.create(gate, "", ", 'sink.user'='" + user + "', 'sink.password'='" + password + "'");
        f.write(0, 1);
        f.assertRows(0, 1);
        final long generation = pipe(f.name).getCreationTime();
        sql(receiver, "revoke WRITE on " + f.database + ".** from user " + user);
        final int noPermission = TSStatusCode.NO_PERMISSION.getStatusCode();
        final int rejectedBefore = gate.rejectedRequests(noPermission);
        f.write(1, 8);
        await().until(() -> gate.rejectedRequests(noPermission) > rejectedBefore);
        f.assertBacklog(0, 8, 0, 1);
        f.assertRows(0, 1);
        final TShowPipeInfo failed = pipe(f.name);
        assertFalse(failed.toString().contains(password));
        sql(receiver, "grant WRITE on " + f.database + ".** to user " + user);
        sql(receiver, "grant SYSTEM on root.** to user " + user);
        // Authorization errors may stop a Pipe; START resumes that same durable generation.
        if ("STOPPED".equals(pipe(f.name).getState())) {
          sql(sender, "start pipe " + f.name);
        }
        f.assertRows(0, 8);
        assertEquals(generation, pipe(f.name).getCreationTime());
        awaitDrained(f.name);
      }
    }
  }

  @Test
  public void testTsFileInterruptedBeforeSealRemainsInvisibleThenResumes() throws Exception {
    // Real pieces have reached disk, but no seal has been sent. A restart must not load the
    // residue.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("file_piece", gate)) {
      f.write(0, 16);
      sql(sender, "flush");
      try (PipeTransferGate.Hold seal = gate.holdNext(PipeTransferGate::isSeal, false)) {
        f.create(gate, ", 'source.realtime.mode'='file'", ", 'sink.format'='tsfile'");
        seal.awaitReached();
        assertTrue("No piece reached the real receiver", gate.completedPieces() > 0);
        assertFalse(
            "No incomplete file was staged on the receiver", stagingFiles(receiver).isEmpty());
        assertEquals(0, gate.completedSeals());
        f.assertRows();
        final DataNodeWrapper dn = receiver.getDataNodeWrapper(0);
        try (Cleanup restart =
            new Cleanup(
                () -> {
                  if (!dn.isAlive()) {
                    dn.start();
                  }
                })) {
          dn.stopForcibly();
          await().until(() -> !dn.isAlive());
          seal.disconnect();
          // Hold the retried seal too, so restart visibility is checked before a complete file
          // can be loaded by the retry. A fast reconnect must not erase this failure boundary.
          try (PipeTransferGate.Hold retry = gate.holdNext(PipeTransferGate::isSeal, false)) {
            dn.start();
            awaitDataNode(receiver, dn);
            retry.awaitReached();
            assertEquals(0, gate.completedSeals());
            f.assertRows();
          }
        }
      }
      f.assertRows(0, 16);
      f.assertSchema();
      awaitDrained(f.name);
    }
  }

  @Test
  public void testTsFileLostSealResponseRetriesWithoutChangingFinalValues() throws Exception {
    // The receiver has sealed and loaded the file; only its successful response is discarded.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("seal_ack", gate)) {
      f.write(0, 16);
      sql(sender, "flush");
      // This case also verifies receiver auto-creation from the sealed file's schema.
      sql(receiver, "drop timeseries " + f.device + ".*");
      try (PipeTransferGate.Hold seal = gate.holdNext(PipeTransferGate::isSeal, true)) {
        f.create(gate, "", ", 'sink.format'='tsfile'");
        seal.awaitReached();
        f.assertRows(0, 16);
        final int sealsBeforeRetry = gate.completedSeals();
        seal.disconnect();
        await().until(() -> gate.completedSeals() > sealsBeforeRetry);
      }
      f.assertRows(0, 16);
      awaitDrained(f.name);
      f.assertSchema();
    }
  }

  @Test
  public void testBlockedPipeDoesNotStarveHealthyPipeAndDropReclaimsOwnedMemory() throws Exception {
    // Both Pipes share sender tasks/receiver runtime; only one has an unacknowledged batch.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture blocked = new Fixture("blocked", gate);
        Fixture healthy = new Fixture("healthy", null)) {
      blocked.create(gate, "", "");
      healthy.create(null, "", "");
      blocked.write(0, 1);
      blocked.assertRows(0, 1);
      try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
        blocked.write(1, 2);
        ack.awaitReached();
        blocked.write(2, 8);
        blocked.assertBacklog(0, 8, 0, 2);
        await()
            .untilAsserted(
                () -> assertTrue(hasOwnedMemory(blocked.name, BaseEnv.TREE_SQL_DIALECT)));
        healthy.write(0, 8);
        healthy.assertRows(0, 8);
        awaitDrained(healthy.name);
        operateWhileHeld(
            "drop pipe " + blocked.name,
            ack,
            () -> assertEquals("PRE_DELETE", pipe(blocked.name).getState()));
      }
      awaitNoPipeResources(blocked.name);
      healthy.write(8, 12);
      healthy.assertRows(0, 12);
      awaitDrained(healthy.name);
    }
  }

  @Test
  public void testTreeSchemaConflictDoesNotBlockSubsequentOverwriteDeleteAndSchemaChanges()
      throws Exception {
    // Disable type conversion: a real schema conflict must be observable without silently
    // converting the sender's INT64 value into the receiver's BOOLEAN series.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("tree_schema", gate)) {
      sql(receiver, "create timeseries " + f.device + ".extra BOOLEAN");
      f.create(
          gate,
          ", 'source.inclusion'='data,schema'",
          ", 'sink.exception.data.convert-on-type-mismatch'='false'");
      f.write(0, 2);
      f.assertRows(0, 2);
      awaitDrained(f.name);
      final int ignoredConflict =
          TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode();
      final int rejectedSchemaBefore =
          gate.rejectedRequests(
              type -> type == PipeRequestType.TRANSFER_PLAN_NODE.getType(), ignoredConflict);
      final int rejectedDataBefore =
          gate.rejectedRequests(PipeTransferGate::isTablet, ignoredConflict);
      sql(sender, "create timeseries " + f.device + ".extra INT64 tags(stage=old)");
      await()
          .until(
              () ->
                  gate.rejectedRequests(
                          type -> type == PipeRequestType.TRANSFER_PLAN_NODE.getType(),
                          ignoredConflict)
                      > rejectedSchemaBefore);
      sql(sender, "insert into " + f.device + "(time,extra) values(3,303)");
      // Schema and data use independent sinks. Both requests must encounter the conflict before
      // deleting the receiver series; otherwise a delayed INSERT can auto-create it and let the
      // following ALTER succeed without exercising the missing-schema retry boundary.
      await()
          .until(
              () ->
                  gate.rejectedRequests(PipeTransferGate::isTablet, ignoredConflict)
                      > rejectedDataBefore);
      // A path-exists/type-mismatch response is an idempotent receiver conflict and the original
      // DDL/DML is intentionally not replayed after the receiver schema is deleted. A subsequent
      // alter of a missing series is a retryable user conflict, which gives us a deterministic
      // recovery boundary for the same Pipe generation.
      sql(receiver, "drop timeseries " + f.device + ".extra");
      final int retryableConflict =
          TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode();
      final int rejectedBeforeAlter = gate.rejectedRequests(retryableConflict);
      sql(
          sender,
          "alter timeseries "
              + f.device
              + ".extra upsert alias=renamed tags(stage=new) attributes(unit=ms)");
      await().until(() -> gate.rejectedRequests(retryableConflict) > rejectedBeforeAlter);

      // Repair the receiver with a compatible schema. The pending ALTER can now be retried on the
      // same durable Pipe generation; the original idempotent-conflict INSERT is sent again below.
      sql(receiver, "create timeseries " + f.device + ".extra INT64 tags(stage=repair)");
      if ("STOPPED".equals(pipe(f.name).getState())) {
        sql(sender, "start pipe " + f.name);
      }
      sql(sender, "insert into " + f.device + "(time,extra) values(3,303)");
      awaitRows(
          receiver,
          "select extra from " + f.device,
          new String[] {"Time", f.device + ".extra"},
          Collections.singletonList("3,303,"));
      sql(sender, "insert into " + f.device + "(time,v,flag) values(1,999,false)");
      sql(sender, "delete from " + f.device + ".v where time=0");
      awaitRows(
          receiver,
          "select v,flag from " + f.device + " where time<=1",
          new String[] {"Time", f.device + ".v", f.device + ".flag"},
          Arrays.asList("0,null,true,", "1,999,false,"));
      await()
          .untilAsserted(
              () -> {
                try (Connection c = connection(receiver);
                    Statement s = statement(c);
                    ResultSet r = s.executeQuery("show timeseries " + f.device + ".extra")) {
                  assertTrue(r.next());
                  assertEquals("INT64", r.getString("DataType"));
                  assertEquals("renamed", r.getString("Alias"));
                  assertNotNull(r.getString("Tags"));
                  assertNotNull(r.getString("Attributes"));
                  assertEquals(
                      JsonParser.parseString("{\"stage\":\"new\"}"),
                      JsonParser.parseString(r.getString("Tags")));
                  assertEquals(
                      JsonParser.parseString("{\"unit\":\"ms\"}"),
                      JsonParser.parseString(r.getString("Attributes")));
                  assertFalse(r.next());
                }
              });
      awaitDrained(f.name);
    }
  }

  @Test
  public void testTableSchemaChangesAfterHeldBatchPreserveColumnsAndFinalRows() throws Exception {
    // Table column lifecycle is distinct from Tree aliases; queued DDL follows a real in-flight
    // row.
    final String database = "p1_table_schema";
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("table_schema", gate);
        Cleanup cleanup =
            new Cleanup(
                () -> tableSql(sender, "drop pipe if exists " + f.name),
                () -> awaitNoPipeResources(f.name),
                () -> tableSql(sender, "drop database if exists " + database),
                () -> tableSql(receiver, "drop database if exists " + database))) {
      tableSql(sender, "create database " + database);
      tableSql(receiver, "create database " + database);
      tableSql(sender, database, "create table t(id STRING TAG, v INT64 FIELD)");
      tableSql(
          sender,
          database,
          "create pipe "
              + f.name
              + " with source ('source.database-name'='"
              + database
              + "', 'source.table-name'='t', 'source.inclusion'='data,schema', 'source.realtime.mode'='log')"
              + sink(gate, ""));
      tableSql(sender, database, "insert into t(time,id,v) values(0,'business0',10)");
      awaitTableRows(
          database, "time,id,v", Arrays.asList("1970-01-01T00:00:00.000Z,business0,10,"));
      try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
        tableSql(sender, database, "insert into t(time,id,v) values(1,'business1',11)");
        ack.awaitReached();
        tableSql(sender, database, "alter table t add column extra INT32 FIELD");
        tableSql(sender, database, "insert into t(time,id,v,extra) values(2,'business2',12,102)");
        awaitTableQuery(
            sender,
            database,
            "select time,id,v,extra from t order by time,id",
            new String[] {"time", "id", "v", "extra"},
            Arrays.asList(
                "1970-01-01T00:00:00.000Z,business0,10,null,",
                "1970-01-01T00:00:00.001Z,business1,11,null,",
                "1970-01-01T00:00:00.002Z,business2,12,102,"));
        awaitTableRows(
            database,
            "time,id,v",
            Arrays.asList(
                "1970-01-01T00:00:00.000Z,business0,10,",
                "1970-01-01T00:00:00.001Z,business1,11,"));
      }
      awaitTableQuery(
          database,
          "desc t",
          new String[] {"ColumnName", "DataType", "Category"},
          Arrays.asList(
              "time,TIMESTAMP,TIME,", "id,STRING,TAG,", "v,INT64,FIELD,", "extra,INT32,FIELD,"));
      awaitTableRows(
          database,
          "time,id,v,extra",
          Arrays.asList(
              "1970-01-01T00:00:00.000Z,business0,10,null,",
              "1970-01-01T00:00:00.001Z,business1,11,null,",
              "1970-01-01T00:00:00.002Z,business2,12,102,"));
      tableSql(sender, database, "alter table t drop column extra");
      tableSql(sender, database, "insert into t(time,id,v) values(1,'business1',999)");
      tableSql(sender, database, "delete from t where time=0");
      awaitTableRows(
          database,
          "time,id,v",
          Arrays.asList(
              "1970-01-01T00:00:00.001Z,business1,999,", "1970-01-01T00:00:00.002Z,business2,12,"));
      awaitTableQuery(
          database,
          "desc t",
          new String[] {"ColumnName", "DataType", "Category"},
          Arrays.asList("time,TIMESTAMP,TIME,", "id,STRING,TAG,", "v,INT64,FIELD,"));
      awaitDrained(f.name, BaseEnv.TABLE_SQL_DIALECT);
    }
  }

  @Test
  public void testRegionRedistributionAfterScaleOutPreservesInflightAndPendingRows()
      throws Exception {
    // Registration alone is insufficient: prove this database's populated Region changes owner.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("region_move", gate)) {
      f.create(gate, "", "");
      f.write(0, 1);
      f.assertRows(0, 1);
      final Map<Integer, Integer> businessRegion = dataRegionOwners(f.database);
      assertFalse("The business device must have an assigned DataRegion", businessRegion.isEmpty());
      final int region = businessRegion.keySet().iterator().next();
      final int oldOwner = businessRegion.get(region);
      final List<String> regionDevices = new ArrayList<>();
      for (int i = 0; i < 32 && dataRegionOwners(f.database).size() < 2; i++) {
        final String device = f.database + ".region_" + i;
        regionDevices.add(device);
        sql(sender, "create timeseries " + device + ".v INT64 encoding=PLAIN");
        sql(
            sender,
            "insert into " + device + "(time,v) values(" + (100 + i) + "," + (17000 + i) + ")");
      }
      final Map<Integer, Integer> before = dataRegionOwners(f.database);
      assertTrue("The fixture must populate at least two DataRegions", before.size() >= 2);
      for (int i = 0; i < regionDevices.size(); i++) {
        awaitRows(
            receiver,
            "select v from " + regionDevices.get(i),
            new String[] {"Time", regionDevices.get(i) + ".v"},
            Collections.singletonList((100 + i) + "," + (17000 + i) + ","));
      }
      try (TemporaryDataNode added = new TemporaryDataNode(before, f.database)) {
        added.register();
        final int newOwner = added.getNodeId();
        assertNotEquals(oldOwner, newOwner);
        try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
          f.write(1, 2);
          ack.awaitReached();
          f.write(2, 8);
          f.assertBacklog(0, 8, 0, 2);
          for (int i = 0; i < regionDevices.size(); i++) {
            sql(
                sender,
                "insert into "
                    + regionDevices.get(i)
                    + "(time,v) values("
                    + (1000 + i)
                    + ","
                    + (27000 + i)
                    + ")");
          }
          sql(sender, "migrate region " + region + " from " + oldOwner + " to " + newOwner);
          await()
              .untilAsserted(
                  () ->
                      assertEquals(
                          Integer.valueOf(newOwner), dataRegionOwners(f.database).get(region)));
          f.write(8, 12);
          for (int i = 0; i < regionDevices.size(); i++) {
            sql(
                sender,
                "insert into "
                    + regionDevices.get(i)
                    + "(time,v) values("
                    + (2000 + i)
                    + ","
                    + (37000 + i)
                    + ")");
          }
          ack.disconnect();
        }
        f.assertRows(0, 12);
        f.assertRows(sender, 0, 12);
        for (int i = 0; i < regionDevices.size(); i++) {
          for (final BaseEnv env : Arrays.asList(sender, receiver)) {
            awaitRows(
                env,
                "select v from " + regionDevices.get(i),
                new String[] {"Time", regionDevices.get(i) + ".v"},
                Arrays.asList(
                    (100 + i) + "," + (17000 + i) + ",",
                    (1000 + i) + "," + (27000 + i) + ",",
                    (2000 + i) + "," + (37000 + i) + ","));
          }
        }
        final Map<Integer, Integer> after = dataRegionOwners(f.database);
        assertEquals(before.keySet(), after.keySet());
        for (final Map.Entry<Integer, Integer> entry : before.entrySet()) {
          final int expectedOwner = entry.getKey() == region ? newOwner : entry.getValue();
          assertEquals(Integer.valueOf(expectedOwner), after.get(entry.getKey()));
          awaitRegionOwnerRunning(f.database, entry.getKey(), expectedOwner);
        }
        awaitRows(
            receiver,
            "select v,flag from " + f.device + " where time=8",
            new String[] {"Time", f.device + ".v", f.device + ".flag"},
            Collections.singletonList("8,1136,true,"));
        awaitDrained(f.name);
        f.assertSchema();
        awaitRegionOwnerRunning(f.database, region, newOwner);
      }
    }
  }

  @Test
  public void testAggregateProcessorRestartAfterLostAckRestoresOpenWindowExactly()
      throws Exception {
    // Closing one window persists the next open window. Crash with the second output
    // unacknowledged.
    final String output = "root.p1_aggregate_output";
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("aggregate_restart", gate);
        Cleanup cleanup =
            new Cleanup(
                () -> sql(sender, "drop pipe if exists " + f.name),
                () -> awaitNoPipeResources(f.name),
                () -> sql(receiver, "delete database " + output))) {
      sql(receiver, "create database " + output);
      sql(receiver, "create timeseries " + output + ".business.v.mean DOUBLE encoding=PLAIN");
      sql(
          sender,
          "create pipe "
              + f.name
              + " with source ('source.path'='"
              + f.device
              + ".v', 'source.realtime.mode'='log')"
              + " with processor ('processor'='aggregate-processor', 'processor.operators'='avg',"
              + " 'processor.output.measurements'='mean', 'processor.sliding.seconds'='10',"
              + " 'processor.output.min-report-interval-seconds'='0', 'processor.output.database'='"
              + output
              + "')"
              + sink(gate, ""));
      final String insert = "insert into " + f.device + "(time,v) values";
      // Keep the boundary-crossing row in its own source event so the serialized open window
      // definitely contains the value at 10001 before the lost-ACK batch is injected.
      sql(sender, insert + "(0,2),(1000,4)");
      sql(sender, insert + "(10001,10)");
      final String series = output + ".business.v.mean";
      awaitRows(
          receiver,
          "select mean from " + output + ".business.v where time<10000",
          new String[] {"Time", series},
          Collections.singletonList("0,3.0,"));
      awaitDrained(f.name);
      // The aggregate processor emits the 10000 window only when the 20000 boundary is seen.
      // The same event also serializes the next open window with its value at 20000.
      try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
        sql(sender, insert + "(20000,30)");
        ack.awaitReached();
        awaitRows(
            receiver,
            "select mean from " + output + ".business.v where time<20000",
            new String[] {"Time", series},
            Arrays.asList("0,3.0,", "10000,10.0,"));
        restartDataNode(sender, ack);
      }
      awaitRows(
          receiver,
          "select mean from " + output + ".business.v where time<20000",
          new String[] {"Time", series},
          Arrays.asList("0,3.0,", "10000,10.0,"));
      if ("STOPPED".equals(pipe(f.name).getState())) {
        sql(sender, "start pipe " + f.name);
      }
      awaitDrained(f.name);
      // The new value differs from the restored one: losing or merging the restored open
      // window twice changes 35.0 to 40.0 or 33.333..., even if the old output is overwritten.
      sql(sender, insert + "(21000,40),(30001,50)");
      awaitRows(
          receiver,
          "select mean from " + output + ".business.v where time<=20000",
          new String[] {"Time", series},
          Arrays.asList("0,3.0,", "10000,10.0,", "20000,35.0,"));
      awaitRows(
          sender,
          "select v from " + f.device,
          new String[] {"Time", f.device + ".v"},
          Arrays.asList("0,2,", "1000,4,", "10001,10,", "20000,30,", "21000,40,", "30001,50,"));
      awaitRows(
          receiver,
          "select mean from " + output + ".business.v",
          new String[] {"Time", series},
          Arrays.asList("0,3.0,", "10000,10.0,", "20000,35.0,"));
      awaitDrained(f.name);
    }
  }

  @Test
  public void testForwardingDisabledBreaksReturnLoopWhilePeerPipeIsStoppedAndRecovered()
      throws Exception {
    // A return link receives local writes but excludes replicated events, including after restart.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        PipeTransferGate returnGate = new PipeTransferGate(sender.getDataNodeWrapper(0));
        Fixture f = new Fixture("forward_loop", gate)) {
      final String returning = f.name + "_return";
      final int returnConnectionsBefore = receiverConnections(sender);
      try (Cleanup cleanup =
          new Cleanup(
              () -> sql(receiver, "drop pipe if exists " + returning),
              () -> awaitNoPipeResources(receiver, sender, returning, BaseEnv.TREE_SQL_DIALECT),
              () ->
                  await()
                      .untilAsserted(
                          () -> {
                            assertEquals(0, returnGate.openConnections());
                            assertTrue(receiverConnections(sender) <= returnConnectionsBefore);
                          }))) {
        f.create(gate, ", 'source.forwarding-pipe-requests'='false'", "");
        sql(
            receiver,
            "create pipe "
                + returning
                + " with source ('source.path'='"
                + f.database
                + ".**', 'source.forwarding-pipe-requests'='false', 'source.realtime.mode'='log')"
                + sink(sender, returnGate, ""));
        final int reverseBefore = returnGate.completedDataRequests();
        f.write(0, 2);
        f.assertRows(0, 2);
        awaitDrained(f.name);
        awaitDrained(receiver, returning, BaseEnv.TREE_SQL_DIALECT);
        assertQuietRequests(returnGate, reverseBefore);
        // Snapshot before the fault: a leaked replay during START must not escape observation.
        final int forwardBeforeStop = gate.completedDataRequests();
        sql(sender, "stop pipe " + f.name);
        sql(receiver, "insert into " + f.device + "(time,v,flag) values(10,1170,true)");
        awaitRows(
            sender,
            "select v,flag from " + f.device,
            new String[] {"Time", f.device + ".v", f.device + ".flag"},
            Arrays.asList("0,1000,true,", "1,1017,false,", "10,1170,true,"));
        awaitDrained(receiver, returning, BaseEnv.TREE_SQL_DIALECT);
        assertTrue(returnGate.completedDataRequests() > reverseBefore);
        final int reverseAfterLocalWrite = returnGate.completedDataRequests();
        sql(sender, "start pipe " + f.name);
        awaitDrained(f.name);
        awaitDrained(receiver, returning, BaseEnv.TREE_SQL_DIALECT);
        assertQuietRequests(gate, forwardBeforeStop);
        assertQuietRequests(returnGate, reverseAfterLocalWrite);
        f.write(2, 4);
        f.assertRows(0, 4, 10, 11);
        f.assertRows(sender, 0, 4, 10, 11);
        awaitDrained(f.name);
        awaitDrained(receiver, returning, BaseEnv.TREE_SQL_DIALECT);
        assertQuietRequests(returnGate, reverseAfterLocalWrite);
        f.assertSchema();
      }
    }
  }

  private static void assertQuietRequests(final PipeTransferGate gate, final int expected) {
    await()
        .during(3, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertEquals(
                    "Replicated events were forwarded again",
                    expected,
                    gate.completedDataRequests()));
  }

  @Test
  public void testDropProcedureSurvivesConfigNodeRestartWithRealTransferOutstanding()
      throws Exception {
    // Restart the ConfigNode while DROP is persisted as PRE_DELETE and its DataNode operation is
    // waiting for a real in-flight transfer to release.
    try (PipeTransferGate gate = new PipeTransferGate(receiver.getDataNodeWrapper(0));
        Fixture f = new Fixture("procedure_replay", gate)) {
      f.create(gate, "", "");
      f.write(0, 1);
      f.assertRows(0, 1);
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      try (Cleanup cleanup =
          new Cleanup(
              () -> {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
              })) {
        try (PipeTransferGate.Hold ack = gate.holdNext(PipeTransferGate::isTablet, true)) {
          f.write(1, 2);
          ack.awaitReached();
          f.write(2, 8);
          f.assertBacklog(0, 8, 0, 2);
          final Future<?> drop =
              executor.submit(
                  () -> {
                    sql(sender, "drop pipe " + f.name);
                    return null;
                  });
          await().untilAsserted(() -> assertEquals("PRE_DELETE", pipe(f.name).getState()));
          restartConfigNode();
          assertEquals("PRE_DELETE", pipe(f.name).getState());
          ack.disconnect();
          try {
            drop.get(60, TimeUnit.SECONDS);
          } catch (final java.util.concurrent.ExecutionException e) {
            // Only a transport failure can be explained by losing the response during restart.
            // Product/procedure errors must fail this test even if background cleanup later wins.
            assertTrue("Unexpected DROP failure: " + e.getCause(), causedByTransport(e));
          }
        }
        awaitNoPipeResources(f.name);
        f.assertRows(0, 2);
      }
    }
  }

  private static boolean causedByTransport(final Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof TTransportException) {
        return true;
      }
    }
    return false;
  }

  private static final class TemporaryDataNode implements AutoCloseable {
    private final int originalDataNodeCount = sender.getDataNodeWrapperList().size();
    private final Map<Integer, Integer> originalRegionOwners;
    private final String database;
    private DataNodeWrapper dataNode;
    private TDataNodeLocation location;

    private TemporaryDataNode(
        final Map<Integer, Integer> originalRegionOwners, final String database) {
      this.originalRegionOwners = originalRegionOwners;
      this.database = database;
    }

    private void register() throws Exception {
      dataNode = sender.generateRandomDataNodeWrapper();
      sender.registerNewDataNode(dataNode, true);
      location = findLocation();
      assertNotNull("The newly registered DataNode is missing from SHOW CLUSTER", location);
    }

    private int getNodeId() {
      assertNotNull("The temporary DataNode has not been registered", location);
      return location.getDataNodeId();
    }

    private TDataNodeLocation findLocation() throws Exception {
      if (sender.getDataNodeWrapperList().size() <= originalDataNodeCount) {
        return null;
      }
      dataNode = sender.getDataNodeWrapper(originalDataNodeCount);
      try (SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) sender.getLeaderConfigNodeConnection()) {
        return client.showCluster().getDataNodeList().stream()
            .filter(n -> n.getClientRpcEndPoint().getPort() == dataNode.getPort())
            .findFirst()
            .orElse(null);
      }
    }

    @Override
    public void close() throws Exception {
      if (dataNode == null && sender.getDataNodeWrapperList().size() > originalDataNodeCount) {
        dataNode = sender.getDataNodeWrapper(originalDataNodeCount);
      }
      if (dataNode == null) {
        return;
      }

      new Cleanup(
              this::restoreRegions,
              this::removeFromCluster,
              () -> dataNode.stopForcibly(),
              () -> await().until(() -> !dataNode.isAlive()),
              () -> {
                assertFalse("The temporary DataNode is still alive", dataNode.isAlive());
                dataNode.destroyDir();
              },
              () -> {
                if (!dataNode.isAlive()) {
                  Files.deleteIfExists(Paths.get(EnvUtils.getLockFilePath(dataNode.getPort())));
                  sender.getDataNodeWrapperList().remove(dataNode);
                }
              })
          .close();
    }

    private void restoreRegions() throws Exception {
      if (location == null) {
        location = findLocation();
      }
      if (location == null) {
        return; // Startup failed before registration; physical resources still need cleanup.
      }
      final int nodeId = location.getDataNodeId();
      final int fallbackOwner = originalRegionOwners.values().iterator().next();
      for (final Map.Entry<Integer, Integer> entry : dataRegionOwners(database).entrySet()) {
        if (entry.getValue() == nodeId) {
          final int targetOwner = originalRegionOwners.getOrDefault(entry.getKey(), fallbackOwner);
          sql(
              sender,
              "migrate region " + entry.getKey() + " from " + nodeId + " to " + targetOwner);
          await()
              .untilAsserted(
                  () ->
                      assertEquals(
                          Integer.valueOf(targetOwner),
                          dataRegionOwners(database).get(entry.getKey())));
          awaitRegionOwnerRunning(database, entry.getKey(), targetOwner);
        }
      }
    }

    private void removeFromCluster() throws Exception {
      if (location == null) {
        location = findLocation();
      }
      if (location == null) {
        return;
      }
      final int nodeId = location.getDataNodeId();
      try (SyncConfigNodeIServiceClient client =
          (SyncConfigNodeIServiceClient) sender.getLeaderConfigNodeConnection()) {
        final org.apache.iotdb.common.rpc.thrift.TSStatus status =
            client
                .removeDataNode(new TDataNodeRemoveReq(Collections.singletonList(location)))
                .getStatus();
        assertEquals(
            status.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
      await()
          .untilAsserted(
              () -> {
                try (SyncConfigNodeIServiceClient client =
                    (SyncConfigNodeIServiceClient) sender.getLeaderConfigNodeConnection()) {
                  assertTrue(
                      client.showCluster().getDataNodeList().stream()
                          .noneMatch(n -> n.getDataNodeId() == nodeId));
                }
                assertTrue(
                    dataRegionOwners(database).values().stream().noneMatch(id -> id == nodeId));
              });
    }
  }

  private static Map<Integer, Integer> dataRegionOwners(final String database) throws Exception {
    final Map<Integer, Integer> owners = new HashMap<>();
    // The scale-out fixture may have a temporary DataNode in the environment list while it is
    // being removed. Query through the original DataNode so an intentionally stopped temporary
    // endpoint cannot make this metadata assertion fail.
    try (Connection c = connection(sender.getDataNodeWrapper(0), BaseEnv.TREE_SQL_DIALECT);
        Statement s = statement(c);
        ResultSet r = s.executeQuery("show regions")) {
      while (r.next()) {
        if (database.equals(r.getString("Database")) && "DataRegion".equals(r.getString("Type"))) {
          if (r.getLong("TimeSlotNum") > 0) {
            owners.put(r.getInt("RegionId"), r.getInt("DataNodeId"));
          }
        }
      }
    }
    return owners;
  }

  private static void awaitRegionOwnerRunning(
      final String database, final int regionId, final int expectedOwner) {
    await()
        .untilAsserted(
            () -> {
              boolean found = false;
              try (Connection c =
                      connection(sender.getDataNodeWrapper(0), BaseEnv.TREE_SQL_DIALECT);
                  Statement s = statement(c);
                  ResultSet r = s.executeQuery("show regions")) {
                while (r.next()) {
                  if (database.equals(r.getString("Database"))
                      && "DataRegion".equals(r.getString("Type"))
                      && r.getInt("RegionId") == regionId
                      && r.getLong("TimeSlotNum") > 0) {
                    found = true;
                    assertEquals(expectedOwner, r.getInt("DataNodeId"));
                    assertEquals("Running", r.getString("Status"));
                  }
                }
              }
              assertTrue("Region " + regionId + " was not visible", found);
            });
  }

  private static void operateWhileHeld(
      final String operation,
      final PipeTransferGate.Hold held,
      final ThrowingRunnable persistedState)
      throws Exception {
    // Closing a sync sink waits for its transfer lock. Observe the ConfigNode-side transition
    // before
    // discarding the ACK, then require the original caller to complete successfully.
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<?> result =
        executor.submit(
            () -> {
              sql(sender, operation);
              return null;
            });
    try (Cleanup cleanup =
        new Cleanup(
            held::close,
            () -> {
              executor.shutdownNow();
              assertTrue(
                  "Pipe operation executor did not terminate",
                  executor.awaitTermination(10, TimeUnit.SECONDS));
            })) {
      await().untilAsserted(persistedState);
      held.disconnect();
      result.get(60, TimeUnit.SECONDS);
    }
  }

  private static org.awaitility.core.ConditionFactory await() {
    return Awaitility.await()
        .pollInSameThread()
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(90, TimeUnit.SECONDS);
  }

  private static TShowPipeInfo pipe(final String name) throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) sender.getLeaderConfigNodeConnection()) {
      final TShowPipeResp result =
          client.showPipe(
              new TShowPipeReq()
                  .setPipeName(name)
                  .setWhereClause(true)
                  .setIsTableModel("p1_table_schema".equals(name))
                  .setUserName(SessionConfig.DEFAULT_USER));
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getStatus().getCode());
      assertEquals("Expected one generation of " + name, 1, result.getPipeInfoListSize());
      return result.getPipeInfoList().get(0);
    }
  }

  private static void awaitDrained(final String name) {
    awaitDrained(name, BaseEnv.TREE_SQL_DIALECT);
  }

  private static void awaitDrained(final String name, final String dialect) {
    awaitDrained(sender, name, dialect);
  }

  private static void awaitDrained(final BaseEnv env, final String name, final String dialect) {
    await()
        .during(3, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              boolean found = false;
              for (final DataNodeWrapper dataNode : env.getDataNodeWrapperList()) {
                try (Connection connection = connection(dataNode, dialect);
                    Statement statement = statement(connection);
                    ResultSet result = statement.executeQuery("show pipes")) {
                  while (result.next()) {
                    if (name.equals(result.getString("ID"))) {
                      found = true;
                      assertEquals("RUNNING", result.getString("State"));
                      assertEquals("0", result.getString("RemainingEventCount"));
                    }
                  }
                }
              }
              assertTrue("Pipe " + name + " was not visible on any sender DataNode", found);
            });
  }

  private static void awaitNoReceiver(final BaseEnv env, final String name) {
    await()
        .untilAsserted(
            () -> {
              try (Connection c = connection(env);
                  Statement s = statement(c);
                  ResultSet r = s.executeQuery("show receivers")) {
                while (r.next()) {
                  final String ids = r.getString("PipeIDs");
                  assertTrue(ids, ids == null || !ids.contains(name + "@"));
                }
              }
            });
  }

  private static boolean hasOwnedMemory(final String name, final String dialect) throws Exception {
    return hasOwnedMemory(sender, name, dialect);
  }

  private static boolean hasOwnedMemory(final BaseEnv env, final String name, final String dialect)
      throws Exception {
    boolean found = false;
    for (final DataNodeWrapper dn : env.getDataNodeWrapperList()) {
      try (Connection c = connection(dn, dialect);
          Statement s = statement(c);
          ResultSet r = s.executeQuery("show pipe memory")) {
        while (r.next()) {
          found |= name.equals(r.getString("pipe_name"));
        }
      }
    }
    return found;
  }

  private static void awaitNoPipeResources(final String name) {
    awaitNoPipeResources(
        sender,
        receiver,
        name,
        "p1_table_schema".equals(name) ? BaseEnv.TABLE_SQL_DIALECT : BaseEnv.TREE_SQL_DIALECT);
  }

  private static void awaitNoPipeResources(
      final BaseEnv source, final BaseEnv target, final String name, final String dialect) {
    awaitNoReceiver(target, name);
    await()
        .untilAsserted(
            () -> {
              try (SyncConfigNodeIServiceClient client =
                  (SyncConfigNodeIServiceClient) source.getLeaderConfigNodeConnection()) {
                final TShowPipeResp result =
                    client.showPipe(
                        new TShowPipeReq()
                            .setUserName(SessionConfig.DEFAULT_USER)
                            .setIsTableModel(BaseEnv.TABLE_SQL_DIALECT.equals(dialect)));
                assertEquals(
                    TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getStatus().getCode());
                assertTrue(
                    result.getPipeInfoList().stream().noneMatch(p -> name.equals(p.getId())));
              }
              assertFalse(
                  "Owned queue/processor/sink memory remains for " + name,
                  hasOwnedMemory(source, name, dialect));
            });
  }

  private static void restartConfigNode() throws Exception {
    final ConfigNodeWrapper cn = sender.getConfigNodeWrapper(0);
    cn.stopForcibly();
    cn.start();
    await()
        .untilAsserted(
            () -> {
              try (SyncConfigNodeIServiceClient ignored =
                  (SyncConfigNodeIServiceClient) sender.getLeaderConfigNodeConnection()) {
                assertNotNull(ignored);
              }
            });
  }

  private static void restartDataNode(final BaseEnv env, final PipeTransferGate.Hold outstanding)
      throws Exception {
    final DataNodeWrapper dn = env.getDataNodeWrapper(0);
    try {
      dn.stopForcibly();
      await().until(() -> !dn.isAlive());
      if (outstanding != null) {
        outstanding.disconnect();
      }
    } finally {
      if (!dn.isAlive()) {
        dn.start();
      }
    }
    awaitDataNode(env, dn);
  }

  private static void awaitDataNode(final BaseEnv env, final DataNodeWrapper dn) {
    env.ensureNodeStatus(
        Collections.singletonList(dn), Collections.singletonList(NodeStatus.Running));
    await()
        .until(
            () -> {
              try (Connection ignored = connection(dn, BaseEnv.TREE_SQL_DIALECT)) {
                return true;
              } catch (SQLException e) {
                return false;
              }
            });
  }

  private static void sql(final BaseEnv env, final String sql) throws SQLException {
    execute(env, BaseEnv.TREE_SQL_DIALECT, sql);
  }

  private static void tableSql(final BaseEnv env, final String sql) throws SQLException {
    execute(env, BaseEnv.TABLE_SQL_DIALECT, sql);
  }

  private static void tableSql(final BaseEnv env, final String database, final String sql)
      throws SQLException {
    try (Connection connection = connection(env, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = statement(connection)) {
      statement.execute("use " + database);
      statement.execute(sql);
    }
  }

  private static void execute(final BaseEnv env, final String dialect, final String sql)
      throws SQLException {
    try (Connection connection = connection(env, dialect);
        Statement statement = statement(connection)) {
      statement.execute(sql);
    }
  }

  private static Connection connection(final BaseEnv env) throws SQLException {
    return connection(env, BaseEnv.TREE_SQL_DIALECT);
  }

  private static Connection connection(final DataNodeWrapper dn, final String dialect)
      throws SQLException {
    // Env connections use network_timeout=0. A query deadline cannot interrupt a blocked socket
    // (including openSession/closeSession), so give every fresh connection its own finite timeout.
    final Properties properties =
        BaseEnv.constructProperties(
            SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, dialect);
    properties.setProperty(Config.NETWORK_TIMEOUT, Integer.toString(SOCKET_TIMEOUT_MS));
    properties.setProperty(Config.TIME_ZONE, "+00:00");
    return DriverManager.getConnection("jdbc:iotdb://" + dn.getIpAndPortString() + "/", properties);
  }

  private static Connection connection(final BaseEnv env, final String dialect)
      throws SQLException {
    final List<NodeConnection> reads = new ArrayList<>();
    final DataNodeWrapper writer = env.getDataNodeWrapper(0);
    final Connection write = connection(writer, dialect);
    try {
      for (final DataNodeWrapper dn : env.getDataNodeWrapperList()) {
        reads.add(
            new NodeConnection(
                dn.getIpAndPortString(),
                NodeConnection.NodeRole.DATA_NODE,
                NodeConnection.ConnectionRole.READ,
                connection(dn, dialect)));
      }
      // Retain the standard IT wrapper and unordered assertion helper: every DataNode is checked.
      return new ClusterTestConnection(
          new NodeConnection(
              writer.getIpAndPortString(),
              NodeConnection.NodeRole.DATA_NODE,
              NodeConnection.ConnectionRole.WRITE,
              write),
          reads,
          (AbstractEnv) env);
    } catch (final SQLException | RuntimeException | Error e) {
      try {
        final List<ThrowingRunnable> close = new ArrayList<>();
        close.add(write::close);
        reads.forEach(n -> close.add(() -> n.getUnderlyingConnection().close()));
        new Cleanup(close.toArray(new ThrowingRunnable[0])).close();
      } catch (final Exception | Error cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  private static Statement statement(final Connection connection) throws SQLException {
    final Statement statement = connection.createStatement();
    try {
      statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
      return statement;
    } catch (final SQLException e) {
      try {
        statement.close();
      } catch (final SQLException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  private static int receiverConnections(final BaseEnv env) throws SQLException {
    int count = 0;
    for (final DataNodeWrapper dn : env.getDataNodeWrapperList()) {
      try (Connection c = connection(dn, BaseEnv.TREE_SQL_DIALECT);
          Statement s = statement(c);
          ResultSet r = s.executeQuery("show receivers")) {
        while (r.next()) {
          count += r.getInt("ConnectionCount");
        }
      }
    }
    return count;
  }

  private static Set<Path> stagingFiles(final BaseEnv env) throws Exception {
    final Set<Path> files = new java.util.HashSet<>();
    for (final DataNodeWrapper dn : env.getDataNodeWrapperList()) {
      // This fixture retains the default system/receiver directory under each isolated node.
      final Path directory = Paths.get(dn.getDataNodeDir(), "system", "pipe", "receiver");
      if (Files.exists(directory)) {
        try (Stream<Path> paths = Files.walk(directory)) {
          files.addAll(paths.filter(Files::isRegularFile).collect(Collectors.toSet()));
        }
      }
    }
    return files;
  }

  /** Run every cleanup and let try-with-resources preserve the original test failure. */
  private static final class Cleanup implements AutoCloseable {
    private final ThrowingRunnable[] actions;

    private Cleanup(final ThrowingRunnable... actions) {
      this.actions = actions;
    }

    @Override
    public void close() throws Exception {
      Throwable failure = null;
      for (final ThrowingRunnable action : actions) {
        try {
          action.run();
        } catch (final Throwable e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      if (failure != null) {
        throw (Exception) failure;
      }
    }
  }

  private static String sink(final PipeTransferGate gate, final String extra) {
    return sink(receiver, gate, extra);
  }

  private static String sink(
      final BaseEnv target, final PipeTransferGate gate, final String extra) {
    final DataNodeWrapper dn = target.getDataNodeWrapper(0);
    return " with sink ('sink'='iotdb-thrift-sync-sink', 'sink.ip'='"
        + dn.getIp()
        + "', 'sink.port'='"
        + (gate == null ? dn.getPort() : gate.getPort())
        + "', 'sink.batch.enable'='false', 'sink.parallel.tasks'='1', 'sink.leader-cache.enable'='false',"
        + " 'sink.exception.others.retry-max-time-seconds'='-1',"
        + " 'sink.exception.conflict.resolve-strategy'='retry',"
        + " 'sink.exception.conflict.retry-max-time-seconds'='-1'"
        + extra
        + ")";
  }

  private static void awaitTableRows(
      final String database, final String columns, final List<String> expected) {
    awaitTableQuery(
        database, "select " + columns + " from t order by time,id", columns.split(","), expected);
  }

  private static void awaitTableQuery(
      final String database,
      final String query,
      final String[] headers,
      final List<String> expected) {
    awaitTableQuery(receiver, database, query, headers, expected);
  }

  private static void awaitTableQuery(
      final BaseEnv env,
      final String database,
      final String query,
      final String[] headers,
      final List<String> expected) {
    await()
        .untilAsserted(
            () -> {
              try (Connection connection = connection(env, BaseEnv.TABLE_SQL_DIALECT);
                  Statement statement = statement(connection)) {
                statement.execute("use " + database);
                try (ResultSet result = statement.executeQuery(query)) {
                  TestUtils.assertResultSetEqualUnordered(
                      result, String.join(",", headers) + ",", expected);
                }
              }
            });
  }

  private static void awaitRows(
      final BaseEnv env, final String query, final String[] headers, final List<String> expected) {
    awaitRows(env, BaseEnv.TREE_SQL_DIALECT, query, headers, expected);
  }

  private static void awaitRows(
      final BaseEnv env,
      final String dialect,
      final String query,
      final String[] headers,
      final List<String> expected) {
    await()
        .alias("Rows on " + (env == sender ? "sender" : "receiver") + ": " + query)
        .untilAsserted(
            () -> {
              try (Connection connection = connection(env, dialect);
                  Statement statement = statement(connection);
                  ResultSet result = statement.executeQuery(query)) {
                TestUtils.assertResultSetEqualUnordered(
                    result, String.join(",", headers) + ",", expected);
              }
            });
  }

  private static final class Fixture implements AutoCloseable {
    private final String name;
    private final String database;
    private final String device;
    private final PipeTransferGate gate;
    private final int receiverConnectionsBefore;
    private final Set<Path> stagingFilesBefore;

    private Fixture(final String suffix, final PipeTransferGate gate) throws Exception {
      name = "p1_" + suffix;
      database = "root." + name;
      device = database + ".business";
      this.gate = gate;
      receiverConnectionsBefore = receiverConnections(receiver);
      stagingFilesBefore = stagingFiles(receiver);
      // Explicit compatible schema prevents auto-create privileges from confounding permission ITs.
      final List<ThrowingRunnable> rollback = new ArrayList<>();
      try {
        for (final BaseEnv env : Arrays.asList(sender, receiver)) {
          sql(env, "create database " + database);
          rollback.add(() -> sql(env, "delete database " + database));
          sql(env, "create timeseries " + device + ".v INT64 encoding=PLAIN");
          sql(env, "create timeseries " + device + ".flag BOOLEAN encoding=PLAIN");
        }
      } catch (final SQLException | RuntimeException | Error e) {
        try {
          new Cleanup(rollback.toArray(new ThrowingRunnable[0])).close();
        } catch (final Exception | Error cleanup) {
          e.addSuppressed(cleanup);
        }
        throw e;
      }
    }

    private void create(final PipeTransferGate gate, final String source, final String sink)
        throws SQLException {
      sql(
          sender,
          "create pipe "
              + name
              + " with source ('source.path'='"
              + database
              + ".**'"
              + (source.contains("source.realtime.mode") ? "" : ", 'source.realtime.mode'='log'")
              + (source.contains("source.inclusion") ? "" : ", 'source.inclusion'='data'")
              + source
              + ")"
              + sink(gate, sink));
    }

    private void write(final int from, final int to) throws SQLException {
      try (Connection c = connection(sender);
          Statement s = statement(c)) {
        for (int id = from; id < to; id++) {
          s.execute(
              "insert into "
                  + device
                  + "(time,v,flag) values("
                  + id
                  + ","
                  + (1000 + 17 * id)
                  + ","
                  + (id % 2 == 0)
                  + ")");
        }
      }
    }

    private void assertRows(final long... ranges) {
      assertRows(receiver, ranges);
    }

    private void assertBacklog(final int from, final int to, final long... visibleRanges) {
      // A held successful response (or a rejected request) proves the uncommitted transfer.
      // Check the durable sender writes and the receiver gap directly: SHOW PIPES estimates
      // exclude some source/assigner queues and are not a barrier for an in-flight request.
      assertRows(sender, from, to);
      assertRows(visibleRanges);
    }

    private void assertRows(final BaseEnv env, final long... ranges) {
      final List<String> expected = new ArrayList<>();
      for (int i = 0; i < ranges.length; i += 2) {
        for (long id = ranges[i]; id < ranges[i + 1]; id++) {
          expected.add(id + "," + (1000 + 17 * id) + "," + (id % 2 == 0) + ",");
        }
      }
      awaitRows(
          env,
          "select v,flag from " + device,
          new String[] {"Time", device + ".v", device + ".flag"},
          expected);
    }

    private void assertSchema() {
      await()
          .untilAsserted(
              () -> {
                try (Connection c = connection(receiver);
                    Statement s = statement(c);
                    ResultSet r = s.executeQuery("show timeseries " + device + ".*")) {
                  final Map<String, String> actual = new HashMap<>();
                  while (r.next()) {
                    assertEquals(
                        null, actual.put(r.getString("Timeseries"), r.getString("DataType")));
                    assertEquals("PLAIN", r.getString("Encoding"));
                  }
                  assertEquals(Map.of(device + ".v", "INT64", device + ".flag", "BOOLEAN"), actual);
                }
              });
    }

    @Override
    public void close() throws Exception {
      new Cleanup(
              () -> sql(sender, "drop pipe if exists " + name),
              () -> awaitNoPipeResources(name),
              () -> {
                if (gate != null) {
                  // Check while the relay is still open: closing the test socket must not hide a
                  // leak.
                  await()
                      .untilAsserted(
                          () -> {
                            assertEquals("Pipe left live relay sockets", 0, gate.openConnections());
                            assertTrue(
                                "Receiver connections did not return to baseline",
                                receiverConnections(receiver) <= receiverConnectionsBefore);
                          });
                }
              },
              () ->
                  await()
                      .untilAsserted(
                          () -> {
                            final Set<Path> residue = stagingFiles(receiver);
                            residue.removeAll(stagingFilesBefore);
                            assertTrue(
                                "New receiver staging files remain after DROP: " + residue,
                                residue.isEmpty());
                          }),
              () -> sql(sender, "delete database " + database),
              () -> sql(receiver, "delete database " + database))
          .close();
    }
  }
}
