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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.db.it.utils.MetadataFaultTestUtils;
import org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.Fault;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.fault.metadata.MetadataTestHooks;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.FENCE_MS;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.TIMEOUT_SECONDS;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.assertAllRows;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.assertFenced;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.assertRows;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.awaitAllRows;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.connect;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.execute;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.interruptProcedure;
import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Metadata P0 fault windows missing from normal DDL/device tests: live but fenced client nodes,
 * durable real-procedure failover, interrupted blacklists, object reuse and attribute backlogs. The
 * Memory-only table schema engine uses one class-scoped 3CN/3DN cluster with Ratis RF=3.
 */
@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class IoTDBTableMetadataP0RecoveryIT {

  @BeforeClass
  public static void setUp() throws Exception {
    MetadataFaultTestUtils.configureCluster();
    EnvFactory.getEnv().getConfig().getCommonConfig().setSchemaEngineMode("Memory");
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  /** P0-1/P0-2: a recovered real procedure must also respect the isolated node's lease. */
  @Test
  public void testConfigNodeFailoverRetainsLeaseSafetyForUnackedNode() throws Exception {
    final String database = "p0_failover_lease";
    final DataNodeWrapper victim = EnvFactory.getEnv().getDataNodeWrapper(2);
    try (Connection connection = connect(2, TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        Fault isolation = new Fault(victim, MetadataTestHooks.CONFIG_NODE_RPC);
        Fault heartbeat = new Fault(victim, "heartbeat-rejected").arm("observe")) {
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create database " + database,
          "create table " + database + ".t(id string tag, old int32, kept int32)",
          "insert into " + database + ".t(time,id,old,kept) values(1,'d',10,20)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,old,kept from " + database + ".t",
          "id,old,kept,",
          "d,10,20,");
      interruptProcedure(
          "DropTableColumnProcedure",
          "INVALIDATE_CACHE",
          false,
          () -> execute(0, TABLE_SQL_DIALECT, "alter table " + database + ".t drop column old"),
          () -> {
            isolation.arm("fail");
            heartbeat.awaitHit();
          });
      // The new leader has recovered the same procedure and committed a definition the victim
      // never acknowledged. Its existing physical client session must therefore be fenced.
      assertTrue(victim.isAlive());
      assertFenced(statement, "insert into " + database + ".t(time,id,old) values(2,'d',999)");
      assertRows(
          0,
          TABLE_SQL_DIALECT,
          "describe " + database + ".t",
          "ColumnName,DataType,Category,",
          "time,TIMESTAMP,TIME,",
          "id,STRING,TAG,",
          "kept,INT32,FIELD,");
      isolation.release();
      awaitAllRows(
          TABLE_SQL_DIALECT, "select id,kept from " + database + ".t", "id,kept,", "d,20,");
      statement.execute("insert into " + database + ".t(time,id,kept) values(3,'d',30)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,kept from " + database + ".t",
          "id,kept,",
          "d,20,",
          "d,30,");
    } finally {
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /** P0-1: this endpoint keeps its process, JDBC session and inter-DataNode links throughout. */
  @Test
  public void testLivePartitionedNodeFencesAndRetriesFailedResync() throws Exception {
    final String database = "p0_table_lease";
    final DataNodeWrapper victim = EnvFactory.getEnv().getDataNodeWrapper(2);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection connection = connect(2, TABLE_SQL_DIALECT);
        Statement victimStatement = connection.createStatement()) {
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create database " + database,
          "create table "
              + database
              + ".t (id string tag, a string attribute, old int32, kept int32)",
          "insert into " + database + ".t(time,id,a,old,kept) values(1,'d','v1',10,20)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,old,kept from " + database + ".t where id='d'",
          "id,a,old,kept,",
          "d,v1,10,20,");
      try (Fault isolation = new Fault(victim, MetadataTestHooks.CONFIG_NODE_RPC);
          Fault heartbeat = new Fault(victim, "heartbeat-rejected").arm("observe");
          Fault invalidation = new Fault(victim, "cache-broadcast-rejected").arm("observe");
          Fault recovery = new Fault(victim, MetadataTestHooks.LEASE_RECOVERY).arm("fail")) {
        final long disconnectedAt = System.nanoTime();
        isolation.arm("fail");
        heartbeat.awaitHit();
        // A separate connection owns the background statement and propagates failures via Future.
        final Future<?> ddl =
            executor.submit(
                () -> {
                  execute(0, TABLE_SQL_DIALECT, "alter table " + database + ".t drop column old");
                  return null;
                });
        invalidation.awaitHit();
        await()
            .during(2, TimeUnit.SECONDS)
            .atMost(3, TimeUnit.SECONDS)
            .untilAsserted(() -> assertFalse("Unacked, unfenced node was skipped", ddl.isDone()));
        await()
            .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    assertFenced(
                        victimStatement, "select old from " + database + ".t where id='d'"));
        assertTrue("The partitioned process was stopped", victim.isAlive());
        assertFenced(
            victimStatement, "insert into " + database + ".t(time,id,old) values(2,'d',999)");
        ddl.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(
            "DDL crossed the fence boundary too early",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - disconnectedAt) >= FENCE_MS - 2000);
        assertRows(
            0,
            TABLE_SQL_DIALECT,
            "describe " + database + ".t",
            "ColumnName,DataType,Category,",
            "time,TIMESTAMP,TIME,",
            "id,STRING,TAG,",
            "a,STRING,ATTRIBUTE,",
            "kept,INT32,FIELD,");
        isolation.release();
        recovery.awaitHit();
        // A heartbeat alone must not re-enable service when metadata pulling failed.
        assertFenced(victimStatement, "select kept from " + database + ".t where id='d'");
        recovery.release();
        awaitAllRows(
            TABLE_SQL_DIALECT,
            "select id,a,kept from " + database + ".t",
            "id,a,kept,",
            "d,v1,20,");
        assertAllRows(
            TABLE_SQL_DIALECT,
            "describe " + database + ".t",
            "ColumnName,DataType,Category,",
            "time,TIMESTAMP,TIME,",
            "id,STRING,TAG,",
            "a,STRING,ATTRIBUTE,",
            "kept,INT32,FIELD,");
        victimStatement.execute("insert into " + database + ".t(time,id,kept) values(3,'d',30)");
        assertAllRows(
            TABLE_SQL_DIALECT,
            "select id,kept from " + database + ".t",
            "id,kept,",
            "d,20,",
            "d,30,");
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  /** P0-2: PRE_CREATE is durable, PRE_RELEASE is still a rollback-capable boundary. */
  @Test
  public void testPreCreatedTableRollsBackAfterConfigNodeFailover() throws Exception {
    final String database = "p0_create_rollback";
    try {
      execute(0, TABLE_SQL_DIALECT, "create database " + database);
      interruptProcedure(
          "CreateTableProcedure",
          "PRE_RELEASE",
          true,
          () ->
              execute(
                  0,
                  TABLE_SQL_DIALECT,
                  "create table " + database + ".t(id string tag, ghost int32)"));
      awaitAllRows(TABLE_SQL_DIALECT, "show tables from " + database, "TableName,TTL(ms),");
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create table if not exists " + database + ".t(id string tag, v boolean)",
          "insert into " + database + ".t(time,id,v) values(1,'d',true)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "describe " + database + ".t",
          "ColumnName,DataType,Category,",
          "time,TIMESTAMP,TIME,",
          "id,STRING,TAG,",
          "v,BOOLEAN,FIELD,");
      assertAllRows(TABLE_SQL_DIALECT, "select id,v from " + database + ".t", "id,v,", "d,true,");
    } finally {
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  /** P0-2: the committed catalog survives loss of the original leader/client response. */
  @Test
  public void testCommittedCreateContinuesAfterConfigNodeFailover() throws Exception {
    final String database = "p0_create_commit";
    try {
      execute(0, TABLE_SQL_DIALECT, "create database " + database);
      interruptProcedure(
          "CreateTableProcedure",
          "COMMIT_RELEASE",
          false,
          () ->
              execute(
                  0, TABLE_SQL_DIALECT, "create table " + database + ".t(id string tag, v int32)"));
      awaitAllRows(
          TABLE_SQL_DIALECT,
          "describe " + database + ".t",
          "ColumnName,DataType,Category,",
          "time,TIMESTAMP,TIME,",
          "id,STRING,TAG,",
          "v,INT32,FIELD,");
      // Resolve the unknown response using the documented idempotent form, after observing commit.
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create table if not exists " + database + ".t(id string tag, v int32)",
          "insert into " + database + ".t(time,id,v) values(1,'d',17)");
      assertAllRows(TABLE_SQL_DIALECT, "select id,v from " + database + ".t", "id,v,", "d,17,");
    } finally {
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  /**
   * P0-3: both blacklist rollback and deletion continuation operate on actual cross-region keys.
   */
  @Test
  public void testCrossRegionDeviceDeleteRecoversBlacklistAndDataDeletion() throws Exception {
    final String database = "p0_delete_devices";
    final List<IDeviceID> devices = new ArrayList<>();
    final List<String> expected = new ArrayList<>();
    try {
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create database " + database,
          "create table " + database + ".t(id string tag, a string attribute, v int32)",
          "insert into " + database + ".t(time,id,a,v) values(1,'keep','protected',77)");
      // Few stable keys suffice; verify their real partition mapping rather than node count.
      for (int i = 0; i < 16; i++) {
        final String id = "delete_" + i;
        execute(
            0,
            TABLE_SQL_DIALECT,
            "insert into " + database + ".t(time,id,a,v) values(1,'" + id + "','old'," + i + ")");
        devices.add(IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"t", id}));
        expected.add(id + ",old," + i + ",");
      }
      MetadataFaultTestUtils.assertSpansRegions(database, devices);
      expected.add("keep,protected,77,");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t",
          "id,a,v,",
          expected.toArray(new String[0]));
      final String delete = "delete devices from " + database + ".t where id <> 'keep'";
      interruptProcedure(
          "DeleteDevicesProcedure",
          "CLEAN_DATANODE_SCHEMA_CACHE",
          true,
          () -> execute(0, TABLE_SQL_DIALECT, delete));
      awaitAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t",
          "id,a,v,",
          expected.toArray(new String[0]));
      assertAllRows(
          TABLE_SQL_DIALECT, "count devices from " + database + ".t", "count(devices),", "17,");
      interruptProcedure(
          "DeleteDevicesProcedure",
          "DELETE_DEVICE_SCHEMA",
          false,
          () -> execute(0, TABLE_SQL_DIALECT, delete));
      awaitAllRows(
          TABLE_SQL_DIALECT, "show devices from " + database + ".t", "id,a,", "keep,protected,");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t",
          "id,a,v,",
          "keep,protected,77,");
      assertAllRows(
          TABLE_SQL_DIALECT, "count devices from " + database + ".t", "count(devices),", "1,");
      execute(
          0,
          TABLE_SQL_DIALECT,
          delete,
          "insert into " + database + ".t(time,id,a,v) values(2,'delete_0','new',100)",
          "insert into " + database + ".t(time,id,v) values(2,'keep',78)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t",
          "id,a,v,",
          "keep,protected,77,",
          "keep,protected,78,",
          "delete_0,new,100,");
    } finally {
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  /**
   * P0-2/P0-4: async DROP recovery, physical mod progress, shared-file retention and name reuse.
   */
  @Test
  public void testAsyncDropFailoverAndSameNameRecreateBeforeOldNodeReturns() throws Exception {
    final String database = "p0_drop_recreate";
    final DataNodeWrapper victim = EnvFactory.getEnv().getDataNodeWrapper(2);
    try {
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create database " + database,
          "create table "
              + database
              + ".t(id string tag, a string attribute, v int32) with(ttl=10000000000000)",
          "create table " + database + ".retained(id string tag, a string attribute, v int32)",
          "insert into " + database + ".t(time,id,a,v) values(1,'old','obsolete',10)",
          "insert into " + database + ".retained(time,id,a,v) values(1,'keep','protected',77)",
          "flush");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t",
          "id,a,v,",
          "old,obsolete,10,");
      victim.stopForcibly();
      assertFalse(victim.isAlive());
      interruptProcedure(
          "DropTableProcedure",
          "DELETE_DATA",
          false,
          () -> {
            execute(0, TABLE_SQL_DIALECT, "drop table " + database + ".t");
            // DROP returns asynchronously; retain the client operation until the paused procedure
            // ends.
            MetadataFaultTestUtils.awaitDropSuccess(database, "t");
          },
          () -> {
            try {
              execute(
                  0,
                  TABLE_SQL_DIALECT,
                  "create table " + database + ".t(id string tag, v boolean)");
              fail("Recreation must wait for the original DROP to complete");
            } catch (SQLException e) {
              assertTrue(
                  e.toString(),
                  e.getErrorCode() == TSStatusCode.TABLE_IN_PRE_DELETE.getStatusCode()
                      || e.getErrorCode() == TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode()
                      || e.getErrorCode()
                          == TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode());
            }
          });
      MetadataFaultTestUtils.awaitDropSuccess(database, "t");
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create table " + database + ".t(id string tag, a string attribute, v boolean)",
          "insert into " + database + ".t(time,id,a,v) values(2,'new','fresh',true)");
      victim.start();
      awaitAllRows(
          TABLE_SQL_DIALECT,
          "describe " + database + ".t",
          "ColumnName,DataType,Category,",
          "time,TIMESTAMP,TIME,",
          "id,STRING,TAG,",
          "a,STRING,ATTRIBUTE,",
          "v,BOOLEAN,FIELD,");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "show tables from " + database,
          "TableName,TTL(ms),",
          "t,INF,",
          "retained,INF,");
      assertAllRows(
          TABLE_SQL_DIALECT, "show devices from " + database + ".t", "id,a,", "new,fresh,");
      assertAllRows(
          TABLE_SQL_DIALECT, "select id,a,v from " + database + ".t", "id,a,v,", "new,fresh,true,");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".retained",
          "id,a,v,",
          "keep,protected,77,");
      execute(
          2,
          TABLE_SQL_DIALECT,
          "insert into " + database + ".t(time,id,v) values(3,'new',false)",
          "insert into " + database + ".retained(time,id,v) values(3,'keep',78)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t",
          "id,a,v,",
          "new,fresh,true,",
          "new,fresh,false,");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".retained",
          "id,a,v,",
          "keep,protected,77,",
          "keep,protected,78,");
    } finally {
      if (!victim.isAlive()) {
        victim.start();
      }
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  /** P0-5: a real captured v2 batch is overtaken by NULL/v3 and a SchemaRegion leader change. */
  @Test
  public void testAttributeBacklogAndDelayedBatchSurviveSchemaLeaderChange() throws Exception {
    final String database = "p0_attribute_backlog";
    DataNodeWrapper oldLeader = null;
    try {
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create database " + database,
          "create table " + database + ".t(id string tag, a string attribute, v int32)",
          "insert into "
              + database
              + ".t(time,id,a,v) values(1,'d','v1',10),(1,'keep','protected',77)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t where id='d'",
          "id,a,v,",
          "d,v1,10,");
      final int regionId = MetadataFaultTestUtils.tableDeviceRegion(database, "t", "d");
      oldLeader = MetadataFaultTestUtils.schemaLeader(database, regionId);
      final int target = oldLeader == EnvFactory.getEnv().getDataNodeWrapper(2) ? 1 : 2;
      final DataNodeWrapper victim = EnvFactory.getEnv().getDataNodeWrapper(target);
      try (Fault delayed = new Fault(victim, MetadataTestHooks.ATTRIBUTE_UPDATE).arm("pause")) {
        execute(0, TABLE_SQL_DIALECT, "update " + database + ".t set a='v2' where id='d'");
        final String batch = delayed.awaitHit();
        try (Fault delivered =
            new Fault(victim, "attribute-update-returned." + batch).arm("observe")) {
          // Hold the captured old call and reject its retries, leaving the last confirmed values
          // in the replicated updater backlog when leadership changes.
          delayed.arm("fail");
          execute(
              0,
              TABLE_SQL_DIALECT,
              "update " + database + ".t set a=NULL where id='d'",
              "update " + database + ".t set a='v3' where id='d'");
          delayed.awaitFailure();
          oldLeader.stopForcibly();
          MetadataFaultTestUtils.awaitDifferentSchemaLeader(database, regionId, oldLeader);
          delayed.disarm("fail");
          oldLeader.start();
          // Stop all attribute writes. The new leader must drain the existing backlog itself.
          awaitAllRows(
              TABLE_SQL_DIALECT,
              "select id,a,v from " + database + ".t where id='d'",
              "id,a,v,",
              "d,v3,10,");
          delayed.release();
          delivered.awaitHit();
          awaitAllRows(
              TABLE_SQL_DIALECT,
              "show devices from " + database + ".t",
              "id,a,",
              "d,v3,",
              "keep,protected,");
          assertAllRows(
              TABLE_SQL_DIALECT,
              "count devices from " + database + ".t where a='v3'",
              "count(devices),",
              "1,");
          assertAllRows(
              TABLE_SQL_DIALECT,
              "select id,a,v from " + database + ".t where a='v3'",
              "id,a,v,",
              "d,v3,10,");
          assertAllRows(
              TABLE_SQL_DIALECT,
              "select id,a,v from " + database + ".t where id='keep'",
              "id,a,v,",
              "keep,protected,77,");
        }
      }
    } finally {
      if (oldLeader != null && !oldLeader.isAlive()) {
        oldLeader.start();
      }
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  /** P0-4/5: deliver a captured old attribute request after a supported DELETE/INSERT lifecycle. */
  @Test
  public void testDelayedAttributeCannotPolluteRecreatedDevice() throws Exception {
    final String database = "p0_attribute_recreate";
    try {
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create database " + database,
          "create table " + database + ".t(id string tag, a string attribute, v int32)",
          "insert into "
              + database
              + ".t(time,id,a,v) values(1,'d','old',10),(1,'keep','protected',77)");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t",
          "id,a,v,",
          "d,old,10,",
          "keep,protected,77,");
      final int regionId = MetadataFaultTestUtils.tableDeviceRegion(database, "t", "d");
      final DataNodeWrapper leader = MetadataFaultTestUtils.schemaLeader(database, regionId);
      final DataNodeWrapper victim =
          EnvFactory.getEnv().getDataNodeWrapperList().stream()
              .filter(node -> node != leader)
              .findFirst()
              .orElseThrow(AssertionError::new);
      try (Fault delayed = new Fault(victim, MetadataTestHooks.ATTRIBUTE_UPDATE).arm("pause")) {
        execute(0, TABLE_SQL_DIALECT, "update " + database + ".t set a='obsolete' where id='d'");
        final String batch = delayed.awaitHit();
        try (Fault delivered =
            new Fault(victim, "attribute-update-returned." + batch).arm("observe")) {
          execute(
              0,
              TABLE_SQL_DIALECT,
              "delete devices from " + database + ".t where id='d'",
              "insert into " + database + ".t(time,id,a,v) values(2,'d','fresh',20)");
          awaitAllRows(
              TABLE_SQL_DIALECT,
              "select id,a,v from " + database + ".t",
              "id,a,v,",
              "d,fresh,20,",
              "keep,protected,77,");
          delayed.release();
          delivered.awaitHit();
          assertAllRows(
              TABLE_SQL_DIALECT,
              "show devices from " + database + ".t",
              "id,a,",
              "d,fresh,",
              "keep,protected,");
          assertAllRows(
              TABLE_SQL_DIALECT,
              "count devices from " + database + ".t where a='obsolete'",
              "count(devices),",
              "0,");
          assertAllRows(
              TABLE_SQL_DIALECT,
              "select id,a,v from " + database + ".t",
              "id,a,v,",
              "d,fresh,20,",
              "keep,protected,77,");
        }
      }
    } finally {
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  /**
   * P0-6: SchemaRegion snapshot payload plus a separate log tail, including NULL and recreation.
   */
  @Test
  public void testSchemaSnapshotAndLogTailRestoreDevicesAndAttributes() throws Exception {
    final String database = "p0_table_snapshot";
    try {
      MetadataFaultTestUtils.createSingleSchemaRegionDatabase(database, TABLE_SQL_DIALECT);
      execute(
          0,
          TABLE_SQL_DIALECT,
          "create table " + database + ".t(id string tag, a string attribute, v int32)",
          "create table " + database + ".retained(id string tag, a string attribute, v int32)",
          "insert into "
              + database
              + ".t(time,id,a,v) values(1,'d','baseline',10),(1,'n',NULL,11),"
              + "(1,'gone','deleted',12),(1,'reborn','old',13)",
          "insert into " + database + ".retained(time,id,a,v) values(1,'keep','protected',77)",
          "delete devices from " + database + ".t where id='gone'");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "show devices from " + database + ".t",
          "id,a,",
          "d,baseline,",
          "n,null,",
          "reborn,old,");
      final int regionId = MetadataFaultTestUtils.tableDeviceRegion(database, "t", "d");
      assertTrue(
          MetadataFaultTestUtils.schemaRegions(database).stream()
              .allMatch(info -> info.getConsensusGroupId().getId() == regionId));
      MetadataFaultTestUtils.installSchemaSnapshot(
          database,
          regionId,
          () -> {
            // Metadata-only log entries in the same region trigger a real snapshot and log purge.
            for (int i = 0; i < 32; i++) {
              execute(
                  0,
                  TABLE_SQL_DIALECT,
                  "update " + database + ".t set a='baseline" + i + "' where id='d'");
            }
          },
          () -> {
            execute(
                0,
                TABLE_SQL_DIALECT,
                "update " + database + ".t set a='tail' where id='d'",
                "update " + database + ".t set a=NULL where id='reborn'",
                "delete devices from " + database + ".t where id='reborn'",
                "insert into "
                    + database
                    + ".t(time,id,a,v) values(2,'reborn','fresh',20),"
                    + "(2,'added','new',21)");
          },
          () -> assertSnapshotTableState(database));
      // First reconcile the old manifest; only then prove the recovered leader accepts new work.
      execute(0, TABLE_SQL_DIALECT, "update " + database + ".t set a='after' where id='d'");
      awaitAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".t where id='d'",
          "id,a,v,",
          "d,after,10,");
      assertAllRows(
          TABLE_SQL_DIALECT,
          "select id,a,v from " + database + ".retained",
          "id,a,v,",
          "keep,protected,77,");
    } finally {
      execute(0, TABLE_SQL_DIALECT, "drop database if exists " + database);
    }
  }

  private static void assertSnapshotTableState(final String database) throws Exception {
    awaitAllRows(
        TABLE_SQL_DIALECT,
        "show devices from " + database + ".t",
        "id,a,",
        "d,tail,",
        "n,null,",
        "reborn,fresh,",
        "added,new,");
    assertAllRows(
        TABLE_SQL_DIALECT,
        "select id,a,v from " + database + ".t",
        "id,a,v,",
        "d,tail,10,",
        "n,null,11,",
        "reborn,fresh,20,",
        "added,new,21,");
    assertAllRows(
        TABLE_SQL_DIALECT,
        "count devices from " + database + ".t where a is null",
        "count(devices),",
        "1,");
    assertAllRows(
        TABLE_SQL_DIALECT,
        "select id,a,v from " + database + ".t where a='tail'",
        "id,a,v,",
        "d,tail,10,");
    assertAllRows(
        TABLE_SQL_DIALECT,
        "select id,a,v from " + database + ".retained",
        "id,a,v,",
        "keep,protected,77,");
    // Catalog definitions are ConfigNode state, checked separately from the SchemaRegion payload.
    assertAllRows(
        TABLE_SQL_DIALECT,
        "describe " + database + ".t",
        "ColumnName,DataType,Category,",
        "time,TIMESTAMP,TIME,",
        "id,STRING,TAG,",
        "a,STRING,ATTRIBUTE,",
        "v,INT32,FIELD,");
  }
}
