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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.db.it.utils.MetadataFaultTestUtils;
import org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.Fault;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.fault.metadata.MetadataTestHooks;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.assertAllRows;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.assertFenced;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.assertRows;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.awaitAllRows;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.connect;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.execute;
import static org.apache.iotdb.db.it.utils.MetadataFaultTestUtils.interruptProcedure;
import static org.apache.iotdb.itbase.env.BaseEnv.TREE_SQL_DIALECT;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tree-model metadata fault windows. AbstractSchemaIT runs this class once for Memory and once for
 * PBTree, while the shared cluster topology provides three ConfigNodes and replicated
 * SchemaRegions.
 */
@Category(ClusterIT.class)
public class IoTDBSchemaMetadataRecoveryIT extends AbstractSchemaIT {

  private static final String DB_LEASE = "root.p0_tree_lease";
  private static final String DB_DELETE = "root.p0_tree_delete";
  private static final String DB_LIFECYCLE = "root.p0_tree_lifecycle";
  private static final String DB_SNAPSHOT = "root.p0_tree_snapshot";
  private static final String TIMESERIES_HEADER =
      "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,";

  public IoTDBSchemaMetadataRecoveryIT(final SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    MetadataFaultTestUtils.configureCluster();
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    // Delete databases before dropping their bound template. Propagate cleanup failures instead
    // of hiding an unfinished procedure behind AbstractSchemaIT.clearSchema's best-effort delete.
    try (Connection connection = connect(0, TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final List<String> databases = new ArrayList<>();
      try (ResultSet result = statement.executeQuery("show databases root.p0_tree_*")) {
        while (result.next()) {
          databases.add(result.getString("Database"));
        }
      }
      for (String database : databases) {
        statement.execute("delete database " + database);
      }
      boolean hasTemplate = false;
      try (ResultSet result = statement.executeQuery("show device templates")) {
        while (result.next()) {
          hasTemplate |= "p0_tree_template".equals(result.getString(1));
        }
      }
      if (hasTemplate) {
        statement.execute("drop device template p0_tree_template");
      }
    }
  }

  /** P0-1: a live tree DataNode is fenced while a delete invalidates its warm schema cache. */
  @Test
  public void testTreeLiveNodeFencesOnDeleteAndReloadsBeforeWrites() throws Exception {
    final var victim = EnvFactory.getEnv().getDataNodeWrapper(2);
    try (Connection connection = connect(2, TREE_SQL_DIALECT);
        Statement victimStatement = connection.createStatement()) {
      execute(
          0,
          TREE_SQL_DIALECT,
          "create database " + DB_LEASE,
          "create timeseries "
              + DB_LEASE
              + ".d.s with datatype=INT32, encoding=PLAIN "
              + "tags('kind'='protected') attributes('owner'='old')",
          "insert into " + DB_LEASE + ".d(time,s) values(1,10)");
      assertRows(
          2,
          TREE_SQL_DIALECT,
          "select s from " + DB_LEASE + ".d",
          "Time," + DB_LEASE + ".d.s,",
          "1,10,");
      try (Fault isolation = new Fault(victim, MetadataTestHooks.CONFIG_NODE_RPC);
          Fault heartbeat = new Fault(victim, "heartbeat-rejected").arm("observe");
          Fault invalidation = new Fault(victim, "cache-broadcast-rejected").arm("observe");
          Fault recovery = new Fault(victim, MetadataTestHooks.LEASE_RECOVERY).arm("fail")) {
        isolation.arm("fail");
        heartbeat.awaitHit();
        execute(0, TREE_SQL_DIALECT, "delete timeseries " + DB_LEASE + ".d.s");
        invalidation.awaitHit();
        await()
            .atMost(MetadataFaultTestUtils.TIMEOUT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
            .untilAsserted(() -> assertFenced(victimStatement, "select s from " + DB_LEASE + ".d"));
        assertTrue(victim.isAlive());
        isolation.release();
        recovery.awaitHit();
        assertFenced(victimStatement, "insert into " + DB_LEASE + ".d(time,s) values(2,20)");
        recovery.release();
        awaitAllRows(
            TREE_SQL_DIALECT, "count timeseries " + DB_LEASE + ".**", "count(timeseries),", "0,");
        execute(
            2,
            TREE_SQL_DIALECT,
            "create timeseries " + DB_LEASE + ".d.s with datatype=DOUBLE, encoding=PLAIN",
            "insert into " + DB_LEASE + ".d(time,s) values(3,2.5)");
        awaitAllRows(
            TREE_SQL_DIALECT,
            "select s from " + DB_LEASE + ".d",
            "Time," + DB_LEASE + ".d.s,",
            "3,2.5,");
      }
    }
  }

  /**
   * P0-3: a real cross-SchemaRegion blacklist rolls back, then a later state completes deletion.
   */
  @Test
  public void testCrossRegionDeleteTimeseriesRollsBackAndContinues() throws Exception {
    final List<IDeviceID> devices = new ArrayList<>();
    final List<String> setup = new ArrayList<>();
    final List<String> metadata = new ArrayList<>();
    final List<String> data = new ArrayList<>();
    setup.add("create database " + DB_DELETE);
    setup.add(
        "create aligned timeseries "
            + DB_DELETE
            + ".protected(s1 INT32 encoding=PLAIN compressor=SNAPPY, "
            + "s2 DOUBLE encoding=PLAIN compressor=SNAPPY)");
    setup.add("insert into " + DB_DELETE + ".protected(time,s1,s2) aligned values(1,77,7.7)");
    for (int i = 0; i < 24; i++) {
      final String device = DB_DELETE + ".d" + i;
      final String properties = " tags('group'='delete') attributes('old'='yes')";
      setup.add(
          i % 2 == 0
              ? "create timeseries "
                  + device
                  + ".s(alias"
                  + i
                  + ") with datatype=INT32, encoding=PLAIN, compressor=SNAPPY"
                  + properties
              : "create aligned timeseries "
                  + device
                  + "(s(alias"
                  + i
                  + ") INT32 encoding=PLAIN compressor=SNAPPY"
                  + properties
                  + ")");
      setup.add(
          "insert into "
              + device
              + "(time,s)"
              + (i % 2 == 0 ? "" : " aligned")
              + " values(1,"
              + i
              + ")");
      devices.add(IDeviceID.Factory.DEFAULT_FACTORY.create(device));
      metadata.add(
          device
              + ".s,alias"
              + i
              + ","
              + DB_DELETE
              + ",INT32,PLAIN,SNAPPY,{\"group\":\"delete\"},{\"old\":\"yes\"},null,null,BASE,");
      data.add("1," + device + "," + i + ",");
    }
    execute(0, TREE_SQL_DIALECT, setup.toArray(new String[0]));
    MetadataFaultTestUtils.assertSpansRegions(DB_DELETE, devices);
    assertAllRows(
        TREE_SQL_DIALECT,
        "show timeseries " + DB_DELETE + ".d*.s",
        TIMESERIES_HEADER,
        metadata.toArray(new String[0]));
    assertAllRows(
        TREE_SQL_DIALECT,
        "select s from " + DB_DELETE + ".d* align by device",
        "Time,Device,s,",
        data.toArray(new String[0]));
    assertAllRows(
        TREE_SQL_DIALECT, "count timeseries " + DB_DELETE + ".d*.s", "count(timeseries),", "24,");
    final String delete = "delete timeseries " + DB_DELETE + ".d*.s";
    interruptProcedure(
        "DeleteTimeSeriesProcedure",
        "CLEAN_DATANODE_SCHEMA_CACHE",
        true,
        () -> execute(0, TREE_SQL_DIALECT, delete));
    awaitAllRows(
        TREE_SQL_DIALECT, "count timeseries " + DB_DELETE + ".d*.s", "count(timeseries),", "24,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "show timeseries " + DB_DELETE + ".d*.s",
        TIMESERIES_HEADER,
        metadata.toArray(new String[0]));
    assertAllRows(
        TREE_SQL_DIALECT,
        "select s from " + DB_DELETE + ".d* align by device",
        "Time,Device,s,",
        data.toArray(new String[0]));
    assertAllRows(
        TREE_SQL_DIALECT,
        "select s1,s2 from " + DB_DELETE + ".protected",
        "Time," + DB_DELETE + ".protected.s1," + DB_DELETE + ".protected.s2,",
        "1,77,7.7,");
    interruptProcedure(
        "DeleteTimeSeriesProcedure",
        "DELETE_TIMESERIES_SCHEMA",
        false,
        () -> execute(0, TREE_SQL_DIALECT, delete));
    awaitAllRows(
        TREE_SQL_DIALECT, "count timeseries " + DB_DELETE + ".d*.s", "count(timeseries),", "0,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "show devices " + DB_DELETE + ".protected",
        "Device,IsAligned,Template,TTL(ms),",
        DB_DELETE + ".protected,true,null,INF,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "select s1,s2 from " + DB_DELETE + ".protected",
        "Time," + DB_DELETE + ".protected.s1," + DB_DELETE + ".protected.s2,",
        "1,77,7.7,");
    execute(
        0,
        TREE_SQL_DIALECT,
        "create timeseries "
            + DB_DELETE
            + ".d0.s with datatype=DOUBLE, encoding=PLAIN tags('group'='new')",
        "insert into " + DB_DELETE + ".d0(time,s) values(2,100.5)");
    awaitAllRows(
        TREE_SQL_DIALECT,
        "select s from " + DB_DELETE + ".d0",
        "Time," + DB_DELETE + ".d0.s,",
        "2,100.5,");
  }

  /**
   * P0-4: an old stopped node returns after a tree sequence is deleted and recreated with a new
   * type.
   */
  @Test
  public void testTreeDeleteRecreateDoesNotResurrectOldSchema() throws Exception {
    final var victim = EnvFactory.getEnv().getDataNodeWrapper(1);
    try {
      execute(
          0,
          TREE_SQL_DIALECT,
          "create database " + DB_LIFECYCLE,
          "create timeseries "
              + DB_LIFECYCLE
              + ".d.s with datatype=INT32, encoding=PLAIN, compressor=SNAPPY "
              + "tags('generation'='old') attributes('owner'='old')",
          "insert into " + DB_LIFECYCLE + ".d(time,s) values(1,10)");
      assertAllRows(
          TREE_SQL_DIALECT,
          "select s from " + DB_LIFECYCLE + ".d",
          "Time," + DB_LIFECYCLE + ".d.s,",
          "1,10,");
      victim.stopForcibly();
      assertFalse(victim.isAlive());
      execute(
          0,
          TREE_SQL_DIALECT,
          "delete timeseries " + DB_LIFECYCLE + ".d.s",
          "create timeseries "
              + DB_LIFECYCLE
              + ".d.s with datatype=DOUBLE, encoding=PLAIN, compressor=SNAPPY "
              + "tags('generation'='new') attributes('owner'='new')",
          "insert into " + DB_LIFECYCLE + ".d(time,s) values(2,2.5)");
      victim.start();
      awaitAllRows(
          TREE_SQL_DIALECT,
          "show timeseries " + DB_LIFECYCLE + ".d.s",
          TIMESERIES_HEADER,
          DB_LIFECYCLE
              + ".d.s,null,"
              + DB_LIFECYCLE
              + ",DOUBLE,PLAIN,SNAPPY,{\"generation\":\"new\"},{\"owner\":\"new\"},null,null,BASE,");
      awaitAllRows(
          TREE_SQL_DIALECT,
          "select s from " + DB_LIFECYCLE + ".d",
          "Time," + DB_LIFECYCLE + ".d.s,",
          "2,2.5,");
    } finally {
      if (!victim.isAlive()) {
        victim.start();
      }
    }
  }

  /** P0-6: a stopped replica installs a tree snapshot and then replays a metadata log tail. */
  @Test
  public void testTreeSchemaSnapshotRestoresRichMetadataAndTail() throws Exception {
    final String template = "p0_tree_template";
    MetadataFaultTestUtils.createSingleSchemaRegionDatabase(DB_SNAPSHOT, TREE_SQL_DIALECT);
    execute(
        0,
        TREE_SQL_DIALECT,
        "create timeseries "
            + DB_SNAPSHOT
            + ".n.s1(alias1) with datatype=INT32, encoding=PLAIN, compressor=SNAPPY "
            + "tags('kind'='base') attributes('owner'='base')",
        "create aligned timeseries "
            + DB_SNAPSHOT
            + ".a(s1 INT32 encoding=PLAIN compressor=SNAPPY, s2 DOUBLE encoding=PLAIN compressor=SNAPPY)",
        "insert into " + DB_SNAPSHOT + ".n(time,s1) values(1,10)",
        "insert into " + DB_SNAPSHOT + ".a(time,s1,s2) aligned values(1,20,2.0)",
        "create timeseries " + DB_SNAPSHOT + ".gone.s with datatype=INT32, encoding=PLAIN",
        "delete timeseries " + DB_SNAPSHOT + ".gone.s",
        "create device template " + template + " (ts INT32 ENCODING=PLAIN COMPRESSOR=SNAPPY)",
        "set device template " + template + " to " + DB_SNAPSHOT + ".templ",
        "create timeseries using device template on " + DB_SNAPSHOT + ".templ",
        "insert into " + DB_SNAPSHOT + ".templ(time,ts) values(1,31)",
        "alter device template " + template + " add (ext DOUBLE ENCODING=RLE COMPRESSOR=SNAPPY)",
        "insert into " + DB_SNAPSHOT + ".templ(time,ext) values(1,3.1)");
    final var mapping =
        MetadataFaultTestUtils.partitions(
            DB_SNAPSHOT,
            Arrays.asList(
                IDeviceID.Factory.DEFAULT_FACTORY.create(DB_SNAPSHOT + ".n"),
                IDeviceID.Factory.DEFAULT_FACTORY.create(DB_SNAPSHOT + ".a"),
                IDeviceID.Factory.DEFAULT_FACTORY.create(DB_SNAPSHOT + ".templ")));
    assertEquals(1L, mapping.values().stream().distinct().count());
    final int regionId = mapping.values().iterator().next().getId();
    assertTrue(
        MetadataFaultTestUtils.schemaRegions(DB_SNAPSHOT).stream()
            .allMatch(info -> info.getConsensusGroupId().getId() == regionId));
    MetadataFaultTestUtils.installSchemaSnapshot(
        DB_SNAPSHOT,
        regionId,
        () -> {
          for (int i = 0; i < 32; i++) {
            execute(
                0,
                TREE_SQL_DIALECT,
                "create timeseries "
                    + DB_SNAPSHOT
                    + ".n.extra"
                    + i
                    + " with datatype=INT32, encoding=PLAIN, compressor=SNAPPY tags('batch'='"
                    + i
                    + "')");
          }
        },
        () ->
            execute(
                0,
                TREE_SQL_DIALECT,
                "alter timeseries "
                    + DB_SNAPSHOT
                    + ".n.s1 upsert alias=tail_alias tags('kind'='tail')",
                "alter device template "
                    + template
                    + " add (tail BOOLEAN ENCODING=PLAIN COMPRESSOR=SNAPPY)",
                "insert into " + DB_SNAPSHOT + ".templ(time,tail) values(2,true)",
                "delete timeseries " + DB_SNAPSHOT + ".n.extra0",
                "create timeseries " + DB_SNAPSHOT + ".tail.s INT64"),
        () -> assertTreeSnapshotState(template));
    execute(
        0,
        TREE_SQL_DIALECT,
        "alter timeseries " + DB_SNAPSHOT + ".n.s1 upsert attributes('owner'='after')",
        "insert into " + DB_SNAPSHOT + ".tail(time,s) values(2,99)",
        "insert into " + DB_SNAPSHOT + ".templ(time,tail) values(3,false)");
    awaitAllRows(
        TREE_SQL_DIALECT,
        "select s from " + DB_SNAPSHOT + ".tail",
        "Time," + DB_SNAPSHOT + ".tail.s,",
        "2,99,");
  }

  private static void assertTreeSnapshotState(final String template) throws Exception {
    awaitAllRows(
        TREE_SQL_DIALECT,
        "show timeseries " + DB_SNAPSHOT + ".n.s1",
        TIMESERIES_HEADER,
        DB_SNAPSHOT
            + ".n.s1,tail_alias,"
            + DB_SNAPSHOT
            + ",INT32,PLAIN,SNAPPY,{\"kind\":\"tail\"},{\"owner\":\"base\"},null,null,BASE,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "select tail_alias from " + DB_SNAPSHOT + ".n",
        "Time," + DB_SNAPSHOT + ".n.s1,",
        "1,10,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "show timeseries " + DB_SNAPSHOT + ".** where TAGS(kind)='base'",
        TIMESERIES_HEADER);
    assertAllRows(
        TREE_SQL_DIALECT,
        "show timeseries " + DB_SNAPSHOT + ".** where TAGS(kind)='tail'",
        TIMESERIES_HEADER,
        DB_SNAPSHOT
            + ".n.s1,tail_alias,"
            + DB_SNAPSHOT
            + ",INT32,PLAIN,SNAPPY,{\"kind\":\"tail\"},{\"owner\":\"base\"},null,null,BASE,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "count timeseries " + DB_SNAPSHOT + ".gone.**",
        "count(timeseries),",
        "0,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "count timeseries " + DB_SNAPSHOT + ".n.extra0",
        "count(timeseries),",
        "0,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "count timeseries " + DB_SNAPSHOT + ".n.extra1",
        "count(timeseries),",
        "1,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "show paths set device template " + template,
        "Paths,",
        DB_SNAPSHOT + ".templ,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "show paths using device template " + template,
        "Paths,",
        DB_SNAPSHOT + ".templ,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "select s1,s2 from " + DB_SNAPSHOT + ".a",
        "Time," + DB_SNAPSHOT + ".a.s1," + DB_SNAPSHOT + ".a.s2,",
        "1,20,2.0,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "show devices " + DB_SNAPSHOT + ".a",
        "Device,IsAligned,Template,TTL(ms),",
        DB_SNAPSHOT + ".a,true,null,INF,");
    assertAllRows(
        TREE_SQL_DIALECT,
        "select ts,ext,tail from " + DB_SNAPSHOT + ".templ",
        "Time,"
            + DB_SNAPSHOT
            + ".templ.ts,"
            + DB_SNAPSHOT
            + ".templ.ext,"
            + DB_SNAPSHOT
            + ".templ.tail,",
        "1,31,3.1,null,",
        "2,null,null,true,");
    assertAllRows(
        TREE_SQL_DIALECT, "count timeseries " + DB_SNAPSHOT + ".**", "count(timeseries),", "38,");
  }
}
