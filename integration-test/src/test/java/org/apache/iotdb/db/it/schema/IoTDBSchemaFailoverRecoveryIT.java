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

import org.apache.iotdb.commons.utils.KillPoint.MetadataTestKillPoints;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.schema.DeactivateTemplateState;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.util.AbstractSchemaIT;
import org.apache.iotdb.util.MetadataRecoveryTestUtils.Gate;

import org.apache.tsfile.enums.TSDataType;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.util.MetadataRecoveryTestUtils.TIMESERIES_HEADER;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.assertRows;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.connection;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.execute;
import static org.apache.iotdb.util.MetadataRecoveryTestUtils.schemaRegion;
import static org.junit.Assert.*;

/** P1-1/2/6: metadata payloads across real failover and SchemaRegion migration boundaries. */
@Category(ClusterIT.class)
public class IoTDBSchemaFailoverRecoveryIT extends AbstractSchemaIT {
  public IoTDBSchemaFailoverRecoveryIT(SchemaTestMode mode) {
    super(mode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3)
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(2);
    EnvFactory.getEnv().initClusterEnvironment(3, 4);
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Test
  public void testConcurrentAutoCreationAcrossConfigNodeFailover() throws Exception {
    final String database = "root.p1_auto";
    try (Connection connection = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
      execute(connection, "CREATE DATABASE " + database);
      // Allocate a real SchemaRegion before the two writers race for the missing measurement.
      execute(connection, "CREATE TIMESERIES " + database + ".seed.s INT32");
      raceAutoCreation(connection, database + ".same", false);
      raceAutoCreation(connection, database + ".conflict", true);
    } finally {
      try (Connection connection = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
        execute(connection, "DELETE DATABASE " + database);
      }
    }
  }

  private void raceAutoCreation(Connection control, String device, boolean conflict)
      throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    ConfigNodeWrapper oldLeader = EnvFactory.getEnv().getConfigNodeWrapper(leader);
    try (Gate first =
            new Gate(node(0), MetadataTestKillPoints.BEFORE_AUTO_CREATE_TIMESERIES, true);
        Gate second =
            new Gate(node(1), MetadataTestKillPoints.BEFORE_AUTO_CREATE_TIMESERIES, true)) {
      CyclicBarrier start = new CyclicBarrier(2);
      List<Future<Boolean>> results = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        final int writer = i;
        results.add(
            workers.submit(
                () -> {
                  try (Session session =
                      new Session.Builder()
                          .host(node(writer).getIp())
                          .port(node(writer).getPort())
                          .username(SessionConfig.DEFAULT_USER)
                          .password(SessionConfig.DEFAULT_PASSWORD)
                          .enableRedirection(false)
                          .build()) {
                    session.open();
                    start.await(30, TimeUnit.SECONDS);
                    try {
                      session.insertRecord(
                          device,
                          writer + 1L,
                          Collections.singletonList("s"),
                          Collections.singletonList(
                              conflict && writer == 1 ? TSDataType.TEXT : TSDataType.INT32),
                          Collections.singletonList(
                              conflict && writer == 1 ? "not_an_integer" : (Object) (writer + 11)));
                      return true;
                    } catch (StatementExecutionException e) {
                      if (!conflict
                          || (e.getStatusCode() != TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode()
                              && e.getStatusCode() != TSStatusCode.METADATA_ERROR.getStatusCode())
                          || !e.getMessage().contains(device + ".s")
                          || !e.getMessage().contains("INT32")
                          || !e.getMessage().contains("TEXT")
                          || (e.getStatusCode() == TSStatusCode.METADATA_ERROR.getStatusCode()
                              && !e.getMessage()
                                  .contains(DataTypeMismatchException.REGISTERED_TYPE_STRING))) {
                        throw e;
                      }
                      return false;
                    }
                  }
                }));
      }
      first.await();
      second.await();
      assertFalse(results.get(0).isDone());
      assertFalse(results.get(1).isDone());
      oldLeader.stopForcibly();
      assertNotEquals(leader, EnvFactory.getEnv().getLeaderConfigNodeIndex());
      first.release();
      second.release();
      List<String> expected = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        if (results.get(i).get(90, TimeUnit.SECONDS)) {
          expected.add((i + 1) + "," + (conflict && i == 1 ? "not_an_integer" : i + 11) + ",");
        }
      }
      assertTrue("At least one competing definition must commit", !expected.isEmpty());
      if (!conflict) {
        assertEquals(2, expected.size());
      }
      String definition;
      try (Statement statement = control.createStatement();
          ResultSet schema = statement.executeQuery("SHOW TIMESERIES " + device + ".s")) {
        assertTrue(schema.next());
        String type = schema.getString("DataType");
        assertTrue(type, type.equals("INT32") || (conflict && type.equals("TEXT")));
        definition =
            device
                + ".s,null,"
                + schema.getString("Database")
                + ","
                + type
                + ","
                + schema.getString("Encoding")
                + ","
                + schema.getString("Compression")
                + ",null,null,null,null,BASE,";
        assertFalse(schema.next());
      }
      for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
        try (Connection read = connection(dataNode, BaseEnv.TREE_SQL_DIALECT)) {
          assertRows(read, "SHOW TIMESERIES " + device + ".s", TIMESERIES_HEADER, definition);
          assertRows(read, "COUNT TIMESERIES " + device + ".*", "count(timeseries),", "1,");
          assertRows(
              read,
              "SHOW DEVICES " + device,
              "Device,IsAligned,Template,TTL(ms),",
              device + ",false,null,INF,");
          assertRows(
              read,
              "SELECT s FROM " + device,
              "Time," + device + ".s,",
              expected.toArray(new String[0]));
        }
      }
    } finally {
      workers.shutdownNow();
      try {
        if (!oldLeader.isAlive()) {
          oldLeader.start();
          EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
        }
      } finally {
        assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  public void testExtendedTemplateAndInterruptedDeactivation() throws Exception {
    final String database = "root.p1_template";
    final String template = "p1_template";
    DataNodeWrapper stale = node(3);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    ConfigNodeWrapper oldLeader = null;
    try (Connection control = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
      execute(
          control,
          "CREATE DATABASE " + database,
          "CREATE DEVICE TEMPLATE " + template + " (s INT64 ENCODING=PLAIN)",
          "SET DEVICE TEMPLATE " + template + " TO " + database);
      List<String> devices = new ArrayList<>();
      Set<Integer> regions = new HashSet<>();
      for (int i = 0; i < 16 && regions.size() < 2; i++) {
        String device = database + ".d" + i;
        execute(control, "INSERT INTO " + device + "(time,s) VALUES(1,10)");
        devices.add(device);
        regions.add(schemaRegion(database, device + ".s"));
      }
      assertEquals("Activated devices must actually span SchemaRegions", 2, regions.size());
      try (Connection warm = connection(stale, BaseEnv.TREE_SQL_DIALECT)) {
        assertRows(
            warm, "SELECT s FROM " + devices.get(0), "Time," + devices.get(0) + ".s,", "1,10,");
      }
      stale.stopForcibly();
      // Extension is catalog-committed before broadcasting. A failed broadcast is an explicit
      // result-unknown operation, so inspect the committed definition instead of replaying ALTER.
      try {
        execute(control, "ALTER DEVICE TEMPLATE " + template + " ADD(extra INT64 ENCODING=PLAIN)");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("Failed to sync template"));
      }
      assertRows(
          control,
          "SHOW NODES IN DEVICE TEMPLATE " + template,
          "ChildNodes,DataType,Encoding,Compression,",
          "s,INT64,PLAIN,LZ4,",
          "extra,INT64,PLAIN,LZ4,");
      execute(control, "INSERT INTO " + devices.get(0) + "(time,extra) VALUES(2,20)");
      stale.start();
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
      try (Connection recovered = connection(stale, BaseEnv.TREE_SQL_DIALECT)) {
        assertRows(
            recovered,
            "SELECT extra FROM " + devices.get(0),
            "Time," + devices.get(0) + ".extra,",
            "2,20,");
        execute(recovered, "INSERT INTO " + devices.get(1) + "(time,extra) VALUES(2,21)");
      }
      int leader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
      oldLeader = EnvFactory.getEnv().getConfigNodeWrapper(leader);
      try (Gate gate =
          new Gate(oldLeader, DeactivateTemplateState.CLEAN_DATANODE_SCHEMA_CACHE, true)) {
        Future<?> deactivation =
            worker.submit(
                () -> {
                  try (Connection ddl = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
                    execute(
                        ddl,
                        "DEACTIVATE DEVICE TEMPLATE " + template + " FROM " + database + ".**");
                  }
                  return null;
                });
        gate.await();
        assertFalse("DDL must still own the persisted blacklist", deactivation.isDone());
        oldLeader.stopForcibly();
        assertNotEquals(leader, EnvFactory.getEnv().getLeaderConfigNodeIndex());
        deactivation.get(90, TimeUnit.SECONDS);
      }
      oldLeader.start();
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
      for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
        try (Connection read = connection(dataNode, BaseEnv.TREE_SQL_DIALECT)) {
          assertRows(read, "COUNT TIMESERIES " + database + ".**", "count(timeseries),", "0,");
          assertRows(read, "SHOW PATHS USING DEVICE TEMPLATE " + template, "Paths,");
          assertRows(read, "SHOW PATHS SET DEVICE TEMPLATE " + template, "Paths,", database + ",");
          assertRows(read, "SELECT s,extra FROM " + devices.get(0), "Time,");
        }
      }
      // A legal reactivation must have new data only, and no leftover blacklist may block UNSET.
      execute(control, "INSERT INTO " + devices.get(0) + "(time,extra) VALUES(3,30)");
      assertRows(
          control,
          "SELECT extra FROM " + devices.get(0),
          "Time," + devices.get(0) + ".extra,",
          "3,30,");
      execute(
          control,
          "DEACTIVATE DEVICE TEMPLATE " + template + " FROM " + database + ".**",
          "UNSET DEVICE TEMPLATE " + template + " FROM " + database,
          "DROP DEVICE TEMPLATE " + template);
    } finally {
      worker.shutdownNow();
      try {
        if (!stale.isAlive()) {
          stale.start();
        }
        if (oldLeader != null && !oldLeader.isAlive()) {
          oldLeader.start();
        }
        EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
        try (Connection cleanup = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
          execute(cleanup, "DELETE DATABASE " + database);
          try (Statement statement = cleanup.createStatement();
              ResultSet templates = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
            while (templates.next()) {
              if (template.equals(templates.getString(1))) {
                execute(cleanup, "DROP DEVICE TEMPLATE " + template);
              }
            }
          }
        }
      } finally {
        assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  public void testSchemaRegionMigrationWithMetadataChanges() throws Exception {
    String database = "root.p1_migrate";
    ConfigNodeWrapper leader =
        EnvFactory.getEnv().getConfigNodeWrapper(EnvFactory.getEnv().getLeaderConfigNodeIndex());
    DataNodeWrapper original = null;
    try (Connection control = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
      execute(
          control,
          "CREATE DATABASE " + database,
          "CREATE TIMESERIES "
              + database
              + ".d.s(old_alias) INT64 ENCODING=PLAIN COMPRESSION=LZ4 TAGS(site='old') ATTRIBUTES(owner='alice')",
          "CREATE TIMESERIES " + database + ".d.deleted INT64",
          "INSERT INTO " + database + ".d(time,s,deleted) VALUES(1,10,100)",
          "CREATE DATABASE root.p1_migrate_control",
          "CREATE TIMESERIES root.p1_migrate_control.d.s INT64 ENCODING=PLAIN COMPRESSION=LZ4 TAGS(site='control') ATTRIBUTES(owner='keep')",
          "INSERT INTO root.p1_migrate_control.d(time,s) VALUES(1,99)");
      int region = schemaRegion(database, database + ".d.s");
      Set<Integer> replicas = new HashSet<>();
      try (Statement statement = control.createStatement();
          ResultSet rs = statement.executeQuery("SHOW REGIONS")) {
        while (rs.next()) {
          if (rs.getInt("RegionId") == region) {
            assertEquals("SchemaRegion", rs.getString("Type"));
            replicas.add(rs.getInt("DataNodeId"));
          }
        }
      }
      assertEquals(3, replicas.size());
      int source = replicas.iterator().next();
      int destination = -1;
      try (Statement statement = control.createStatement();
          ResultSet rs = statement.executeQuery("SHOW DATANODES")) {
        while (rs.next()) {
          if (!replicas.contains(rs.getInt("NodeID"))) {
            destination = rs.getInt("NodeID");
            break;
          }
        }
      }
      assertTrue(destination >= 0);
      original = EnvFactory.getEnv().dataNodeIdToWrapper(source).orElseThrow();
      try (Gate gate = new Gate(leader, AddRegionPeerState.CREATE_NEW_REGION_PEER, true)) {
        execute(control, "MIGRATE REGION " + region + " FROM " + source + " TO " + destination);
        gate.await();
        // The new SchemaRegion peer exists, but has not joined consensus yet.
        execute(
            control,
            "ALTER TIMESERIES "
                + database
                + ".d.s UPSERT ALIAS=new_alias TAGS(site='new') ATTRIBUTES(owner='bob')",
            "DELETE TIMESERIES " + database + ".d.deleted",
            "CREATE TIMESERIES " + database + ".d.added INT64 ENCODING=PLAIN COMPRESSION=LZ4",
            "INSERT INTO " + database + ".d(time,s,added) VALUES(2,20,200)");
        gate.release();
      }
      final int target = destination;
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Set<Integer> actual = new HashSet<>();
                try (Statement statement = control.createStatement();
                    ResultSet rs = statement.executeQuery("SHOW REGIONS")) {
                  while (rs.next()) {
                    if (rs.getInt("RegionId") == region) {
                      assertEquals("SchemaRegion", rs.getString("Type"));
                      assertEquals("Running", rs.getString("Status"));
                      actual.add(rs.getInt("DataNodeId"));
                    }
                  }
                }
                assertEquals(3, actual.size());
                assertTrue(actual.contains(target));
                assertFalse(actual.contains(source));
              });
      original.stopForcibly();
      for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
        if (!dataNode.isAlive()) {
          continue;
        }
        try (Connection read = connection(dataNode, BaseEnv.TREE_SQL_DIALECT)) {
          String updated =
              database
                  + ".d.s,new_alias,"
                  + database
                  + ",INT64,PLAIN,LZ4,{\"site\":\"new\"},{\"owner\":\"bob\"},null,null,BASE,";
          assertRows(
              read,
              "SHOW TIMESERIES " + database + ".**",
              TIMESERIES_HEADER,
              updated,
              database
                  + ".d.added,null,"
                  + database
                  + ",INT64,PLAIN,LZ4,null,null,null,null,BASE,");
          assertRows(
              read,
              "SHOW TIMESERIES " + database + ".** WHERE TAGS(site)='new'",
              TIMESERIES_HEADER,
              updated);
          assertRows(read, "SELECT old_alias,deleted FROM " + database + ".d", "Time,");
          assertRows(
              read,
              "SELECT new_alias,added FROM " + database + ".d",
              "Time," + database + ".d.s," + database + ".d.added,",
              "1,10,null,",
              "2,20,200,");
          assertRows(
              read,
              "SHOW TIMESERIES " + database + ".** WHERE TAGS(site)='old'",
              TIMESERIES_HEADER);
          assertRows(read, "COUNT TIMESERIES " + database + ".**", "count(timeseries),", "2,");
          assertRows(
              read,
              "SHOW TIMESERIES root.p1_migrate_control.**",
              TIMESERIES_HEADER,
              "root.p1_migrate_control.d.s,null,root.p1_migrate_control,INT64,PLAIN,LZ4,{\"site\":\"control\"},{\"owner\":\"keep\"},null,null,BASE,");
          assertRows(
              read,
              "SELECT s FROM root.p1_migrate_control.d",
              "Time,root.p1_migrate_control.d.s,",
              "1,99,");
        }
      }
      try (Connection write =
          connection(
              EnvFactory.getEnv().dataNodeIdToWrapper(destination).orElseThrow(),
              BaseEnv.TREE_SQL_DIALECT)) {
        execute(write, "INSERT INTO " + database + ".d(time,new_alias) VALUES(3,30)");
        assertRows(
            write,
            "SELECT s FROM " + database + ".d",
            "Time," + database + ".d.s,",
            "1,10,",
            "2,20,",
            "3,30,");
      }
    } finally {
      if (original != null && !original.isAlive()) {
        original.start();
      }
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
      try (Connection cleanup = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
        execute(cleanup, "DELETE DATABASE " + database, "DELETE DATABASE root.p1_migrate_control");
      }
    }
  }

  private static DataNodeWrapper node(int index) {
    return EnvFactory.getEnv().getDataNodeWrapper(index);
  }
}
