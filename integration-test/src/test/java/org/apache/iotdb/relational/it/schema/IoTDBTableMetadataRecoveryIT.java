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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.util.MetadataRecoveryTestUtils.*;
import static org.junit.Assert.*;

/** P1-4/5: a previously warmed DataNode misses catalog broadcasts and then serves clients again. */
@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class IoTDBTableMetadataRecoveryIT {
  @BeforeClass
  public static void before() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaEngineMode("Memory")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3);
    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setMetadataLeaseFenceMs(20000);
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @AfterClass
  public static void after() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDatabaseTtlInheritanceAfterMissedBroadcast() throws Exception {
    String database = "p1_ttl";
    long day = TimeUnit.DAYS.toMillis(1);
    long old = System.currentTimeMillis() - 2 * day;
    long recent = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1);
    DataNodeWrapper stale = node(2);
    try (Connection healthy = connection(node(0), BaseEnv.TABLE_SQL_DIALECT)) {
      execute(
          healthy,
          "CREATE DATABASE " + database + " WITH (ttl=" + 7 * day + ")",
          "USE " + database,
          "CREATE TABLE inherited(dev STRING TAG,v INT32 FIELD)",
          "CREATE TABLE overridden(dev STRING TAG,v INT32 FIELD) WITH (ttl=" + 30 * day + ")");
      for (String table : new String[] {"inherited", "overridden"}) {
        execute(
            healthy,
            "INSERT INTO "
                + table
                + "(time,dev,v) VALUES("
                + old
                + ",'d',1),("
                + recent
                + ",'d',2)");
      }
      for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
        try (Connection warm = connection(dataNode, BaseEnv.TABLE_SQL_DIALECT)) {
          execute(warm, "USE " + database);
          assertRows(
              warm,
              "SELECT CAST(time AS INT64) AS ts,dev,v FROM inherited",
              "ts,dev,v,",
              old + ",d,1,",
              recent + ",d,2,");
        }
      }
      stale.stopForcibly();
      assertFalse(stale.isAlive());
      execute(healthy, "ALTER DATABASE " + database + " SET PROPERTIES ttl=" + day);
      assertTtls(healthy, day, 30 * day);
      assertRows(
          healthy,
          "SELECT CAST(time AS INT64) AS ts,dev,v FROM inherited",
          "ts,dev,v,",
          recent + ",d,2,");
      assertRows(
          healthy,
          "SELECT CAST(time AS INT64) AS ts,dev,v FROM overridden",
          "ts,dev,v,",
          old + ",d,1,",
          recent + ",d,2,");
      stale.start();
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
      try (Connection recovered = connection(stale, BaseEnv.TABLE_SQL_DIALECT)) {
        execute(recovered, "USE " + database);
        // Check recovery before doing any new write; another DDL or cache clear must not repair it.
        assertTtls(recovered, day, 30 * day);
        assertRows(
            recovered,
            "SELECT CAST(time AS INT64) AS ts,dev,v FROM inherited",
            "ts,dev,v,",
            recent + ",d,2,");
        assertRows(
            recovered,
            "SELECT CAST(time AS INT64) AS ts,dev,v FROM overridden",
            "ts,dev,v,",
            old + ",d,1,",
            recent + ",d,2,");
        try {
          execute(recovered, "INSERT INTO inherited(time,dev,v) VALUES(" + (old + 1) + ",'d',3)");
          fail("Recovered node must enforce the new inherited TTL");
        } catch (SQLException e) {
          assertEquals(e.getMessage(), TSStatusCode.OUT_OF_TTL.getStatusCode(), e.getErrorCode());
        }
        execute(
            recovered,
            "INSERT INTO overridden(time,dev,v) VALUES(" + (old + 1) + ",'d',3)",
            "INSERT INTO inherited(time,dev,v) VALUES(" + (recent + 1) + ",'d',4)");
      }
      assertRows(
          healthy,
          "SELECT CAST(time AS INT64) AS ts,dev,v FROM inherited",
          "ts,dev,v,",
          recent + ",d,2,",
          (recent + 1) + ",d,4,");
      assertRows(
          healthy,
          "SELECT CAST(time AS INT64) AS ts,dev,v FROM overridden",
          "ts,dev,v,",
          old + ",d,1,",
          (old + 1) + ",d,3,",
          recent + ",d,2,");
    } finally {
      if (!stale.isAlive()) {
        stale.start();
        EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
      }
      try (Connection cleanup = connection(node(0), BaseEnv.TABLE_SQL_DIALECT)) {
        execute(cleanup, "DROP DATABASE IF EXISTS " + database);
      }
    }
  }

  private void assertTtls(Connection connection, long inherited, long overridden)
      throws SQLException {
    Map<String, Long> actual = new HashMap<>();
    try (Statement statement = connection.createStatement();
        ResultSet tables = statement.executeQuery("SHOW TABLES")) {
      while (tables.next()) {
        assertNull(actual.put(tables.getString(1), Long.parseLong(tables.getString(2))));
      }
    }
    Map<String, Long> expected = new HashMap<>();
    expected.put("inherited", inherited);
    expected.put("overridden", overridden);
    assertEquals(expected, actual);
  }

  @Test
  public void testViewAutoSyncAfterMissedRenameReplaceAndDisable() throws Exception {
    String treeDb = "root.p1_views";
    String viewDb = "p1_views";
    DataNodeWrapper stale = node(2);
    try (Connection tree = connection(node(0), BaseEnv.TREE_SQL_DIALECT);
        Connection table = connection(node(0), BaseEnv.TABLE_SQL_DIALECT)) {
      execute(
          tree,
          "CREATE DATABASE " + treeDb,
          "CREATE TIMESERIES " + treeDb + ".old.d.seed INT32",
          "INSERT INTO " + treeDb + ".old.d(time,seed) VALUES(1,1)");
      execute(
          table,
          "CREATE DATABASE " + viewDb,
          "USE " + viewDb,
          "CREATE VIEW v(dev STRING TAG) WITH (auto_sync_schema=true) AS " + treeDb + ".old.**",
          "CREATE VIEW disabled(dev STRING TAG) WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".old.**");
      try (Connection warmTree = connection(stale, BaseEnv.TREE_SQL_DIALECT);
          Connection warmTable = connection(stale, BaseEnv.TABLE_SQL_DIALECT)) {
        execute(warmTable, "USE " + viewDb);
        assertRows(
            warmTable,
            "SELECT CAST(time AS INT64) AS ts,dev,seed FROM v",
            "ts,dev,seed,",
            "1,d,1,");
        // Creating a measurement on this node warms its view matching index as well as the query
        // cache.
        execute(warmTree, "CREATE TIMESERIES " + treeDb + ".old.d.warm INT32");
        awaitColumns(
            warmTable,
            "v",
            "time:TIMESTAMP:TIME",
            "dev:STRING:TAG",
            "seed:INT32:FIELD",
            "warm:INT32:FIELD");
      }
      stale.stopForcibly();
      assertFalse(stale.isAlive());
      execute(
          table,
          "ALTER VIEW v RENAME TO renamed",
          "CREATE OR REPLACE VIEW renamed(dev STRING TAG) WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".current.**",
          "ALTER VIEW disabled SET PROPERTIES auto_sync_schema=false");
      execute(
          tree,
          "CREATE TIMESERIES " + treeDb + ".current.d.during INT32",
          "INSERT INTO " + treeDb + ".current.d(time,during) VALUES(2,2)",
          "CREATE TIMESERIES " + treeDb + ".old.d.ignored_during INT32");
      awaitColumns(table, "renamed", "time:TIMESTAMP:TIME", "dev:STRING:TAG", "during:INT32:FIELD");
      stale.start();
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
      try (Connection recoveredTable = connection(stale, BaseEnv.TABLE_SQL_DIALECT);
          Connection recoveredTree = connection(stale, BaseEnv.TREE_SQL_DIALECT)) {
        execute(recoveredTable, "USE " + viewDb);
        awaitColumns(
            recoveredTable,
            "renamed",
            "time:TIMESTAMP:TIME",
            "dev:STRING:TAG",
            "during:INT32:FIELD");
        assertRows(
            recoveredTable,
            "SELECT CAST(time AS INT64) AS ts,dev,during FROM renamed",
            "ts,dev,during,",
            "2,d,2,");
        execute(
            recoveredTree,
            "CREATE TIMESERIES " + treeDb + ".current.d.after INT64",
            "INSERT INTO " + treeDb + ".current.d(time,after) VALUES(3,3)",
            "CREATE TIMESERIES " + treeDb + ".old.d.ignored_after BOOLEAN");
        awaitColumns(
            recoveredTable,
            "renamed",
            "time:TIMESTAMP:TIME",
            "dev:STRING:TAG",
            "during:INT32:FIELD",
            "after:INT64:FIELD");
        assertColumns(
            recoveredTable,
            "disabled",
            "time:TIMESTAMP:TIME",
            "dev:STRING:TAG",
            "seed:INT32:FIELD",
            "warm:INT32:FIELD");
        assertMissingView(recoveredTable, "v");
      }
      for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
        try (Connection read = connection(dataNode, BaseEnv.TABLE_SQL_DIALECT)) {
          execute(read, "USE " + viewDb);
          awaitColumns(
              read,
              "renamed",
              "time:TIMESTAMP:TIME",
              "dev:STRING:TAG",
              "during:INT32:FIELD",
              "after:INT64:FIELD");
          assertColumns(
              read,
              "disabled",
              "time:TIMESTAMP:TIME",
              "dev:STRING:TAG",
              "seed:INT32:FIELD",
              "warm:INT32:FIELD");
          assertMissingView(read, "v");
          assertRows(
              read,
              "SELECT CAST(time AS INT64) AS ts,dev,during,after FROM renamed",
              "ts,dev,during,after,",
              "2,d,2,null,",
              "3,d,null,3,");
          assertRows(
              read,
              "SELECT CAST(time AS INT64) AS ts,dev,seed FROM disabled",
              "ts,dev,seed,",
              "1,d,1,");
        }
      }
    } finally {
      if (!stale.isAlive()) {
        stale.start();
        EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
      }
      try (Connection table = connection(node(0), BaseEnv.TABLE_SQL_DIALECT);
          Connection tree = connection(node(0), BaseEnv.TREE_SQL_DIALECT)) {
        execute(table, "DROP DATABASE IF EXISTS " + viewDb);
        execute(tree, "DELETE DATABASE " + treeDb);
      }
    }
  }

  static void awaitColumns(Connection connection, String table, String... columns) {
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertColumns(connection, table, columns));
  }

  static void assertColumns(Connection connection, String table, String... columns)
      throws SQLException {
    Map<String, String> actual = new HashMap<>();
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("DESCRIBE " + table)) {
      while (result.next()) {
        assertNull(
            actual.put(
                result.getString("ColumnName"),
                result.getString("DataType") + ":" + result.getString("Category")));
      }
    }
    Map<String, String> expected = new HashMap<>();
    for (String column : columns) {
      String[] parts = column.split(":", 2);
      expected.put(parts[0], parts[1]);
    }
    assertEquals(expected, actual);
  }

  private void assertMissingView(Connection connection, String view) throws SQLException {
    try {
      execute(connection, "DESCRIBE " + view);
      fail("Old view name must not remain in the cache");
    } catch (SQLException e) {
      assertEquals(e.getMessage(), TSStatusCode.TABLE_NOT_EXISTS.getStatusCode(), e.getErrorCode());
    }
  }

  private static DataNodeWrapper node(int index) {
    return EnvFactory.getEnv().getDataNodeWrapper(index);
  }
}
