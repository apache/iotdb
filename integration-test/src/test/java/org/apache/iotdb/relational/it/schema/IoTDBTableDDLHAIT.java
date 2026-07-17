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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBTableDDLHAIT {

  private final Logger LOGGER = LoggerFactory.getLogger(IoTDBTableDDLHAIT.class);

  private final String databaseName = "test_table_ddl_ha";
  private final String tableName = "table_ddl_ha";
  private final String createdAfterDownTableName = "table_ddl_ha_created_after_down";

  private final String sourceTableName = "SOURCE_TABLE";
  private final String writableViewName = "view1";
  private final String renamedWritableViewName = "view1_modify";
  private final String sameWritableViewName = "same_view2";

  private static void initCluster() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2);

    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setMetadataLeaseFenceMs(20000);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  private static void cleanCluster() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void preTableData(Statement statement, String databaseName, String tableName)
      throws SQLException {
    statement.execute("CREATE DATABASE " + databaseName);
    statement.execute("USE " + databaseName);
    statement.execute("CREATE TABLE " + tableName + " (dev STRING TAG, s1 INT32 FIELD)");
    statement.execute(
        "INSERT INTO "
            + tableName
            + "(time, dev, s1) VALUES(1, 'dev01', 1), (2, 'dev02', 2), (3, 'dev03', 3)");
    // ready for the drop database
    statement.execute("CREATE TABLE TABLE1 (dev STRING TAG, s1 INT32 FIELD)");
    statement.execute(
        "INSERT INTO TABLE1 (time, dev, s1) VALUES(1, 'dev01', 1), (2, 'dev02', 2), (3, 'dev03', 3)");

    // ready for the drop database
    statement.execute(
        "CREATE TABLE " + sourceTableName + " (dev STRING TAG, s1 INT32 FIELD, s4 INT32 FIELD)");
    statement.execute(
        "INSERT INTO "
            + sourceTableName
            + " (time, dev, s1) VALUES(1, 'dev01', 1), (2, 'dev02', 2), (3, 'dev03', 3)");
  }

  @Test
  public void testHAWithOneDataNodeIsDown() throws Exception {
    initCluster();
    try {
      final DataNodeWrapper liveDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      final DataNodeWrapper victimDataNode = EnvFactory.getEnv().getDataNodeWrapper(2);
      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      liveDataNode,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        preTableData(statement, databaseName, tableName);

        // Take one DataNode down. Its last successful ConfigNode contact is now frozen; after
        // T_proceed the ConfigNode can treat it as self-fenced and stop waiting for its ack.
        victimDataNode.stop();
        Assert.assertFalse("victim DataNode should be stopped", victimDataNode.isAlive());

        executeSpecificSql(databaseName, tableName, statement, createdAfterDownTableName);
        testDropDeviceForTreeModel(statement);
      }
    } finally {
      cleanCluster();
    }
  }

  @Test
  public void testHAWithOneDataNodeIsReadOnly() throws Exception {
    initCluster();
    try {
      final DataNodeWrapper liveDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      final DataNodeWrapper victimDataNode = EnvFactory.getEnv().getDataNodeWrapper(2);

      // Prepare data first so the table exists on all DataNodes.
      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      liveDataNode,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        preTableData(statement, databaseName, tableName);

        // try to set the DN to readOnly status
        try (final Connection victimConn =
                EnvFactory.getEnv()
                    .getConnection(
                        victimDataNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TABLE_SQL_DIALECT);
            final Statement victimStmt = victimConn.createStatement()) {

          victimStmt.execute("SET SYSTEM TO READONLY ON LOCAL");

          EnvFactory.getEnv()
              .ensureNodeStatus(
                  Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(2)),
                  Collections.singletonList(NodeStatus.ReadOnly));
        }

        // start to test
        // Run DDL HA tests via live DataNode. Operations should still succeed because
        // consensus can proceed with 2/3 nodes, and the ReadOnly victim still accepts
        // committed entries as a follower.
        executeSpecificSql(databaseName, tableName, statement, createdAfterDownTableName);
      }

    } finally {
      cleanCluster();
    }
  }

  public void executeSpecificSql(
      String databaseName, String tableName, Statement statement, String createdAfterDownTableName)
      throws Exception {

    // Take one DataNode down. Its last successful ConfigNode contact is now frozen; after
    // T_proceed the ConfigNode can treat it as self-fenced and stop waiting for its ack.

    // The DDL broadcast can no longer reach the stopped DataNode. Previously this hard-failed;
    // now it must still succeed (after blocking ~T_proceed while the fence is proven).
    LOGGER.info("0. start to test high availability of creating table procedure");
    assertStatementEffect(
        statement,
        "CREATE TABLE "
            + createdAfterDownTableName
            + " (region STRING TAG, temperature FLOAT FIELD)",
        () -> tableExists(statement, createdAfterDownTableName),
        "CREATE TABLE must succeed with one DataNode down");

    LOGGER.info("1. start to test high availability of adding column procedure");
    assertStatementEffect(
        statement,
        "ALTER TABLE " + tableName + " ADD COLUMN s2 INT32 FIELD",
        () -> columnHasType(statement, tableName, "s2", "INT32"),
        "ADD COLUMN must succeed with one DataNode down");

    LOGGER.info("2. start to test high availability of altering column type procedure");
    assertStatementEffect(
        statement,
        "ALTER TABLE " + tableName + " ALTER COLUMN s2 SET DATA TYPE INT64",
        () -> columnHasType(statement, tableName, "s2", "INT64"),
        "ALTER COLUMN TYPE must succeed with one DataNode down");

    LOGGER.info("3. start to test high availability of altering table ttl procedure");
    assertStatementEffect(
        statement,
        "ALTER TABLE " + tableName + " SET PROPERTIES ttl = 864000",
        () -> tableHasTtl(statement, tableName, "864000"),
        "ALTER TABLE TTL must succeed with one DataNode down");

    LOGGER.info("4. start to test high availability of resetting table ttl procedure");
    assertStatementEffect(
        statement,
        "ALTER TABLE " + tableName + " SET PROPERTIES ttl = 'INF'",
        () -> tableHasTtl(statement, tableName, "INF"),
        "ALTER TABLE TTL reset must succeed with one DataNode down");

    LOGGER.info("5. start to test high availability of deleting devices procedure");
    assertStatementEffect(
        statement,
        "DELETE DEVICES FROM " + tableName + " WHERE dev = 'dev02'",
        () -> !deviceExists(statement, tableName, "dev02"),
        "DELETE DEVICES must succeed with one DataNode down");

    LOGGER.info("6. start to test high availability of dropping table procedure");
    assertStatementEffect(
        statement,
        "DROP TABLE " + tableName,
        () -> !tableExists(statement, tableName),
        "DROP TABLE must succeed with one DataNode down");

    // writable view tests
    writableViewRelated(statement);
    LOGGER.info("17. start to test high availability of dropping database procedure");
    assertStatementEffect(
        statement,
        "DROP DATABASE " + databaseName,
        () -> !databaseExists(statement, databaseName),
        "DROP DATABASE must succeed with one DataNode down");
  }

  private void testDropDeviceForTreeModel(Statement statement) throws Exception {
    final String treeDevicePath = "root.db" + ".dev01";
    LOGGER.info("18. start to test high availability of deleting tree model device procedure");
    statement.execute("SET SQL_DIALECT=tree");
    assertStatementEffect(
        statement,
        "INSERT INTO " + treeDevicePath + "(s1, s2, s3) VALUES(1, 2, 3)",
        () ->
            timeSeriesExists(statement, treeDevicePath + ".s1")
                && timeSeriesExists(statement, treeDevicePath + ".s2")
                && timeSeriesExists(statement, treeDevicePath + ".s3"),
        "Tree model device data must be writable with one DataNode down");
    assertStatementEffect(
        statement,
        "DELETE TIMESERIES " + treeDevicePath + ".**",
        () ->
            !timeSeriesExists(statement, treeDevicePath + ".s1")
                && !timeSeriesExists(statement, treeDevicePath + ".s2")
                && !timeSeriesExists(statement, treeDevicePath + ".s3"),
        "DELETE TIMESERIES must delete the tree model device with one DataNode down");
  }

  private void writableViewRelated(Statement statement) throws Exception {
    // ---------------------------------for writable view---------------------------------
    LOGGER.info("7. start to test high availability of creating writable view procedure");
    assertStatementEffect(
        statement,
        "CREATE WRITABLE VIEW "
            + writableViewName
            + " AS SELECT dev, s1 AS s1_view FROM "
            + sourceTableName
            + " WITH (schema_cascade=true)",
        () ->
            tableExists(statement, writableViewName)
                && columnHasType(statement, writableViewName, "s1_view", "INT32"),
        "CREATE WRITABLE VIEW must succeed with one DataNode down");
    statement.execute(
        "INSERT INTO " + writableViewName + "(time, dev, s1_view) VALUES(10, 'dev01', 100)");
    assertColumnValue(
        statement,
        "SELECT s1_view FROM " + writableViewName + " WHERE time = 10 AND dev = 'dev01'",
        "100",
        "Writable view must return the inserted value");
    assertColumnValue(
        statement,
        "SELECT s1 FROM " + sourceTableName + " WHERE time = 10 AND dev = 'dev01'",
        "100",
        "Source table must contain the value inserted through the writable view");

    LOGGER.info("8. start to test high availability of renaming writable view procedure");
    assertStatementEffect(
        statement,
        "ALTER VIEW " + writableViewName + " RENAME TO " + renamedWritableViewName,
        () ->
            !tableExists(statement, writableViewName)
                && tableExists(statement, renamedWritableViewName)
                && tableExists(statement, sourceTableName),
        "RENAME WRITABLE VIEW must succeed with one DataNode down");
    statement.execute(
        "INSERT INTO " + renamedWritableViewName + "(time, dev, s1_view) VALUES(11, 'dev01', 101)");
    assertColumnValue(
        statement,
        "SELECT s1_view FROM " + renamedWritableViewName + " WHERE time = 11 AND dev = 'dev01'",
        "101",
        "Renamed writable view must remain writable");
    assertColumnValue(
        statement,
        "SELECT s1 FROM " + sourceTableName + " WHERE time = 11 AND dev = 'dev01'",
        "101",
        "Renamed writable view must write to the unchanged source table");

    LOGGER.info(
        "9. start to test high availability of altering writable view column type procedure");
    assertStatementEffect(
        statement,
        "ALTER TABLE " + renamedWritableViewName + " ALTER COLUMN s1_view SET DATA TYPE INT64",
        () ->
            columnHasType(statement, renamedWritableViewName, "s1_view", "INT64")
                && columnHasType(statement, sourceTableName, "s1", "INT64"),
        "ALTER WRITABLE VIEW COLUMN TYPE must be synchronized to the source table");
    statement.execute(
        "INSERT INTO "
            + renamedWritableViewName
            + "(time, dev, s1_view) VALUES(12, 'dev01', 3000000000)");
    assertColumnValue(
        statement,
        "SELECT s1_view FROM " + renamedWritableViewName + " WHERE time = 12 AND dev = 'dev01'",
        "3000000000",
        "Writable view must accept the altered INT64 column type");
    assertColumnValue(
        statement,
        "SELECT s1 FROM " + sourceTableName + " WHERE time = 12 AND dev = 'dev01'",
        "3000000000",
        "Source table must contain the altered INT64 value");

    LOGGER.info("10. start to test high availability of adding writable view column procedure");
    assertStatementEffect(
        statement,
        "ALTER VIEW " + renamedWritableViewName + " ADD COLUMN s3 INT64 FIELD",
        () ->
            columnHasType(statement, renamedWritableViewName, "s3", "INT64")
                && columnHasType(statement, sourceTableName, "s3", "INT64"),
        "ADD WRITABLE VIEW COLUMN must be synchronized to the source table");
    statement.execute(
        "INSERT INTO "
            + renamedWritableViewName
            + "(time, dev, s1_view, s3) VALUES(13, 'dev01', 3000000000, 300)");
    assertColumnValue(
        statement,
        "SELECT s3 FROM " + renamedWritableViewName + " WHERE time = 13 AND dev = 'dev01'",
        "300",
        "Writable view must accept writes to the cascaded column");
    assertColumnValue(
        statement,
        "SELECT s3 FROM " + sourceTableName + " WHERE time = 13 AND dev = 'dev01'",
        "300",
        "Source table must contain the cascaded column value");

    final String nonCascadeViewName = "non_cascade_view";
    final String nonCascadeMissingViewName = "non_cascade_missing_view";
    statement.execute(
        "CREATE WRITABLE VIEW "
            + nonCascadeViewName
            + " AS SELECT dev, s1 AS s1_view FROM "
            + sourceTableName
            + " WITH (schema_cascade=false)");
    assertStatementEffect(
        statement,
        "ALTER VIEW " + nonCascadeViewName + " ADD COLUMN s3 AS s3_view",
        () ->
            columnHasType(statement, nonCascadeViewName, "s3_view", "INT64")
                && columnMapsTo(statement, nonCascadeViewName, "s3_view", "s3"),
        "Non-cascading writable view must map an existing source column");
    statement.execute(
        "INSERT INTO "
            + nonCascadeViewName
            + "(time, dev, s1_view, s3_view) VALUES(14, 'dev01', 3000000001, 301)");
    assertColumnValue(
        statement,
        "SELECT s3_view FROM " + nonCascadeViewName + " WHERE time = 14 AND dev = 'dev01'",
        "301",
        "Non-cascading writable view must return the mapped value");
    assertColumnValue(
        statement,
        "SELECT s3 FROM " + sourceTableName + " WHERE time = 14 AND dev = 'dev01'",
        "301",
        "Non-cascading writable view must write to the existing source column");

    statement.execute(
        "CREATE WRITABLE VIEW "
            + nonCascadeMissingViewName
            + " AS SELECT dev, s1 AS s1_view FROM "
            + sourceTableName
            + " WITH (schema_cascade=false)");
    assertStatementFailure(
        statement,
        "ALTER VIEW " + nonCascadeMissingViewName + " ADD COLUMN s5 INT64 FIELD",
        "Non-cascading writable view must reject a source column that does not exist",
        "source",
        "s5");
    Assert.assertFalse(columnExists(statement, nonCascadeMissingViewName, "s5"));
    Assert.assertFalse(columnExists(statement, sourceTableName, "s5"));
    statement.execute("DROP VIEW " + nonCascadeMissingViewName);
    statement.execute("DROP VIEW " + nonCascadeViewName);

    LOGGER.info("11. start to test high availability of renaming writable view column procedure");
    assertStatementEffect(
        statement,
        "ALTER VIEW " + renamedWritableViewName + " RENAME COLUMN s3 TO s3_modfify",
        () ->
            columnHasType(statement, renamedWritableViewName, "s3_modfify", "INT64")
                && !columnExists(statement, renamedWritableViewName, "s3")
                && columnHasType(statement, sourceTableName, "s3", "INT64")
                && !columnExists(statement, sourceTableName, "s3_modfify")
                && columnMapsTo(statement, renamedWritableViewName, "s3_modfify", "s3"),
        "RENAME WRITABLE VIEW COLUMN must update the view mapping without renaming the source column");
    statement.execute(
        "INSERT INTO "
            + renamedWritableViewName
            + "(time, dev, s1_view, s3_modfify) VALUES(15, 'dev01', 3000000002, 302)");
    assertColumnValue(
        statement,
        "SELECT s3_modfify FROM " + renamedWritableViewName + " WHERE time = 15 AND dev = 'dev01'",
        "302",
        "Renamed writable view column must remain writable");
    assertColumnValue(
        statement,
        "SELECT s3 FROM " + sourceTableName + " WHERE time = 15 AND dev = 'dev01'",
        "302",
        "Renamed writable view column must write to the original source column");
    assertStatementFailure(
        statement,
        "SELECT s3 FROM " + renamedWritableViewName,
        "Old writable view column name must no longer be queryable",
        "s3");

    LOGGER.info("12. start to test high availability of altering writable view ttl procedure");
    assertStatementEffect(
        statement,
        "ALTER VIEW " + renamedWritableViewName + " SET PROPERTIES ttl = 864000",
        () ->
            tableHasTtl(statement, renamedWritableViewName, "864000")
                && tableHasTtl(statement, sourceTableName, "864000"),
        "ALTER WRITABLE VIEW TTL must be synchronized to the source table");
    statement.execute(
        "INSERT INTO "
            + renamedWritableViewName
            + "(dev, s1_view, s3_modfify) VALUES('dev01', 3000000003, 303)");
    assertColumnValue(
        statement,
        "SELECT s3_modfify FROM " + renamedWritableViewName + " WHERE dev = 'dev01'",
        "303",
        "Writable view must remain usable after the TTL update");
    assertColumnValue(
        statement,
        "SELECT s3 FROM " + sourceTableName + " WHERE dev = 'dev01'",
        "303",
        "Source table must remain synchronized after the TTL update");

    LOGGER.info("13. start to test high availability of resetting writable view ttl procedure");
    assertStatementEffect(
        statement,
        "ALTER VIEW " + renamedWritableViewName + " SET PROPERTIES ttl = 'INF'",
        () ->
            tableHasTtl(statement, renamedWritableViewName, "INF")
                && tableHasTtl(statement, sourceTableName, "INF"),
        "Resetting WRITABLE VIEW TTL must be synchronized to the source table");

    LOGGER.info("14. start to test high availability of creating same writable view procedure");
    assertStatementEffect(
        statement,
        "CREATE WRITABLE VIEW "
            + sameWritableViewName
            + " AS SELECT * FROM "
            + sourceTableName
            + " WITH (schema_cascade=true)",
        () ->
            tableExists(statement, sameWritableViewName)
                && columnHasType(statement, sameWritableViewName, "s1", "INT64")
                && columnHasType(statement, sameWritableViewName, "s3", "INT64")
                && columnHasType(statement, sameWritableViewName, "s4", "INT32")
                && !columnExists(statement, renamedWritableViewName, "s4"),
        "CREATE same writable view must succeed with one DataNode down");

    LOGGER.info("15. start to test high availability of dropping writable view procedure");
    assertStatementEffect(
        statement,
        "DROP VIEW " + renamedWritableViewName,
        () ->
            !tableExists(statement, renamedWritableViewName)
                && tableExists(statement, sourceTableName)
                && tableExists(statement, sameWritableViewName)
                && columnExists(statement, sourceTableName, "s4"),
        "Dropping a writable view with incomplete source-column coverage must keep the source table");

    LOGGER.info("16. start to test high availability of dropping same writable view procedure");
    assertStatementEffect(
        statement,
        "DROP VIEW " + sameWritableViewName,
        () ->
            !tableExists(statement, sameWritableViewName)
                && !tableExists(statement, sourceTableName),
        "Dropping a writable view with complete source-column coverage must drop the source table");
  }

  private void assertStatementEffect(
      final Statement statement,
      final String sql,
      final Callable<Boolean> effect,
      final String message)
      throws Exception {
    statement.execute(sql);
    assertTrue(message, effect.call());
  }

  private boolean tableExists(final Statement statement, final String tableName) throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SHOW TABLES")) {
      while (resultSet.next()) {
        if (tableName.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean columnHasType(
      final Statement statement,
      final String tableName,
      final String columnName,
      final String dataType)
      throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("DESCRIBE " + tableName)) {
      while (resultSet.next()) {
        if (columnName.equalsIgnoreCase(resultSet.getString(1))) {
          return dataType.equalsIgnoreCase(resultSet.getString(2));
        }
      }
    }
    return false;
  }

  private boolean columnExists(
      final Statement statement, final String tableName, final String columnName) throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("DESCRIBE " + tableName)) {
      while (resultSet.next()) {
        if (columnName.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean columnMapsTo(
      final Statement statement,
      final String viewName,
      final String viewColumnName,
      final String sourceColumnName)
      throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("DESCRIBE " + viewName + " DETAILS")) {
      while (resultSet.next()) {
        if (viewColumnName.equalsIgnoreCase(resultSet.getString(1))) {
          return sourceColumnName.equalsIgnoreCase(resultSet.getString(6));
        }
      }
    }
    return false;
  }

  private boolean columnValueEquals(
      final Statement statement, final String sql, final String expectedValue) throws Exception {
    try (final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        if (expectedValue.equals(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }

  private void assertColumnValue(
      final Statement statement, final String sql, final String expectedValue, final String message)
      throws Exception {
    Assert.assertTrue(message, columnValueEquals(statement, sql, expectedValue));
  }

  private void assertStatementFailure(
      final Statement statement,
      final String sql,
      final String message,
      final String... expectedMessageFragments)
      throws Exception {
    try {
      statement.execute(sql);
      Assert.fail(message);
    } catch (final SQLException e) {
      final String errorMessage = String.valueOf(e.getMessage()).toLowerCase();
      for (final String expectedMessageFragment : expectedMessageFragments) {
        Assert.assertTrue(
            message + ": " + e.getMessage(),
            errorMessage.contains(expectedMessageFragment.toLowerCase()));
      }
    }
  }

  private boolean timeSeriesExists(final Statement statement, final String path) throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES " + path)) {
      return resultSet.next();
    }
  }

  private boolean tableHasTtl(final Statement statement, final String tableName, final String ttl)
      throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SHOW TABLES")) {
      while (resultSet.next()) {
        if (tableName.equalsIgnoreCase(resultSet.getString(1))) {
          return ttl.equalsIgnoreCase(resultSet.getString(2));
        }
      }
    }
    return false;
  }

  private boolean deviceExists(
      final Statement statement, final String tableName, final String device) throws Exception {
    try (final ResultSet resultSet =
        statement.executeQuery(
            "SHOW DEVICES FROM " + tableName + " WHERE dev = '" + device + "'")) {
      return resultSet.next();
    }
  }

  private boolean databaseExists(final Statement statement, final String databaseName)
      throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        if (databaseName.equalsIgnoreCase(resultSet.getString(1))) {
          return true;
        }
      }
    }
    return false;
  }
}
