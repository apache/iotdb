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

  private static void initCluster() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMetadataLeaseFenceMs(20000)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3);
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
  }

  @Test
  public void testHAWithOneDataNodeIsDown() throws Exception {
    initCluster();
    try {
      final DataNodeWrapper liveDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      final DataNodeWrapper victimDataNode = EnvFactory.getEnv().getDataNodeWrapper(2);
      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnection(liveDataNode, "root", "root", BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        preTableData(statement, databaseName, tableName);

        // Take one DataNode down. Its last successful ConfigNode contact is now frozen; after
        // T_proceed the ConfigNode can treat it as self-fenced and stop waiting for its ack.
        victimDataNode.stop();
        Assert.assertFalse("victim DataNode should be stopped", victimDataNode.isAlive());

        executeSpecificSql(databaseName, tableName, statement, createdAfterDownTableName);
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
                  .getConnection(liveDataNode, "root", "root", BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        preTableData(statement, databaseName, tableName);

        // try to set the DN to readOnly status
        try (final Connection victimConn =
                EnvFactory.getEnv()
                    .getConnection(victimDataNode, "root", "root", BaseEnv.TABLE_SQL_DIALECT);
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

    LOGGER.info("7. start to test high availability of dropping database procedure");
    assertStatementEffect(
        statement,
        "DROP DATABASE " + databaseName,
        () -> !databaseExists(statement, databaseName),
        "DROP DATABASE must succeed with one DataNode down");
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
