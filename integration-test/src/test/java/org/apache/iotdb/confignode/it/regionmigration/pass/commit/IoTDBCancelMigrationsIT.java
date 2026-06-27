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

package org.apache.iotdb.confignode.it.regionmigration.pass.commit;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBCancelMigrationsIT extends IoTDBRegionOperationReliabilityITFramework {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBCancelMigrationsIT.class);

  private static final String SHOW_MIGRATIONS = "show migrations";
  private static final String CANCEL_ALL_MIGRATIONS = "cancel all migrations";
  private static final String MIGRATE_REGION_FORMAT = "migrate region %d from %d to %d";
  private static final String EXTEND_REGION_FORMAT = "extend region %d to %d";
  private static final String REMOVE_REGION_FORMAT = "remove region %d from %d";
  private static final String RECONSTRUCT_REGION_FORMAT = "reconstruct region %d on %d";

  @Test
  public void cancelMigrateRegionByTreeDialectTest() throws Exception {
    initCluster(1, 1, 3, AddRegionPeerState.DO_ADD_REGION_PEER);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      Map<Integer, Set<Integer>> regionMap = prepareTreeData(statement);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int selectedRegion = selectRegion(regionMap);
      int sourceDataNode = selectDataNodeContainsRegion(allDataNodes, regionMap, selectedRegion);
      int targetDataNode = selectDataNodeNotContainsRegion(allDataNodes, regionMap, selectedRegion);

      // Drive a deterministic MIGRATE (the same RegionMigrateProcedure that LOAD BALANCE produces
      // internally) so the test does not depend on the CAR balancer's heuristic decision to
      // migrate.
      CapturedMigration migration =
          runAndCaptureMigration(
              String.format(MIGRATE_REGION_FORMAT, selectedRegion, sourceDataNode, targetDataNode),
              "MIGRATE",
              selectedRegion,
              false);
      Assert.assertEquals(Integer.valueOf(sourceDataNode), migration.row.fromNodeId);
      Assert.assertEquals(Integer.valueOf(targetDataNode), migration.row.toNodeId);

      executeCancel(BaseEnv.TREE_SQL_DIALECT);

      awaitNoMigrations(statement);
      awaitRegionMembers(statement, selectedRegion, regionMap.get(selectedRegion));
      awaitCommandThreadFinished(migration);
    }
  }

  @Test
  public void cancelExtendRegionByTableDialectTest() throws Exception {
    initCluster(1, 1, 3, AddRegionPeerState.DO_ADD_REGION_PEER);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      Map<Integer, Set<Integer>> regionMap = prepareTreeData(statement);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int selectedRegion = selectRegion(regionMap);
      int targetDataNode = selectDataNodeNotContainsRegion(allDataNodes, regionMap, selectedRegion);

      CapturedMigration migration =
          runAndCaptureMigration(
              String.format(EXTEND_REGION_FORMAT, selectedRegion, targetDataNode),
              "EXTEND",
              selectedRegion,
              false);
      Assert.assertEquals(Integer.valueOf(targetDataNode), migration.row.toNodeId);

      executeCancel(BaseEnv.TABLE_SQL_DIALECT);

      awaitNoMigrations(statement);
      awaitRegionMembers(statement, selectedRegion, regionMap.get(selectedRegion));
      awaitCommandThreadFinished(migration);
    }
  }

  @Test
  public void cancelRemoveRegionByTreeDialectTest() throws Exception {
    initCluster(2, 1, 3, RemoveRegionPeerState.TRANSFER_REGION_LEADER);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      Map<Integer, Set<Integer>> regionMap = prepareTreeData(statement);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int selectedRegion = selectRegion(regionMap);
      int targetDataNode = selectDataNodeContainsRegion(allDataNodes, regionMap, selectedRegion);

      CapturedMigration migration =
          runAndCaptureMigration(
              String.format(REMOVE_REGION_FORMAT, selectedRegion, targetDataNode),
              "REMOVE",
              selectedRegion,
              false);
      Assert.assertEquals(Integer.valueOf(targetDataNode), migration.row.fromNodeId);

      executeCancel(BaseEnv.TREE_SQL_DIALECT);

      awaitNoMigrations(statement);
      awaitRegionMembers(statement, selectedRegion, regionMap.get(selectedRegion));
      awaitCommandThreadFinished(migration);
    }
  }

  @Test
  public void cancelReconstructRegionByTableDialectTest() throws Exception {
    initCluster(2, 1, 3, RemoveRegionPeerState.TRANSFER_REGION_LEADER);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      Map<Integer, Set<Integer>> regionMap = prepareTreeData(statement);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int selectedRegion = selectRegion(regionMap);
      int targetDataNode = selectDataNodeContainsRegion(allDataNodes, regionMap, selectedRegion);

      CapturedMigration migration =
          runAndCaptureMigration(
              String.format(RECONSTRUCT_REGION_FORMAT, selectedRegion, targetDataNode),
              "RECONSTRUCT",
              selectedRegion,
              false);
      Assert.assertEquals(Integer.valueOf(targetDataNode), migration.row.fromNodeId);
      Assert.assertEquals(Integer.valueOf(targetDataNode), migration.row.toNodeId);

      executeCancel(BaseEnv.TABLE_SQL_DIALECT);

      awaitNoMigrations(statement);
      awaitRegionMembers(statement, selectedRegion, regionMap.get(selectedRegion));
      awaitCommandThreadFinished(migration);
    }
  }

  private void initCluster(
      int dataReplicationFactor,
      int schemaReplicationFactor,
      int dataNodeNum,
      Enum<?>... configNodeKillPoints) {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(dataReplicationFactor)
        .setSchemaReplicationFactor(schemaReplicationFactor)
        .setDataRegionGroupExtensionPolicy("AUTO")
        .setSchemaRegionGroupExtensionPolicy("AUTO")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDefaultSchemaRegionGroupNumPerDatabase(1);
    EnvFactory.getEnv().registerConfigNodeKillPoints(killPointNames(configNodeKillPoints));
    EnvFactory.getEnv().initClusterEnvironment(1, dataNodeNum);
  }

  private Map<Integer, Set<Integer>> prepareTreeData(Statement statement) throws Exception {
    statement.execute(INSERTION1);
    statement.execute(FLUSH_COMMAND);
    Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
    Assert.assertFalse(regionMap.isEmpty());
    return regionMap;
  }

  private List<String> killPointNames(Enum<?>... killPoints) {
    if (killPoints == null || killPoints.length == 0) {
      return new ArrayList<>();
    }
    return Arrays.stream(killPoints).map(KillPoint::enumToString).collect(Collectors.toList());
  }

  private void executeCancel(String sqlDialect) throws Exception {
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection(sqlDialect));
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      statement.execute(CANCEL_ALL_MIGRATIONS);
    }
  }

  private CapturedMigration runAndCaptureMigration(
      String command, String expectedOperationType, Integer expectedRegion, boolean retryCommand)
      throws Exception {
    AtomicReference<Exception> commandError = new AtomicReference<>();
    AtomicBoolean stopRetry = new AtomicBoolean(false);
    Thread commandThread =
        new Thread(
            () -> executeCommand(command, retryCommand, stopRetry, commandError),
            "cancel-migrations-command");
    commandThread.start();

    AtomicReference<MigrationRow> captured = new AtomicReference<>();
    try (Connection pollConnection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement pollStatement = makeItCloseQuietly(pollConnection.createStatement())) {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollInterval(50, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                MigrationRow row =
                    queryMigrationRow(pollStatement, expectedOperationType, expectedRegion);
                if (row != null) {
                  stopRetry.set(true);
                  captured.set(row);
                  return true;
                }
                if (commandError.get() != null && !commandThread.isAlive()) {
                  throw commandError.get();
                }
                return false;
              });
    }

    Assert.assertNotNull(
        String.format("%s should be visible in SHOW MIGRATIONS", expectedOperationType),
        captured.get());
    return new CapturedMigration(captured.get(), commandThread);
  }

  private void executeCommand(
      String command,
      boolean retryCommand,
      AtomicBoolean stopRetry,
      AtomicReference<Exception> commandError) {
    long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
    Exception lastException = null;
    do {
      try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
          Statement statement = makeItCloseQuietly(connection.createStatement())) {
        LOGGER.info("Executing migration command: {}", command);
        statement.execute(command);
        return;
      } catch (Exception e) {
        lastException = e;
        if (!retryCommand || stopRetry.get()) {
          commandError.set(e);
          return;
        }
        sleepQuietly(1_000L);
      }
    } while (System.currentTimeMillis() < deadline);
    commandError.set(lastException);
  }

  private void awaitCommandThreadFinished(CapturedMigration migration) throws Exception {
    migration.commandThread.join(TimeUnit.MINUTES.toMillis(2));
    Assert.assertFalse(
        "The original migration command should finish after cancellation",
        migration.commandThread.isAlive());
  }

  private MigrationRow queryMigrationRow(
      Statement statement, String expectedOperationType, Integer expectedRegion) throws Exception {
    try (ResultSet rs = statement.executeQuery(SHOW_MIGRATIONS)) {
      while (rs.next()) {
        String regionType = rs.getString(ColumnHeaderConstant.TYPE);
        if (!String.valueOf(TConsensusGroupType.DataRegion).equals(regionType)) {
          continue;
        }
        String operationType = rs.getString(ColumnHeaderConstant.OPERATION_TYPE);
        int regionId = rs.getInt(ColumnHeaderConstant.REGION_ID);
        if (!expectedOperationType.equals(operationType)
            || (expectedRegion != null && expectedRegion != regionId)) {
          continue;
        }
        return new MigrationRow(
            operationType,
            regionId,
            readNullableInt(rs, ColumnHeaderConstant.FROM_NODE_ID),
            readNullableInt(rs, ColumnHeaderConstant.TO_NODE_ID));
      }
    }
    return null;
  }

  private Integer readNullableInt(ResultSet rs, String columnName) throws Exception {
    int value = rs.getInt(columnName);
    return rs.wasNull() ? null : value;
  }

  private void awaitNoMigrations(Statement statement) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              try (ResultSet rs = statement.executeQuery(SHOW_MIGRATIONS)) {
                return !rs.next();
              }
            });
  }

  private void awaitRegionMembers(
      Statement statement, int regionId, Set<Integer> expectedDataNodes) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> expectedDataNodes.equals(getDataRegionMap(statement).get(regionId)));
  }

  private void sleepQuietly(long millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static class CapturedMigration {
    final MigrationRow row;
    final Thread commandThread;

    private CapturedMigration(MigrationRow row, Thread commandThread) {
      this.row = row;
      this.commandThread = commandThread;
    }
  }

  private static class MigrationRow {
    final String operationType;
    final int regionId;
    final Integer fromNodeId;
    final Integer toNodeId;

    private MigrationRow(String operationType, int regionId, Integer fromNodeId, Integer toNodeId) {
      this.operationType = operationType;
      this.regionId = regionId;
      this.fromNodeId = fromNodeId;
      this.toNodeId = toNodeId;
    }
  }
}
