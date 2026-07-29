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
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.awaitility.Awaitility;
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
  private static final String MULTI_REGION_MIGRATE_FORMAT = "migrate region %s from %d to %d";
  private static final String EXTEND_REGION_FORMAT = "extend region %d to %d";
  private static final String REMOVE_REGION_FORMAT = "remove region %d from %d";
  private static final String RECONSTRUCT_REGION_FORMAT = "reconstruct region %d on %d";
  private static final String CANCEL_TREE_USER = "cancel_migration_tree_user";
  private static final String CANCEL_TABLE_USER = "cancel_migration_table_user";
  private static final String CANCEL_USER_PASSWORD = "CancelMigration@123456";
  private static final String ONE_MIGRATION_CANCEL_RESPONSE =
      "Successfully signalled 1 migration(s) to cancel";
  private static final String NO_MIGRATION_CANCEL_RESPONSE =
      "Successfully signalled 0 migration(s) to cancel";

  @Test
  public void cancelMigrateRegionByTreeDialectTest() throws Exception {
    initCluster(1, 1, 3, AddRegionPeerState.DO_ADD_REGION_PEER);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
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
  public void cancelAllMigrationsIdleIsIdempotentForBothDialectsTest() throws Exception {
    initCluster(1, 1, 3);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      Map<Integer, Set<Integer>> regionMap = prepareTreeData(statement);
      awaitNoMigrations(statement);

      String treeResponse =
          executeCancelAndGetMessage(BaseEnv.TREE_SQL_DIALECT, "CaNcEl AlL MiGrAtIoNs");
      Assert.assertEquals(NO_MIGRATION_CANCEL_RESPONSE, treeResponse);
      awaitNoMigrations(statement);
      Assert.assertEquals(regionMap, getDataRegionMap(statement));

      String tableResponse =
          executeCancelAndGetMessage(BaseEnv.TABLE_SQL_DIALECT, CANCEL_ALL_MIGRATIONS);
      Assert.assertEquals(NO_MIGRATION_CANCEL_RESPONSE, tableResponse);
      awaitNoMigrations(statement);
      Assert.assertEquals(regionMap, getDataRegionMap(statement));
    }
  }

  @Test
  public void cancelAllMigrationsPermissionTest() throws Exception {
    initCluster(1, 1, 1);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      statement.execute(
          String.format("create user %s '%s'", CANCEL_TREE_USER, CANCEL_USER_PASSWORD));
      statement.execute(
          String.format("create user %s '%s'", CANCEL_TABLE_USER, CANCEL_USER_PASSWORD));

      Assert.assertEquals(
          NO_MIGRATION_CANCEL_RESPONSE,
          executeCancelAndGetMessage(BaseEnv.TREE_SQL_DIALECT, CANCEL_ALL_MIGRATIONS));
      Assert.assertEquals(
          NO_MIGRATION_CANCEL_RESPONSE,
          executeCancelAndGetMessage(BaseEnv.TABLE_SQL_DIALECT, CANCEL_ALL_MIGRATIONS));

      assertCancelPermissionDenied(
          CANCEL_TREE_USER, CANCEL_USER_PASSWORD, BaseEnv.TREE_SQL_DIALECT, "SYSTEM");
      assertCancelPermissionDenied(
          CANCEL_TABLE_USER, CANCEL_USER_PASSWORD, BaseEnv.TABLE_SQL_DIALECT, "SYSTEM");

      statement.execute(String.format("grant system on root.** to user %s", CANCEL_TREE_USER));
      executeCancelAs(CANCEL_TREE_USER, CANCEL_USER_PASSWORD, BaseEnv.TREE_SQL_DIALECT);
      assertCancelPermissionDenied(
          CANCEL_TABLE_USER, CANCEL_USER_PASSWORD, BaseEnv.TABLE_SQL_DIALECT, "SYSTEM");

      executeAdminCommand(
          BaseEnv.TABLE_SQL_DIALECT, String.format("grant system to user %s", CANCEL_TABLE_USER));
      executeCancelAs(CANCEL_TABLE_USER, CANCEL_USER_PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
    }
  }

  @Test
  public void cancelAllMigrationsClientReceiptIncludesCancelledCountTest() throws Exception {
    initClusterWithDataRegionGroups(1, 1, 3, 4, AddRegionPeerState.DO_ADD_REGION_PEER);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      Map<Integer, Set<Integer>> regionMap = prepareTreeData(statement);
      Set<Integer> allDataNodes = getAllDataNodes(statement);
      int sourceDataNode = selectDataNodeHostingRegions(allDataNodes, regionMap, 2);
      List<Integer> selectedRegions =
          regionsOnDataNode(regionMap, sourceDataNode).stream()
              .limit(2)
              .collect(Collectors.toList());
      int targetDataNode = selectDataNodeExcept(allDataNodes, sourceDataNode);

      CapturedMigrations migrations =
          runAndCaptureMigrations(
              String.format(
                  MULTI_REGION_MIGRATE_FORMAT,
                  selectedRegions.stream().map(String::valueOf).collect(Collectors.joining(",")),
                  sourceDataNode,
                  targetDataNode),
              "MIGRATE",
              selectedRegions);

      String response = executeCancelAndGetMessage(BaseEnv.TREE_SQL_DIALECT, CANCEL_ALL_MIGRATIONS);
      Assert.assertEquals("Successfully signalled 2 migration(s) to cancel", response);

      awaitNoMigrations(statement);
      for (Integer regionId : selectedRegions) {
        awaitRegionMembers(statement, regionId, regionMap.get(regionId));
      }
      awaitCommandThreadFinished(migrations);
    }
  }

  @Test
  public void cancelExtendRegionByTableDialectTest() throws Exception {
    initCluster(1, 1, 3, AddRegionPeerState.DO_ADD_REGION_PEER);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
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

      Assert.assertEquals(
          ONE_MIGRATION_CANCEL_RESPONSE,
          executeCancelAndGetMessage(BaseEnv.TABLE_SQL_DIALECT, CANCEL_ALL_MIGRATIONS));

      awaitNoMigrations(statement);
      awaitRegionMembers(statement, selectedRegion, regionMap.get(selectedRegion));
      awaitCommandThreadFinished(migration);
    }
  }

  @Test
  public void cancelRemoveRegionByTreeDialectTest() throws Exception {
    initCluster(2, 1, 3, RemoveRegionPeerState.TRANSFER_REGION_LEADER);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
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
  public void rejectCancelRemoveRegionAfterRemoteTaskSubmittedTest() throws Exception {
    initCluster(2, 1, 3, RemoveRegionPeerState.REMOVE_REGION_PEER);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
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
      awaitMigrationState(
          statement, "REMOVE", selectedRegion, RemoveRegionPeerState.REMOVE_REGION_PEER.name());

      String response = executeCancelAndGetMessage(BaseEnv.TREE_SQL_DIALECT, CANCEL_ALL_MIGRATIONS);
      Assert.assertEquals(NO_MIGRATION_CANCEL_RESPONSE, response);

      Set<Integer> expectedDataNodes =
          regionMap.get(selectedRegion).stream()
              .filter(dataNodeId -> dataNodeId != targetDataNode)
              .collect(Collectors.toSet());
      awaitNoMigrations(statement);
      awaitRegionMembers(statement, selectedRegion, expectedDataNodes);
      awaitCommandThreadFinished(migration);
    }
  }

  @Test
  public void cancelReconstructRegionByTableDialectTest() throws Exception {
    initCluster(2, 1, 3, RemoveRegionPeerState.TRANSFER_REGION_LEADER);

    try (Connection connection = makeItCloseQuietly(getSingleDataNodeConnection());
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

      Assert.assertEquals(
          ONE_MIGRATION_CANCEL_RESPONSE,
          executeCancelAndGetMessage(BaseEnv.TABLE_SQL_DIALECT, CANCEL_ALL_MIGRATIONS));

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
    initClusterWithDataRegionGroups(
        dataReplicationFactor, schemaReplicationFactor, dataNodeNum, 1, configNodeKillPoints);
  }

  private void initClusterWithDataRegionGroups(
      int dataReplicationFactor,
      int schemaReplicationFactor,
      int dataNodeNum,
      int dataRegionGroupNum,
      Enum<?>... configNodeKillPoints) {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(dataReplicationFactor)
        .setSchemaReplicationFactor(schemaReplicationFactor)
        .setDataRegionGroupExtensionPolicy(dataRegionGroupNum > 1 ? "CUSTOM" : "AUTO")
        .setSchemaRegionGroupExtensionPolicy("AUTO")
        .setDefaultDataRegionGroupNumPerDatabase(dataRegionGroupNum)
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

  private Connection getSingleDataNodeConnection() throws SQLException {
    // SHOW MIGRATIONS reflects live procedure state. A default cluster-test connection fans out
    // reads to every DataNode and requires identical results, but those requests can reach the
    // ConfigNode on opposite sides of a migration state transition. Pin all test polling to one
    // DataNode so each observation comes from a single point in time.
    return EnvFactory.getEnv().getConnection(EnvFactory.getEnv().getDataNodeWrapperList().get(0));
  }

  private void executeCancel(String sqlDialect) throws Exception {
    executeCancelAndGetMessage(sqlDialect, CANCEL_ALL_MIGRATIONS);
  }

  private String executeCancelAndGetMessage(String sqlDialect, String command) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(sqlDialect);
        Statement statement = connection.createStatement()) {
      statement.execute(command);
      return getLastStatementMessage(connection);
    }
  }

  private void executeCancelAs(String username, String password, String sqlDialect)
      throws Exception {
    try (Connection connection =
            makeItCloseQuietly(EnvFactory.getEnv().getConnection(username, password, sqlDialect));
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      statement.execute(CANCEL_ALL_MIGRATIONS);
    }
  }

  private void executeAdminCommand(String sqlDialect, String command) throws Exception {
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection(sqlDialect));
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      statement.execute(command);
    }
  }

  private void assertCancelPermissionDenied(
      String username, String password, String sqlDialect, String expectedPrivilege) {
    SQLException exception =
        Assert.assertThrows(
            SQLException.class, () -> executeCancelAs(username, password, sqlDialect));
    Assert.assertTrue(
        exception.getMessage(),
        exception
            .getMessage()
            .contains(
                "No permissions for this operation, please add privilege " + expectedPrivilege));
  }

  private String getLastStatementMessage(Connection connection) {
    Connection underlyingConnection = connection;
    if (connection instanceof ClusterTestConnection) {
      underlyingConnection =
          ((ClusterTestConnection) connection).writeConnection.getUnderlyingConnection();
    }
    Assert.assertTrue(underlyingConnection instanceof IoTDBConnection);
    return ((IoTDBConnection) underlyingConnection).getLastStatementMessage();
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
    try (Connection pollConnection = makeItCloseQuietly(getSingleDataNodeConnection());
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

  private CapturedMigrations runAndCaptureMigrations(
      String command, String expectedOperationType, List<Integer> expectedRegions)
      throws Exception {
    AtomicReference<Exception> commandError = new AtomicReference<>();
    AtomicBoolean stopRetry = new AtomicBoolean(false);
    Thread commandThread =
        new Thread(
            () -> executeCommand(command, false, stopRetry, commandError),
            "cancel-migrations-multi-region-command");
    commandThread.start();

    try (Connection pollConnection = makeItCloseQuietly(getSingleDataNodeConnection());
        Statement pollStatement = makeItCloseQuietly(pollConnection.createStatement())) {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollInterval(50, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                List<MigrationRow> rows =
                    queryMigrationRows(pollStatement, expectedOperationType, expectedRegions);
                if (rows.size() >= expectedRegions.size()) {
                  stopRetry.set(true);
                  return true;
                }
                if (commandError.get() != null && !commandThread.isAlive()) {
                  throw commandError.get();
                }
                return false;
              });
    }

    return new CapturedMigrations(commandThread);
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
    awaitCommandThreadFinished(migration.commandThread);
  }

  private void awaitCommandThreadFinished(CapturedMigrations migrations) throws Exception {
    awaitCommandThreadFinished(migrations.commandThread);
  }

  private void awaitCommandThreadFinished(Thread commandThread) throws Exception {
    commandThread.join(TimeUnit.MINUTES.toMillis(2));
    Assert.assertFalse(
        "The original migration command should finish after cancellation", commandThread.isAlive());
  }

  private MigrationRow queryMigrationRow(
      Statement statement, String expectedOperationType, Integer expectedRegion) throws Exception {
    List<MigrationRow> rows =
        queryMigrationRows(
            statement,
            expectedOperationType,
            expectedRegion == null ? null : Arrays.asList(expectedRegion));
    return rows.isEmpty() ? null : rows.get(0);
  }

  private List<MigrationRow> queryMigrationRows(
      Statement statement, String expectedOperationType, List<Integer> expectedRegions)
      throws Exception {
    List<MigrationRow> rows = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery(SHOW_MIGRATIONS)) {
      while (rs.next()) {
        String regionType = rs.getString(ColumnHeaderConstant.TYPE);
        if (!String.valueOf(TConsensusGroupType.DataRegion).equals(regionType)) {
          continue;
        }
        String operationType = rs.getString(ColumnHeaderConstant.OPERATION_TYPE);
        int regionId = rs.getInt(ColumnHeaderConstant.REGION_ID);
        if (!expectedOperationType.equals(operationType)
            || (expectedRegions != null && !expectedRegions.contains(regionId))) {
          continue;
        }
        rows.add(
            new MigrationRow(
                operationType,
                regionId,
                readNullableInt(rs, ColumnHeaderConstant.FROM_NODE_ID),
                readNullableInt(rs, ColumnHeaderConstant.TO_NODE_ID),
                rs.getString(ColumnHeaderConstant.CURRENT_STATE)));
      }
    }
    return rows;
  }

  private void awaitMigrationState(
      Statement statement, String operationType, int regionId, String expectedState) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(20, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              MigrationRow row = queryMigrationRow(statement, operationType, regionId);
              return row != null && expectedState.equals(row.currentState);
            });
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

  private int selectDataNodeHostingRegions(
      Set<Integer> allDataNodes, Map<Integer, Set<Integer>> regionMap, int minRegionNum) {
    return allDataNodes.stream()
        .filter(dataNodeId -> regionsOnDataNode(regionMap, dataNodeId).size() >= minRegionNum)
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find DataNode hosting enough regions"));
  }

  private int selectDataNodeExcept(Set<Integer> allDataNodes, int excludedDataNode) {
    return allDataNodes.stream()
        .filter(dataNodeId -> dataNodeId != excludedDataNode)
        .findAny()
        .orElseThrow(() -> new RuntimeException("cannot find another DataNode"));
  }

  private List<Integer> regionsOnDataNode(Map<Integer, Set<Integer>> regionMap, int dataNodeId) {
    return regionMap.entrySet().stream()
        .filter(entry -> entry.getValue().contains(dataNodeId))
        .map(Map.Entry::getKey)
        .sorted()
        .collect(Collectors.toList());
  }

  private static class CapturedMigration {
    final MigrationRow row;
    final Thread commandThread;

    private CapturedMigration(MigrationRow row, Thread commandThread) {
      this.row = row;
      this.commandThread = commandThread;
    }
  }

  private static class CapturedMigrations {
    final Thread commandThread;

    private CapturedMigrations(Thread commandThread) {
      this.commandThread = commandThread;
    }
  }

  private static class MigrationRow {
    final String operationType;
    final int regionId;
    final Integer fromNodeId;
    final Integer toNodeId;
    final String currentState;

    private MigrationRow(
        String operationType,
        int regionId,
        Integer fromNodeId,
        Integer toNodeId,
        String currentState) {
      this.operationType = operationType;
      this.regionId = regionId;
      this.fromNodeId = fromNodeId;
      this.toNodeId = toNodeId;
      this.currentState = currentState;
    }
  }
}
