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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

/**
 * Verifies that EXTEND REGION and REMOVE REGION operations are visible in {@code SHOW MIGRATIONS}
 * (previously only MIGRATE REGION was), with the correct {@code OperationType} label and FROM/TO
 * node mapping (EXTEND has only a target -> ToNodeId, REMOVE has only a target -> FromNodeId).
 */
@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBShowMigrationsIT extends IoTDBRegionOperationReliabilityITFramework {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBShowMigrationsIT.class);

  private static final String EXPAND_FORMAT = "extend region %d to %d";
  private static final String SHRINK_FORMAT = "remove region %d from %d";
  private static final String SHOW_MIGRATIONS = "show migrations";

  /**
   * A single migration row captured from {@code SHOW MIGRATIONS}. {@code fromNodeId}/{@code
   * toNodeId} are {@code null} when the corresponding cell is blank.
   */
  private static class MigrationRow {
    final String operationType;
    final int regionId;
    final Integer fromNodeId;
    final Integer toNodeId;

    MigrationRow(String operationType, int regionId, Integer fromNodeId, Integer toNodeId) {
      this.operationType = operationType;
      this.regionId = regionId;
      this.fromNodeId = fromNodeId;
      this.toNodeId = toNodeId;
    }
  }

  @Test
  public void extendAndRemoveShownInMigrationsTest() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);

    EnvFactory.getEnv().initClusterEnvironment(1, 3);

    // Pin every statement to a single DataNode. SHOW MIGRATIONS reflects live, non-finished
    // procedure state on the ConfigNode, so a row appears/disappears while the operation is in
    // flight. The default fan-out connection runs each read against all three DataNodes and
    // requires identical result sets; because the three parallel queries hit the ConfigNode at
    // slightly different instants, one can observe the migration row a moment before/after the
    // others, yielding an InconsistentDataException (e.g. next() -> [false, false, true]). Reading
    // from one DataNode removes that cross-node comparison; correctness is unaffected since every
    // DataNode forwards SHOW MIGRATIONS to the same ConfigNode.
    try (final Connection connection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(EnvFactory.getEnv().getDataNodeWrapperList().get(0)));
        final Statement statement = makeItCloseQuietly(connection.createStatement())) {
      // prepare some real data so that EXTEND has TsFiles to copy, widening the in-flight window
      statement.execute(INSERTION1);
      statement.execute(FLUSH_COMMAND);

      // pick a data region and a DataNode that does not yet hold it
      final Map<Integer, Set<Integer>> regionMap = getDataRegionMap(statement);
      final Set<Integer> allDataNodeId = getAllDataNodes(statement);
      final int selectedRegion = selectRegion(regionMap);
      final int targetDataNode =
          selectDataNodeNotContainsRegion(allDataNodeId, regionMap, selectedRegion);

      // ---- EXTEND: target appears as ToNodeId, FromNodeId is blank ----
      MigrationRow extendRow =
          runAndCaptureMigration(
              String.format(EXPAND_FORMAT, selectedRegion, targetDataNode),
              selectedRegion,
              "EXTEND");
      Assert.assertNotNull(
          "EXTEND REGION should be visible in SHOW MIGRATIONS while in flight", extendRow);
      Assert.assertEquals("EXTEND", extendRow.operationType);
      Assert.assertEquals(selectedRegion, extendRow.regionId);
      Assert.assertNull(
          "EXTEND has no source DataNode (FromNodeId should be blank)", extendRow.fromNodeId);
      Assert.assertEquals(
          "EXTEND target should be ToNodeId", Integer.valueOf(targetDataNode), extendRow.toNodeId);

      // Wait until the extend has fully finished before issuing the remove. Two conditions must
      // hold: (1) the region actually contains the target DataNode, and (2) the EXTEND procedure
      // is gone from SHOW MIGRATIONS. The region can report the new member slightly before the
      // AddRegionPeer procedure finishes, so without (2) the subsequent REMOVE poll could still
      // capture the lingering EXTEND row and read its operationType as "EXTEND".
      awaitRegionContainsDataNode(statement, selectedRegion, targetDataNode, true);
      awaitNoMigrationForRegion(statement, selectedRegion);

      // ---- REMOVE: target appears as FromNodeId, ToNodeId is blank ----
      MigrationRow removeRow =
          runAndCaptureMigration(
              String.format(SHRINK_FORMAT, selectedRegion, targetDataNode),
              selectedRegion,
              "REMOVE");
      Assert.assertNotNull(
          "REMOVE REGION should be visible in SHOW MIGRATIONS while in flight", removeRow);
      Assert.assertEquals("REMOVE", removeRow.operationType);
      Assert.assertEquals(selectedRegion, removeRow.regionId);
      Assert.assertEquals(
          "REMOVE target should be FromNodeId",
          Integer.valueOf(targetDataNode),
          removeRow.fromNodeId);
      Assert.assertNull(
          "REMOVE has no destination DataNode (ToNodeId should be blank)", removeRow.toNodeId);

      awaitRegionContainsDataNode(statement, selectedRegion, targetDataNode, false);
      LOGGER.info("SHOW MIGRATIONS test passed for EXTEND and REMOVE");
    }
  }

  /**
   * Issue {@code command} on a background thread and concurrently poll {@code SHOW MIGRATIONS}
   * until a row for {@code expectedRegion} with {@code expectedOperationType} appears, returning
   * that row. The operation runs on its own connection so the polling loop on the main connection
   * is not blocked by the (synchronous) statement execution.
   *
   * <p>Filtering by {@code expectedOperationType} makes the capture robust against a previous
   * operation's row that has not been cleared yet: only a row of the expected type is captured.
   */
  private MigrationRow runAndCaptureMigration(
      String command, int expectedRegion, String expectedOperationType) throws Exception {
    final AtomicReference<Exception> commandError = new AtomicReference<>();
    final Thread commandThread =
        new Thread(
            () -> {
              try (final Connection conn = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
                  final Statement stmt = makeItCloseQuietly(conn.createStatement())) {
                LOGGER.info("Executing: {}", command);
                stmt.execute(command);
              } catch (Exception e) {
                commandError.set(e);
              }
            });
    commandThread.start();

    // Poll from a single DataNode: SHOW MIGRATIONS reflects live procedure state, so a fan-out
    // read across DataNodes can see the in-flight row on some nodes but not others and fail the
    // cross-node consistency check. See the note in extendAndRemoveShownInMigrationsTest.
    final AtomicReference<MigrationRow> captured = new AtomicReference<>();
    try (final Connection pollConn =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(EnvFactory.getEnv().getDataNodeWrapperList().get(0)));
        final Statement pollStmt = makeItCloseQuietly(pollConn.createStatement())) {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollInterval(50, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                MigrationRow row =
                    queryMigrationRow(pollStmt, expectedRegion, expectedOperationType);
                if (row != null) {
                  captured.set(row);
                  return true;
                }
                // Stop early once the command thread has finished: either it failed, or it
                // completed so quickly the row was never observed. Either way, continuing to
                // poll is pointless — let the caller's assertions report the outcome.
                return !commandThread.isAlive();
              });
    }

    commandThread.join(TimeUnit.MINUTES.toMillis(2));
    if (commandError.get() != null && captured.get() == null) {
      throw commandError.get();
    }
    return captured.get();
  }

  /**
   * Return the first {@code SHOW MIGRATIONS} row matching {@code expectedRegion} and, when {@code
   * expectedOperationType} is non-null, that operation type, or null.
   */
  private MigrationRow queryMigrationRow(
      Statement statement, int expectedRegion, String expectedOperationType) throws Exception {
    List<MigrationRow> rows = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery(SHOW_MIGRATIONS)) {
      while (rs.next()) {
        int regionId = rs.getInt(ColumnHeaderConstant.REGION_ID);
        if (regionId != expectedRegion) {
          continue;
        }
        String operationType = rs.getString(ColumnHeaderConstant.OPERATION_TYPE);
        if (expectedOperationType != null && !expectedOperationType.equals(operationType)) {
          continue;
        }
        int fromNodeId = rs.getInt(ColumnHeaderConstant.FROM_NODE_ID);
        Integer from = rs.wasNull() ? null : fromNodeId;
        int toNodeId = rs.getInt(ColumnHeaderConstant.TO_NODE_ID);
        Integer to = rs.wasNull() ? null : toNodeId;
        rows.add(new MigrationRow(operationType, regionId, from, to));
      }
    }
    return rows.isEmpty() ? null : rows.get(0);
  }

  /** Await until {@code SHOW MIGRATIONS} reports no in-flight operation for {@code regionId}. */
  private void awaitNoMigrationForRegion(Statement statement, int regionId) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> queryMigrationRow(statement, regionId, null) == null);
  }

  /** Await until {@code show regions} reports that the region (does not) contain the DataNode. */
  private void awaitRegionContainsDataNode(
      Statement statement, int regionId, int dataNodeId, boolean shouldContain) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              Map<Integer, Set<Integer>> regionMap = getAllRegionMap(statement);
              Set<Integer> dataNodes = regionMap.get(regionId);
              boolean contains = dataNodes != null && dataNodes.contains(dataNodeId);
              return contains == shouldContain;
            });
  }
}
