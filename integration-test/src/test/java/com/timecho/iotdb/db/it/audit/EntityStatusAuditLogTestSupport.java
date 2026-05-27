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

package com.timecho.iotdb.db.it.audit;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import com.timecho.iotdb.db.it.audit.EntityStatusAuditLogChecker.GroupKey;
import com.timecho.iotdb.db.it.audit.EntityStatusAuditLogChecker.ParsedAuditLogs;
import com.timecho.iotdb.db.it.audit.EntityStatusAuditLogChecker.RegionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Shared helpers for the ENTITY_STATUS_CHANGED audit-log integration tests
 * (TimechoDBAuditLogEntityStatusChangedBasicIT and TimechoDBAuditLogEntityStatusChangedClusterIT).
 * The two ITs differ only in cluster size and @Category; everything else lives here.
 *
 * <p>Test shape: configure the env so the audit log only records ENTITY_STATUS_CHANGED, init the
 * cluster, create a test DB to provoke RegionGroup / Region creation, poll SHOW CLUSTER + SHOW
 * REGIONS until every node is Running and every region has a leader, let EventService publish a few
 * more heartbeat ticks, then read the audit log and assert each observed entity reached the
 * terminal Running / leader-elected state.
 */
final class EntityStatusAuditLogTestSupport {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EntityStatusAuditLogTestSupport.class);

  // EventService aggregates state on a ~1s tick; wait long enough that the final transition for
  // every entity has been published before we read the audit log.
  private static final long AUDIT_FLUSH_WAIT_MS = 6_000L;

  // Cluster activation can take a few seconds when running under load; keep this generous so we
  // don't flake on slow CI hosts.
  private static final long CLUSTER_READY_TIMEOUT_MS = 120_000L;
  private static final long CLUSTER_READY_POLL_INTERVAL_MS = 500L;

  // Tree audit table view (SELECT * ... ALIGN BY DEVICE) projects Time, Device, then 10
  // measurements in the order: result, privilege_level, privilege_type, database, operation_type,
  // log, sql_string, audit_event_type, cli_hostname, username. So "log" sits at position 8.
  static final int TREE_LOG_COLUMN_INDEX = 8;
  // Table audit table (SELECT * ORDER BY TIME) projects Time, then the 12 declared columns ending
  // with "log".
  static final int TABLE_LOG_COLUMN_INDEX = 13;

  private EntityStatusAuditLogTestSupport() {}

  /**
   * Applies the audit configuration that keeps only ENTITY_STATUS_CHANGED in the audit_log. We
   * still leave CONTROL as the operation category so the event can pass through, but the
   * control-event allow-list is reduced to just ENTITY_STATUS_CHANGED — login/logout and other
   * CONTROL noise stays out of the query result and the assertions stay focused.
   */
  static void configureAuditEnv() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimestampPrecision("ns")
        .setEnableAuditLog(true)
        .setAuditableOperationType(AuditLogOperation.CONTROL.toString())
        .setAuditableOperationLevel(PrivilegeLevel.GLOBAL.toString())
        .setAuditableOperationResult("SUCCESS,FAIL")
        .setAuditableControlEventType(AuditEventType.ENTITY_STATUS_CHANGED.name());
  }

  private static final String RUNNING_STATUS = "Running";
  private static final String LEADER_ROLE = "Leader";

  /** Snapshot of expected (Node, RegionGroup, Region, ConsensusGroup) entities to verify. */
  static final class ExpectedEntities {
    final Set<Integer> nodeIds;
    final Set<GroupKey> regionGroups;
    final Set<RegionKey> regions;

    ExpectedEntities(Set<Integer> nodeIds, Set<GroupKey> regionGroups, Set<RegionKey> regions) {
      this.nodeIds = nodeIds;
      this.regionGroups = regionGroups;
      this.regions = regions;
    }

    @Override
    public String toString() {
      return "ExpectedEntities{nodes="
          + nodeIds
          + ", regionGroups="
          + regionGroups
          + ", regions="
          + regions
          + "}";
    }
  }

  /**
   * Full snapshot of SHOW CLUSTER + SHOW REGIONS, including non-Running rows. We need every row
   * (not just the Running ones) so we can require <em>all</em> known entities to be Running before
   * declaring the cluster ready — otherwise the test would race against unfinished startup
   * transitions and miss them in the audit log.
   */
  static final class ClusterSnapshot {
    final Map<Integer, String> nodeStatus = new LinkedHashMap<>();
    final Map<RegionKey, String> regionStatus = new LinkedHashMap<>();
    // RegionGroups derived from the SHOW REGIONS rows, regardless of status.
    final Set<GroupKey> regionGroups = new LinkedHashSet<>();
    // RegionGroups that have at least one Leader-role replica visible.
    final Set<GroupKey> regionGroupsWithLeader = new LinkedHashSet<>();

    boolean isReady() {
      if (nodeStatus.isEmpty() || regionStatus.isEmpty()) {
        return false;
      }
      for (String status : nodeStatus.values()) {
        if (!RUNNING_STATUS.equals(status)) {
          return false;
        }
      }
      for (String status : regionStatus.values()) {
        if (!RUNNING_STATUS.equals(status)) {
          return false;
        }
      }
      // Every RegionGroup must have a Leader; otherwise the ConsensusGroup audit log row for that
      // group hasn't been emitted yet.
      return regionGroupsWithLeader.equals(regionGroups);
    }

    ExpectedEntities toExpectedEntities() {
      return new ExpectedEntities(
          new LinkedHashSet<>(nodeStatus.keySet()),
          new LinkedHashSet<>(regionGroups),
          new LinkedHashSet<>(regionStatus.keySet()));
    }

    @Override
    public String toString() {
      return "ClusterSnapshot{nodes="
          + nodeStatus
          + ", regions="
          + regionStatus
          + ", regionGroupsWithLeader="
          + regionGroupsWithLeader
          + "}";
    }
  }

  /**
   * Creates a tree-model database + timeseries + one data point to force the cluster to allocate a
   * SchemaRegion and a DataRegion if it hasn't already (the cluster auto-creates internal __audit.*
   * regions, but exercising a user database makes the test self-contained).
   */
  static void provokeRegionCreationTreeModel() throws SQLException, InterruptedException {
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE root.entityit");
      stmt.execute("CREATE TIMESERIES root.entityit.d1.s1 WITH DATATYPE=BOOLEAN");
      stmt.execute("INSERT INTO root.entityit.d1(time, s1) VALUES (1, true)");
    }
  }

  /** Table-model variant of {@link #provokeRegionCreationTreeModel}. */
  static void provokeRegionCreationTableModel() throws SQLException, InterruptedException {
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE entityit");
      stmt.execute("USE entityit");
      stmt.execute("CREATE TABLE t1(tag1 STRING TAG, s1 INT32 FIELD)");
      stmt.execute("INSERT INTO t1(time, tag1, s1) VALUES (1, 'a', 1)");
    }
  }

  /**
   * Polls SHOW CLUSTER + SHOW REGIONS via the given dialect until every known node and every known
   * region row reports Running status and every RegionGroup has a Leader-role replica. Returns the
   * derived expected-entity sets for downstream assertions; throws on timeout.
   *
   * <p>The dialect matters because the same test class drives both tree-model and table-model
   * paths, and we want each path to stay within its own SQL dialect end-to-end.
   */
  static ExpectedEntities waitForClusterReady(String dialect)
      throws SQLException, InterruptedException {
    long deadline = System.currentTimeMillis() + CLUSTER_READY_TIMEOUT_MS;
    ClusterSnapshot lastSnapshot = null;
    while (System.currentTimeMillis() < deadline) {
      try (Connection conn = EnvFactory.getEnv().getConnection(dialect);
          Statement stmt = conn.createStatement()) {
        lastSnapshot = snapshotCluster(stmt);
        if (lastSnapshot.isReady()) {
          LOGGER.info("Cluster reached ready state: {}", lastSnapshot);
          return lastSnapshot.toExpectedEntities();
        }
        LOGGER.info(
            "Cluster not yet ready (snapshot={}); polling again in {}ms",
            lastSnapshot,
            CLUSTER_READY_POLL_INTERVAL_MS);
      }
      TimeUnit.MILLISECONDS.sleep(CLUSTER_READY_POLL_INTERVAL_MS);
    }
    throw new AssertionError(
        "Cluster did not reach ready state within "
            + CLUSTER_READY_TIMEOUT_MS
            + "ms. Last snapshot: "
            + lastSnapshot);
  }

  /**
   * Reads SHOW CLUSTER + SHOW REGIONS once and records the full picture (including non-Running
   * rows). The caller uses {@link ClusterSnapshot#isReady()} to decide whether to keep polling.
   */
  static ClusterSnapshot snapshotCluster(Statement stmt) throws SQLException {
    ClusterSnapshot snapshot = new ClusterSnapshot();

    // SHOW CLUSTER: NodeID | NodeType | Status | InternalAddress | InternalPort | Version |
    // BuildInfo | ActivateStatus. We record every node so isReady() can require all of them.
    try (ResultSet rs = stmt.executeQuery("SHOW CLUSTER")) {
      while (rs.next()) {
        snapshot.nodeStatus.put(rs.getInt("NodeID"), rs.getString("Status"));
      }
    }

    // SHOW REGIONS: RegionId | Type | Status | Database | ... | DataNodeId | ... | Role | ...
    try (ResultSet rs = stmt.executeQuery("SHOW REGIONS")) {
      while (rs.next()) {
        int regionId = rs.getInt("RegionId");
        String type = rs.getString("Type");
        String status = rs.getString("Status");
        int dataNodeId = rs.getInt("DataNodeId");
        String role = rs.getString("Role");
        GroupKey gk = new GroupKey(type, regionId);
        snapshot.regionGroups.add(gk);
        snapshot.regionStatus.put(new RegionKey(type, regionId, dataNodeId), status);
        if (LEADER_ROLE.equals(role)) {
          snapshot.regionGroupsWithLeader.add(gk);
        }
      }
    }
    return snapshot;
  }

  /**
   * After the cluster has settled, run the test-model-specific assertions: read the audit log,
   * parse the ENTITY_STATUS_CHANGED rows, and verify every observed entity reached the terminal
   * state.
   *
   * <p>{@code logColumnIndex} is the 1-based JDBC column for the "log" field in the result set
   * returned by {@code auditLogQuery}.
   */
  static void assertAuditLogsCover(
      ExpectedEntities expected, String dialect, String auditLogQuery, int logColumnIndex)
      throws SQLException, InterruptedException {
    // Give EventService a few heartbeat ticks to publish the final state transitions before we
    // grab a snapshot of the audit log.
    TimeUnit.MILLISECONDS.sleep(AUDIT_FLUSH_WAIT_MS);

    try (Connection conn = EnvFactory.getEnv().getConnection(dialect);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(auditLogQuery)) {
      ParsedAuditLogs parsed = EntityStatusAuditLogChecker.parse(rs, logColumnIndex);
      // RegionGroup IDs are reused as ConsensusGroup IDs in the audit log, so we verify
      // leader-elected against the same key set.
      EntityStatusAuditLogChecker.assertAllEntitiesReady(
          parsed, expected.nodeIds, expected.regionGroups, expected.regions, expected.regionGroups);
    }
  }

  /** Convenience: run the full tree-model assertion path. */
  static void runTreeModelTest() throws SQLException, InterruptedException {
    provokeRegionCreationTreeModel();
    ExpectedEntities expected = waitForClusterReady(BaseEnv.TREE_SQL_DIALECT);
    assertAuditLogsCover(
        expected,
        BaseEnv.TREE_SQL_DIALECT,
        "SELECT * FROM root.__audit.log.** ORDER BY TIME ALIGN BY DEVICE",
        TREE_LOG_COLUMN_INDEX);
  }

  /** Convenience: run the full table-model assertion path. */
  static void runTableModelTest() throws SQLException, InterruptedException {
    provokeRegionCreationTableModel();
    ExpectedEntities expected = waitForClusterReady(BaseEnv.TABLE_SQL_DIALECT);
    assertAuditLogsCover(
        expected,
        BaseEnv.TABLE_SQL_DIALECT,
        "SELECT * FROM __audit.audit_log ORDER BY TIME",
        TABLE_LOG_COLUMN_INDEX);
  }
}
