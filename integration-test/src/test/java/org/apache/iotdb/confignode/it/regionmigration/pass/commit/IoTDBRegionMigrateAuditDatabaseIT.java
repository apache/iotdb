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
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

/**
 * Reproduces V2-674: migrating a DataRegion of the audit database {@code root.__audit} hangs in the
 * {@code Removing} state when audit logging is enabled and the cluster keeps producing audit
 * records.
 *
 * <p>Unlike the other region-migration ITs (which deliberately exclude {@code root.__audit} from
 * the migratable region set, see {@link
 * IoTDBRegionOperationReliabilityITFramework#getDataRegionMap}), this test specifically targets the
 * audit DataRegion. The cluster is kept under a steady stream of audit-generating control
 * operations (CREATE/DROP USER) during the migration, mimicking the benchmark load in the original
 * report, so the leaving peer's sync-log / resource-release step has a moving target to chase. The
 * test asserts the migration finishes; if the bug reproduces, {@code awaitUntilSuccess} times out
 * and the test fails, and CI uploads the ConfigNode/DataNode logs (which should contain the stuck
 * {@code RemoveRegionPeerProcedure} worker) as artifacts for further analysis.
 *
 * <p>The reporter's environment has transparent data encryption (TDE) enabled, and {@code
 * root.__audit} is force-encrypted (see {@code DataNode.java} {@code TSFileDBToEncryptMap}). An
 * earlier non-encrypted variant of this test passed in CI, so this version turns on full TDE
 * ({@code encrypt_type} + {@code enable_encrypt_config_file} + encryption token) to reproduce the
 * reporter's condition, on the hypothesis that encryption — not data volume — is the trigger.
 */
@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateAuditDatabaseIT extends IoTDBRegionOperationReliabilityITFramework {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRegionMigrateAuditDatabaseIT.class);

  private static final String SHOW_REGIONS = "show regions";
  private static final String FLUSH_COMMAND = "flush on cluster";
  private static final String AUDIT_USER_PASSWORD = "Audit@123456";
  private static final String REGION_MIGRATE_COMMAND_FORMAT = "migrate region %d from %d to %d";
  private static final String ENCRYPT_TYPE = "com.timecho.iotdb.commons.encrypt.AES128.AES128";
  private static final String ENCRYPT_TOKEN = "thisisourtestkey";

  private String originalEncryptionToken;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Enable audit logging so the cluster materializes and keeps writing to root.__audit, and turn
    // on full TDE so that root.__audit is force-encrypted (WAL/TsFile), matching the reporter's
    // env.
    originalEncryptionToken = DataNodeWrapper.getEncryptionToken();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAuditLog(true)
        .setEncryptType(ENCRYPT_TYPE)
        .setEnableEncryptConfigFile(true);
    DataNodeWrapper.setEncryptionToken(ENCRYPT_TOKEN);
  }

  @Override
  @After
  public void tearDown() throws InterruptedException {
    try {
      super.tearDown();
    } finally {
      // Restore the shared static encryption token so other ITs in the same fork are unaffected.
      DataNodeWrapper.setEncryptionToken(originalEncryptionToken);
    }
  }

  @Test
  public void migrateAuditDataRegionIoTV1Test() throws Exception {
    migrateAuditDataRegionTest(ConsensusFactory.IOT_CONSENSUS);
  }

  @Test
  public void migrateAuditDataRegionIoTV2Test() throws Exception {
    migrateAuditDataRegionTest(ConsensusFactory.IOT_CONSENSUS_V2);
  }

  private void migrateAuditDataRegionTest(String dataRegionConsensusProtocol) throws Exception {
    // 1 ConfigNode + 3 DataNodes, data replication factor 2 -> every region group leaves at least
    // one DataNode free to migrate to.
    final int dataReplicationFactor = 2;
    final int schemaReplicationFactor = 1;
    final int configNodeNum = 1;
    final int dataNodeNum = 3;

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(dataRegionConsensusProtocol)
        .setDataReplicationFactor(dataReplicationFactor)
        .setSchemaReplicationFactor(schemaReplicationFactor);
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, dataNodeNum);

    final AtomicBoolean stopBackgroundLoad = new AtomicBoolean(false);
    Thread backgroundLoad = null;
    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Materialize a root.__audit DataRegion by generating audit records.
      final Map<Integer, Set<Integer>> auditRegionMap = awaitAuditDataRegion(statement);
      final Set<Integer> allDataNodeId = getAllDataNodes(statement);

      final int selectedRegion =
          auditRegionMap.keySet().stream()
              .findAny()
              .orElseThrow(() -> new RuntimeException("no root.__audit DataRegion found"));
      final int originalDataNode =
          auditRegionMap.get(selectedRegion).stream()
              .findAny()
              .orElseThrow(() -> new RuntimeException("cannot find original DataNode"));
      final int destDataNode =
          selectDataNodeNotContainsRegion(allDataNodeId, auditRegionMap, selectedRegion);

      LOGGER.info(
          "Migrating root.__audit DataRegion {} from DataNode {} to DataNode {} (consensus={})",
          selectedRegion,
          originalDataNode,
          destDataNode,
          dataRegionConsensusProtocol);

      // Keep producing audit records throughout the migration to mimic the concurrent benchmark
      // load in the original report.
      backgroundLoad = startBackgroundAuditLoad(stopBackgroundLoad);

      // Start the migration.
      statement.execute(
          String.format(
              REGION_MIGRATE_COMMAND_FORMAT, selectedRegion, originalDataNode, destDataNode));

      final Predicate<TShowRegionResp> migrateRegionPredicate =
          resp -> {
            Map<Integer, Set<Integer>> newRegionMap = getRegionMap(resp.getRegionInfoList());
            Set<Integer> dataNodes = newRegionMap.get(selectedRegion);
            return dataNodes != null
                && !dataNodes.contains(originalDataNode)
                && dataNodes.contains(destDataNode);
          };

      try {
        awaitUntilSuccess(
            client,
            selectedRegion,
            migrateRegionPredicate,
            Optional.of(destDataNode),
            Optional.of(originalDataNode));
      } catch (ConditionTimeoutException e) {
        Assert.fail(
            String.format(
                "Audit-database region migration did not finish: region %d from DataNode %d to "
                    + "DataNode %d stayed Removing (check ConfigNode/DataNode logs for a stuck "
                    + "RemoveRegionPeerProcedure worker).",
                selectedRegion, originalDataNode, destDataNode));
      }

      LOGGER.info("root.__audit DataRegion migration finished");
    } finally {
      stopBackgroundLoad.set(true);
      if (backgroundLoad != null) {
        backgroundLoad.join(TimeUnit.SECONDS.toMillis(10));
      }
    }
  }

  /**
   * Repeatedly run audit-generating control operations until a {@code root.__audit} DataRegion
   * shows up in {@code show regions}, then return its regionId -> DataNodeId set map.
   */
  private Map<Integer, Set<Integer>> awaitAuditDataRegion(Statement statement) {
    final AtomicReference<Map<Integer, Set<Integer>>> auditRegionMap =
        new AtomicReference<>(new HashMap<>());
    final AtomicInteger seq = new AtomicInteger();
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .pollDelay(2, TimeUnit.SECONDS)
        .until(
            () -> {
              generateAuditRecords(statement, "audit_probe_" + seq.getAndIncrement());
              try {
                statement.execute(FLUSH_COMMAND);
              } catch (SQLException ignore) {
                // flush is best-effort here
              }
              Map<Integer, Set<Integer>> map = getAuditDataRegionMap(statement);
              auditRegionMap.set(map);
              return !map.isEmpty();
            });
    LOGGER.info("root.__audit DataRegion(s) detected: {}", auditRegionMap.get());
    return auditRegionMap.get();
  }

  /** Start a daemon thread that keeps generating audit records until stopped. */
  private Thread startBackgroundAuditLoad(AtomicBoolean stop) {
    Thread thread =
        new Thread(
            () -> {
              int i = 0;
              try (Connection connection = EnvFactory.getEnv().getConnection();
                  Statement statement = connection.createStatement()) {
                while (!stop.get()) {
                  generateAuditRecords(statement, "audit_load_" + (i++));
                  try {
                    Thread.sleep(50);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              } catch (Exception e) {
                LOGGER.warn("Background audit load stopped early", e);
              }
            },
            "audit-load");
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /**
   * Run a pair of control operations (CREATE/DROP USER) that are audited at the GLOBAL privilege
   * level, producing inserts into root.__audit. Failures are ignored: the goal is to keep the audit
   * write path busy, not to assert on the user operations.
   */
  private void generateAuditRecords(Statement statement, String user) {
    try {
      statement.execute(String.format("CREATE USER %s '%s'", user, AUDIT_USER_PASSWORD));
      statement.execute(String.format("DROP USER %s", user));
    } catch (SQLException ignore) {
      // best-effort audit traffic
    }
  }

  /**
   * Read {@code show regions} and return regionId -> DataNodeId set for root.__audit DataRegions.
   */
  private Map<Integer, Set<Integer>> getAuditDataRegionMap(Statement statement)
      throws SQLException {
    Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    try (ResultSet resultSet = statement.executeQuery(SHOW_REGIONS)) {
      while (resultSet.next()) {
        if (String.valueOf(TConsensusGroupType.DataRegion)
                .equals(resultSet.getString(ColumnHeaderConstant.TYPE))
            && SystemConstant.AUDIT_DATABASE.equals(
                resultSet.getString(ColumnHeaderConstant.DATABASE))) {
          int regionId = resultSet.getInt(ColumnHeaderConstant.REGION_ID);
          int dataNodeId = resultSet.getInt(ColumnHeaderConstant.DATA_NODE_ID);
          regionMap.computeIfAbsent(regionId, id -> new HashSet<>()).add(dataNodeId);
        }
      }
    }
    return regionMap;
  }
}
