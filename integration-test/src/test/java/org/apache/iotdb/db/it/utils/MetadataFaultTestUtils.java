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

package org.apache.iotdb.db.it.utils;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.iot.thrift.IoTConsensusIService;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteRes;
import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.fault.metadata.MetadataTestAgent;
import org.apache.iotdb.it.fault.metadata.MetadataTestHooks;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/** Shared, node-specific fault controls and observations for the metadata P0 recovery ITs. */
public final class MetadataFaultTestUtils {
  public static final long FENCE_MS = 8_000;
  public static final long TIMEOUT_SECONDS = 120;

  private MetadataFaultTestUtils() {}

  public static void configureCluster() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2)
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(2)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setAutoCreateSchemaEnabled(false);
    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setMetadataLeaseFenceMs(FENCE_MS);
    final Properties properties = new Properties();
    properties.setProperty("schema_region_ratis_snapshot_trigger_threshold", "12");
    properties.setProperty("schema_region_ratis_log_segment_size_max_in_byte", "1024");
    properties.setProperty("schema_region_ratis_preserve_logs_num_when_purge", "0");
    properties.setProperty("schema_region_ratis_periodic_snapshot_interval", "0");
    ((MppBaseConfig) EnvFactory.getEnv().getConfig().getConfigNodeConfig())
        .updateProperties(properties);
    final Path agent = MetadataTestAgent.createJar(Paths.get("target", "metadata-test-agent.jar"));
    final String option = "-javaagent:" + agent.toAbsolutePath();
    ((MppJVMConfig) EnvFactory.getEnv().getConfig().getConfigNodeJVMConfig())
        .setJvmOptions(Collections.singletonList(option));
    ((MppJVMConfig) EnvFactory.getEnv().getConfig().getDataNodeJVMConfig())
        .setJvmOptions(Collections.singletonList(option));
  }

  public static void createSingleSchemaRegionDatabase(final String database, final String dialect)
      throws SQLException {
    final boolean treeModel = BaseEnv.TREE_SQL_DIALECT.equals(dialect);
    final String schemaRegionConfig =
        treeModel
            ? "set configuration \"default_schema_region_group_num_per_database\"=\"%s\""
            : "set configuration default_schema_region_group_num_per_database='%s'";
    execute(0, dialect, String.format(schemaRegionConfig, "1"));
    try {
      execute(
          0,
          dialect,
          treeModel
              ? "create database " + database + " with MAX_SCHEMA_REGION_GROUP_NUM=1"
              : "create database " + database + " with(max_schema_region_group_num=1)");
    } finally {
      execute(0, dialect, String.format(schemaRegionConfig, "2"));
    }
  }

  /** A physical JDBC connection has exactly one endpoint and cannot silently redirect to a peer. */
  public static Connection connect(final int index, final String dialect) throws SQLException {
    final DataNodeWrapper node = EnvFactory.getEnv().getDataNodeWrapper(index);
    final Properties properties =
        BaseEnv.constructProperties(
            SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, dialect);
    properties.setProperty(Config.NETWORK_TIMEOUT, "180000");
    return DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + node.getIpAndPortString(), properties);
  }

  public static void execute(final int index, final String dialect, final String... sqls)
      throws SQLException {
    try (Connection connection = connect(index, dialect);
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    }
  }

  public static void assertRows(
      final int index,
      final String dialect,
      final String sql,
      final String header,
      final String... rows)
      throws SQLException {
    try (Connection connection = connect(index, dialect);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql)) {
      TestUtils.assertResultSetEqualUnordered(result, header, Arrays.asList(rows));
    }
  }

  public static void assertAllRows(
      final String dialect, final String sql, final String header, final String... rows)
      throws SQLException {
    // Check physical connections individually; do not read a cross-node wrapper into a Set.
    for (int index = 0; index < EnvFactory.getEnv().getDataNodeWrapperList().size(); index++) {
      assertRows(index, dialect, sql, header, rows);
    }
  }

  public static void awaitAllRows(
      final String dialect, final String sql, final String header, final String... rows) {
    await()
        .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .ignoreException(SQLException.class)
        .untilAsserted(() -> assertAllRows(dialect, sql, header, rows));
  }

  public static void assertFenced(final Statement statement, final String sql) throws SQLException {
    try {
      statement.execute(sql);
      org.junit.Assert.fail("A fenced endpoint accepted: " + sql);
    } catch (SQLException e) {
      assertTrue(
          e.toString(),
          e.getErrorCode() == TSStatusCode.METADATA_LEASE_FENCED.getStatusCode()
              || e.getErrorCode()
                  == TSStatusCode.METADATA_LEASE_FENCED_RETRY_REQUIRED.getStatusCode());
    }
  }

  public static List<TRegionInfo> schemaRegions(final String database) throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      return client
          .showRegion(new TShowRegionReq().setIsTableModel(!database.startsWith("root.")))
          .getRegionInfoList()
          .stream()
          .filter(
              info ->
                  info.getConsensusGroupId().getType() == TConsensusGroupType.SchemaRegion
                      && info.getDatabase().equals(database))
          .collect(Collectors.toList());
    }
  }

  /** Wait for acknowledged fixture writes to reach every actual IoTConsensus data replica. */
  public static void awaitDataReplication(final String database) throws Exception {
    final List<TRegionInfo> replicas;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      replicas =
          client.showRegion(new TShowRegionReq().setIsTableModel(true)).getRegionInfoList().stream()
              .filter(
                  info ->
                      info.getConsensusGroupId().getType() == TConsensusGroupType.DataRegion
                          && info.getDatabase().equals(database))
              .collect(Collectors.toList());
    }
    assertFalse("No actual data replicas for " + database, replicas.isEmpty());
    await()
        .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .ignoreException(TException.class)
        .untilAsserted(
            () -> {
              long localWrites = 0;
              for (TRegionInfo replica : replicas) {
                final DataNodeWrapper node =
                    EnvFactory.getEnv()
                        .dataNodeIdToWrapper(replica.getDataNodeId())
                        .orElseThrow(AssertionError::new);
                try (TTransport transport =
                    DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                        new TSocket(
                            TConfigurationConst.defaultTConfiguration,
                            node.getIp(),
                            node.getDataRegionConsensusPort(),
                            2_000))) {
                  transport.open();
                  final TWaitSyncLogCompleteRes progress =
                      new IoTConsensusIService.Client(new TBinaryProtocol(transport))
                          .waitSyncLogComplete(
                              new TWaitSyncLogCompleteReq(replica.getConsensusGroupId()));
                  assertTrue(progress.toString(), progress.isComplete());
                  localWrites += progress.getSearchIndex();
                }
              }
              assertTrue("No fixture writes observed by data consensus", localWrites > 0);
            });
  }

  public static Map<TSeriesPartitionSlot, TConsensusGroupId> partitions(
      final String database, final List<IDeviceID> devices) throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TClusterParameters parameters = client.showVariables().getClusterParameters();
      final SeriesPartitionExecutor executor =
          SeriesPartitionExecutor.getSeriesPartitionExecutor(
              parameters.getSeriesPartitionExecutorClass(), parameters.getSeriesPartitionSlotNum());
      final List<TSeriesPartitionSlot> slots =
          devices.stream().map(executor::getSeriesPartitionSlot).collect(Collectors.toList());
      final TSchemaPartitionTableResp response =
          client.getSchemaPartitionTableWithSlots(Collections.singletonMap(database, slots));
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      assertTrue(
          "No actual schema partition for " + database,
          response.getSchemaPartitionTable().containsKey(database));
      return response.getSchemaPartitionTable().get(database);
    }
  }

  public static void assertSpansRegions(final String database, final List<IDeviceID> devices)
      throws Exception {
    assertTrue(
        schemaRegions(database).stream().map(TRegionInfo::getConsensusGroupId).distinct().count()
            >= 2);
    assertTrue(
        "The affected devices did not span SchemaRegions",
        partitions(database, devices).values().stream().distinct().count() >= 2);
  }

  public static DataNodeWrapper schemaLeader(final String database) throws Exception {
    final TRegionInfo leader =
        schemaRegions(database).stream()
            .filter(info -> "Leader".equals(info.getRoleType()))
            .findFirst()
            .orElseThrow(AssertionError::new);
    return EnvFactory.getEnv()
        .dataNodeIdToWrapper(leader.getDataNodeId())
        .orElseThrow(AssertionError::new);
  }

  public static DataNodeWrapper schemaLeader(final String database, final int regionId)
      throws Exception {
    final TRegionInfo leader =
        schemaRegions(database).stream()
            .filter(
                info ->
                    info.getConsensusGroupId().getId() == regionId
                        && "Leader".equals(info.getRoleType()))
            .findFirst()
            .orElseThrow(AssertionError::new);
    return EnvFactory.getEnv()
        .dataNodeIdToWrapper(leader.getDataNodeId())
        .orElseThrow(AssertionError::new);
  }

  public static void transferSchemaLeader(
      final String database, final int regionId, final DataNodeWrapper target) throws Exception {
    final TDataNodeLocation location;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      location =
          client.getDataNodeConfiguration(-1).getDataNodeConfigurationMap().values().stream()
              .map(configuration -> configuration.getLocation())
              .filter(candidate -> candidate.getClientRpcEndPoint().getPort() == target.getPort())
              .findFirst()
              .orElseThrow(AssertionError::new);
    }
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try {
      await()
          .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .ignoreExceptionsMatching(e -> e instanceof IOException || e instanceof TException)
          .untilAsserted(
              () -> {
                final DataNodeWrapper current = schemaLeader(database, regionId);
                if (current != target) {
                  // The RPC is handled by the intended new leader, as in RouteBalancer.
                  try (SyncDataNodeInternalServiceClient client =
                      manager.borrowClient(location.getInternalEndPoint())) {
                    assertEquals(
                        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                        client
                            .changeRegionLeader(
                                new TRegionLeaderChangeReq(
                                    new TConsensusGroupId(
                                        TConsensusGroupType.SchemaRegion, regionId),
                                    location))
                            .getStatus()
                            .getCode());
                  }
                }
                assertEquals(target, schemaLeader(database, regionId));
              });
    } finally {
      manager.close();
    }
  }

  /**
   * P0-6: prove installation of a newer snapshot on a previously stopped follower, then lead on it.
   */
  public static void installSchemaSnapshot(
      final String database,
      final int regionId,
      final SqlOperation triggerSnapshot,
      final SqlOperation tailChanges,
      final SqlOperation assertRecoveredState)
      throws Exception {
    final DataNodeWrapper leader = schemaLeader(database, regionId);
    // Both manifests use physical JDBC connections to node 0, so keep that coordinator running.
    final DataNodeWrapper coordinator = EnvFactory.getEnv().getDataNodeWrapper(0);
    final DataNodeWrapper follower =
        schemaRegions(database).stream()
            .filter(info -> info.getConsensusGroupId().getId() == regionId)
            .map(
                info ->
                    EnvFactory.getEnv()
                        .dataNodeIdToWrapper(info.getDataNodeId())
                        .orElseThrow(AssertionError::new))
            .filter(node -> node != leader && node != coordinator)
            .findFirst()
            .orElseThrow(AssertionError::new);
    final String group =
        Utils.fromConsensusGroupIdToRaftGroupId(new SchemaRegionId(regionId)).getUuid().toString();
    final List<Fault> preventNewSnapshots = new ArrayList<>();
    try (Fault created = new Fault(leader, MetadataTestHooks.SNAPSHOT_CREATED + "." + regionId);
        Fault installed =
            new Fault(follower, MetadataTestHooks.SNAPSHOT_LOADED + "." + regionId)
                .arm("observe")) {
      follower.stopForcibly();
      assertFalse(follower.isAlive());
      final long previousIndex = latestSnapshotIndex(follower, group);
      created.arm("observe");
      triggerSnapshot.execute();
      // More than one snapshot may finish during the writes. An older observed directory may
      // already have been purged, so select a completed snapshot that still exists.
      final Path completedSnapshot =
          await()
              .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
              .until(() -> latestCompletedSnapshot(created, previousIndex), Objects::nonNull);
      final long snapshotIndex = snapshotIndex(completedSnapshot.getFileName().toString());
      await()
          .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertTrue(
                      "SchemaRegion logs were not purged past the old snapshot",
                      firstRetainedLogIndex(leader, group) > previousIndex));
      // Keep post-snapshot changes in the Raft log. A later snapshot must not absorb this manifest.
      for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        preventNewSnapshots.add(new Fault(node, "snapshot-create." + regionId).arm("fail"));
      }
      tailChanges.execute();
      follower.start();
      await()
          .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                try (Stream<Path> paths = Files.list(installed.directory)) {
                  assertTrue(
                      "No newer SchemaRegion snapshot was installed on the lagging replica",
                      paths.anyMatch(
                          path -> {
                            final String fileName = path.getFileName().toString();
                            final String prefix =
                                "hit." + MetadataTestHooks.SNAPSHOT_LOADED + "." + regionId + ".";
                            return fileName.startsWith(prefix)
                                && snapshotIndex(fileName.substring(prefix.length()))
                                    >= snapshotIndex;
                          }));
                }
              });
      assertRecoveredState.execute();
      // Client connections can otherwise route schema reads to healthy replicas and hide a bad
      // load.
      transferSchemaLeader(database, regionId, follower);
      assertRecoveredState.execute();
    } finally {
      try {
        for (Fault fault : preventNewSnapshots) {
          fault.close();
        }
      } finally {
        if (!follower.isAlive()) {
          follower.start();
        }
      }
    }
  }

  private static Path latestCompletedSnapshot(final Fault created, final long previousIndex)
      throws IOException {
    Path latest = null;
    long latestIndex = previousIndex;
    try (Stream<Path> paths = Files.list(created.directory)) {
      for (Path evidence : paths.collect(Collectors.toList())) {
        if (!evidence.getFileName().toString().startsWith("hit." + created.point + ".")
            || Files.size(evidence) == 0) {
          continue;
        }
        final Path temporary =
            Paths.get(new String(Files.readAllBytes(evidence), StandardCharsets.UTF_8));
        final String name = temporary.getFileName().toString().replace(".tmp.", "");
        final Path completed = temporary.resolveSibling(name);
        if (Files.isDirectory(completed) && snapshotIndex(name) > latestIndex) {
          latest = completed;
          latestIndex = snapshotIndex(name);
        }
      }
    }
    return latest;
  }

  private static long latestSnapshotIndex(final DataNodeWrapper node, final String group)
      throws IOException {
    try (Stream<Path> paths = Files.walk(Paths.get(node.getNodePath()))) {
      return paths
          .filter(path -> path.toString().contains(group))
          .mapToLong(path -> snapshotIndex(path.getFileName().toString()))
          .max()
          .orElse(0);
    }
  }

  private static long firstRetainedLogIndex(final DataNodeWrapper node, final String group)
      throws IOException {
    try (Stream<Path> paths = Files.walk(Paths.get(node.getNodePath()))) {
      return paths
          .filter(path -> path.toString().contains(group))
          .map(path -> path.getFileName().toString())
          .filter(name -> name.matches("log_([0-9]+-[0-9]+|inprogress_[0-9]+)"))
          .mapToLong(
              name ->
                  Long.parseLong(
                      name.startsWith("log_inprogress_")
                          ? name.substring("log_inprogress_".length())
                          : name.substring("log_".length(), name.indexOf('-'))))
          .min()
          .orElse(0);
    }
  }

  private static long snapshotIndex(final String name) {
    return name.matches("[0-9]+_[0-9]+")
        ? Long.parseLong(name.substring(name.indexOf('_') + 1))
        : 0;
  }

  public static void awaitDifferentSchemaLeader(final String database, final DataNodeWrapper old)
      throws Exception {
    await()
        .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .ignoreExceptionsMatching(e -> e instanceof IOException || e instanceof TException)
        .untilAsserted(() -> assertNotEquals(old, schemaLeader(database)));
  }

  public static void awaitDifferentSchemaLeader(
      final String database, final int regionId, final DataNodeWrapper old) {
    await()
        .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .ignoreExceptionsMatching(e -> e instanceof IOException || e instanceof TException)
        .untilAsserted(() -> assertNotEquals(old, schemaLeader(database, regionId)));
  }

  public static int tableDeviceRegion(final String database, final String table, final String id)
      throws Exception {
    return partitions(
            database,
            Collections.singletonList(
                IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {table, id})))
        .values()
        .iterator()
        .next()
        .getId();
  }

  public static void awaitDropSuccess(final String database, final String table) {
    await()
        .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertRows(
                    0,
                    BaseEnv.TABLE_SQL_DIALECT,
                    "select state, written_mod_file_count, remaining_mod_file_count from information_schema.drop_table_procedures "
                        + "where database='"
                        + database
                        + "' and table_name='"
                        + table
                        + "'",
                    "state,written_mod_file_count,remaining_mod_file_count,",
                    "SUCCESS,1,0,"));
  }

  /** A single-shot process pause; its hit file records the actual procedure ID or captured RPC. */
  public static final class Fault implements AutoCloseable {
    private final Path directory;
    private final String point;
    private final List<Path> controls = new ArrayList<>();

    public Fault(final AbstractNodeWrapper node, final String point) throws IOException {
      this.directory = Paths.get(node.getNodePath(), MetadataTestHooks.DIRECTORY);
      this.point = point;
      assertTrue(
          "Metadata test agent is missing from " + node.getId(),
          Files.exists(directory.resolve("hit.agent-ready")));
      clearEvidence();
    }

    public Fault arm(final String mode) throws IOException {
      final Path control = directory.resolve(mode + "." + point);
      Files.createFile(control);
      controls.add(control);
      return this;
    }

    public void disarm(final String mode) throws IOException {
      Files.deleteIfExists(directory.resolve(mode + "." + point));
    }

    public void release() throws IOException {
      Files.deleteIfExists(directory.resolve("pause." + point));
      Files.deleteIfExists(directory.resolve("blocked." + point));
      Files.deleteIfExists(directory.resolve("fail." + point));
    }

    public String awaitHit() throws IOException {
      return awaitEvidence("hit." + point);
    }

    public void awaitFailure() throws IOException {
      awaitEvidence("hit.failure." + point);
    }

    private String awaitEvidence(final String name) throws IOException {
      final Path hit = directory.resolve(name);
      await()
          .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .until(
              () -> {
                final Path error = directory.resolve("hit.instrumentation-error");
                if (Files.exists(error)) {
                  throw new AssertionError(
                      new String(Files.readAllBytes(error), StandardCharsets.UTF_8));
                }
                return Files.exists(hit) && Files.size(hit) > 0;
              });
      return new String(Files.readAllBytes(hit), StandardCharsets.UTF_8);
    }

    private void clearEvidence() throws IOException {
      try (Stream<Path> paths = Files.list(directory)) {
        for (Path path : paths.collect(Collectors.toList())) {
          final String name = path.getFileName().toString();
          if (name.equals("hit." + point)
              || name.startsWith("hit." + point + ".")
              || name.equals("hit.failure." + point)) {
            Files.deleteIfExists(path);
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      release();
      for (Path control : controls) {
        Files.deleteIfExists(control);
      }
      clearEvidence();
    }
  }

  public interface SqlOperation {
    void execute() throws Exception;
  }

  /**
   * Stop the old leader only after the durable next state is reached, and require another leader to
   * enter the same state with the same procedure ID before releasing it. The client outcome is
   * deliberately separate from the catalog/data assertions made by the caller.
   */
  public static void interruptProcedure(
      final String procedure,
      final String state,
      final boolean rollback,
      final SqlOperation operation)
      throws Exception {
    interruptProcedure(procedure, state, rollback, operation, () -> {});
  }

  public static void interruptProcedure(
      final String procedure,
      final String state,
      final boolean rollback,
      final SqlOperation operation,
      final SqlOperation whilePaused)
      throws Exception {
    final String point = "procedure." + procedure + "." + state;
    final List<Fault> faults = new ArrayList<>();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final int oldIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    final ConfigNodeWrapper old = EnvFactory.getEnv().getConfigNodeWrapper(oldIndex);
    Future<Exception> result = null;
    try {
      for (ConfigNodeWrapper node : EnvFactory.getEnv().getConfigNodeWrapperList()) {
        faults.add(new Fault(node, point).arm("pause"));
      }
      result =
          executor.submit(
              () -> {
                try {
                  operation.execute();
                  return null;
                } catch (SQLException e) {
                  return e;
                }
              });
      final String id = faults.get(oldIndex).awaitHit();
      assertFalse("DDL finished before its pause", result.isDone());
      whilePaused.execute();
      old.stopForcibly();
      assertFalse(old.isAlive());
      await()
          .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .ignoreException(IOException.class)
          .untilAsserted(
              () -> assertNotEquals(oldIndex, EnvFactory.getEnv().getLeaderConfigNodeIndex()));
      final int newIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
      assertEquals(
          "The new leader must recover the original procedure",
          id,
          faults.get(newIndex).awaitHit());
      if (rollback) {
        faults.get(newIndex).arm("fail");
      }
      // Release pauses but keep the requested failure armed until its invocation is observed.
      for (Fault fault : faults) {
        Files.deleteIfExists(fault.directory.resolve("pause." + point));
        Files.deleteIfExists(fault.directory.resolve("blocked." + point));
      }
      if (rollback) {
        faults.get(newIndex).awaitFailure();
      }
      final Exception outcome = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (outcome != null) {
        final int code = ((SQLException) outcome).getErrorCode();
        assertTrue(
            outcome.toString(),
            code == TSStatusCode.INTERNAL_REQUEST_TIME_OUT.getStatusCode()
                || code == TSStatusCode.CAN_NOT_CONNECT_CONFIGNODE.getStatusCode()
                || code == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()
                || code == TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode()
                || (!rollback
                    && "DropTableColumnProcedure".equals(procedure)
                    && "INVALIDATE_CACHE".equals(state)
                    && code == TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                || (!rollback
                    && "CreateTableProcedure".equals(procedure)
                    && "COMMIT_RELEASE".equals(state)
                    && code == TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode()));
      }
    } finally {
      try {
        for (Fault fault : faults) {
          fault.close();
        }
      } finally {
        if (!old.isAlive()) {
          old.start();
        }
        if (result != null && !result.isDone()) {
          result.cancel(true);
        }
        executor.shutdownNow();
        assertTrue(
            "DDL executor did not stop",
            executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        // A shared-cluster successor may immediately broadcast configuration to every ConfigNode.
        // Starting the old process alone does not establish that its Thrift endpoint is ready.
        await()
            .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .ignoreException(IOException.class)
            .ignoreException(TException.class)
            .untilAsserted(
                () -> {
                  try (SyncConfigNodeIServiceClient client =
                      (SyncConfigNodeIServiceClient)
                          EnvFactory.getEnv().getConfigNodeConnection(oldIndex)) {
                    client.showCluster();
                  }
                });
      }
    }
  }
}
