/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.load.LoadTsFileProgress;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadRef;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.runtime.ClusterTestResultSet;
import org.apache.iotdb.jdbc.Config;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBLoadTsFileClusterIT {

  private static final long PARTITION_INTERVAL = 10_000L;
  private static final String DATABASE = "root.load_cluster";
  private static final List<String> DEVICES =
      Arrays.asList(
          DATABASE + ".d1", DATABASE + ".d2", DATABASE + ".d3", DATABASE + ".d4", DATABASE + ".d5");
  private static final List<String> ALIGNED_DEVICES =
      Arrays.asList(DATABASE + ".a1", DATABASE + ".a2");
  private static final List<String> MEASUREMENTS = Arrays.asList("s1", "s2", "s3", "s4");
  private static final int POINT_COUNT_PER_DEVICE = 10_000;
  private static final int REPLICATION_FACTOR = 3;
  private static final int CONFIG_NODE_NUM = 3;
  private static final int DATA_NODE_NUM = 3;

  /** Bounds the load of a cluster that cannot reach consensus, so the test cannot hang forever. */
  private static final int DEGRADED_LOAD_TIMEOUT_SECONDS = 300;

  private File tmpDir;

  @BeforeClass
  public static void setUpClass() {
    // Started once for the class: per-method startup dominates the runtime.
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(PARTITION_INTERVAL);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setDatanodeMemoryProportion("1:10:1:1:1:0");
    EnvFactory.getEnv().getConfig().getCommonConfig().setTargetChunkPointNum(1_000);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxNumberOfPointsInPage(500);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(REPLICATION_FACTOR)
        .setDataReplicationFactor(REPLICATION_FACTOR)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1);
    // The restart case compares the on-disk size of the replicas, so keep compaction from changing
    // those sizes while the test waits for the restarted DataNode to catch up.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("JMX"))
        .setDeleteWalFilesPeriodInMs(500);
    final Properties retentionProperties = new Properties();
    retentionProperties.setProperty("dn_metric_level", "IMPORTANT");
    retentionProperties.setProperty("load_clean_up_task_execution_delay_time_seconds", "1");
    retentionProperties.setProperty("load_clean_up_task_idle_timeout_seconds", "5");
    // The shared executor otherwise rounds the cleaner's one-second period up to twenty seconds.
    retentionProperties.setProperty(
        "pipe_subtask_executor_cron_heartbeat_event_interval_seconds", "1");
    retentionProperties.setProperty("wal_file_size_threshold_in_byte", "16384");
    ((MppDataNodeConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig())
        .updateProperties(retentionProperties);
    EnvFactory.getEnv().initClusterEnvironment(CONFIG_NODE_NUM, DATA_NODE_NUM);
  }

  @AfterClass
  public static void tearDownClass() {
    // Stop every process before filesystem cleanup can fail on an earlier node's directory.
    EnvFactory.getEnv().shutdownForciblyAllDataNodes();
    EnvFactory.getEnv().shutdownForciblyAllConfigNodes();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load-cluster-it").toUri());
  }

  @After
  public void tearDown() throws Exception {
    // Bring back any DataNode a case stopped, before the next case runs.
    for (int dataNodeIndex = 0; dataNodeIndex < DATA_NODE_NUM; dataNodeIndex++) {
      if (!EnvFactory.getEnv().getDataNodeWrapper(dataNodeIndex).isAlive()) {
        EnvFactory.getEnv().getDataNodeWrapper(dataNodeIndex).start();
      }
    }
    waitUntilClusterAnswers();

    try {
      try (final Connection connection = EnvFactory.getEnv().getConnection();
          final Statement statement = connection.createStatement()) {
        boolean exists = false;
        try (final ResultSet databases = statement.executeQuery("show databases")) {
          while (databases.next()) {
            exists |= DATABASE.equals(databases.getString(1));
          }
        }
        if (exists) {
          statement.execute("delete database " + DATABASE);
        }
      }
    } finally {
      final File[] files = tmpDir.listFiles();
      if (files != null) {
        for (final File file : files) {
          Assert.assertTrue(deleteRecursively(file));
        }
      }
      Assert.assertTrue(tmpDir.delete());
    }
  }

  @Test
  public void testLoadTsFileInCluster() throws Exception {
    final long writtenPointCount;
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "load-cluster-1-0-0-0.tsfile"))) {
      final List<IMeasurementSchema> schemas =
          Arrays.asList(
              new MeasurementSchema(MEASUREMENTS.get(0), TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema(MEASUREMENTS.get(1), TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema(MEASUREMENTS.get(2), TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema(MEASUREMENTS.get(3), TSDataType.INT64, TSEncoding.PLAIN));
      for (final String device : DEVICES) {
        generator.registerTimeseries(device, schemas);
        generator.generateData(device, POINT_COUNT_PER_DEVICE, 1L, false);
      }
      for (final String device : ALIGNED_DEVICES) {
        generator.registerAlignedTimeseries(device, schemas);
        generator.generateData(device, POINT_COUNT_PER_DEVICE, 1L, true, 1_000_000L);
      }
      writtenPointCount = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");

      // queryPointCount also covers the aligned devices, which writtenPointCount includes.
      Assert.assertEquals(writtenPointCount, queryPointCount(statement));
    }
  }

  /** Concurrent LOADs on separate files must not interfere with each other. */
  @Test
  public void testLoadConcurrently() throws Exception {
    final int fileCount = 4;
    final Map<String, Long> directoryToPointCount = new LinkedHashMap<>();
    for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
      final File directory = new File(tmpDir, "concurrent-" + fileIndex);
      Assert.assertTrue(directory.mkdirs());
      // One time partition per file, so the concurrent loads do not interleave in one.
      final long startTimestamp = fileIndex * 100_000_000L + 1L;
      directoryToPointCount.put(
          directory.getAbsolutePath(),
          generateTsFile(directory, "load.tsfile", 2_000, startTimestamp));
    }

    long expectedPointCount = 0L;
    for (final long pointCount : directoryToPointCount.values()) {
      expectedPointCount += pointCount;
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
    }

    final ExecutorService loaders = Executors.newFixedThreadPool(fileCount);
    try {
      final List<Future<?>> loads = new ArrayList<>();
      for (final String directory : directoryToPointCount.keySet()) {
        loads.add(
            loaders.submit(
                () -> {
                  try (final Connection connection = EnvFactory.getEnv().getConnection();
                      final Statement statement = connection.createStatement()) {
                    statement.execute("load \"" + directory + "\"");
                  }
                  return null;
                }));
      }
      loaders.shutdown();
      Assert.assertTrue(loaders.awaitTermination(10, TimeUnit.MINUTES));
      // Surface the failure of any loader instead of letting the point count below fail obscurely.
      for (final Future<?> load : loads) {
        load.get();
      }
    } finally {
      loaders.shutdownNow();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      final long expected = expectedPointCount;
      long actualPointCount = queryPointCount(statement);
      for (int retry = 0; actualPointCount < expected && retry < 30; retry++) {
        Thread.sleep(1_000L);
        actualPointCount = queryPointCount(statement);
      }
      Assert.assertEquals(expected, actualPointCount);
    }
  }

  @Test
  public void testLoadAfterOneDataNodeDown() throws Exception {
    final long firstPointCount = generateTsFile(tmpDir, "load-first.tsfile", 5_000, 1L);

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");
    }

    EnvFactory.getEnv().getDataNodeWrapper(0).stop();
    Thread.sleep(1_000L);

    final long secondPointCount = generateTsFile(tmpDir, "load-second.tsfile", 3_000, 100_000L);
    try (final Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX
                    + EnvFactory.getEnv().getDataNodeWrapper(1).getIpAndPortString(),
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD);
        final Statement statement = connection.createStatement()) {
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");

      final long expectedPointCount = firstPointCount + secondPointCount;
      long actualPointCount = queryPointCount(statement);
      for (int retry = 0; actualPointCount < expectedPointCount && retry < 30; retry++) {
        Thread.sleep(1_000L);
        actualPointCount = queryPointCount(statement);
      }
      Assert.assertEquals(expectedPointCount, actualPointCount);
    }
  }

  /**
   * A LOAD that runs while one DataNode is down is replicated by consensus to the surviving
   * replicas. When the stopped DataNode restarts, its replicas replay the LOAD commands they
   * missed, so the restarted DataNode ends up serving the very same file.
   */
  @Test
  public void testRestartedDataNodeIsSyncedWithTheLoadedFile() throws Exception {
    final long firstPointCount = generateTsFile(tmpDir, "sync-first.tsfile", 5_000, 1L);

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");
      awaitPointCount(statement, firstPointCount);
    }

    // LOAD the second file while DataNode 0 is down. With three replicas it holds a copy of every
    // region, so it is the DataNode that has to catch up with the LOAD when it comes back.
    final DataNodeWrapper restartedDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    restartedDataNode.stop();
    Assert.assertFalse(restartedDataNode.isAlive());

    final long secondPointCount = generateTsFile(tmpDir, "sync-second.tsfile", 3_000, 100_000L);
    final long expectedPointCount = firstPointCount + secondPointCount;
    try (final Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX
                    + EnvFactory.getEnv().getDataNodeWrapper(1).getIpAndPortString(),
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD);
        final Statement statement = connection.createStatement()) {
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");
      awaitPointCount(statement, expectedPointCount);
    }

    restartedDataNode.start();
    Assert.assertTrue(restartedDataNode.isAlive());

    // A pinned connection may still read from another replica, so check the restarted node's own
    // replicas before letting it answer the query.
    awaitRestartedLocalReplicasHaveTheLoadedFile(restartedDataNode);
    awaitPointCount(restartedDataNode, expectedPointCount);
  }

  /**
   * LOAD-P0-04: a completed task still owns WAL payloads while a real Region replica is offline.
   * Witness COMMIT above the replication watermark, restart the payload holder, and require the
   * same retained task to survive until the follower replays it. No new writes trigger cleanup.
   * Local TsFile values (including physical duplicate detection) supplement routed SQL reads.
   */
  @Test
  public void testRetainedPayloadSurvivesRestartUntilSlowReplicaCatchesUp() throws Exception {
    final Map<Long, Long> expectedS1 = new TreeMap<>();
    final Map<Long, Long> expectedS2 = new TreeMap<>();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE + " with max_data_region_group_num=1");
      statement.execute("create timeseries " + DEVICES.get(0) + ".s1 INT64");
      statement.execute("create timeseries " + DEVICES.get(0) + ".s2 INT64");
    }
    // Allocate the sole DataRegion through the existing partition fixture. A seed INSERT would
    // otherwise be replayed after a crash from the sender's older durable checkpoint, introducing
    // unrelated unsealed files and duplicate INSERT points into the LOAD-only physical checks.
    ConfigNodeTestUtils.getOrCreateDataPartitionWithRetry(DATABASE, 0, 1, 0, 1, PARTITION_INTERVAL);
    final Map<Integer, Map<Integer, String>> initialRegions =
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .ignoreExceptions()
            .until(IoTDBLoadTsFileClusterIT::dataRegionSizes, regions -> regions.size() == 1);
    final int regionId = initialRegions.keySet().iterator().next();
    final Map<Integer, String> regionReplicas = initialRegions.get(regionId);
    Assert.assertEquals(REPLICATION_FACTOR, regionReplicas.size());
    final Map<Integer, DataNodeWrapper> dataNodesById = new HashMap<>();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      for (final DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        final int nodeId = dataNodeId(statement, node);
        if (regionReplicas.containsKey(nodeId)) {
          dataNodesById.put(nodeId, node);
        }
      }
    }
    final TRegionReplicaSet initialRoute =
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .ignoreExceptions()
            .until(
                () -> dataRegionRoute(regionId),
                replicaSet -> routeNodeIds(replicaSet).equals(regionReplicas.keySet()));
    Assert.assertEquals(regionReplicas.keySet(), routeNodeIds(initialRoute));
    final TDataNodeLocation slowLocation = initialRoute.getDataNodeLocations().get(1);
    final DataNodeWrapper slowReplica = dataNodesById.get(slowLocation.getDataNodeId());
    Assert.assertNotNull(slowReplica);

    slowReplica.stopForcibly();
    Assert.assertFalse("the slow replica process must be stopped", slowReplica.isAlive());
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(slowReplica), Collections.singletonList(NodeStatus.Unknown));
    // IoTConsensus LOAD selects the first peer in the current route, not SHOW REGIONS' row order
    // or its Ratis-style Role column. Re-read after marking the offline peer Unknown.
    final TRegionReplicaSet route = dataRegionRoute(regionId);
    Assert.assertEquals(regionReplicas.keySet(), routeNodeIds(route));
    final DataNodeWrapper coordinator =
        dataNodesById.get(route.getDataNodeLocations().get(0).getDataNodeId());
    Assert.assertNotNull(coordinator);
    Assert.assertNotEquals(coordinator, slowReplica);
    final long searchIndexBeforeLoad = consensusIndex(coordinator, regionId, "searchIndex");
    final long safeIndexBeforeLoad = consensusIndex(coordinator, regionId, "safeIndex");
    final Map<DataNodeWrapper, Set<String>> taskNamesBeforeLoad = new HashMap<>();
    for (final DataNodeWrapper node : dataNodesById.values()) {
      taskNamesBeforeLoad.put(node, taskDirectoryNames(node, regionId));
    }

    final File source = new File(tmpDir, "retained.tsfile");
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
    try (final TsFileWriter writer = new TsFileWriter(source)) {
      writer.registerTimeseries(new org.apache.tsfile.read.common.Path(DEVICES.get(0)), schemas);
      for (int chunk = 0; chunk < 4; chunk++) {
        final Tablet tablet = new Tablet(DEVICES.get(0), schemas);
        for (int row = 0; row < 8; row++) {
          final int ordinal = 8 * chunk + row;
          final long time = chunk * PARTITION_INTERVAL + row + 1;
          final long value = 1000L + 17L * ordinal;
          tablet.addTimestamp(row, time);
          tablet.addValue("s1", row, value);
          tablet.addValue("s2", row, -value);
          expectedS1.put(time, value);
          expectedS2.put(time, -value);
        }
        writer.writeTree(tablet);
        writer.flush();
      }
    }
    try (final ModificationFile mods =
        new ModificationFile(new File(source + ModificationFile.FILE_SUFFIX), false)) {
      mods.write(
          new TreeDeletionEntry(
              new MeasurementPath(DEVICES.get(0), "s2"),
              PARTITION_INTERVAL + 2,
              2 * PARTITION_INTERVAL + 2));
    }
    try (final Connection connection =
            EnvFactory.getEnv().getWriteOnlyConnectionWithSpecifiedDataNode(coordinator);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "load \""
              + source.getAbsolutePath()
              + "\" with ('on-success'='none', 'tablet-conversion-threshold'='0')");
    }

    Assert.assertTrue("on-success=none must keep the source TsFile", source.isFile());
    Assert.assertTrue(
        "on-success=none must keep the source mods",
        new File(source + ModificationFile.FILE_SUFFIX).isFile());

    final RetainedTask retainedTask =
        awaitRetainedTask(
            coordinator,
            regionId,
            taskNamesBeforeLoad.get(coordinator),
            searchIndexBeforeLoad,
            safeIndexBeforeLoad);
    final File task = retainedTask.taskDir();
    final LoadTsFileProgress.TerminalRecord terminal = retainedTask.terminal();
    assertTsFilesData(listTsFiles(task), expectedS1, expectedS2, "retained task " + task);
    final Map<java.nio.file.Path, byte[]> retained = new HashMap<>();
    try (final Stream<java.nio.file.Path> paths = Files.walk(task.toPath())) {
      for (final java.nio.file.Path path :
          (Iterable<java.nio.file.Path>) paths.filter(Files::isRegularFile)::iterator) {
        retained.put(path, Files.readAllBytes(path));
      }
    }
    Assert.assertTrue(
        retained.keySet().stream().anyMatch(path -> path.toString().endsWith(".progress")));
    Assert.assertTrue(
        retained.keySet().stream()
            .anyMatch(path -> path.toString().endsWith(ModificationFile.FILE_SUFFIX)));
    assertLocalReplicaData(coordinator, expectedS1, expectedS2);

    // IoTConsensus periodically persists peer acknowledgements every three minutes. Check the
    // healthy peer's durable checkpoint before the crash so it will not replay an already applied
    // task after its terminal directory was cleaned; only the offline replica needs this replay.
    Awaitility.await()
        .atMost(4, TimeUnit.MINUTES)
        .until(
            () -> {
              assertRetainedTask(task, terminal, retained);
              Assert.assertTrue(
                  consensusIndex(coordinator, regionId, "safeIndex") < terminal.searchIndex());
              boolean healthyPeersFlushed = true;
              for (final TDataNodeLocation peer : route.getDataNodeLocations()) {
                if (dataNodesById.get(peer.getDataNodeId()) != coordinator
                    && peer.getDataNodeId() != slowLocation.getDataNodeId()) {
                  healthyPeersFlushed &=
                      persistedPeerSyncIndex(coordinator, regionId, peer) >= terminal.searchIndex();
                }
              }
              return healthyPeersFlushed;
            });

    // ASYNC WAL may acknowledge before its buffer reaches disk. This scenario needs durable WAL
    // references before the crash; it does not test the separate in-flight durability window.
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(() -> assertTaskWALPayloads(coordinator, task, terminal));
    coordinator.stopForcibly();
    Assert.assertFalse(
        "inspect WAL only after the holder process has stopped", coordinator.isAlive());
    final Set<Long> taskIndexes = assertTaskWALPayloads(coordinator, task, terminal);
    final long firstPieceIndex = Collections.min(taskIndexes);
    Assert.assertTrue(firstPieceIndex > searchIndexBeforeLoad);
    coordinator.start();
    awaitNode(coordinator);
    // Only the periodic disk scan can reclaim this empty, unmanaged directory. It carries no
    // LOAD command or data: its removal witnesses a real scan after the five-second idle timeout,
    // while the original committed task must remain byte-for-byte intact in that same region.
    final File scanProbe =
        Files.createTempDirectory(task.getParentFile().toPath(), "cleaner-scan-probe-").toFile();
    final long retentionStartNanos = System.nanoTime();
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .until(
            () -> {
              Assert.assertTrue(
                  consensusIndex(coordinator, regionId, "safeIndex") < terminal.searchIndex());
              Assert.assertTrue(
                  peerSyncIndex(coordinator, regionId, slowLocation) < firstPieceIndex);
              assertRetainedTask(task, terminal, retained);
              // A Callable condition propagates assertion failures immediately instead of
              // restarting a successful observation period after a retention violation.
              return System.nanoTime() - retentionStartNanos >= TimeUnit.SECONDS.toNanos(6)
                  && !scanProbe.exists();
            });
    slowReplica.start();
    Awaitility.await()
        // safeIndex is the minimum in-memory acknowledgement, not the WAL deletion watermark.
        // With no subscriptions, the latter is the minimum durable peer checkpoint. Keep checking
        // retention while the follower starts and catches up, until the three-minute periodic
        // flush persists the original COMMIT. Waiting for readiness or scanning the follower first
        // would leave an unchecked window where in-memory acknowledgements could release payloads
        // before the durable checkpoint permits it.
        .atMost(4, TimeUnit.MINUTES)
        .until(
            () -> {
              if (persistedSafeIndex(coordinator, regionId, route) >= terminal.searchIndex()) {
                return true;
              }
              try {
                assertRetainedTask(task, terminal, retained);
              } catch (final AssertionError | IOException e) {
                // The checkpoint may advance and legitimately delete files during this read.
                if (persistedSafeIndex(coordinator, regionId, route) < terminal.searchIndex()) {
                  throw e;
                }
                return true;
              }
              return false;
            });
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(
                  consensusIndex(coordinator, regionId, "safeIndex") >= terminal.searchIndex());
              Assert.assertTrue(
                  peerSyncIndex(coordinator, regionId, slowLocation) >= terminal.searchIndex());
              Assert.assertTrue(
                  persistedSafeIndex(coordinator, regionId, route) >= terminal.searchIndex());
              Assert.assertEquals(
                  "no extra writes may drive replication or cleanup",
                  terminal.searchIndex(),
                  consensusIndex(coordinator, regionId, "searchIndex"));
              // The restarted writer has no pre-crash dispatch queue. The original task's WAL
              // records reference the retained bytes above; the target peer now acknowledges
              // COMMIT.
              Assert.assertTrue(
                  consensusIndex(coordinator, regionId, "LogEntriesFromWAL") >= taskIndexes.size());
              assertNoNewTaskDirectories(
                  dataNodesById.values(), regionId, taskNamesBeforeLoad, task.getName());
            });
    // A durable acknowledgement above already proves application of the original COMMIT. Check
    // readiness and physical data only after observing retention through that durable boundary.
    awaitNode(slowReplica);
    // Local payloads remain readable after staging is gone; SQL also checks the mods boundaries.
    for (final DataNodeWrapper node : dataNodesById.values()) {
      assertLocalReplicaData(node, expectedS1, expectedS2);
    }
    final List<String> expectedRows = new ArrayList<>();
    for (final Map.Entry<Long, Long> row : expectedS1.entrySet()) {
      final boolean deleted =
          row.getKey() >= PARTITION_INTERVAL + 2 && row.getKey() <= 2 * PARTITION_INTERVAL + 2;
      expectedRows.add(
          row.getKey()
              + ","
              + row.getValue()
              + ","
              + (deleted ? "null" : Long.toString(-row.getValue()))
              + ",");
    }
    TestUtils.treeResultSetEqualTestUnordered(
        "select s1,s2 from " + DEVICES.get(0),
        new String[] {"Time", DEVICES.get(0) + ".s1", DEVICES.get(0) + ".s2"},
        expectedRows.toArray(new String[0]));
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet result =
            statement.executeQuery("select s1,s2 from " + DEVICES.get(0) + " limit 1")) {
      for (final ResultSet nodeResult : ClusterTestResultSet.getNodeResultSets(result)) {
        Assert.assertEquals(TSDataType.INT64.name(), nodeResult.getMetaData().getColumnTypeName(2));
        Assert.assertEquals(TSDataType.INT64.name(), nodeResult.getMetaData().getColumnTypeName(3));
      }
    }
  }

  private static RetainedTask awaitRetainedTask(
      final DataNodeWrapper writer,
      final int regionId,
      final Set<String> taskNamesBeforeLoad,
      final long searchIndexBeforeLoad,
      final long safeIndexBeforeLoad) {
    final java.util.concurrent.atomic.AtomicReference<File> taskReference =
        new java.util.concurrent.atomic.AtomicReference<>();
    final java.util.concurrent.atomic.AtomicReference<LoadTsFileProgress.TerminalRecord>
        terminalReference = new java.util.concurrent.atomic.AtomicReference<>();
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              final List<File> newTasks =
                  findNewTaskDirectories(writer, regionId, taskNamesBeforeLoad);
              Assert.assertEquals(
                  "the IoTConsensus write peer must retain exactly this LOAD task",
                  1,
                  newTasks.size());
              final File task = newTasks.get(0);
              final LoadTsFileProgress.TerminalRecord terminal =
                  LoadTsFileProgress.readTerminalOfTaskDir(task);
              Assert.assertNotNull("the LOAD task must have a terminal record", terminal);
              Assert.assertEquals(LoadTsFileConsensusOp.COMMIT, terminal.op());
              Assert.assertTrue(
                  "the task must be newer than the pre-LOAD consensus position",
                  terminal.searchIndex() > searchIndexBeforeLoad);
              Assert.assertTrue(
                  "the writer must have applied the task",
                  consensusIndex(writer, regionId, "searchIndex") >= terminal.searchIndex());
              Assert.assertTrue(
                  "the slow replica must keep the task above the safe-deletion watermark",
                  safeIndexBeforeLoad < terminal.searchIndex()
                      && consensusIndex(writer, regionId, "safeIndex") < terminal.searchIndex());
              taskReference.set(task);
              terminalReference.set(terminal);
            });
    return new RetainedTask(taskReference.get(), terminalReference.get());
  }

  private static Set<String> taskDirectoryNames(final DataNodeWrapper node, final int regionId) {
    final Set<String> names = new HashSet<>();
    final File regionDir =
        new File(node.getDataNodeDir(), "data/load/" + DATABASE + "-" + regionId);
    final File[] taskDirs = regionDir.listFiles(File::isDirectory);
    if (taskDirs != null) {
      for (final File taskDir : taskDirs) {
        names.add(taskDir.getName());
      }
    }
    return names;
  }

  private static List<File> findNewTaskDirectories(
      final DataNodeWrapper node, final int regionId, final Set<String> taskNamesBeforeLoad) {
    final File regionDir =
        new File(node.getDataNodeDir(), "data/load/" + DATABASE + "-" + regionId);
    final File[] taskDirs = regionDir.listFiles(File::isDirectory);
    if (taskDirs == null) {
      return Collections.emptyList();
    }
    final List<File> newTaskDirs = new ArrayList<>();
    for (final File taskDir : taskDirs) {
      if (!taskNamesBeforeLoad.contains(taskDir.getName())) {
        newTaskDirs.add(taskDir);
      }
    }
    return newTaskDirs;
  }

  private static void assertNoNewTaskDirectories(
      final Iterable<DataNodeWrapper> nodes,
      final int regionId,
      final Map<DataNodeWrapper, Set<String>> taskNamesBeforeLoad,
      final String expectedTaskName) {
    for (final DataNodeWrapper node : nodes) {
      final List<File> remaining =
          findNewTaskDirectories(node, regionId, taskNamesBeforeLoad.get(node));
      Assert.assertTrue(
          "the retained task must be deleted on DataNode "
              + node.getId()
              + " after the safe watermark; remaining="
              + remaining,
          remaining.stream().noneMatch(task -> task.getName().equals(expectedTaskName)));
      Assert.assertTrue(
          "an unrelated new task must not be left behind on DataNode " + node.getId(),
          remaining.isEmpty());
    }
  }

  private static TRegionReplicaSet dataRegionRoute(final int regionId) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TRegionReplicaSet route =
          client
              .getLatestRegionRouteMap()
              .getRegionRouteMap()
              .get(new DataRegionId(regionId).convertToTConsensusGroupId());
      Assert.assertNotNull("missing DataRegion route " + regionId, route);
      return route;
    }
  }

  private static Set<Integer> routeNodeIds(final TRegionReplicaSet route) {
    final Set<Integer> ids = new HashSet<>();
    for (final TDataNodeLocation location : route.getDataNodeLocations()) {
      ids.add(location.getDataNodeId());
    }
    return ids;
  }

  private static long consensusIndex(
      final DataNodeWrapper node, final int regionId, final String kind) throws Exception {
    return consensusMetric(node, regionId, kind, "");
  }

  private static long peerSyncIndex(
      final DataNodeWrapper writer, final int regionId, final TDataNodeLocation peer)
      throws Exception {
    return consensusMetric(
        writer,
        regionId,
        "currentSyncIndex",
        ",tag.name="
            + ObjectName.quote(
                "logDispatcher-"
                    + peer.getDataRegionConsensusEndPoint().getIp()
                    + ":"
                    + peer.getDataRegionConsensusEndPoint().getPort()));
  }

  private static long persistedPeerSyncIndex(
      final DataNodeWrapper writer, final int regionId, final TDataNodeLocation peer) {
    // IndexController restores the largest <peerId>-<index> checkpoint in this region's directory.
    final File directory =
        new File(
            writer.getDataNodeDir(),
            "consensus/data_region/"
                + new DataRegionId(regionId).getType().getValue()
                + "_"
                + regionId);
    final String prefix = peer.getDataNodeId() + "-";
    final File[] checkpoints = directory.listFiles((dir, name) -> name.matches(prefix + "\\d+"));
    long index = -1;
    if (checkpoints != null) {
      for (final File checkpoint : checkpoints) {
        index = Math.max(index, Long.parseLong(checkpoint.getName().substring(prefix.length())));
      }
    }
    return index;
  }

  private static long persistedSafeIndex(
      final DataNodeWrapper writer, final int regionId, final TRegionReplicaSet route) {
    long index = Long.MAX_VALUE;
    for (final TDataNodeLocation peer : route.getDataNodeLocations()) {
      if (peer.getDataRegionConsensusEndPoint().getPort() != writer.getDataRegionConsensusPort()
          || !peer.getDataRegionConsensusEndPoint().getIp().equals(writer.getIp())) {
        index = Math.min(index, persistedPeerSyncIndex(writer, regionId, peer));
      }
    }
    Assert.assertNotEquals("the writer must have remote peers", Long.MAX_VALUE, index);
    return index;
  }

  private static long consensusMetric(
      final DataNodeWrapper node, final int regionId, final String kind, final String peerTag)
      throws Exception {
    final int[] ports = node.getPortList();
    final JMXServiceURL url =
        new JMXServiceURL(
            "service:jmx:rmi:///jndi/rmi://"
                + node.getIp()
                + ":"
                + ports[ports.length - 1]
                + "/jmxrmi");
    try (final JMXConnector connector = JMXConnectorFactory.connect(url)) {
      final ObjectName pattern =
          new ObjectName(
              "org.apache.iotdb.metrics:name=iot_consensus,tag.region="
                  + ObjectName.quote(new DataRegionId(regionId).toString())
                  + ",tag.type="
                  + ObjectName.quote(kind)
                  + peerTag
                  + ",*");
      final java.util.Set<ObjectName> names =
          connector.getMBeanServerConnection().queryNames(pattern, null);
      Assert.assertEquals("missing consensus metric " + pattern, 1, names.size());
      return ((Number)
              connector.getMBeanServerConnection().getAttribute(names.iterator().next(), "Value"))
          .longValue();
    }
  }

  private static void assertLocalReplicaData(
      final DataNodeWrapper node,
      final Map<Long, Long> expectedS1,
      final Map<Long, Long> expectedS2)
      throws Exception {
    final List<File> files = new ArrayList<>();
    for (final String space : Arrays.asList("sequence", "unsequence")) {
      files.addAll(listTsFiles(new File(node.getDataNodeDir(), "data/" + space + "/" + DATABASE)));
    }
    assertTsFilesData(files, expectedS1, expectedS2, "local replica on " + node.getId());
  }

  private static List<File> listTsFiles(final File directory) throws Exception {
    final List<File> files = new ArrayList<>();
    if (directory.exists()) {
      try (final Stream<java.nio.file.Path> paths = Files.walk(directory.toPath())) {
        paths
            .filter(path -> path.toString().endsWith(".tsfile"))
            .map(java.nio.file.Path::toFile)
            .forEach(files::add);
      }
    }
    return files;
  }

  private static void assertTsFilesData(
      final List<File> files,
      final Map<Long, Long> expectedS1,
      final Map<Long, Long> expectedS2,
      final String description)
      throws Exception {
    final Map<String, Map<Long, Long>> raw = new HashMap<>();
    final Map<String, Map<Long, Long>> visible = new HashMap<>();
    for (final String measurement : Arrays.asList("s1", "s2")) {
      raw.put(measurement, new TreeMap<>());
      visible.put(measurement, new TreeMap<>());
    }
    for (final File file : files) {
      final List<ModEntry> modifications = ModificationFile.readAllModifications(file, false);
      try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
        for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> device :
            reader.getAllTimeseriesMetadata(true).entrySet()) {
          Assert.assertEquals(description, DEVICES.get(0), device.getKey().toString());
          for (final TimeseriesMetadata series : device.getValue()) {
            final String measurement = series.getMeasurementId();
            Assert.assertTrue(
                description + ": unexpected measurement " + measurement,
                raw.containsKey(measurement));
            Assert.assertEquals(description, TSDataType.INT64, series.getTsDataType());
            final MeasurementPath seriesPath = new MeasurementPath(DEVICES.get(0), measurement);
            // Read each physical chunk/page directly: TsFileReader.query may merge equal timestamps
            // within one file and hide a duplicate import even when its SQL count is unchanged.
            for (final IChunkMetadata metadata : series.getChunkMetadataList()) {
              try (final ChunkReader chunk =
                  new ChunkReader(reader.readMemChunk((ChunkMetadata) metadata))) {
                while (chunk.hasNextSatisfiedPage()) {
                  final BatchData page = chunk.nextPageData();
                  while (page.hasCurrent()) {
                    final long time = page.currentTime();
                    final long value = page.getLong();
                    Assert.assertNull(
                        description + ": duplicate physical point " + measurement + "@" + time,
                        raw.get(measurement).put(time, value));
                    boolean deleted = false;
                    for (final ModEntry modification : modifications) {
                      Assert.assertTrue(description, modification instanceof TreeDeletionEntry);
                      final TreeDeletionEntry deletion = (TreeDeletionEntry) modification;
                      deleted |=
                          deletion.matches(seriesPath)
                              && deletion.getStartTime() <= time
                              && time <= deletion.getEndTime();
                    }
                    if (!deleted) {
                      visible.get(measurement).put(time, value);
                    }
                    page.next();
                  }
                }
              }
            }
          }
        }
      }
    }
    Assert.assertEquals(description + ": raw s1", expectedS1, raw.get("s1"));
    Assert.assertEquals(description + ": raw s2", expectedS2, raw.get("s2"));
    final Map<Long, Long> visibleS2 = new TreeMap<>(expectedS2);
    visibleS2
        .keySet()
        .removeIf(time -> time >= PARTITION_INTERVAL + 2 && time <= 2 * PARTITION_INTERVAL + 2);
    // Apply each file's own mods. One correct mods file cannot hide a missing/wrong deletion on
    // another partition, and extra deletions of s1 or of boundary points are detected as well.
    Assert.assertEquals(description + ": visible s1", expectedS1, visible.get("s1"));
    Assert.assertEquals(description + ": visible s2", visibleS2, visible.get("s2"));
  }

  private static Set<Long> assertTaskWALPayloads(
      final DataNodeWrapper writer,
      final File task,
      final LoadTsFileProgress.TerminalRecord terminal)
      throws Exception {
    // The offline peer pins these indexes. Before the stop this read waits for durable ASYNC WAL;
    // after the stop it confirms the same records survived, without an active writer racing it.
    final Map<Long, LoadTsFileConsensusNode> commands = new TreeMap<>();
    final List<String> walFiles = new ArrayList<>();
    try (final Stream<java.nio.file.Path> paths =
        Files.walk(new File(writer.getWalDir()).toPath())) {
      for (final java.nio.file.Path wal :
          (Iterable<java.nio.file.Path>)
              paths.filter(path -> path.toString().endsWith(".wal"))::iterator) {
        walFiles.add(wal.getFileName() + ":" + Files.size(wal));
        try (final WALReader reader = new WALReader(wal.toFile(), true)) {
          while (reader.hasNext()) {
            final Object value = reader.next().getValue();
            if (value instanceof LoadTsFileConsensusNode command
                && task.getName().equals(command.getLoadId())) {
              Assert.assertNull(
                  "duplicate writer WAL index", commands.put(command.getSearchIndex(), command));
            }
          }
        }
      }
    }
    Assert.assertFalse(
        "the retained task must have original writer WAL records; files=" + walFiles,
        commands.isEmpty());
    final LoadTsFileConsensusNode commit = commands.get(terminal.searchIndex());
    Assert.assertNotNull("COMMIT must be in this writer's WAL", commit);
    Assert.assertEquals(LoadTsFileConsensusOp.COMMIT, commit.getOp());
    Assert.assertTrue(
        commands.values().stream()
            .anyMatch(command -> command.getOp() == LoadTsFileConsensusOp.PREPARE));
    final Set<Long> chunkIndexes = new HashSet<>();
    for (final LoadTsFileConsensusNode command : commands.values()) {
      if (command.getOp() != LoadTsFileConsensusOp.PIECE) {
        continue;
      }
      for (final TsFileData data : command.getTsFileDataList()) {
        if (!(data instanceof ChunkData chunk)) {
          continue;
        }
        Assert.assertFalse(
            "WAL must reference staged payload, not inline bytes",
            chunk.getChunkPayloadRefs().isEmpty());
        for (final ChunkPayloadRef ref : chunk.getChunkPayloadRefs()) {
          final File file = stagedReferenceFile(writer, task, ref.getFilePath());
          Assert.assertTrue(ref.getSize() > 0 && ref.getOffset() + ref.getSize() <= file.length());
          Assert.assertTrue(
              "payload reference must match this task's indexed progress",
              new LoadTsFileProgress(file)
                  .readAllRecords().stream()
                      .anyMatch(
                          record ->
                              record.searchIndex() == command.getSearchIndex()
                                  && record.physicalEnd() - record.dataLength() == ref.getOffset()
                                  && record.dataLength() == ref.getSize()));
          chunkIndexes.add(command.getSearchIndex());
        }
      }
    }
    Assert.assertFalse("the LOAD must need chunk payload replay", chunkIndexes.isEmpty());
    final Set<Long> progressIndexes = new HashSet<>();
    for (final File staged : listTsFiles(task)) {
      for (final LoadTsFileProgress.ChunkRangeRecord record :
          new LoadTsFileProgress(staged).readAllRecords()) {
        progressIndexes.add(record.searchIndex());
      }
    }
    Assert.assertEquals(
        "all staged chunks must belong to these WAL PIECEs", progressIndexes, chunkIndexes);
    return commands.keySet();
  }

  private static File stagedReferenceFile(
      final DataNodeWrapper writer, final File task, final String recordedPath) throws Exception {
    final File recorded = new File(recordedPath);
    final File file =
        recorded.isAbsolute()
            ? recorded
            : new File(new File(writer.getDataNodeDir(), "data/load"), recordedPath);
    Assert.assertTrue(
        "WAL must refer to this retained task: " + file,
        file.getCanonicalFile().toPath().startsWith(task.getCanonicalFile().toPath()));
    Assert.assertTrue("missing staged WAL payload " + file, file.isFile());
    return file;
  }

  private static void assertRetainedTask(
      final File task,
      final LoadTsFileProgress.TerminalRecord terminal,
      final Map<java.nio.file.Path, byte[]> retained)
      throws IOException {
    Assert.assertEquals(terminal, LoadTsFileProgress.readTerminalOfTaskDir(task));
    final Set<java.nio.file.Path> currentFiles = new HashSet<>();
    try (final Stream<java.nio.file.Path> paths = Files.walk(task.toPath())) {
      paths.filter(Files::isRegularFile).forEach(currentFiles::add);
    }
    Assert.assertEquals(retained.keySet(), currentFiles);
    for (final Map.Entry<java.nio.file.Path, byte[]> file : retained.entrySet()) {
      Assert.assertArrayEquals(
          "retained payload changed: " + file.getKey(),
          file.getValue(),
          Files.readAllBytes(file.getKey()));
    }
  }

  private record RetainedTask(File taskDir, LoadTsFileProgress.TerminalRecord terminal) {}

  /**
   * Waits until every data region replica on the given DataNode has the same on-disk size as the
   * other replicas of that region. A node that did not replay the LOAD keeps its pre-LOAD size.
   */
  private static void awaitRestartedLocalReplicasHaveTheLoadedFile(
      final DataNodeWrapper restartedDataNode) {
    Awaitility.await()
        .atMost(5, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              final Map<Integer, Map<Integer, String>> regionSizes = dataRegionSizes();
              Assert.assertFalse(
                  "the loaded database must have data regions", regionSizes.isEmpty());

              int restartedDataNodeId;
              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement statement = connection.createStatement()) {
                restartedDataNodeId = dataNodeId(statement, restartedDataNode);
              }

              for (final Map.Entry<Integer, Map<Integer, String>> region : regionSizes.entrySet()) {
                final Map<Integer, String> replicaSizes = region.getValue();
                // A region without data on any replica has nothing to catch up with.
                if (replicaSizes.values().stream().noneMatch(IoTDBLoadTsFileClusterIT::hasData)) {
                  continue;
                }
                final String restartedSize = replicaSizes.get(restartedDataNodeId);
                Assert.assertTrue(
                    "region " + region.getKey() + " has no data on the restarted DataNode",
                    hasData(restartedSize));
                replicaSizes.forEach(
                    (dataNodeId, size) -> {
                      if (dataNodeId != restartedDataNodeId) {
                        Assert.assertEquals(
                            "region " + region.getKey() + " differs on DataNode " + dataNodeId,
                            size,
                            restartedSize);
                      }
                    });
              }
            });
  }

  /** Whether a `show regions` size means the replica holds data. */
  private static boolean hasData(final String size) {
    return size != null && !"Unknown".equals(size) && !"0 B".equals(size);
  }

  /** The DataRegion rows of `show regions`, mapped to each replica's on-disk size. */
  private static Map<Integer, Map<Integer, String>> dataRegionSizes() throws Exception {
    final Map<Integer, Map<Integer, String>> regionSizes = new HashMap<>();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("show regions")) {
      while (resultSet.next()) {
        if (!"DataRegion".equals(resultSet.getString(ColumnHeaderConstant.TYPE))
            || !DATABASE.equals(resultSet.getString(ColumnHeaderConstant.DATABASE))) {
          continue;
        }
        regionSizes
            .computeIfAbsent(
                resultSet.getInt(ColumnHeaderConstant.REGION_ID), id -> new HashMap<>())
            .put(
                resultSet.getInt(ColumnHeaderConstant.DATA_NODE_ID),
                resultSet.getString(ColumnHeaderConstant.TSFILE_SIZE));
      }
    }
    return regionSizes;
  }

  /** The cluster id of the given DataNode, matched by its client RPC endpoint. */
  private static int dataNodeId(final Statement statement, final DataNodeWrapper dataNode)
      throws SQLException {
    try (final ResultSet resultSet = statement.executeQuery("show datanodes")) {
      while (resultSet.next()) {
        if (dataNode.getIp().equals(resultSet.getString(ColumnHeaderConstant.RPC_ADDRESS))
            && dataNode.getPort() == resultSet.getInt(ColumnHeaderConstant.RPC_PORT)) {
          return resultSet.getInt(ColumnHeaderConstant.NODE_ID);
        }
      }
    }
    throw new AssertionError("cannot find " + dataNode.getIpAndPortString() + " in show datanodes");
  }

  /** Waits until the given statement sees the expected number of imported points. */
  private static void awaitPointCount(final Statement statement, final long expectedPointCount) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(() -> Assert.assertEquals(expectedPointCount, queryPointCount(statement)));
  }

  /** Waits until the points of every loaded file are queryable through the given DataNode. */
  private static void awaitPointCount(
      final DataNodeWrapper dataNode, final long expectedPointCount) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              try (final Connection connection = EnvFactory.getEnv().getConnection(dataNode);
                  final Statement statement = connection.createStatement()) {
                Assert.assertEquals(expectedPointCount, queryPointCount(statement));
              }
            });
  }

  /** With two of three DataNodes down the LOAD must fail with a message. */
  @Test
  public void testLoadWithTwoDataNodesDownFailsWithAMeaningfulMessage() throws Exception {
    generateTsFile(tmpDir, "load-two-down.tsfile", 1_000, 1L);

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
    }

    EnvFactory.getEnv().getDataNodeWrapper(1).stop();
    EnvFactory.getEnv().getDataNodeWrapper(2).stop();

    try (final Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX
                    + EnvFactory.getEnv().getDataNodeWrapper(0).getIpAndPortString(),
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD);
        final Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(DEGRADED_LOAD_TIMEOUT_SECONDS);

      final SQLException failure =
          Assert.assertThrows(
              SQLException.class,
              () -> statement.execute("load \"" + tmpDir.getAbsolutePath() + "\""));

      Assert.assertNotNull("a failed LOAD must carry a message", failure.getMessage());
      final String message = failure.getMessage().trim();
      Assert.assertFalse("a failed LOAD must carry a non-empty message", message.isEmpty());
      // A bare exception class name tells an operator nothing.
      Assert.assertFalse(
          "the failure must explain itself instead of naming a class: " + message,
          message.matches("[A-Za-z0-9_.$]*(Exception|Error)"));
    }
  }

  private void createDatabaseAndSchema(final Statement statement) throws SQLException {
    statement.execute("create database " + DATABASE);
    for (final String device : DEVICES) {
      for (final String measurement : MEASUREMENTS) {
        statement.execute(
            "create timeseries " + device + "." + measurement + " " + TSDataType.INT64.name());
      }
    }
    for (final String device : ALIGNED_DEVICES) {
      statement.execute(
          "create aligned timeseries " + device + "(s1 INT64, s2 INT64, s3 INT64, s4 INT64)");
    }
  }

  /** Waits until the cluster answers again, so the next case does not inherit a broken one. */
  private static void awaitNode(final DataNodeWrapper node) {
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .until(
            () -> {
              try (final Connection connection = EnvFactory.getEnv().getConnection(node);
                  final Statement statement = connection.createStatement();
                  final ResultSet resultSet = statement.executeQuery("show databases")) {
                return resultSet.next();
              }
            });
  }

  /** Waits until the cluster answers again, so the next case does not inherit a broken one. */
  private static void waitUntilClusterAnswers() {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .until(
            () -> {
              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement statement = connection.createStatement();
                  final ResultSet resultSet = statement.executeQuery("show databases")) {
                return resultSet.next();
              }
            });
  }

  private static boolean deleteRecursively(final File file) {
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        if (!deleteRecursively(child)) {
          return false;
        }
      }
    }
    return file.delete();
  }

  private long generateTsFile(
      final File directory,
      final String fileName,
      final int pointCountPerDevice,
      final long startTimestamp)
      throws Exception {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(MEASUREMENTS.get(0), TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema(MEASUREMENTS.get(1), TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema(MEASUREMENTS.get(2), TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema(MEASUREMENTS.get(3), TSDataType.INT64, TSEncoding.PLAIN));
    try (final TsFileGenerator generator = new TsFileGenerator(new File(directory, fileName))) {
      for (final String device : DEVICES) {
        generator.registerTimeseries(device, schemas);
        generator.generateData(device, pointCountPerDevice, 1L, false, startTimestamp);
      }
      final long alignedStartTimestamp = startTimestamp + 1_000_000L;
      for (final String device : ALIGNED_DEVICES) {
        generator.registerAlignedTimeseries(device, schemas);
        generator.generateData(device, pointCountPerDevice, 1L, true, alignedStartTimestamp);
      }
      return generator.getTotalNumber();
    }
  }

  private static long queryPointCount(final Statement statement) throws Exception {
    long actualPointCount = 0;
    final List<String> allDevices = new ArrayList<>(DEVICES);
    allDevices.addAll(ALIGNED_DEVICES);
    for (final String device : allDevices) {
      for (final String measurement : MEASUREMENTS) {
        try (final ResultSet resultSet =
            statement.executeQuery("select count(" + measurement + ") from " + device)) {
          Assert.assertTrue(resultSet.next());
          actualPointCount += resultSet.getLong(1);
        }
      }
    }
    return actualPointCount;
  }
}
