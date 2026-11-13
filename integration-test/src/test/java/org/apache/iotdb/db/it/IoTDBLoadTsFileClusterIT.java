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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.load.LoadTsFileProgress;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadRef;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.ClusterConstant;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.runtime.ClusterTestResultSet;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.UrlUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotCompatibleTsFileException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
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
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
// Keep the cleanup and immediate-reuse cases consecutive at the end of this shared-cluster suite.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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

  /** The directories a DataRegion keeps its TsFiles in, below `<datanode>/data`. */
  private static final List<String> TSFILE_DIRECTORIES = Arrays.asList("sequence", "unsequence");

  private static final String TSFILE_SUFFIX = ".tsfile";

  /**
   * The prefix of the temporary directories that hold the hard-linked TsFiles of the restart case.
   */
  private static final String TSFILE_SNAPSHOT_PREFIX = "load-cluster-tsfile";

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
    // Keep every kind of merge off for this class: the restart case hard-links the replicas'
    // TsFiles out of the DataNode directory and compares their points while the restarted DataNode
    // catches up, so compaction must not rewrite those files in the meantime.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Arrays.asList("JMX", "PROMETHEUS"))
        .setDeleteWalFilesPeriodInMs(500);
    final Properties metrics = new Properties();
    metrics.setProperty("prometheus_reporter_async_update", "false");
    ((MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(metrics);
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
    // Every waiting attempt deletes its own snapshot directory; this also removes what a JVM that
    // died in the middle of an attempt left behind next to the DataNode directories.
    final File targetDir =
        new File(System.getProperty(ClusterConstant.USER_DIR), ClusterConstant.TARGET);
    final File[] snapshotDirs =
        targetDir.listFiles((dir, name) -> name.startsWith(TSFILE_SNAPSHOT_PREFIX));
    if (snapshotDirs != null) {
      for (final File snapshotDir : snapshotDirs) {
        deleteRecursively(snapshotDir);
      }
    }
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
      awaitDatabaseDataRegionsDeleted();
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

    awaitAllReplicasHaveData();

    EnvFactory.getEnv().getDataNodeWrapper(0).stop();
    Thread.sleep(1_000L);

    final File secondDir = new File(tmpDir, "load-second");
    Assert.assertTrue(secondDir.mkdirs());
    final long secondPointCount = generateTsFile(secondDir, "load-second.tsfile", 3_000, 100_000L);
    try (final Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX
                    + EnvFactory.getEnv().getDataNodeWrapper(1).getIpAndPortString(),
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD);
        final Statement statement = connection.createStatement()) {
      statement.execute("load \"" + secondDir.getAbsolutePath() + "\"");

      final long expectedPointCount = firstPointCount + secondPointCount;
      long actualPointCount = queryPointCount(statement);
      for (int retry = 0; actualPointCount < expectedPointCount && retry < 30; retry++) {
        Thread.sleep(1_000L);
        actualPointCount = queryPointCount(statement);
      }
      Assert.assertEquals(expectedPointCount, actualPointCount);
    }
  }

  @Test
  public void testZDatabaseDataRegionsDeletedByTeardown() throws Exception {
    final long expectedPointCount = generateTsFile(tmpDir, "delete-database.tsfile", 10, 1L);
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");
      awaitPointCount(statement, expectedPointCount);
    }
  }

  @Test
  public void testZZDatabaseNameCanBeReusedAfterTeardown() throws Exception {
    final long expectedPointCount = generateTsFile(tmpDir, "reuse-database.tsfile", 10, 1L);
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");
      awaitPointCount(statement, expectedPointCount);
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
    awaitAllReplicasHaveData();

    // LOAD only the second file while DataNode 0 is down. With three replicas it holds a copy of
    // every region, so it is the DataNode that has to catch up with the LOAD when it comes back.
    final DataNodeWrapper restartedDataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    restartedDataNode.stop();
    Assert.assertFalse(restartedDataNode.isAlive());

    final File secondDir = new File(tmpDir, "sync-second");
    Assert.assertTrue(secondDir.mkdirs());
    final long secondPointCount = generateTsFile(secondDir, "sync-second.tsfile", 3_000, 100_000L);
    final long expectedPointCount = firstPointCount + secondPointCount;
    try (final Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX
                    + EnvFactory.getEnv().getDataNodeWrapper(1).getIpAndPortString(),
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD);
        final Statement statement = connection.createStatement()) {
      statement.execute("load \"" + secondDir.getAbsolutePath() + "\"");
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
    final Map<Integer, Set<Integer>> initialRegions =
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .ignoreExceptions()
            .until(IoTDBLoadTsFileClusterIT::dataRegionReplicas, regions -> regions.size() == 1);
    final int regionId = initialRegions.keySet().iterator().next();
    final Set<Integer> regionReplicas = initialRegions.get(regionId);
    Assert.assertEquals(REPLICATION_FACTOR, regionReplicas.size());
    final Map<Integer, DataNodeWrapper> dataNodesById = new HashMap<>();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      for (final DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        final int nodeId = dataNodeId(statement, node);
        if (regionReplicas.contains(nodeId)) {
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
                replicaSet -> routeNodeIds(replicaSet).equals(regionReplicas));
    Assert.assertEquals(regionReplicas, routeNodeIds(initialRoute));
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
    Assert.assertEquals(regionReplicas, routeNodeIds(route));
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
   * Waits until every data region replica on the given DataNode holds as many points as the other
   * replicas of that region.
   *
   * <p>A replica that did not replay the LOAD keeps its pre-LOAD data, but the replicas that stayed
   * up may keep the same points in more physical TsFiles than the restarted one, so on-disk sizes
   * are not comparable. Every replica's TsFiles are hard-linked into a temporary directory and read
   * back point by point instead.
   */
  private static void awaitRestartedLocalReplicasHaveTheLoadedFile(
      final DataNodeWrapper restartedDataNode) {
    Awaitility.await()
        .atMost(5, TimeUnit.MINUTES)
        .pollInterval(5, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              final Map<Integer, Set<Integer>> regionReplicas = dataRegionReplicas();
              Assert.assertFalse(
                  "the loaded database must have data regions", regionReplicas.isEmpty());

              final Map<Integer, DataNodeWrapper> dataNodesById = new HashMap<>();
              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement statement = connection.createStatement()) {
                for (int dataNodeIndex = 0; dataNodeIndex < DATA_NODE_NUM; dataNodeIndex++) {
                  final DataNodeWrapper dataNode =
                      EnvFactory.getEnv().getDataNodeWrapper(dataNodeIndex);
                  dataNodesById.put(dataNodeId(statement, dataNode), dataNode);
                }
              }

              // Create the snapshots beside the DataNode directories, so that the hard links stay
              // on the file system of the TsFiles they point at.
              final File snapshotRoot =
                  Files.createTempDirectory(
                          new File(restartedDataNode.getNodePath()).getParentFile().toPath(),
                          TSFILE_SNAPSHOT_PREFIX)
                      .toFile();
              try {
                for (final Map.Entry<Integer, Set<Integer>> region : regionReplicas.entrySet()) {
                  final Map<Integer, Integer> pointCounts = new TreeMap<>();
                  for (final int dataNodeId : region.getValue()) {
                    final DataNodeWrapper dataNode = dataNodesById.get(dataNodeId);
                    Assert.assertNotNull(
                        "region "
                            + region.getKey()
                            + " is hosted by an unknown DataNode "
                            + dataNodeId,
                        dataNode);
                    pointCounts.put(
                        dataNodeId,
                        readLocalReplicaPointCount(dataNode, region.getKey(), snapshotRoot));
                  }
                  assertSamePointCounts(region.getKey(), pointCounts);
                }
              } finally {
                Assert.assertTrue(
                    "cannot delete the TsFile snapshots", deleteRecursively(snapshotRoot));
              }
            });
  }

  /** The DataRegion rows of `show regions`, mapped to the DataNodes that hold a replica. */
  private static Map<Integer, Set<Integer>> dataRegionReplicas() throws SQLException {
    final Map<Integer, Set<Integer>> regionReplicas = new HashMap<>();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("show regions")) {
      while (resultSet.next()) {
        if (!"DataRegion".equals(resultSet.getString(ColumnHeaderConstant.TYPE))
            || !DATABASE.equals(resultSet.getString(ColumnHeaderConstant.DATABASE))) {
          continue;
        }
        regionReplicas
            .computeIfAbsent(
                resultSet.getInt(ColumnHeaderConstant.REGION_ID), id -> new HashSet<>())
            .add(resultSet.getInt(ColumnHeaderConstant.DATA_NODE_ID));
      }
    }
    return regionReplicas;
  }

  /**
   * Counts the points of one local replica, after hard-linking the replica's TsFiles into
   * `<snapshotRoot>/<region>/<node>` so that a concurrent flush cannot change them while they are
   * read.
   */
  private static int readLocalReplicaPointCount(
      final DataNodeWrapper dataNode, final int regionId, final File snapshotRoot)
      throws IOException {
    final File replicaDir =
        new File(new File(snapshotRoot, String.valueOf(regionId)), dataNode.getId());
    final File dataRoot = new File(dataNode.getDataNodeDir(), "data");
    final List<File> tsFiles = new ArrayList<>();
    for (final String directory : TSFILE_DIRECTORIES) {
      final File regionDir =
          new File(new File(dataRoot, directory), DATABASE + File.separator + regionId);
      tsFiles.addAll(snapshotTsFiles(regionDir, new File(replicaDir, directory)));
    }
    final Set<String> points = new HashSet<>();
    for (final File tsFile : tsFiles) {
      readPoints(tsFile, points);
    }
    return points.size();
  }

  @Test
  public void testReplicaSnapshotsSkipUnsealedTsFiles() throws Exception {
    final File regionDir = new File(tmpDir, "region");
    final File partitionDir = new File(regionDir, "0");
    Assert.assertTrue(partitionDir.mkdirs());
    final File sealed = new File(partitionDir, "1-1-0-0.tsfile");
    final long expectedPointCount = generateTsFile(partitionDir, sealed.getName(), 10, 1L);
    new TsFileResource(sealed).serialize();

    // An active writer may have no bytes on disk yet, or just an incomplete header.
    final File empty = new File(partitionDir, "2-1-0-0.tsfile");
    Assert.assertTrue(empty.createNewFile());
    final File unsealed = new File(partitionDir, "3-1-0-0.tsfile");
    Files.write(unsealed.toPath(), TSFileConfig.MAGIC_STRING.getBytes(TSFileConfig.STRING_CHARSET));

    final List<File> snapshots = snapshotTsFiles(regionDir, new File(tmpDir, "snapshots"));
    Assert.assertEquals(1, snapshots.size());
    final Set<String> points = new HashSet<>();
    readPoints(snapshots.get(0), points);
    Assert.assertEquals(expectedPointCount, points.size());

    // A damaged file advertised as sealed must still be read and rejected, rather than skipped.
    new TsFileResource(empty).serialize();
    final List<File> damagedSnapshots =
        snapshotTsFiles(regionDir, new File(tmpDir, "damaged-snapshots"));
    Assert.assertEquals(2, damagedSnapshots.size());
    Assert.assertThrows(
        NotCompatibleTsFileException.class,
        () -> {
          for (final File snapshot : damagedSnapshots) {
            readPoints(snapshot, new HashSet<>());
          }
        });
  }

  /** Hard-links only sealed TsFiles below a replica directory into the snapshot directory. */
  private static List<File> snapshotTsFiles(final File regionDir, final File snapshotDir)
      throws IOException {
    final List<File> tsFiles = new ArrayList<>();
    collectTsFiles(regionDir, tsFiles);
    if (tsFiles.isEmpty()) {
      return tsFiles;
    }
    Files.createDirectories(snapshotDir.toPath());
    final List<File> snapshots = new ArrayList<>();
    for (final File tsFile : tsFiles) {
      // The resource sidecar is published after the writer finishes the file. A hard link to an
      // unsealed file still shares its writes and may not even contain a readable header yet.
      if (!new File(tsFile + TsFileResource.RESOURCE_SUFFIX).exists()) {
        continue;
      }
      // The time partition directory keeps the names unique inside one replica.
      final File snapshot =
          new File(snapshotDir, tsFile.getParentFile().getName() + "-" + tsFile.getName());
      try {
        Files.createLink(snapshot.toPath(), tsFile.toPath());
      } catch (final IOException | UnsupportedOperationException e) {
        // A hard link needs the same file system; copy if the snapshots land on another one.
        Files.copy(tsFile.toPath(), snapshot.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      snapshots.add(snapshot);
    }
    return snapshots;
  }

  private static void collectTsFiles(final File directory, final List<File> tsFiles) {
    final File[] children = directory.listFiles();
    if (children == null) {
      return;
    }
    for (final File child : children) {
      if (child.isDirectory()) {
        collectTsFiles(child, tsFiles);
      } else if (child.getName().endsWith(TSFILE_SUFFIX)) {
        tsFiles.add(child);
      }
    }
  }

  /** Adds every non-null point of the given TsFile to the point set. */
  private static void readPoints(final File tsFile, final Set<String> points) throws IOException {
    try (final TsFileSequenceReader sequenceReader = new TsFileSequenceReader(tsFile.getPath());
        final TsFileReader reader = new TsFileReader(sequenceReader)) {
      final List<Path> queryPaths = new ArrayList<>();
      for (final String device : allDevices()) {
        for (final String measurement : MEASUREMENTS) {
          queryPaths.add(new Path(device, measurement, false));
        }
      }
      final QueryDataSet dataSet = reader.query(QueryExpression.create(queryPaths, null));
      // The data set keeps only the series this TsFile actually holds, in the query order.
      final List<Path> storedPaths = dataSet.getPaths();
      while (dataSet.hasNext()) {
        final RowRecord row = dataSet.next();
        final List<Field> fields = row.getFields();
        for (int column = 0; column < storedPaths.size() && column < fields.size(); column++) {
          final Field field = fields.get(column);
          if (field == null || field.getDataType() == null) {
            continue;
          }
          points.add(
              storedPaths.get(column).getFullPath()
                  + "@"
                  + row.getTimestamp()
                  + "="
                  + field.getObjectValue(field.getDataType()));
        }
      }
    }
  }

  /**
   * Asserts that every replica of the region holds the same number of points. A replica that did
   * not replay the LOAD keeps its pre-LOAD data and stays behind.
   */
  private static void assertSamePointCounts(
      final int regionId, final Map<Integer, Integer> pointCounts) {
    if (new HashSet<>(pointCounts.values()).size() <= 1) {
      return;
    }
    Assert.fail(
        "region "
            + regionId
            + " holds a different number of points on its replicas: "
            + pointCounts);
  }

  /** Whether a local region contains at least one TsFile. */
  private static boolean hasData(final Long size) {
    return size != null && size > 0;
  }

  /** Waits until every DataNode has materialized each DataRegion replica locally. */
  private static void awaitAllReplicasHaveData() {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              final Map<Integer, Map<Integer, Long>> regionSizes = dataRegionSizes();
              Assert.assertFalse(
                  "the loaded database must have data regions", regionSizes.isEmpty());
              for (final Map<Integer, Long> replicaSizes : regionSizes.values()) {
                Assert.assertEquals(DATA_NODE_NUM, replicaSizes.size());
                Assert.assertTrue(
                    replicaSizes.values().stream().allMatch(IoTDBLoadTsFileClusterIT::hasData));
              }
            });
  }

  /** The local TsFile sizes of this database, mapped to each DataNode's region replica. */
  private static Map<Integer, Map<Integer, Long>> dataRegionSizes() throws Exception {
    final Map<Integer, Map<Integer, Long>> regionSizes = new HashMap<>();
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      for (int dataNodeIndex = 0; dataNodeIndex < DATA_NODE_NUM; dataNodeIndex++) {
        final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(dataNodeIndex);
        final int dataNodeId = dataNodeId(statement, dataNode);
        for (final String order : Arrays.asList("sequence", "unsequence")) {
          final java.nio.file.Path databaseDir =
              Paths.get(dataNode.getDataNodeDir(), "data", order, DATABASE);
          if (!Files.isDirectory(databaseDir)) {
            continue;
          }
          try (Stream<java.nio.file.Path> regionDirectories = Files.list(databaseDir)) {
            regionDirectories
                .filter(Files::isDirectory)
                .forEach(
                    regionDirectory -> {
                      try {
                        final int regionId =
                            Integer.parseInt(regionDirectory.getFileName().toString());
                        final long size = tsFileSize(regionDirectory);
                        regionSizes
                            .computeIfAbsent(regionId, ignored -> new HashMap<>())
                            .merge(dataNodeId, size, Long::sum);
                      } catch (final IOException | NumberFormatException e) {
                        throw new AssertionError(
                            "cannot inspect " + regionDirectory + " on " + dataNode, e);
                      }
                    });
          }
        }
      }
    }
    return regionSizes;
  }

  private static long tsFileSize(final java.nio.file.Path regionDirectory) throws IOException {
    try (Stream<java.nio.file.Path> files = Files.walk(regionDirectory)) {
      return files
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".tsfile"))
          .mapToLong(
              path -> {
                try {
                  return Files.size(path);
                } catch (final IOException e) {
                  throw new AssertionError("cannot read " + path, e);
                }
              })
          .sum();
    }
  }

  /** Waits for asynchronous region deletion before the next test reuses this database name. */
  private static void awaitDatabaseDataRegionsDeleted() {
    Awaitility.await()
        .atMost(5, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .until(IoTDBLoadTsFileClusterIT::areDatabaseDataRegionsDeleted);
  }

  private static boolean areDatabaseDataRegionsDeleted() throws IOException {
    for (int dataNodeIndex = 0; dataNodeIndex < DATA_NODE_NUM; dataNodeIndex++) {
      final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(dataNodeIndex);
      for (final String order : TSFILE_DIRECTORIES) {
        final java.nio.file.Path databaseDir =
            Paths.get(dataNode.getDataNodeDir(), "data", order, DATABASE);
        if (!Files.isDirectory(databaseDir)) {
          continue;
        }
        try (Stream<java.nio.file.Path> regionDirs = Files.list(databaseDir)) {
          if (regionDirs.anyMatch(Files::isDirectory)) {
            return false;
          }
        }
      }
    }
    return true;
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

  @Test
  public void testDirectoryQueryFallbackRetainsOnlyFailedSources() throws Exception {
    for (final boolean delete : new boolean[] {false, true}) {
      final File directory = new File(tmpDir, delete ? "batch-delete" : "batch-none");
      Assert.assertTrue(directory.mkdir());
      final String devicePrefix = DATABASE + (delete ? ".batch_delete" : ".batch_none");
      final File normal = new File(directory, "401-0-0-0.tsfile");
      final File recoverable = new File(directory, "402-0-0-0.tsfile");
      final File failed = new File(directory, "403-0-0-0.tsfile");
      final File[] sources = {normal, recoverable, failed};
      long recoverableMarkerOffset = -1;
      for (int i = 0; i < sources.length; i++) {
        final long groupMarkerOffset =
            writeFallbackSource(sources[i], devicePrefix + ".d" + i, i == 1);
        if (i == 1) {
          recoverableMarkerOffset = groupMarkerOffset;
        }
      }
      // All footer metadata stays valid, so LOAD must reach scheduling and query fallback.
      replaceByte(recoverable, recoverableMarkerOffset, 0x7f);
      final long compressionOffset = firstChunkCompressionOffset(failed);
      final int originalCompression = replaceByte(failed, compressionOffset, 0x7f);

      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnectionWithSpecifiedDataNode(EnvFactory.getEnv().getDataNodeWrapper(0));
          final Statement statement = connection.createStatement()) {
        final SQLException failure =
            Assert.assertThrows(
                SQLException.class, () -> statement.execute(fallbackLoadSql(directory, delete)));
        assertFallbackLoadFailure(failure, failed);
        Assert.assertFalse(failure.getMessage(), failure.getMessage().contains(normal.getName()));
        Assert.assertFalse(
            failure.getMessage(), failure.getMessage().contains(recoverable.getName()));
        assertFallbackRows(statement, devicePrefix + ".d0");
        assertFallbackRows(statement, devicePrefix + ".d1");
        Assert.assertTrue(
            EnvFactory.getEnv()
                .getDataNodeWrapper(0)
                .logContains(
                    DataNodeQueryMessages
                        .LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED
                        .replace("{}", recoverable.getAbsolutePath())));
        assertFallbackSourceExists(normal, !delete);
        assertFallbackSourceExists(recoverable, !delete);
        assertFallbackSourceExists(failed, true);

        replaceByte(failed, compressionOffset, originalCompression);
        statement.execute(fallbackLoadSql(failed, delete));
        assertFallbackRows(statement, devicePrefix + ".d2");
        assertFallbackRows(statement, devicePrefix + ".d0");
        assertFallbackRows(statement, devicePrefix + ".d1");
        assertFallbackSourceExists(failed, !delete);
      }
    }
  }

  @Test
  public void testConcurrentQueryFallbackFailureRetainsSourceForRetry() throws Exception {
    final File good = new File(tmpDir, "301-0-0-0.tsfile");
    final File bad = new File(tmpDir, "302-0-0-0.tsfile");
    final String device = DATABASE + ".concurrent.d";
    writeFallbackSource(good, device, false, 1);
    writeFallbackSource(bad, device, false, 101);
    final long compressionOffset = firstChunkCompressionOffset(bad);
    final int originalCompression = replaceByte(bad, compressionOffset, 0x7f);
    // One device and partition guarantee identical routing without changing the class's Region
    // allocation policy. Disjoint timestamps expose a failed load borrowing the healthy data.
    final int sharedRegion;
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      // Match the aligned source and avoid racing automatic schema creation in the two LOADs.
      statement.execute(
          "create aligned timeseries " + device + "(region_marker INT64,s1 INT64,s2 INT64)");
      statement.execute("insert into " + device + "(time,region_marker) aligned values(0,0)");
      sharedRegion = assertSharedDataRegion(device);
    }
    final ExecutorService loaders = Executors.newFixedThreadPool(2);
    final CountDownLatch ready = new CountDownLatch(2);
    final CountDownLatch start = new CountDownLatch(1);
    try {
      final List<Future<?>> loads = new ArrayList<>();
      for (final File source : Arrays.asList(good, bad)) {
        loads.add(
            loaders.submit(
                () -> {
                  try (final Connection connection =
                          EnvFactory.getEnv()
                              .getConnectionWithSpecifiedDataNode(
                                  EnvFactory.getEnv().getDataNodeWrapper(0));
                      final Statement statement = connection.createStatement()) {
                    ready.countDown();
                    Assert.assertTrue(start.await(30, TimeUnit.SECONDS));
                    if (source.equals(bad)) {
                      final SQLException failure =
                          Assert.assertThrows(
                              SQLException.class,
                              () -> statement.execute(fallbackLoadSql(source, true)));
                      assertFallbackLoadFailure(failure, bad);
                    } else {
                      statement.execute(fallbackLoadSql(source, true));
                    }
                  }
                  return null;
                }));
      }
      Assert.assertTrue(ready.await(30, TimeUnit.SECONDS));
      start.countDown();
      for (final Future<?> load : loads) {
        load.get(90, TimeUnit.SECONDS);
      }
      assertFallbackSourceExists(good, false);
      assertFallbackSourceExists(bad, true);
      assertLoadsOverlapped(good, bad);
      // Check the failed attempt before a new LOAD can hide or release leaked state.
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> assertLoadMemoryReleased());
      try (final Connection connection = EnvFactory.getEnv().getConnection();
          final Statement statement = connection.createStatement()) {
        assertFallbackRows(statement, device);
        replaceByte(bad, compressionOffset, originalCompression);
        // This is an explicitly new submission after source repair, not restoration of the
        // original failed task.
        statement.execute(fallbackLoadSql(bad, true));
        assertFallbackRows(statement, device, 1, 101);
        Assert.assertEquals(sharedRegion, assertSharedDataRegion(device));
      }
      assertFallbackSourceExists(bad, false);
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> assertLoadMemoryReleased());
    } finally {
      start.countDown();
      loaders.shutdownNow();
      Assert.assertTrue(loaders.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private static int assertSharedDataRegion(final String device) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final ByteArrayOutputStream serializedDevice = new ByteArrayOutputStream();
      IDeviceID.Factory.DEFAULT_FACTORY.create(device).serialize(serializedDevice);
      final TGetRegionIdResp response =
          client.getRegionId(
              new TGetRegionIdReq(TConsensusGroupType.DataRegion)
                  .setDatabase(DATABASE)
                  .setDevice(serializedDevice.toByteArray())
                  .setStartTimeSlot(new TTimePartitionSlot(0))
                  .setEndTimeSlot(new TTimePartitionSlot(0)));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      Assert.assertEquals(1, response.getDataRegionIdListSize());
      final TConsensusGroupId region = response.getDataRegionIdList().get(0);
      final TRegionRouteMapResp routes = client.getLatestRegionRouteMap();
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), routes.getStatus().getCode());
      final List<Integer> replicas = new ArrayList<>();
      routes
          .getRegionRouteMap()
          .get(region)
          .getDataNodeLocations()
          .forEach(node -> replicas.add(node.getClientRpcEndPoint().getPort()));
      final List<Integer> expected = new ArrayList<>();
      for (final DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        expected.add(node.getPort());
      }
      Collections.sort(replicas);
      Collections.sort(expected);
      Assert.assertEquals(REPLICATION_FACTOR, replicas.size());
      Assert.assertEquals(expected, replicas);
      return region.getId();
    }
  }

  private static void assertLoadsOverlapped(final File good, final File bad) throws IOException {
    final DataNodeWrapper coordinator = EnvFactory.getEnv().getDataNodeWrapper(0);
    final List<String> lines =
        Files.readAllLines(
            new File(coordinator.getLogDirPath(), coordinator.getId() + ".log").toPath(),
            StandardCharsets.UTF_8);
    final int goodStart =
        findLogLine(
            lines,
            DataNodeQueryMessages.TSFILE_ARG_IS_A_ARG_MODEL_FILE,
            good.getAbsolutePath(),
            "tree");
    final int badStart =
        findLogLine(
            lines,
            DataNodeQueryMessages.TSFILE_ARG_IS_A_ARG_MODEL_FILE,
            bad.getAbsolutePath(),
            "tree");
    final int goodEnd =
        findLogLine(
            lines,
            DataNodeQueryMessages.LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG,
            good.getAbsolutePath(),
            1,
            1);
    final int badSplitEnd =
        findLogLine(
            lines,
            DataNodeQueryMessages
                .LOAD_TSFILE_S_FAILED_WILL_TRY_TO_CONVERT_TO_TABLETS_AND_INSERT_FAILED_TSFILES_ARG,
            bad.getAbsolutePath());
    Assert.assertTrue(
        "Both LOADs must reach the server before either initial load finishes",
        Math.max(goodStart, badStart) < Math.min(goodEnd, badSplitEnd));
  }

  private static int findLogLine(
      final List<String> lines, final String template, final Object... arguments) {
    final String message = String.format(template.replace("{}", "%s"), arguments);
    for (int line = 0; line < lines.size(); line++) {
      if (lines.get(line).contains(message)) {
        return line;
      }
    }
    throw new AssertionError("Missing LOAD log: " + message);
  }

  private static void assertLoadMemoryReleased() throws Exception {
    // Only DataNodes own LOAD memory. Bound each HTTP request and the outer eventual check.
    for (final DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
      final HttpURLConnection metrics =
          (HttpURLConnection)
              new URL(
                      "http://"
                          + UrlUtils.formatTEndPointIpv4AndIpv6Url(
                              node.getIp(), node.getMetricPort())
                          + "/metrics")
                  .openConnection();
      metrics.setConnectTimeout(5000);
      metrics.setReadTimeout(5000);
      try {
        Assert.assertEquals(HttpURLConnection.HTTP_OK, metrics.getResponseCode());
        final Set<String> gauges = new HashSet<>();
        try (final BufferedReader lines =
            new BufferedReader(
                new InputStreamReader(metrics.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = lines.readLine()) != null) {
            for (final String name :
                Arrays.asList(
                    "LoadTsFileUsedMemory", "LoadTsFileDataCacheMemory", "LoadTsFileOtherMemory")) {
              if (line.startsWith("load_mem{") && line.contains("name=\"" + name + "\"")) {
                Assert.assertEquals(
                    line, 0.0, Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)), 0.0);
                Assert.assertTrue("Duplicate gauge: " + line, gauges.add(name));
              }
            }
          }
        }
        Assert.assertEquals(
            node.getIpAndPortString(),
            new HashSet<>(
                Arrays.asList(
                    "LoadTsFileUsedMemory", "LoadTsFileDataCacheMemory", "LoadTsFileOtherMemory")),
            gauges);
      } finally {
        metrics.disconnect();
      }
    }
  }

  private static String fallbackLoadSql(final File source, final boolean delete) {
    return String.format(
        "load '%s' with ('database-level'='1','on-success'='%s',"
            + "'tablet-conversion-threshold'='-1')",
        source.getAbsolutePath(), delete ? "delete" : "none");
  }

  private static void assertFallbackLoadFailure(final SQLException failure, final File source) {
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), failure.getErrorCode());
    Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(source.getName()));
  }

  private static long writeFallbackSource(
      final File file, final String device, final boolean includeAnchor) throws Exception {
    return writeFallbackSource(file, device, includeAnchor, 1);
  }

  private static long writeFallbackSource(
      final File file, final String device, final boolean includeAnchor, final long startTimestamp)
      throws Exception {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(
                "s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED),
            new MeasurementSchema(
                "s2", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    final String anchor = DATABASE + ".a_fallback_anchor";
    final long groupMarkerOffset;
    try (final TsFileWriter writer = new TsFileWriter(file)) {
      if (includeAnchor) {
        // Preserve a valid first group for the encryption-header check, and keep physical device
        // order consistent with the metadata index so query fallback can resume after this group.
        writer.registerAlignedTimeseries(anchor, schemas);
        final Tablet anchorTablet = new Tablet(anchor, schemas);
        anchorTablet.addTimestamp(0, 0);
        anchorTablet.addValue(0, 0, 0L);
        anchorTablet.addValue(0, 1, 0L);
        writer.writeAligned(anchorTablet);
        writer.flush();
      }
      groupMarkerOffset = writer.getIOWriter().getPos();
      writer.registerAlignedTimeseries(device, schemas);
      final Tablet tablet = new Tablet(device, schemas);
      for (int i = 0; i < 4; i++) {
        tablet.addTimestamp(i, i + startTimestamp);
        tablet.addValue(i, 0, (long) i);
        tablet.addValue(i, 1, (long) i * 10);
      }
      writer.writeAligned(tablet);
    }
    final TsFileResource resource = new TsFileResource(file);
    final IDeviceID deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create(device);
    resource.updateStartTime(deviceId, startTimestamp);
    resource.updateEndTime(deviceId, startTimestamp + 3);
    if (includeAnchor) {
      final IDeviceID anchorId = IDeviceID.Factory.DEFAULT_FACTORY.create(anchor);
      resource.updateStartTime(anchorId, 0);
      resource.updateEndTime(anchorId, 0);
    }
    resource.serialize();
    try (final ModificationFile mods = resource.getExclusiveModFile()) {
      mods.write(
          new TreeDeletionEntry(
              new MeasurementPath(device + ".s1"), startTimestamp + 1, startTimestamp + 1));
    }
    return groupMarkerOffset;
  }

  private static long firstChunkCompressionOffset(final File file) throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      reader.position(TSFileConfig.MAGIC_STRING.length() + 1L);
      reader.readMarker();
      reader.readChunkGroupHeader();
      reader.readChunkHeader(reader.readMarker());
      // The final two header bytes are compression and encoding; corrupt only compression.
      return reader.position() - 2;
    }
  }

  private static int replaceByte(final File file, final long offset, final int value)
      throws IOException {
    try (final RandomAccessFile bytes = new RandomAccessFile(file, "rw")) {
      bytes.seek(offset);
      final int original = bytes.readUnsignedByte();
      bytes.seek(offset);
      bytes.writeByte(value);
      return original;
    }
  }

  private static void assertFallbackSourceExists(final File file, final boolean expected) {
    Assert.assertEquals(file.toString(), expected, file.exists());
    Assert.assertEquals(
        file.toString(), expected, new File(file + TsFileResource.RESOURCE_SUFFIX).exists());
    Assert.assertEquals(
        file.toString(), expected, new File(file + ModificationFile.FILE_SUFFIX).exists());
  }

  private static void assertFallbackRows(final Statement statement, final String device)
      throws SQLException {
    assertFallbackRows(statement, device, 1);
  }

  private static void assertFallbackRows(
      final Statement statement, final String device, final long... startTimestamps)
      throws SQLException {
    try (final ResultSet rows = statement.executeQuery("select s1,s2 from " + device)) {
      for (final long startTimestamp : startTimestamps) {
        for (int i = 0; i < 4; i++) {
          Assert.assertTrue(device, rows.next());
          Assert.assertEquals(i + startTimestamp, rows.getLong(1));
          if (i == 1) {
            Assert.assertNull(rows.getObject(2));
          } else {
            Assert.assertEquals((long) i, rows.getObject(2));
          }
          Assert.assertEquals(i * 10L, rows.getObject(3));
        }
      }
      Assert.assertFalse(device, rows.next());
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
                // A healthy cluster may have no user databases.
                return true;
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
    for (final String device : allDevices()) {
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

  /** Every device the loaded files hold. */
  private static List<String> allDevices() {
    final List<String> devices = new ArrayList<>(DEVICES);
    devices.addAll(ALIGNED_DEVICES);
    return devices;
  }
}
