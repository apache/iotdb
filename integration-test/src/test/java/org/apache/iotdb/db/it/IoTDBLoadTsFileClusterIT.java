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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.ClusterConstant;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.jdbc.Config;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
        .setDataReplicationFactor(REPLICATION_FACTOR);
    // Keep every kind of merge off for this class: the restart case hard-links the replicas'
    // TsFiles out of the DataNode directory and compares their points while the restarted DataNode
    // catches up, so compaction must not rewrite those files in the meantime.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment(CONFIG_NODE_NUM, DATA_NODE_NUM);
  }

  @AfterClass
  public static void tearDownClass() {
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

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("delete database " + DATABASE);
    } catch (final Exception ignored) {
    }

    final File[] files = tmpDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        Assert.assertTrue(deleteRecursively(file));
      }
    }
    Assert.assertTrue(tmpDir.delete());
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
                "root",
                "root");
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
                "root",
                "root");
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

  /** Hard-links the TsFiles below a replica directory into the snapshot directory. */
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
                "root",
                "root");
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
