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

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
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
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LOAD TsFile has to keep working when the topology changes while a file is being imported: the
 * route of a region can be re-resolved after a piece was rejected, and a LOAD that notices a
 * migration falls back to inserting the file through the ordinary write path. Either way every
 * point of the file has to end up in the cluster and the migration has to finish.
 *
 * <p>The data replication factor is 2 with three DataNodes, so a region always has exactly one
 * DataNode that can be its migration destination.
 */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBLoadTsFileRegionMigrationIT {

  private static final long PARTITION_INTERVAL = 100_000_000L;
  private static final String DATABASE = "root.load_migrate";
  private static final List<String> DEVICES =
      Arrays.asList(
          DATABASE + ".d1", DATABASE + ".d2", DATABASE + ".d3", DATABASE + ".d4", DATABASE + ".d5");
  private static final List<String> MEASUREMENTS = Arrays.asList("s1", "s2", "s3", "s4");
  private static final int POINT_COUNT_PER_DEVICE = 20_000;
  private static final int DATA_REPLICATION_FACTOR = 2;
  private static final int SCHEMA_REPLICATION_FACTOR = 3;
  private static final int CONFIG_NODE_NUM = 1;
  private static final int DATA_NODE_NUM = 3;

  /**
   * Keep the LOAD blocked after it dispatches a piece until the migration becomes active, so the
   * two operations cannot silently run one after the other on a fast machine.
   */
  private static final double THROTTLED_LOAD_WRITE_THROUGHPUT_BYTES_PER_SECOND = 1;

  /** Sets the LOAD write throughput in the DataNode properties before the cluster starts. */
  private static void setLoadWriteThroughput(final double bytesPerSecond) {
    final MppDataNodeConfig dataNodeConfig =
        (MppDataNodeConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig();
    final Properties properties = new Properties();
    properties.setProperty(
        "load_write_throughput_bytes_per_second", String.valueOf(bytesPerSecond));
    dataNodeConfig.updateProperties(properties);
  }

  private File tmpDir;

  @BeforeClass
  public static void setUpClass() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(PARTITION_INTERVAL);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setDatanodeMemoryProportion("1:10:1:1:1:0");
    EnvFactory.getEnv().getConfig().getCommonConfig().setTargetChunkPointNum(1_000);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxNumberOfPointsInPage(500);
    setLoadWriteThroughput(THROTTLED_LOAD_WRITE_THROUGHPUT_BYTES_PER_SECOND);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(SCHEMA_REPLICATION_FACTOR)
        .setDataReplicationFactor(DATA_REPLICATION_FACTOR);
    EnvFactory.getEnv().initClusterEnvironment(CONFIG_NODE_NUM, DATA_NODE_NUM);
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load-migrate-it").toUri());
  }

  @After
  public void tearDown() throws Exception {
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

  /**
   * Starts a LOAD of a file large enough to still be dispatching pieces when a region of its
   * database is migrated away from a DataNode it was routed to, then asserts that both the LOAD and
   * the migration finish with every point of the file imported.
   */
  @Test
  public void testLoadWhileARegionMigrates() throws Exception {
    final long writtenPointCount = generateTsFile(new File(tmpDir, "load-migrating.tsfile"));

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      createDatabaseAndSchema(statement);
    }

    final ExecutorService loader = Executors.newSingleThreadExecutor();
    try {
      final Future<Exception> load =
          loader.submit(
              () -> {
                try (final Connection connection = EnvFactory.getEnv().getConnection();
                    final Statement statement = connection.createStatement()) {
                  statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");
                  return null;
                } catch (final Exception e) {
                  return e;
                }
              });

      // Wait until the LOAD allocated the data region of its points, then migrate that region while
      // the LOAD is still splitting and dispatching the pieces of the file.
      final int[] migration = awaitMigratableRegion();
      Assert.assertFalse("the LOAD finished before the migration could overlap it", load.isDone());
      try (final Connection connection = EnvFactory.getEnv().getConnection();
          final Statement statement = connection.createStatement()) {
        statement.execute(
            "migrate region " + migration[0] + " from " + migration[1] + " to " + migration[2]);
      }

      // The migration procedure is submitted asynchronously. Keep the LOAD throttled until the
      // procedure has actually started changing the region replicas, then resume it so the two
      // operations overlap instead of running one after the other.
      awaitRegionMigrationActive(migration[0]);
      Assert.assertFalse("the LOAD finished while the migration was starting", load.isDone());
      restoreLoadWriteThroughput();

      // The LOAD may follow the migration with a refreshed route or fall back to the tablet path,
      // but it must not report a failure to the client.
      final Exception loadFailure = load.get(15, TimeUnit.MINUTES);
      Assert.assertNull("the LOAD must survive a region migration: " + loadFailure, loadFailure);

      awaitRegionMigrated(migration[0], migration[1], migration[2]);

      try (final Connection connection = EnvFactory.getEnv().getConnection();
          final Statement statement = connection.createStatement()) {
        awaitPointCount(statement, writtenPointCount);
      }
    } finally {
      try {
        restoreLoadWriteThroughput();
      } catch (final Exception ignored) {
        // Preserve the original failure; the cluster is discarded by tearDownClass.
      }
      loader.shutdownNow();
    }
  }

  private static void restoreLoadWriteThroughput() throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"load_write_throughput_bytes_per_second\"=\"-1\"");
    }
  }

  /**
   * Waits until the LOAD has allocated a data region of the database, and returns the region, one
   * of the DataNodes that replicates it and a DataNode that does not, in that order.
   */
  private static int[] awaitMigratableRegion() {
    final AtomicReference<int[]> migration = new AtomicReference<>();
    Awaitility.await()
        .atMost(5, TimeUnit.MINUTES)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement statement = connection.createStatement()) {
                final Set<Integer> allDataNodes = allDataNodeIds(statement);
                final Map<Integer, Set<Integer>> regionMap = dataRegionMap(statement);
                Assert.assertFalse("the LOAD must allocate a data region", regionMap.isEmpty());

                for (final Map.Entry<Integer, Set<Integer>> entry : regionMap.entrySet()) {
                  final Set<Integer> replicas = entry.getValue();
                  final int destination =
                      allDataNodes.stream()
                          .filter(dataNodeId -> !replicas.contains(dataNodeId))
                          .findAny()
                          .orElse(-1);
                  if (destination < 0) {
                    continue;
                  }
                  migration.set(
                      new int[] {entry.getKey(), replicas.iterator().next(), destination});
                  return;
                }
                Assert.fail("no data region has a free migration destination yet");
              }
            });
    return migration.get();
  }

  /** Waits until the migration procedure has started changing the region's replicas. */
  private static void awaitRegionMigrationActive(final int regionId) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement statement = connection.createStatement();
                  final ResultSet resultSet = statement.executeQuery("show regions")) {
                while (resultSet.next()) {
                  if (!"DataRegion".equals(resultSet.getString(ColumnHeaderConstant.TYPE))
                      || regionId != resultSet.getInt(ColumnHeaderConstant.REGION_ID)) {
                    continue;
                  }
                  final String status = resultSet.getString(ColumnHeaderConstant.STATUS);
                  if (RegionStatus.Adding.getStatus().equals(status)
                      || RegionStatus.Removing.getStatus().equals(status)) {
                    return;
                  }
                }
              }
              Assert.fail("region " + regionId + " has not started migrating");
            });
  }

  /** Waits until the migrated region left the source DataNode and reached the destination. */
  private static void awaitRegionMigrated(
      final int regionId, final int sourceDataNode, final int destinationDataNode) {
    Awaitility.await()
        .atMost(5, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              try (final Connection connection = EnvFactory.getEnv().getConnection();
                  final Statement statement = connection.createStatement()) {
                final Set<Integer> replicas = dataRegionMap(statement).get(regionId);
                Assert.assertNotNull("the migrated region must still exist", replicas);
                Assert.assertFalse(
                    "the migrated region must leave DataNode " + sourceDataNode,
                    replicas.contains(sourceDataNode));
                Assert.assertTrue(
                    "the migrated region must reach DataNode " + destinationDataNode,
                    replicas.contains(destinationDataNode));
              }
            });
  }

  /** Waits until the given statement sees the expected number of imported points. */
  private static void awaitPointCount(final Statement statement, final long expectedPointCount) {
    Awaitility.await()
        .atMost(5, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptions()
        .untilAsserted(() -> Assert.assertEquals(expectedPointCount, queryPointCount(statement)));
  }

  private void createDatabaseAndSchema(final Statement statement) throws SQLException {
    statement.execute("create database " + DATABASE);
    for (final String device : DEVICES) {
      for (final String measurement : MEASUREMENTS) {
        statement.execute(
            "create timeseries " + device + "." + measurement + " " + TSDataType.INT64.name());
      }
    }
  }

  /** The DataRegion rows of `show regions`, mapped to the DataNodes that replicate each region. */
  private static Map<Integer, Set<Integer>> dataRegionMap(final Statement statement)
      throws SQLException {
    final Map<Integer, Set<Integer>> regionMap = new HashMap<>();
    try (final ResultSet resultSet = statement.executeQuery("show regions")) {
      while (resultSet.next()) {
        if (!"DataRegion".equals(resultSet.getString(ColumnHeaderConstant.TYPE))
            || !DATABASE.equals(resultSet.getString(ColumnHeaderConstant.DATABASE))) {
          continue;
        }
        final int regionId = resultSet.getInt(ColumnHeaderConstant.REGION_ID);
        final int dataNodeId = resultSet.getInt(ColumnHeaderConstant.DATA_NODE_ID);
        regionMap.computeIfAbsent(regionId, id -> new HashSet<>()).add(dataNodeId);
      }
    }
    return regionMap;
  }

  private static Set<Integer> allDataNodeIds(final Statement statement) throws SQLException {
    final Set<Integer> dataNodeIds = new HashSet<>();
    try (final ResultSet resultSet = statement.executeQuery("show datanodes")) {
      while (resultSet.next()) {
        dataNodeIds.add(resultSet.getInt(ColumnHeaderConstant.NODE_ID));
      }
    }
    return dataNodeIds;
  }

  private long generateTsFile(final File tsFile) throws Exception {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(MEASUREMENTS.get(0), TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema(MEASUREMENTS.get(1), TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema(MEASUREMENTS.get(2), TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema(MEASUREMENTS.get(3), TSDataType.INT64, TSEncoding.PLAIN));
    try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
      for (final String device : DEVICES) {
        generator.registerTimeseries(device, schemas);
        generator.generateData(device, POINT_COUNT_PER_DEVICE, 1L, false, 1L);
      }
      return generator.getTotalNumber();
    }
  }

  private static long queryPointCount(final Statement statement) throws Exception {
    long actualPointCount = 0;
    for (final String device : DEVICES) {
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
}
