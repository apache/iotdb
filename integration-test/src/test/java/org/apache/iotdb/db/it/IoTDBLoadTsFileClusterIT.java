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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.jdbc.Config;

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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    EnvFactory.getEnv().initClusterEnvironment(CONFIG_NODE_NUM, DATA_NODE_NUM);
  }

  @AfterClass
  public static void tearDownClass() {
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

  private long queryPointCount(final Statement statement) throws Exception {
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
