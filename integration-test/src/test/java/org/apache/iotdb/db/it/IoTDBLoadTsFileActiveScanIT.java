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

package org.apache.iotdb.db.it;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.exception.InconsistentDataException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBLoadTsFileActiveScanIT {
  private static final String ORIGINAL_DIR = "ext/load/p1-original";
  private static final String RELOADED_DIR = "ext/load/p1-reloaded";
  private static final List<IMeasurementSchema> SCHEMAS =
      Arrays.asList(
          new MeasurementSchema("s", TSDataType.INT64),
          new MeasurementSchema("txt", TSDataType.TEXT));

  @BeforeClass
  public static void setUpClass() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimePartitionInterval(10_000)
        .setDataReplicationFactor(3)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setLoadActiveListeningCheckIntervalSeconds(1);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void restoreListeningDirectory() throws Exception {
    setListeningDirectory("ext/load/pending");
  }

  @Test
  public void testIncompletePublicationSurvivesScannerRestartAndPreservesMods() throws Exception {
    final String device = "root.p1scan.incomplete.d";
    final String pendingDirectory = "ext/load/p1-incomplete";
    final File directory = listeningDirectory(pendingDirectory);
    final File source = new File(directory, "710-0-0-0.tsfile");
    setListeningDirectory(pendingDirectory);
    try (final TsFileWriter writer = openIncompleteSource(source, device)) {
      assertIncomplete(source);
      // A complete canary is loaded by the real scanner, without an explicit LOAD statement.
      publishCanary(directory, "711-0-0-0.tsfile", "root.p1scan.incomplete.before", 11);
      awaitRows("root.p1scan.incomplete.before", new Object[][] {{1L, 11L, "canary"}});
      assertPending(source, device);

      TestUtils.restartCluster(EnvFactory.getEnv());
      publishCanary(directory, "712-0-0-0.tsfile", "root.p1scan.incomplete.after", 12);
      awaitRows("root.p1scan.incomplete.after", new Object[][] {{1L, 12L, "canary"}});
      assertPending(source, device);
      // Publish the sidecars while tail magic is still absent. Closing the writer publishes the
      // complete file only after both deletion data and its resource are present.
      final TsFileResource resource = new TsFileResource(source);
      resource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device), 9999);
      resource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device), 20000);
      resource.serialize();
      try (final ModificationFile mods = resource.getExclusiveModFile()) {
        mods.write(new TreeDeletionEntry(new MeasurementPath(device + ".s"), 10000, 10000));
      }
      assertIncomplete(source);
    }
    awaitRows(device, new Object[][] {{9999L, 41L, "first"}, {20000L, 43L, "last"}});
    awaitSourceDeleted(source);
    assertNoFailedFiles();
  }

  @Test
  public void testDirectoryHotReloadLeavesOldIncompletePublicationForLater() throws Exception {
    final String device = "root.p1scan.reload.d";
    final File original = listeningDirectory(ORIGINAL_DIR);
    final File reloaded = listeningDirectory(RELOADED_DIR);
    final File source = new File(original, "720-0-0-0.tsfile");
    setListeningDirectory(ORIGINAL_DIR);
    try (final TsFileWriter writer = openIncompleteSource(source, device)) {
      publishCanary(original, "721-0-0-0.tsfile", "root.p1scan.reload.before", 21);
      awaitRows("root.p1scan.reload.before", new Object[][] {{1L, 21L, "canary"}});
      assertPending(source, device);
      setListeningDirectory(RELOADED_DIR);
      publishCanary(reloaded, "722-0-0-0.tsfile", "root.p1scan.reload.changed", 22);
      awaitRows("root.p1scan.reload.changed", new Object[][] {{1L, 22L, "canary"}});
      assertPending(source, device);
    }
    // Complete the old file only after a canary proves the new directory is being scanned.
    publishCanary(reloaded, "723-0-0-0.tsfile", "root.p1scan.reload.later", 23);
    awaitRows("root.p1scan.reload.later", new Object[][] {{1L, 23L, "canary"}});
    Assert.assertTrue("Completed old-directory file must remain pending", source.exists());
    assertRows(device, new Object[0][]);
    assertNoFailedFiles();
    setListeningDirectory(ORIGINAL_DIR);
    awaitRows(
        device, new Object[][] {{9999L, 41L, "first"}, {10000L, 42L, null}, {20000L, 43L, "last"}});
    awaitSourceDeleted(source);
    assertNoFailedFiles();
  }

  /** P1-14: an online read-only coordinator can still route LOAD to writable target replicas. */
  @Test
  public void testReadOnlyCoordinatorLoadsThroughWritableTargets() throws Exception {
    final String device = "root.p1readonly.routed.d";
    final File directory = listeningDirectory("ext/load/readonly-sources");
    final File source = new File(directory, "730-0-0-0.tsfile");
    try (TsFileWriter ignored = openIncompleteSource(source, device)) {
      // Close publishes a complete source outside all listening directories.
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database root.p1readonly");
      statement.execute("create aligned timeseries " + device + "(s INT64,txt TEXT)");
      statement.execute("create aligned timeseries root.p1readonly.routing.d(s INT64,txt TEXT)");
      for (long timestamp : new long[] {9999, 10000, 20000}) {
        statement.execute(
            "insert into root.p1readonly.routing.d(time,s,txt) values("
                + timestamp
                + ",1,'route')");
      }
    }
    final DataNodeWrapper coordinator = EnvFactory.getEnv().getDataNodeWrapper(0);
    try {
      setNodeStatus(coordinator, NodeStatus.ReadOnly);
      assertWritableDataRegionReplicas("root.p1readonly", coordinator);
      try (Connection connection =
              EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(coordinator);
          Statement statement = connection.createStatement()) {
        statement.execute(
            "load '"
                + source.getAbsolutePath()
                + "' with ('on-success'='none','auto-create-schema'='false',"
                + "'tablet-conversion-threshold'='-1')");
      }
      assertRows(
          device,
          new Object[][] {{9999L, 41L, "first"}, {10000L, 42L, null}, {20000L, 43L, "last"}});
      Assert.assertTrue(source.isFile());
      // Confirm this was a LOAD through a still-online read-only node, rather than connection
      // discovery silently selecting another coordinator.
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(coordinator),
              Collections.singletonList(NodeStatus.ReadOnly));
    } finally {
      try {
        setNodeStatus(coordinator, NodeStatus.Running);
      } finally {
        deleteSourceGroup(source);
      }
    }
  }

  /**
   * P1-14: scanner pause is observable, and the original publication resumes without re-copying.
   */
  @Test
  public void testReadOnlyScannerPreservesPublicationUntilWritable() throws Exception {
    final String device = "root.p1scan.readonly.d";
    final String relative = "ext/load/readonly-pending";
    final File directory = listeningDirectory(relative);
    setListeningDirectory(relative);
    final DataNodeWrapper node = EnvFactory.getEnv().getDataNodeWrapper(0);
    final File source = new File(directory, "740-0-0-0.tsfile");
    final String readyDevice = device + "_scanner_ready";
    publishCanary(directory, "739-0-0-0.tsfile", readyDevice, 73);
    awaitRows(readyDevice, new Object[][] {{1L, 73L, "canary"}});
    awaitSourceDeleted(new File(directory, "739-0-0-0.tsfile"));
    final int oldReadOnlyScanLogs =
        logCount(
            node, org.apache.iotdb.db.i18n.StorageEngineMessages.SYSTEM_READ_ONLY_SKIP_ACTIVE_SCAN);
    try {
      try {
        setNodeStatus(node, NodeStatus.ReadOnly);
        assertWritableDataRegionReplicas("root.p1scan", node);
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(
                () ->
                    Assert.assertTrue(
                        logCount(
                                node,
                                org.apache.iotdb.db.i18n.StorageEngineMessages
                                    .SYSTEM_READ_ONLY_SKIP_ACTIVE_SCAN)
                            > oldReadOnlyScanLogs));
        publishCanary(directory, source.getName(), device, 74);
        Awaitility.await()
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .during(2, TimeUnit.SECONDS)
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  Assert.assertTrue(source.isFile());
                  assertRows(device, new Object[0][]);
                });
        assertNoFailedFiles();
      } finally {
        setNodeStatus(node, NodeStatus.Running);
      }
      awaitRows(device, new Object[][] {{1L, 74L, "canary"}});
      awaitSourceDeleted(source);
      publishCanary(directory, "741-0-0-0.tsfile", device + "_next", 75);
      awaitRows(device + "_next", new Object[][] {{1L, 75L, "canary"}});
      assertNoFailedFiles();
    } finally {
      try {
        setNodeStatus(node, NodeStatus.Running);
      } finally {
        deleteSourceGroup(source);
      }
    }
  }

  private static void assertWritableDataRegionReplicas(String database, DataNodeWrapper coordinator)
      throws Exception {
    int coordinatorId = -1;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet nodes = statement.executeQuery("show datanodes")) {
      while (nodes.next()) {
        if (nodes.getInt(ColumnHeaderConstant.RPC_PORT) == coordinator.getPort()) {
          coordinatorId = nodes.getInt(ColumnHeaderConstant.NODE_ID);
          break;
        }
      }
    }
    Assert.assertNotEquals("Coordinator must be registered", -1, coordinatorId);
    final int registeredCoordinatorId = coordinatorId;

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              final java.util.Map<Integer, String> coordinatorRegionStatus =
                  new java.util.HashMap<>();
              final java.util.Map<Integer, Boolean> hasWritableTarget = new java.util.HashMap<>();
              try (Connection connection = EnvFactory.getEnv().getConnection();
                  Statement statement = connection.createStatement();
                  ResultSet regions = statement.executeQuery("show regions")) {
                while (regions.next()) {
                  if (!database.equals(regions.getString(ColumnHeaderConstant.DATABASE))
                      || !"DataRegion".equals(regions.getString(ColumnHeaderConstant.TYPE))) {
                    continue;
                  }
                  final int regionId = regions.getInt(ColumnHeaderConstant.REGION_ID);
                  final int dataNodeId = regions.getInt(ColumnHeaderConstant.DATA_NODE_ID);
                  final String status = regions.getString(ColumnHeaderConstant.STATUS);
                  if (dataNodeId == registeredCoordinatorId) {
                    coordinatorRegionStatus.put(regionId, status);
                  } else {
                    hasWritableTarget.merge(
                        regionId,
                        NodeStatus.Running.getStatus().equals(status),
                        Boolean::logicalOr);
                  }
                }
              }
              Assert.assertFalse(
                  "No preallocated DataRegion for " + database, coordinatorRegionStatus.isEmpty());
              Assert.assertEquals(
                  "Coordinator and targets must report the same DataRegions",
                  coordinatorRegionStatus.keySet(),
                  hasWritableTarget.keySet());
              for (java.util.Map.Entry<Integer, String> region :
                  coordinatorRegionStatus.entrySet()) {
                Assert.assertEquals(
                    "Coordinator DataRegion " + region.getKey() + " must be read-only",
                    NodeStatus.ReadOnly.getStatus(),
                    region.getValue());
                Assert.assertTrue(
                    "DataRegion "
                        + region.getKey()
                        + " has no writable replica outside the coordinator",
                    hasWritableTarget.get(region.getKey()));
              }
            });
  }

  private static int logCount(DataNodeWrapper node, String message) throws Exception {
    try (Stream<String> lines =
        Files.lines(new File(node.getLogDirPath(), node.getId() + ".log").toPath())) {
      return (int) lines.filter(line -> line.contains(message)).count();
    }
  }

  private static void deleteSourceGroup(File source) throws Exception {
    Files.deleteIfExists(source.toPath());
    Files.deleteIfExists(new File(source + TsFileResource.RESOURCE_SUFFIX).toPath());
    Files.deleteIfExists(new File(source + ModificationFile.FILE_SUFFIX).toPath());
  }

  private static void setNodeStatus(DataNodeWrapper node, NodeStatus status) throws Exception {
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (SyncDataNodeInternalServiceClient client =
        manager.borrowClient(new TEndPoint(node.getIp(), node.getInternalPort()))) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.setSystemStatus(status.getStatus()).getCode());
    } finally {
      manager.close();
    }
    EnvFactory.getEnv()
        .ensureNodeStatus(Collections.singletonList(node), Collections.singletonList(status));
  }

  private static File listeningDirectory(final String relativePath) throws Exception {
    final File directory =
        new File(EnvFactory.getEnv().getDataNodeWrapper(0).getNodePath(), relativePath);
    Files.createDirectories(directory.toPath());
    return directory;
  }

  private static void setListeningDirectory(final String path) throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("set configuration 'load_active_listening_dirs'='" + path + "'");
    }
  }

  private static TsFileWriter openIncompleteSource(final File source, final String device)
      throws Exception {
    final TsFileWriter writer = new TsFileWriter(source);
    try {
      writer.registerAlignedTimeseries(device, SCHEMAS);
      final Tablet tablet = new Tablet(device, SCHEMAS, 3);
      final long[] times = {9999, 10000, 20000};
      for (int row = 0; row < times.length; row++) {
        tablet.addTimestamp(row, times[row]);
        tablet.addValue(row, "s", 41L + row);
        tablet.addValue(row, "txt", row == 0 ? "first" : row == 2 ? "last" : null);
      }
      writer.writeAligned(tablet);
      writer.flush();
      return writer;
    } catch (final Exception e) {
      try {
        writer.close();
      } catch (final Exception closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  private static void publishCanary(
      final File directory, final String name, final String device, final long value)
      throws Exception {
    final File staged = new File(directory.getParentFile(), name + ".publishing");
    try {
      try (final TsFileWriter writer = new TsFileWriter(staged)) {
        writer.registerAlignedTimeseries(device, SCHEMAS);
        final Tablet tablet = new Tablet(device, SCHEMAS, 1);
        tablet.addTimestamp(0, 1);
        tablet.addValue(0, "s", value);
        tablet.addValue(0, "txt", "canary");
        writer.writeAligned(tablet);
      }
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(staged.getAbsolutePath(), false)) {
        Assert.assertEquals(TSFileConfig.MAGIC_STRING, reader.readTailMagic());
      }
      Files.move(
          staged.toPath(), new File(directory, name).toPath(), StandardCopyOption.ATOMIC_MOVE);
    } finally {
      Files.deleteIfExists(staged.toPath());
    }
  }

  private static void assertIncomplete(final File source) throws Exception {
    try (final TsFileSequenceReader reader =
        new TsFileSequenceReader(source.getAbsolutePath(), false)) {
      Assert.assertNotEquals(TSFileConfig.MAGIC_STRING, reader.readTailMagic());
    }
  }

  private static void assertPending(final File source, final String device) throws Exception {
    Assert.assertTrue(source.exists());
    assertIncomplete(source);
    assertRows(device, new Object[0][]);
    assertNoFailedFiles();
  }

  private static void awaitRows(final String device, final Object[][] expected) {
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreExceptionsInstanceOf(InconsistentDataException.class)
        .untilAsserted(() -> assertRows(device, expected));
  }

  private static void assertRows(final String device, final Object[][] expected) throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet rows = statement.executeQuery("select s,txt from " + device)) {
      for (final Object[] row : expected) {
        Assert.assertTrue(device, rows.next());
        Assert.assertEquals(row[0], rows.getLong(1));
        Assert.assertEquals(row[1], rows.getObject(2));
        Assert.assertEquals(row[2], rows.getString(3));
      }
      Assert.assertFalse(device, rows.next());
    }
  }

  private static void awaitSourceDeleted(final File source) {
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertFalse(source.exists());
              Assert.assertFalse(new File(source + TsFileResource.RESOURCE_SUFFIX).exists());
              Assert.assertFalse(new File(source + ModificationFile.FILE_SUFFIX).exists());
            });
  }

  private static void assertNoFailedFiles() throws Exception {
    for (int node = 0; node < 3; node++) {
      final File failed =
          new File(EnvFactory.getEnv().getDataNodeWrapper(node).getNodePath(), "ext/load/failed");
      if (failed.exists()) {
        try (final Stream<java.nio.file.Path> files = Files.walk(failed.toPath())) {
          Assert.assertEquals(
              "Unexpected files in " + failed, 0, files.filter(Files::isRegularFile).count());
        }
      }
    }
  }
}
