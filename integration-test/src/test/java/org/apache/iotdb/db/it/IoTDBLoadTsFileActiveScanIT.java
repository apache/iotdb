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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.exception.InconsistentDataException;

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
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(10_000);
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
