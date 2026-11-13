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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadTsFileActiveRetryIT {

  private static final String DATABASE = "root.sg.test_0";
  private static final String DEVICE = DATABASE + ".d_0";
  private static final String MEASUREMENT = "sensor_00";
  private static final String COLLISION_DATABASE = "root.load_p118";
  private static final String COLLISION_DEVICE = COLLISION_DATABASE + ".d0";
  private static final long UNALLOCATABLE_TABLET_CONVERSION_BATCH_MEMORY_SIZE_IN_BYTES =
      Long.MAX_VALUE / 4;
  private static final MeasurementSchema TSFILE_SCHEMA =
      new MeasurementSchema(MEASUREMENT, TSDataType.INT32, TSEncoding.RLE);

  private File tmpDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load-active-retry").toUri());
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMaxAllocateMemoryRatioForLoad(1.0)
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(1)
        .setLoadTsFileTabletConversionBatchMemorySizeInBytes(
            UNALLOCATABLE_TABLET_CONVERSION_BATCH_MEMORY_SIZE_IN_BYTES)
        .setLoadActiveListeningCheckIntervalSeconds(1);

    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("delete database " + DATABASE);
    } catch (final Exception ignored) {
      // ignore cleanup failure
    } finally {
      EnvFactory.getEnv().cleanClusterEnvironment();
      deleteRecursively(tmpDir);
    }
  }

  @Test
  public void testActiveLoadTemporaryUnavailableShouldKeepFileForRetry() throws Exception {
    final DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    final File retryTsFile = new File(tmpDir, "1-0-0-0.tsfile");
    final File permanentFailureTsFile = new File(tmpDir, "2-0-0-0.tsfile");
    generateTsFile(retryTsFile);
    generateTsFile(permanentFailureTsFile);

    try (final Connection connection =
            EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(dataNodeWrapper);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      statement.execute(
          String.format(
              "create timeseries %s.%s %s", DEVICE, MEASUREMENT, TSDataType.INT64.name()));

      statement.execute(
          String.format(
              "load \"%s\" with ('database-level'='3', 'async'='true', 'on-success'='none', "
                  + "'convert-on-type-mismatch'='true')",
              retryTsFile.getAbsolutePath()));
      statement.execute(
          String.format(
              "load \"%s\" with ('database-level'='3', 'async'='true', 'on-success'='none', "
                  + "'convert-on-type-mismatch'='false')",
              permanentFailureTsFile.getAbsolutePath()));

      final File activeDir = getActiveLoadDir(dataNodeWrapper);
      final File failDir = getActiveLoadFailDir(dataNodeWrapper);
      final File activeTsFile = waitForFile(activeDir, retryTsFile.getName(), 30_000L);

      Assert.assertNotNull(
          "Async load should copy tsfile into active load directory", activeTsFile);

      Assert.assertNotNull(
          "Permanent active load failure should be moved to fail dir",
          waitForFile(failDir, permanentFailureTsFile.getName(), TimeUnit.SECONDS.toMillis(60)));

      assertFileKeptForRetry(
          activeDir, failDir, retryTsFile.getName(), TimeUnit.SECONDS.toMillis(12));
    }
  }

  @Test
  public void testPermanentFailureKeepsSameNamedTsFileGroupsRecoverable() throws Exception {
    final DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    final File sourceRoot = new File(tmpDir, "collision-sources");
    Assert.assertTrue(sourceRoot.mkdir());

    final List<File> sourceTsFiles = new ArrayList<>();
    final File identicalFirst =
        createFailureGroup(new File(sourceRoot, "identical-first"), 1, 10, false, 1, 1);
    sourceTsFiles.add(identicalFirst);

    final File identicalSecondDir = new File(sourceRoot, "identical-second");
    Assert.assertTrue(identicalSecondDir.mkdir());
    final File identicalSecond = new File(identicalSecondDir, identicalFirst.getName());
    Files.copy(identicalFirst.toPath(), identicalSecond.toPath());
    writeResource(identicalSecond);
    writeModification(identicalSecond, 2);
    sourceTsFiles.add(identicalSecond);

    final File sameSizeDifferentContent =
        createFailureGroup(
            new File(sourceRoot, "same-size-different-content"), 2, 10, false, 101, 103);
    sourceTsFiles.add(sameSizeDifferentContent);

    final File differentSize =
        createFailureGroup(new File(sourceRoot, "different-size"), 3, 50, true, 201, 205);
    sourceTsFiles.add(differentSize);

    final List<File> expectedTsFiles =
        snapshotTsFileGroups(sourceTsFiles, new File(tmpDir, "collision-expected"));

    Assert.assertEquals(identicalFirst.length(), sameSizeDifferentContent.length());
    Assert.assertNotEquals(
        -1L, Files.mismatch(identicalFirst.toPath(), sameSizeDifferentContent.toPath()));
    Assert.assertNotEquals(identicalFirst.length(), differentSize.length());

    try (Connection connection =
            EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(dataNodeWrapper);
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + COLLISION_DATABASE);
      statement.execute(
          String.format(
              "create timeseries %s.%s %s",
              COLLISION_DEVICE, MEASUREMENT, TSDataType.INT64.name()));
      for (final File tsFile : sourceTsFiles) {
        statement.execute(
            String.format(
                "load \"%s\" with ('database-level'='2','async'='true','on-success'='none',"
                    + "'auto-create-schema'='false','convert-on-type-mismatch'='false')",
                tsFile.getAbsolutePath()));
      }
    }

    final File failDir = getActiveLoadFailDir(dataNodeWrapper);
    final List<File> failedGroups =
        waitForCompleteFailedGroups(failDir, sourceTsFiles.size(), 60_000L);
    Assert.assertEquals(sourceTsFiles.size(), failedGroups.size());
    final Map<File, File> failedGroupsBySource =
        assertFailedGroupsMatchSources(
            expectedTsFiles, sourceTsFiles, getActiveLoadDir(dataNodeWrapper), failedGroups);
    final File recoveryInputRoot = new File(tmpDir, "recovery-inputs");

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("select count(" + MEASUREMENT + ") from " + COLLISION_DEVICE)) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("Schema mismatch failures must not load data", 0, resultSet.getLong(1));
      }
      final long[] startTimes = {1, 1, 101, 201};
      final long[] endTimes = {10, 10, 110, 250};
      final long[] deletedTimes = {1, 2, 103, 205};
      final long[] expectedCounts = {9, 9, 9, 49};
      final long[] seeds = {1, 1, 2, 3};
      for (int i = 0; i < expectedTsFiles.size(); i++) {
        statement.execute("delete database " + COLLISION_DATABASE);
        statement.execute("create database " + COLLISION_DATABASE);
        statement.execute(
            String.format(
                "create timeseries %s.%s %s",
                COLLISION_DEVICE, MEASUREMENT, TSDataType.INT32.name()));
        if (i == 3) {
          statement.execute("create timeseries " + COLLISION_DEVICE + ".extra INT32");
        }

        final File failedGroup = failedGroupsBySource.get(expectedTsFiles.get(i));
        Assert.assertNotNull("Failed source group could not be located", failedGroup);
        final File failedTsFile = copyFailedGroup(failedGroup, recoveryInputRoot, i);
        statement.execute(
            String.format(
                "load \"%s\" with ('database-level'='2','on-success'='none',"
                    + "'auto-create-schema'='false','convert-on-type-mismatch'='false')",
                failedTsFile.getAbsolutePath()));
        assertCount(
            statement,
            COLLISION_DEVICE,
            String.format("time >= %d and time <= %d", startTimes[i], endTimes[i]),
            expectedCounts[i]);
        assertCount(statement, COLLISION_DEVICE, "time = " + deletedTimes[i], 0);
        assertRecoveredRows(
            statement, startTimes[i], endTimes[i], deletedTimes[i], seeds[i], i == 3);
      }

      final File normalTsFile = new File(tmpDir, "normal-after-failure.tsfile");
      try (final TsFileGenerator generator = new TsFileGenerator(normalTsFile)) {
        generator.registerTimeseries(COLLISION_DEVICE, Collections.singletonList(TSFILE_SCHEMA));
        generator.generateData(COLLISION_DEVICE, 10, 1, false, 300);
      }
      try (Connection activeConnection =
              EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(dataNodeWrapper);
          Statement activeStatement = activeConnection.createStatement()) {
        activeStatement.execute(
            String.format(
                "load \"%s\" with ('database-level'='2','async'='true','on-success'='none',"
                    + "'auto-create-schema'='false','convert-on-type-mismatch'='false')",
                normalTsFile.getAbsolutePath()));
      }
      TestUtils.assertDataEventuallyOnEnv(
          EnvFactory.getEnv(),
          String.format(
              "select count(%s) from %s where time >= 301 and time <= 310",
              MEASUREMENT, COLLISION_DEVICE),
          "count(" + COLLISION_DEVICE + "." + MEASUREMENT + "),",
          Collections.singleton("10,"));
    }
  }

  private File createFailureGroup(
      final File groupDir,
      final long randomSeed,
      final int rowCount,
      final boolean withExtraMeasurement,
      final long startTimestamp,
      final long deletionTimestamp)
      throws Exception {
    Assert.assertTrue(groupDir.mkdir());
    final File tsFile = new File(groupDir, "1-0-0-0.tsfile");
    final List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(TSFILE_SCHEMA);
    if (withExtraMeasurement) {
      schemas.add(new MeasurementSchema("extra", TSDataType.INT32, TSEncoding.RLE));
    }
    try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
      generator.resetRandom(randomSeed);
      generator.registerTimeseries(COLLISION_DEVICE, schemas);
      generator.generateData(COLLISION_DEVICE, rowCount, 1, false, startTimestamp - 1);
    }
    writeResource(tsFile);
    writeModification(tsFile, deletionTimestamp);
    return tsFile;
  }

  private void writeResource(final File tsFile) throws Exception {
    final TsFileResource resource = new TsFileResource(tsFile);
    try (final SealedTsFileRecoverPerformer performer =
        new SealedTsFileRecoverPerformer(resource)) {
      performer.recover();
    }
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.deserialize();
    resource.serialize();
  }

  private void writeModification(final File tsFile, final long timestamp) throws Exception {
    try (final ModificationFile modificationFile =
        new ModificationFile(
            new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX), false)) {
      modificationFile.write(
          new TreeDeletionEntry(
              new MeasurementPath(COLLISION_DEVICE, MEASUREMENT), timestamp, timestamp));
    }
  }

  private List<File> waitForCompleteFailedGroups(
      final File failDir, final int expectedCount, final long timeoutMs)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    List<File> groups;
    do {
      groups = findFailedGroups(failDir);
      if (groups.size() >= expectedCount
          && groups.stream().allMatch(this::hasCompleteFailedGroup)) {
        return groups;
      }
      if (System.currentTimeMillis() >= deadline) {
        return groups;
      }
      Thread.sleep(250L);
    } while (true);
  }

  private boolean hasCompleteFailedGroup(final File groupDir) {
    final File tsFile = findTsFile(groupDir);
    return tsFile != null
        && new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).isFile()
        && new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).isFile();
  }

  private List<File> findFailedGroups(final File failDir) {
    final List<File> groups = new ArrayList<>();
    if (findTsFile(failDir) != null) {
      groups.add(failDir);
    }
    if (failDir == null || !failDir.isDirectory()) {
      return groups;
    }
    final File[] children = failDir.listFiles();
    if (children == null) {
      return groups;
    }
    for (final File child : children) {
      if (child.isDirectory()
          && !child.getName().startsWith(".active-load-staging-")
          && findTsFile(child) != null) {
        groups.add(child);
      }
    }
    return groups;
  }

  private File findTsFile(final File directory) {
    if (directory == null || !directory.isDirectory()) {
      return null;
    }
    final File[] children = directory.listFiles();
    if (children == null) {
      return null;
    }
    for (final File child : children) {
      if (child.isFile() && child.getName().endsWith(".tsfile")) {
        return child;
      }
    }
    return null;
  }

  private Map<File, File> assertFailedGroupsMatchSources(
      final List<File> expectedTsFiles,
      final List<File> submittedTsFiles,
      final File activeLoadDir,
      final List<File> failedGroups)
      throws Exception {
    Assert.assertEquals(expectedTsFiles.size(), submittedTsFiles.size());
    final Set<File> unmatchedGroups = new HashSet<>(failedGroups);
    final Map<File, File> groupsBySource = new HashMap<>();
    final Set<String> activeLoadFiles = new HashSet<>();
    for (int sourceIndex = 0; sourceIndex < expectedTsFiles.size(); sourceIndex++) {
      final File expectedTsFile = expectedTsFiles.get(sourceIndex);
      final File submittedTsFile = submittedTsFiles.get(sourceIndex);
      Assert.assertTrue(
          "Expected TsFile snapshot differs from submitted source",
          sameBytes(expectedTsFile, submittedTsFile));
      Assert.assertTrue(
          "Expected resource snapshot differs from submitted source",
          sameBytes(
              new File(expectedTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
              new File(submittedTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX)));
      Assert.assertTrue(
          "Expected mods snapshot differs from submitted source",
          sameBytes(
              new File(expectedTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX),
              new File(submittedTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)));
      File matchedGroup = null;
      for (final File groupDir : unmatchedGroups) {
        final File failedTsFile = findTsFile(groupDir);
        if (failedTsFile != null && sameBytes(expectedTsFile, failedTsFile)) {
          final File sourceResource =
              new File(expectedTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
          final File failedResource =
              new File(failedTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
          final File sourceMods =
              new File(expectedTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
          final File failedMods =
              new File(failedTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
          if (sourceResource.exists()
              && failedResource.exists()
              && sourceMods.exists()
              && failedMods.exists()
              && sameBytes(sourceResource, failedResource)
              && sameBytes(sourceMods, failedMods)) {
            final File failureInfoFile =
                new File(groupDir, failedTsFile.getName() + ".load-failure");
            if (!failureInfoFile.isFile()) {
              continue;
            }
            final Properties failureInfo = new Properties();
            try (InputStream inputStream = Files.newInputStream(failureInfoFile.toPath())) {
              failureInfo.load(inputStream);
            }
            final String activeLoadFile = failureInfo.getProperty("activeLoadFile");
            final String pendingDir = failureInfo.getProperty("pendingDir");
            if (activeLoadFile == null
                || !new File(activeLoadFile).getName().equals(failedTsFile.getName())
                || !new File(activeLoadFile)
                    .toPath()
                    .toAbsolutePath()
                    .normalize()
                    .startsWith(activeLoadDir.toPath().toAbsolutePath().normalize())
                || pendingDir == null
                || !new File(pendingDir)
                    .toPath()
                    .toAbsolutePath()
                    .normalize()
                    .equals(activeLoadDir.toPath().toAbsolutePath().normalize())
                || Boolean.parseBoolean(failureInfo.getProperty("generatedByPipe", "true"))
                || Boolean.parseBoolean(failureInfo.getProperty("tableModel", "true"))
                || !activeLoadFiles.add(activeLoadFile)) {
              continue;
            }
            matchedGroup = groupDir;
            break;
          }
        }
      }
      Assert.assertNotNull("Failed directory lost or cross-paired a TsFile group", matchedGroup);
      unmatchedGroups.remove(matchedGroup);
      groupsBySource.put(expectedTsFile, matchedGroup);
    }
    Assert.assertTrue("Unexpected failed file groups remain", unmatchedGroups.isEmpty());
    return groupsBySource;
  }

  private boolean sameBytes(final File left, final File right) throws Exception {
    return Files.mismatch(left.toPath(), right.toPath()) == -1;
  }

  private List<File> snapshotTsFileGroups(final List<File> sourceTsFiles, final File snapshotRoot)
      throws Exception {
    final List<File> snapshots = new ArrayList<>();
    for (int i = 0; i < sourceTsFiles.size(); i++) {
      final File sourceTsFile = sourceTsFiles.get(i);
      final File groupDir = new File(snapshotRoot, Integer.toString(i));
      Assert.assertTrue(groupDir.mkdirs());
      final File snapshotTsFile = new File(groupDir, sourceTsFile.getName());
      Files.copy(sourceTsFile.toPath(), snapshotTsFile.toPath());
      final File sourceResource =
          new File(sourceTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
      final File sourceMods =
          new File(sourceTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
      Files.copy(
          sourceResource.toPath(),
          new File(snapshotTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      Files.copy(
          sourceMods.toPath(),
          new File(snapshotTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).toPath());
      snapshots.add(snapshotTsFile);
    }
    return snapshots;
  }

  private File copyFailedGroup(final File failedGroup, final File recoveryRoot, final int index)
      throws Exception {
    final File recoveryGroup = new File(recoveryRoot, Integer.toString(index));
    Assert.assertTrue(recoveryGroup.mkdirs());
    final File[] files = failedGroup.listFiles(File::isFile);
    Assert.assertNotNull(files);
    for (final File file : files) {
      Files.copy(file.toPath(), new File(recoveryGroup, file.getName()).toPath());
    }
    final File recoveryTsFile = findTsFile(recoveryGroup);
    Assert.assertNotNull("Failed group copy must contain its TsFile", recoveryTsFile);
    return recoveryTsFile;
  }

  private void assertCount(
      final Statement statement, final String device, final String filter, final long expected)
      throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery(
            String.format("select count(%s) from %s where %s", MEASUREMENT, device, filter))) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expected, resultSet.getLong(1));
    }
  }

  private void assertRecoveredRows(
      final Statement statement,
      final long startTime,
      final long endTime,
      final long deletedTime,
      final long seed,
      final boolean withExtraMeasurement)
      throws Exception {
    final Random expectedValues = new Random(seed);
    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select time, %s from %s order by time", MEASUREMENT, COLLISION_DEVICE))) {
      for (long time = startTime; time <= endTime; time++) {
        final int value = expectedValues.nextInt();
        if (withExtraMeasurement) {
          expectedValues.nextInt();
        }
        if (time == deletedTime) {
          continue;
        }
        Assert.assertTrue("Recovered group lost a surviving key", resultSet.next());
        Assert.assertEquals(time, resultSet.getLong(1));
        Assert.assertEquals("Recovered value differs from its source", value, resultSet.getInt(2));
        Assert.assertFalse(resultSet.wasNull());
      }
      Assert.assertFalse(resultSet.next());
    }
    if (withExtraMeasurement) {
      final Random expectedExtraValues = new Random(seed);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select time, extra from " + COLLISION_DEVICE + " order by time")) {
        for (long time = startTime; time <= endTime; time++) {
          expectedExtraValues.nextInt();
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(time, resultSet.getLong(1));
          Assert.assertEquals(expectedExtraValues.nextInt(), resultSet.getInt(2));
          Assert.assertFalse(resultSet.wasNull());
        }
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  private void generateTsFile(final File tsFile) throws Exception {
    try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
      generator.registerTimeseries(DEVICE, Collections.singletonList(TSFILE_SCHEMA));
      generator.generateData(DEVICE, 10, 1, false);
    }
  }

  private File getActiveLoadDir(final DataNodeWrapper dataNodeWrapper) {
    return new File(
        dataNodeWrapper.getNodePath()
            + File.separator
            + "ext"
            + File.separator
            + "load"
            + File.separator
            + "pending");
  }

  private File getActiveLoadFailDir(final DataNodeWrapper dataNodeWrapper) {
    return new File(
        dataNodeWrapper.getNodePath()
            + File.separator
            + "ext"
            + File.separator
            + "load"
            + File.separator
            + "failed");
  }

  private File waitForFile(final File root, final String fileName, final long timeoutMs)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final File file = findFile(root, fileName);
      if (file != null) {
        return file;
      }
      Thread.sleep(500L);
    }
    return null;
  }

  private boolean containsFile(final File root, final String fileName) {
    return findFile(root, fileName) != null;
  }

  private void assertFileKeptForRetry(
      final File activeDir, final File failDir, final String fileName, final long observationMs)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + observationMs;
    while (System.currentTimeMillis() < deadline) {
      Assert.assertTrue(
          "Temporary unavailable active load should keep tsfile for retry",
          containsFile(activeDir, fileName));
      Assert.assertFalse(
          "Temporary unavailable active load must not move tsfile to fail dir",
          containsFile(failDir, fileName));
      Thread.sleep(500L);
    }
  }

  private File findFile(final File root, final String fileName) {
    if (root == null || !root.exists()) {
      return null;
    }
    if (root.isFile()) {
      return root.getName().equals(fileName) ? root : null;
    }

    final File[] children = root.listFiles();
    if (children == null) {
      return null;
    }
    for (final File child : children) {
      final File file = findFile(child, fileName);
      if (file != null) {
        return file;
      }
    }
    return null;
  }

  private void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Assert.assertTrue(file.delete());
  }
}
