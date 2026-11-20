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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
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
import org.slf4j.helpers.MessageFormatter;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.revokeUserSeriesPrivilege;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadTsFileActiveRetryIT {
  private static final String DATABASE = "root.sg.active_retry";
  private static final String DEVICE = DATABASE + ".d0";
  private static final String PERMANENT_DEVICE = DATABASE + ".permanent";
  private static final String MEASUREMENT = "s0";
  private static final int ROWS_PER_FILE = 12;
  private static final String USER = "active_retry_user";
  private static final String IDENTITY_USER = "active_retry_identity";
  private static final String IDENTITY_FILE_NAME = "identity-restart.tsfile";
  private static final String PASSWORD = "Retry_Load#2026";
  private static final String COLLISION_DATABASE = "root.load_p118";
  private static final String COLLISION_DEVICE = COLLISION_DATABASE + ".d0";
  private static final String COLLISION_MEASUREMENT = "sensor_00";
  private static final MeasurementSchema COLLISION_TSFILE_SCHEMA =
      new MeasurementSchema(COLLISION_MEASUREMENT, TSDataType.INT32, TSEncoding.RLE);
  private static final MeasurementSchema TSFILE_SCHEMA =
      new MeasurementSchema(MEASUREMENT, TSDataType.INT64, TSEncoding.RLE);

  private File tmpDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    configureRetryMetrics();
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMaxAllocateMemoryRatioForLoad(1.0)
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(1)
        .setLoadTsFileTabletConversionBatchMemorySizeInBytes(Long.MAX_VALUE / 4)
        .setLoadActiveListeningCheckIntervalSeconds(1);
    EnvFactory.getEnv().initClusterEnvironment();
    createUser(USER, PASSWORD);
    for (final PrivilegeType privilege :
        Arrays.asList(
            PrivilegeType.WRITE_DATA,
            PrivilegeType.READ_DATA,
            PrivilegeType.READ_SCHEMA,
            PrivilegeType.WRITE_SCHEMA)) {
      grantUserSeriesPrivilege(USER, privilege, DATABASE + ".**");
    }
  }

  @AfterClass
  public static void tearDownClass() {
    // A filesystem cleanup failure must not leave the remaining test processes running.
    EnvFactory.getEnv().shutdownForciblyAllDataNodes();
    EnvFactory.getEnv().shutdownForciblyAllConfigNodes();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = Files.createTempDirectory("load-active-retry").toFile();
  }

  @After
  public void tearDown() throws Exception {
    final DataNodeWrapper node = EnvFactory.getEnv().getDataNodeWrapper(0);
    if (!node.isAlive()) {
      node.start();
    }
    awaitNode(node);
    try {
      try (final Connection connection = EnvFactory.getEnv().getConnection();
          final Statement statement = connection.createStatement()) {
        for (final String database : Arrays.asList(DATABASE, COLLISION_DATABASE)) {
          final boolean exists;
          try (final ResultSet databases = statement.executeQuery("show databases " + database)) {
            exists = databases.next();
          }
          if (exists) {
            statement.execute("delete database " + database);
          }
        }
      }
    } finally {
      FileUtils.deleteDirectory(activeDir(node));
      FileUtils.deleteDirectory(failedDir(node));
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  /**
   * LOAD-P0-06: remove the cause of a witnessed temporary failure and require the original handoffs
   * to finish without another LOAD. Different mods on same-named files protect pairing; a permanent
   * failure must not starve recoverable work. The old case only checked retention.
   */
  @Test
  public void testTemporaryFailureRecoversWithoutResubmission() throws Exception {
    assertRetryCompletes(false);
  }

  /**
   * LOAD-P0-06: restart with the original pending paths on disk, then repair the schema. The new
   * process must rediscover those paths and preserve database-level, verify and on-success. A
   * separate handoff loses WRITE_DATA after restart and must fail under its stored user, while the
   * other original handoffs finish. Its disjoint timestamps must never be imported.
   */
  @Test
  public void testPendingFilesRecoverAfterDataNodeRestart() throws Exception {
    assertRetryCompletes(true);
  }

  private void assertRetryCompletes(final boolean restart) throws Exception {
    final DataNodeWrapper node = EnvFactory.getEnv().getDataNodeWrapper(0);
    final List<File> sourceFiles = new ArrayList<>();
    for (int fileIndex = 0; fileIndex < 2; fileIndex++) {
      final File directory = new File(tmpDir, "source-" + fileIndex);
      Assert.assertTrue(directory.mkdirs());
      final File file = new File(directory, "1-0-0-0.tsfile");
      generateTsFile(file, DEVICE, fileIndex);
      sourceFiles.add(file);
    }
    // Class-scoped nodes retain their logs between methods. Give each permanent failure its own
    // name so an earlier method's log cannot satisfy this method's failure-reason assertion.
    final File permanentFile =
        new File(tmpDir, restart ? "permanent-restart.tsfile" : "permanent-live.tsfile");
    generateTsFile(permanentFile, PERMANENT_DEVICE, 2);
    final File identitySource = restart ? new File(tmpDir, IDENTITY_FILE_NAME) : null;
    if (restart) {
      generateTsFile(identitySource, DEVICE, 3);
      createUser(IDENTITY_USER, PASSWORD);
      for (final PrivilegeType privilege :
          Arrays.asList(
              PrivilegeType.WRITE_DATA,
              PrivilegeType.READ_DATA,
              PrivilegeType.READ_SCHEMA,
              PrivilegeType.WRITE_SCHEMA)) {
        grantUserSeriesPrivilege(IDENTITY_USER, privilege, DATABASE + ".**");
      }
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(node);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      statement.execute("create timeseries " + DEVICE + "." + MEASUREMENT + " INT32");
      statement.execute("create timeseries " + PERMANENT_DEVICE + "." + MEASUREMENT + " INT32");
    }
    try (final Connection connection =
            EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(node, USER, PASSWORD);
        final Statement statement = connection.createStatement()) {
      for (int i = 0; i < sourceFiles.size(); i++) {
        statement.execute(loadSql(sourceFiles.get(i), i == 0 ? "none" : "delete", true));
      }
      statement.execute(loadSql(permanentFile, "none", false));
    }
    if (restart) {
      try (final Connection connection =
              EnvFactory.getEnv()
                  .getConnectionWithSpecifiedDataNode(node, IDENTITY_USER, PASSWORD);
          final Statement statement = connection.createStatement()) {
        statement.execute(loadSql(identitySource, "none", true));
      }
    }

    final List<File> pendingFiles = new ArrayList<>();
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              pendingFiles.clear();
              pendingFiles.addAll(findTsFiles(activeDir(node)));
              Assert.assertEquals(
                  "all temporarily rejected handoffs must remain pending",
                  restart ? 3 : 2,
                  pendingFiles.size());
              final List<File> failedFiles = findTsFiles(failedDir(node));
              Assert.assertEquals(1, failedFiles.size());
              Assert.assertEquals(permanentFile.getName(), failedFiles.get(0).getName());
              Assert.assertTrue(
                  new File(failedFiles.get(0) + ModificationFile.FILE_SUFFIX).isFile());
              assertPermanentFailureWasLogged(node, permanentFile.getName());
              for (final File file : pendingFiles) {
                Assert.assertTrue(new File(file + ModificationFile.FILE_SUFFIX).isFile());
                assertTemporaryFailureWasLogged(node, file);
                assertHandoffAttributes(file, node);
              }
            });

    Assert.assertTrue(sourceFiles.get(0).isFile());
    Assert.assertTrue(new File(sourceFiles.get(0) + ModificationFile.FILE_SUFFIX).isFile());
    // async on-success=delete transfers ownership at handoff, before background completion.
    Assert.assertFalse(sourceFiles.get(1).exists());
    Assert.assertFalse(new File(sourceFiles.get(1) + ModificationFile.FILE_SUFFIX).exists());
    Assert.assertTrue(permanentFile.isFile());
    assertRows(DEVICE, Collections.emptyList());

    if (restart) {
      node.stopForcibly();
      Assert.assertFalse(node.isAlive());
      for (final File file : pendingFiles) {
        Assert.assertTrue("a process crash must preserve the handoff", file.isFile());
      }
      node.start();
      awaitNode(node);
      for (final File file : pendingFiles) {
        assertHandoffAttributes(file, node);
      }
      // Changing only this user's grant distinguishes restoring the saved identity from running
      // every recovered handoff as the superuser. The two other handoffs keep their write access.
      revokeUserSeriesPrivilege(IDENTITY_USER, PrivilegeType.WRITE_DATA, DATABASE + ".**");
      final File originalIdentityHandoff =
          pendingFiles.stream()
              .filter(file -> IDENTITY_FILE_NAME.equals(file.getName()))
              .findFirst()
              .orElseThrow(AssertionError::new);
      // Permission checks precede schema verification. A retry that passed authorization before
      // REVOKE must finish against the conflicting schema before we widen it; otherwise it may
      // legitimately import the identity file and make this test depend on thread scheduling.
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertPermissionFailureWasLogged(node, originalIdentityHandoff);
                Assert.assertFalse(originalIdentityHandoff.exists());
                final File failedIdentity = failedFile(node, IDENTITY_FILE_NAME);
                Assert.assertTrue(failedIdentity.isFile());
                Assert.assertTrue(new File(failedIdentity + ModificationFile.FILE_SUFFIX).isFile());
              });
    }

    // Widen the empty schema in place so it matches the pending INT64 files without conversion.
    // Dropping the series instead races the scanner: a retry during deletion can be classified as
    // permanent and move the original handoff to failed before a matching schema can be created.
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("alter timeseries " + DEVICE + "." + MEASUREMENT + " set data type INT64");
    }

    final List<String> expected = new ArrayList<>();
    for (int fileIndex = 0; fileIndex < 2; fileIndex++) {
      for (int row = 0; row < ROWS_PER_FILE; row++) {
        if (row != fileIndex + 2) {
          expected.add(timestamp(fileIndex, row) + "," + value(fileIndex, row) + ",");
        }
      }
    }
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertRows(DEVICE, expected);
              assertRetryQueueEmpty(node);
              for (final File file : pendingFiles) {
                Assert.assertFalse(file.exists());
                Assert.assertFalse(new File(file + ModificationFile.FILE_SUFFIX).exists());
              }
            });
    Assert.assertEquals(
        "only permanent failures belong in failed",
        restart ? 2 : 1,
        findTsFiles(failedDir(node)).size());
    Assert.assertTrue(failedFile(node, permanentFile.getName()).isFile());
    assertPermanentFailureWasLogged(node, permanentFile.getName());
    if (restart) {
      final File failedIdentity = failedFile(node, IDENTITY_FILE_NAME);
      Assert.assertTrue(failedIdentity.isFile());
      Assert.assertTrue(new File(failedIdentity + ModificationFile.FILE_SUFFIX).isFile());
      final File originalIdentityHandoff =
          pendingFiles.stream()
              .filter(file -> IDENTITY_FILE_NAME.equals(file.getName()))
              .findFirst()
              .orElseThrow(AssertionError::new);
      assertPermissionFailureWasLogged(node, originalIdentityHandoff);
      Assert.assertTrue(identitySource.isFile());
      Assert.assertTrue(new File(identitySource + ModificationFile.FILE_SUFFIX).isFile());
    }
    Assert.assertTrue(sourceFiles.get(0).isFile());
    Assert.assertTrue(new File(sourceFiles.get(0) + ModificationFile.FILE_SUFFIX).isFile());
    Assert.assertFalse(sourceFiles.get(1).exists());
    assertRows(PERMANENT_DEVICE, Collections.emptyList());
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
              COLLISION_DEVICE, COLLISION_MEASUREMENT, TSDataType.BOOLEAN.name()));
      for (final File tsFile : sourceTsFiles) {
        statement.execute(
            String.format(
                "load \"%s\" with ('database-level'='1','async'='true','on-success'='none',"
                    + "'auto-create-schema'='false','convert-on-type-mismatch'='false')",
                tsFile.getAbsolutePath()));
      }
    }

    final File failDir = failedDir(dataNodeWrapper);
    final List<File> failedGroups =
        waitForCompleteFailedGroups(failDir, sourceTsFiles.size(), 60_000L);
    Assert.assertEquals(sourceTsFiles.size(), failedGroups.size());
    final Map<File, File> failedGroupsBySource =
        assertFailedGroupsMatchSources(
            expectedTsFiles, sourceTsFiles, activeDir(dataNodeWrapper), failedGroups);
    final File recoveryInputRoot = new File(tmpDir, "recovery-inputs");

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(" + COLLISION_MEASUREMENT + ") from " + COLLISION_DEVICE)) {
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
                COLLISION_DEVICE, COLLISION_MEASUREMENT, TSDataType.INT32.name()));
        if (i == 3) {
          statement.execute("create timeseries " + COLLISION_DEVICE + ".extra INT32");
        }

        final File failedGroup = failedGroupsBySource.get(expectedTsFiles.get(i));
        Assert.assertNotNull("Failed source group could not be located", failedGroup);
        final File failedTsFile = copyFailedGroup(failedGroup, recoveryInputRoot, i);
        statement.execute(
            String.format(
                "load \"%s\" with ('database-level'='1','on-success'='none',"
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
        generator.registerTimeseries(
            COLLISION_DEVICE, Collections.singletonList(COLLISION_TSFILE_SCHEMA));
        generator.generateData(COLLISION_DEVICE, 10, 1, false, 300);
      }
      try (Connection activeConnection =
              EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(dataNodeWrapper);
          Statement activeStatement = activeConnection.createStatement()) {
        activeStatement.execute(
            String.format(
                "load \"%s\" with ('database-level'='1','async'='true','on-success'='none',"
                    + "'auto-create-schema'='false','convert-on-type-mismatch'='false')",
                normalTsFile.getAbsolutePath()));
      }
      TestUtils.assertDataEventuallyOnEnv(
          EnvFactory.getEnv(),
          String.format(
              "select count(%s) from %s where time >= 301 and time <= 310",
              COLLISION_MEASUREMENT, COLLISION_DEVICE),
          "count(" + COLLISION_DEVICE + "." + COLLISION_MEASUREMENT + "),",
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
    schemas.add(COLLISION_TSFILE_SCHEMA);
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
              new MeasurementPath(COLLISION_DEVICE, COLLISION_MEASUREMENT), timestamp, timestamp));
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
            String.format(
                "select count(%s) from %s where %s", COLLISION_MEASUREMENT, device, filter))) {
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
                "select %s from %s order by time", COLLISION_MEASUREMENT, COLLISION_DEVICE))) {
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
          statement.executeQuery("select extra from " + COLLISION_DEVICE + " order by time")) {
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

  private static String loadSql(final File file, final String onSuccess, final boolean convert) {
    return String.format(
        "load \"%s\" with ('database-level'='3', 'async'='true', 'on-success'='%s', "
            + "'verify'='true', 'tablet-conversion-threshold'='0', 'convert-on-type-mismatch'='%s')",
        file.getAbsolutePath(), onSuccess, convert);
  }

  private static void assertHandoffAttributes(final File file, final DataNodeWrapper node) {
    final Map<String, String> attributes =
        ActiveLoadPathHelper.parseAttributes(file, activeDir(node));
    Assert.assertEquals(
        IDENTITY_FILE_NAME.equals(file.getName()) ? IDENTITY_USER : USER,
        attributes.get(ActiveLoadPathHelper.USER_KEY));
    Assert.assertEquals("3", attributes.get("database-level"));
    Assert.assertEquals("true", attributes.get("convert-on-type-mismatch"));
    Assert.assertEquals("true", attributes.get("verify"));
    Assert.assertEquals("0", attributes.get("tablet-conversion-threshold"));
  }

  private static void assertPermanentFailureWasLogged(
      final DataNodeWrapper node, final String fileName) throws Exception {
    final String expectedMismatch =
        String.format(
            DataNodeQueryMessages
                .EXCEPTION_DATA_TYPE_MISMATCH_FOR_MEASUREMENT_ARGARGARG_TYPE_IN_TSFILE_ARG_TYPE_IN_IOTDB_ARG_C5BA7DBD,
            PERMANENT_DEVICE,
            ".",
            MEASUREMENT,
            TSDataType.INT64,
            TSDataType.INT32);
    final java.nio.file.Path log = new File(node.getLogDirPath(), node.getId() + ".log").toPath();
    try (final Stream<String> lines = Files.lines(log, StandardCharsets.UTF_8)) {
      Assert.assertTrue(
          "no permanent failure was logged for " + fileName,
          lines.anyMatch(
              line ->
                  line.contains(fileName)
                      && line.contains("ActiveLoadTsFileLoader")
                      && line.contains(expectedMismatch)
                      && line.contains("code:" + TSStatusCode.LOAD_FILE_ERROR.getStatusCode())));
    }
  }

  private static void assertPermissionFailureWasLogged(
      final DataNodeWrapper node, final File originalHandoff) throws Exception {
    final java.nio.file.Path log = new File(node.getLogDirPath(), node.getId() + ".log").toPath();
    try (final Stream<String> lines = Files.lines(log, StandardCharsets.UTF_8)) {
      Assert.assertTrue(
          "no stored-user permission failure was logged for " + originalHandoff,
          lines.anyMatch(
              line ->
                  line.contains(originalHandoff.getAbsolutePath())
                      && line.contains("ActiveLoadTsFileLoader")
                      && line.contains("WRITE_DATA")
                      && line.contains("code:" + TSStatusCode.NO_PERMISSION.getStatusCode())));
    }
  }

  private static long timestamp(final int fileIndex, final int row) {
    return 100L * fileIndex + row + 1;
  }

  private static int value(final int fileIndex, final int row) {
    return 1000 * (fileIndex + 1) + 7 * row;
  }

  private static void generateTsFile(final File file, final String device, final int fileIndex)
      throws Exception {
    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(device), Collections.singletonList(TSFILE_SCHEMA));
      final Tablet tablet = new Tablet(device, Collections.singletonList(TSFILE_SCHEMA));
      for (int row = 0; row < ROWS_PER_FILE; row++) {
        tablet.addTimestamp(row, timestamp(fileIndex, row));
        tablet.addValue(MEASUREMENT, row, (long) value(fileIndex, row));
      }
      writer.writeTree(tablet);
    }
    try (final ModificationFile mods =
        new ModificationFile(new File(file + ModificationFile.FILE_SUFFIX), false)) {
      final long deletedTime = timestamp(fileIndex, fileIndex + 2);
      mods.write(
          new TreeDeletionEntry(
              new MeasurementPath(device, MEASUREMENT), deletedTime, deletedTime));
    }
  }

  private static void assertRows(final String device, final List<String> expected)
      throws Exception {
    TestUtils.treeResultSetEqualTestUnordered(
        "select " + MEASUREMENT + " from " + device,
        new String[] {"Time", device + "." + MEASUREMENT},
        expected.toArray(new String[0]));
    if (!expected.isEmpty()) {
      try (final Connection connection = EnvFactory.getEnv().getConnection();
          final Statement statement = connection.createStatement();
          final ResultSet result =
              statement.executeQuery("select " + MEASUREMENT + " from " + device + " limit 1")) {
        Assert.assertEquals(TSDataType.INT64.name(), result.getMetaData().getColumnTypeName(2));
      }
    }
  }

  static void configureRetryMetrics() {
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("JMX"));
    final Properties properties = new Properties();
    properties.setProperty("dn_metric_level", "IMPORTANT");
    ((MppDataNodeConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig())
        .updateProperties(properties);
  }

  static void assertRetryQueueEmpty(final DataNodeWrapper node) throws Exception {
    if (activeDir(node).exists()) {
      try (final Stream<java.nio.file.Path> paths = Files.walk(activeDir(node).toPath())) {
        Assert.assertFalse(
            "pending copies, mods and resources must be gone",
            paths.anyMatch(Files::isRegularFile));
      }
    }
    final int[] ports = node.getPortList();
    final JMXServiceURL url =
        new JMXServiceURL(
            "service:jmx:rmi:///jndi/rmi://"
                + node.getIp()
                + ":"
                + ports[ports.length - 1]
                + "/jmxrmi");
    try (final JMXConnector connector = JMXConnectorFactory.connect(url)) {
      for (final String type : Arrays.asList("queuing", "loading")) {
        final ObjectName pattern =
            new ObjectName(
                "org.apache.iotdb.metrics:name=active_loading_files_number,tag.type="
                    + ObjectName.quote(type)
                    + ",*");
        final java.util.Set<ObjectName> names =
            connector.getMBeanServerConnection().queryNames(pattern, null);
        Assert.assertEquals("missing active load metric " + type, 1, names.size());
        Assert.assertEquals(
            "the original handoffs must leave " + type,
            0L,
            ((Number)
                    connector
                        .getMBeanServerConnection()
                        .getAttribute(names.iterator().next(), "Count"))
                .longValue());
      }
    }
  }

  static void assertSchemaMemoryRejectionWasLogged(final DataNodeWrapper node, final File file)
      throws Exception {
    // Schema-analysis allocation uses the legacy memory-message retry classification. Tablet
    // conversion has a separate typed LOAD_TEMPORARY_UNAVAILABLE status checked below.
    final String expected =
        MessageFormatter.format(
                StorageEngineMessages
                    .STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_MEMORY_9A60DF29,
                file.getAbsolutePath(),
                false)
            .getMessage();
    final java.nio.file.Path log = new File(node.getLogDirPath(), node.getId() + ".log").toPath();
    try (final Stream<String> lines = Files.lines(log, StandardCharsets.UTF_8)) {
      Assert.assertTrue(
          "no retryable schema-memory rejection observed for " + file,
          lines.anyMatch(
              line -> line.contains("ActiveLoadFailedMessageHandler") && line.contains(expected)));
    }
  }

  static void assertTemporaryFailureWasLogged(final DataNodeWrapper node, final File file)
      throws Exception {
    final java.nio.file.Path log = new File(node.getLogDirPath(), node.getId() + ".log").toPath();
    try (final Stream<String> lines = Files.lines(log, StandardCharsets.UTF_8)) {
      Assert.assertTrue(
          "no temporary failure observed for " + file,
          lines.anyMatch(
              line ->
                  line.contains(file.getAbsolutePath())
                      && line.contains("ActiveLoadFailedMessageHandler")
                      && line.contains(
                          "code:"
                              + TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION
                                  .getStatusCode())));
    }
  }

  static List<File> findTsFiles(final File directory) throws Exception {
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

  static File activeDir(final DataNodeWrapper node) {
    return new File(node.getNodePath(), "ext/load/pending");
  }

  private static File failedFile(final DataNodeWrapper node, final String fileName)
      throws Exception {
    return findTsFiles(failedDir(node)).stream()
        .filter(file -> fileName.equals(file.getName()))
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  static File failedDir(final DataNodeWrapper node) {
    return new File(node.getNodePath(), "ext/load/failed");
  }

  static void awaitNode(final DataNodeWrapper node) {
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .ignoreExceptions()
        .until(
            () -> {
              try (final Connection connection = EnvFactory.getEnv().getConnection(node);
                  final Statement statement = connection.createStatement();
                  final ResultSet result = statement.executeQuery("show version")) {
                return result.next();
              }
            });
  }
}
