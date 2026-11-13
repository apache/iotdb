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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadTsFileBoundaryIT {
  private static final String MINI_DATABASE = "root.load_p115";
  private static final String MIXED_DATABASE = "root.load_p115_mixed";
  private static final String ALL_MINI_DATABASE = "root.load_p115_all_mini";
  private static final String INVALID_DATABASE = "root.load_p120";

  private File tmpDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(10_000L);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = Files.createTempDirectory("load-boundary").toFile();
  }

  @After
  public void tearDown() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      deleteDatabase(statement, MINI_DATABASE);
      for (int i = 0; i < 4; i++) {
        deleteDatabase(statement, MINI_DATABASE + "_threshold_" + i);
      }
      deleteDatabase(statement, MIXED_DATABASE);
      deleteDatabase(statement, ALL_MINI_DATABASE);
      deleteDatabase(statement, INVALID_DATABASE);
    } catch (SQLException ignored) {
      // The cluster may already be unavailable after a failed test.
    } finally {
      deleteRecursively(tmpDir);
    }
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMiniFileThresholdAndMixedStatementReconstruction() throws Exception {
    for (int thresholdIndex = 0; thresholdIndex < 4; thresholdIndex++) {
      final String thresholdDatabase = MINI_DATABASE + "_threshold_" + thresholdIndex;
      final String thresholdDevice = thresholdDatabase + ".threshold_device";
      final File thresholdDir = new File(tmpDir, "threshold-" + thresholdIndex);
      Assert.assertTrue(thresholdDir.mkdir());
      final File thresholdFile = new File(thresholdDir, "threshold.tsfile");
      writeTreeFile(
          thresholdFile, thresholdDevice, new long[] {9999, 10000, 20000}, new long[] {9, 10, 20});
      writeTreeDeletion(thresholdFile, thresholdDevice, "value", 9999, 9999);
      final long fileSize = thresholdFile.length();
      final long threshold = thresholdIndex == 0 ? -1 : fileSize + thresholdIndex - 2;

      final long conversionsBefore = getMiniConversionCount();
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("create database " + thresholdDatabase);
        statement.execute("create timeseries " + thresholdDevice + ".value INT64");
        statement.execute(
            String.format(
                "load \"%s\" with ('database-level'='2','auto-create-schema'='false',"
                    + "'on-success'='none','tablet-conversion-threshold'='%d')",
                thresholdFile.getAbsolutePath(), threshold));
        assertThresholdFileContents(statement, thresholdDevice);
      }

      final long expectedConversionDelta = thresholdIndex >= 2 ? 1 : 0;
      Assert.assertEquals(
          "The load_time_cost analysis_cast_tablets metric must prove the selected path",
          expectedConversionDelta,
          getMiniConversionCount() - conversionsBefore);
      Assert.assertTrue(thresholdFile.exists());
      Assert.assertTrue(
          new File(thresholdFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());

      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        deleteDatabase(statement, thresholdDatabase);
      }
    }

    final File deleteOnSuccessFile = new File(tmpDir, "threshold-delete-on-success.tsfile");
    final String deleteOnSuccessDevice = MINI_DATABASE + ".delete_on_success";
    writeTreeFile(
        deleteOnSuccessFile,
        deleteOnSuccessDevice,
        new long[] {9999, 10000, 20000},
        new long[] {9, 10, 20});
    writeTreeDeletion(deleteOnSuccessFile, deleteOnSuccessDevice, "value", 9999, 9999);
    final long deleteConversionBefore = getMiniConversionCount();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + MINI_DATABASE);
      statement.execute("create timeseries " + deleteOnSuccessDevice + ".value INT64");
      statement.execute(
          String.format(
              "load \"%s\" with ('database-level'='2','auto-create-schema'='false',"
                  + "'on-success'='delete','tablet-conversion-threshold'='%d')",
              deleteOnSuccessFile.getAbsolutePath(), deleteOnSuccessFile.length()));
      assertThresholdFileContents(statement, deleteOnSuccessDevice);
    }
    Assert.assertEquals(1, getMiniConversionCount() - deleteConversionBefore);
    Assert.assertFalse(deleteOnSuccessFile.exists());
    Assert.assertFalse(
        new File(deleteOnSuccessFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      deleteDatabase(statement, MINI_DATABASE);
    }

    final File mixedDir = new File(tmpDir, "mixed");
    Assert.assertTrue(mixedDir.mkdir());
    final File firstMini = new File(mixedDir, "01-first-mini.tsfile");
    final File firstNormal = new File(mixedDir, "02-first-normal.tsfile");
    final File middleMini = new File(mixedDir, "03-middle-mini.tsfile");
    final File lastNormal = new File(mixedDir, "04-last-normal.tsfile");
    final File lastMini = new File(mixedDir, "05-last-mini.tsfile");
    writeTreeFile(firstMini, MIXED_DATABASE + ".small", new long[] {1}, new long[] {11});
    writeTreeFile(
        firstNormal, MIXED_DATABASE + ".normal_first", buildTimes(64, 100), buildValues(64, 1000));
    writeTreeFile(middleMini, MIXED_DATABASE + ".middle", new long[] {2}, new long[] {22});
    writeTreeFile(
        lastNormal, MIXED_DATABASE + ".normal_last", buildTimes(64, 100), buildValues(64, 2000));
    writeTreeFile(lastMini, MIXED_DATABASE + ".tail", new long[] {2}, new long[] {33});

    assertTsFileOrder(
        mixedDir,
        "01-first-mini.tsfile",
        "02-first-normal.tsfile",
        "03-middle-mini.tsfile",
        "04-last-normal.tsfile",
        "05-last-mini.tsfile");
    final long threshold =
        Math.max(Math.max(firstMini.length(), middleMini.length()), lastMini.length());
    Assert.assertTrue(
        "The normal files must remain above the conversion threshold",
        firstNormal.length() > threshold && lastNormal.length() > threshold);
    final long conversionsBefore = getMiniConversionCount();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + MIXED_DATABASE);
      for (final String device :
          List.of(
              MIXED_DATABASE + ".small",
              MIXED_DATABASE + ".normal_first",
              MIXED_DATABASE + ".middle",
              MIXED_DATABASE + ".normal_last",
              MIXED_DATABASE + ".tail")) {
        statement.execute("create timeseries " + device + ".value INT64");
      }
      statement.execute(buildLoadSql(mixedDir, threshold, "2", "none"));
      assertOneTreeValue(statement, MIXED_DATABASE + ".small", 1, 11);
      assertOneTreeValue(statement, MIXED_DATABASE + ".middle", 2, 22);
      assertOneTreeValue(statement, MIXED_DATABASE + ".tail", 2, 33);
      assertTreeSeriesValues(statement, MIXED_DATABASE + ".normal_first", 64, 1000);
      assertTreeSeriesValues(statement, MIXED_DATABASE + ".normal_last", 64, 2000);
    }
    Assert.assertEquals(3, getMiniConversionCount() - conversionsBefore);
    Assert.assertTrue(firstMini.exists());
    Assert.assertTrue(firstNormal.exists());
    Assert.assertTrue(middleMini.exists());
    Assert.assertTrue(lastNormal.exists());
    Assert.assertTrue(lastMini.exists());

    final File allMiniDir = new File(tmpDir, "all-mini");
    Assert.assertTrue(allMiniDir.mkdir());
    final File[] allMiniFiles = {
      new File(allMiniDir, "01-one.tsfile"),
      new File(allMiniDir, "02-two.tsfile"),
      new File(allMiniDir, "03-three.tsfile")
    };
    for (int i = 0; i < allMiniFiles.length; i++) {
      writeTreeFile(
          allMiniFiles[i],
          ALL_MINI_DATABASE + ".device_" + i,
          new long[] {i + 1},
          new long[] {100 + i});
    }
    assertTsFileOrder(allMiniDir, "01-one.tsfile", "02-two.tsfile", "03-three.tsfile");
    final long allMiniThreshold =
        Arrays.stream(allMiniFiles).mapToLong(File::length).max().orElseThrow();
    final long allMiniConversionsBefore = getMiniConversionCount();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + ALL_MINI_DATABASE);
      for (int i = 0; i < allMiniFiles.length; i++) {
        statement.execute(
            "create timeseries " + ALL_MINI_DATABASE + ".device_" + i + ".value INT64");
      }
      statement.execute(buildLoadSql(allMiniDir, allMiniThreshold, "2", "none"));
      for (int i = 0; i < allMiniFiles.length; i++) {
        assertOneTreeValue(statement, ALL_MINI_DATABASE + ".device_" + i, i + 1, 100 + i);
        Assert.assertTrue(allMiniFiles[i].exists());
      }
    }
    Assert.assertEquals(3, getMiniConversionCount() - allMiniConversionsBefore);
  }

  @Test
  public void testMiniFileSuccessDoesNotHideFollowingFailure() throws Exception {
    final File loadDir = new File(tmpDir, "mini-before-failure");
    Assert.assertTrue(loadDir.mkdir());
    final String smallDevice = MIXED_DATABASE + ".small_before_failure";
    final String failingDevice = MIXED_DATABASE + ".schema_mismatch";
    final String unprocessedDevice = MIXED_DATABASE + ".mini_after_failure";
    final File firstMini = new File(loadDir, "01-mini.tsfile");
    final File failingNormal = new File(loadDir, "02-normal-failure.tsfile");
    final File tailMini = new File(loadDir, "03-tail-mini.tsfile");
    writeTreeFile(firstMini, smallDevice, new long[] {1}, new long[] {11});
    final long[] failingTimes = buildTimes(64, 10);
    final Object[] failingValues = new Object[failingTimes.length];
    Arrays.fill(failingValues, 1.5D);
    writeTypedTreeFile(
        failingNormal, failingDevice, TSDataType.DOUBLE, failingTimes, failingValues);
    writeTreeFile(tailMini, unprocessedDevice, new long[] {2}, new long[] {22});
    assertTsFileOrder(loadDir, "01-mini.tsfile", "02-normal-failure.tsfile", "03-tail-mini.tsfile");

    final long threshold = Math.max(firstMini.length(), tailMini.length());
    Assert.assertTrue(failingNormal.length() > threshold);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + MIXED_DATABASE);
      statement.execute("create timeseries " + smallDevice + ".value INT64");
      statement.execute("create timeseries " + failingDevice + ".value INT32");
    }

    final long conversionsBefore = getMiniConversionCount();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            String.format(
                "load \"%s\" with ('database-level'='2','tablet-conversion-threshold'='%d',"
                    + "'auto-create-schema'='false','convert-on-type-mismatch'='false',"
                    + "'on-success'='none')",
                loadDir.getAbsolutePath(), threshold));
        Assert.fail("The later type mismatch must fail the whole LOAD statement");
      } catch (SQLException expected) {
        Assert.assertTrue(
            "The type mismatch for the middle file must be the reported failure",
            expected.getMessage().contains(failingDevice));
      }
      assertOneTreeValue(statement, smallDevice, 1, 11);
      try (ResultSet resultSet =
          statement.executeQuery("select count(value) from " + failingDevice)) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(0, resultSet.getLong(1));
      }
      try (ResultSet resultSet =
          statement.executeQuery("select count(value) from " + unprocessedDevice)) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(0, resultSet.getLong(1));
      }
    }
    Assert.assertEquals(1, getMiniConversionCount() - conversionsBefore);
    Assert.assertTrue("on-success=none must retain the successful mini file", firstMini.exists());
    Assert.assertTrue("The failed source file must be retained", failingNormal.exists());
    Assert.assertTrue("Files after the failed entry must remain unprocessed", tailMini.exists());
  }

  @Test
  public void testMiniFileChecksSubmittingUser() throws Exception {
    final String device = MINI_DATABASE + ".permission_probe";
    final String user = "load_p115_user";
    final String password = "test123123456";
    final File tsFile = new File(tmpDir, "mini-user.tsfile");
    writeTreeFile(tsFile, device, new long[] {1}, new long[] {42});
    writeTreeDeletion(tsFile, device, "value", 2, 2);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + MINI_DATABASE);
      statement.execute("create timeseries " + device + ".value INT64");
      createUser(user, password);
      try {
        final String loadSql =
            String.format(
                "load \"%s\" with ('auto-create-schema'='false','verify'='false',"
                    + "'on-success'='delete','tablet-conversion-threshold'='%d')",
                tsFile.getAbsolutePath(), tsFile.length());
        final long conversionsBefore = getMiniConversionCount();
        try (Connection userConnection = EnvFactory.getEnv().getConnection(user, password);
            Statement userStatement = userConnection.createStatement()) {
          try {
            userStatement.execute(loadSql);
            Assert.fail("Mini-file conversion must check the submitting user's WRITE_DATA");
          } catch (SQLException expected) {
            Assert.assertEquals(
                TSStatusCode.NO_PERMISSION.getStatusCode(), expected.getErrorCode());
          }
        }
        Assert.assertEquals(1, getMiniConversionCount() - conversionsBefore);
        Assert.assertTrue(tsFile.exists());
        Assert.assertTrue(
            new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
        try (ResultSet resultSet = statement.executeQuery("select count(value) from " + device)) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(0, resultSet.getLong(1));
        }
        grantUserSeriesPrivilege(user, PrivilegeType.WRITE_DATA, MINI_DATABASE + ".**");
        try (Connection userConnection = EnvFactory.getEnv().getConnection(user, password);
            Statement userStatement = userConnection.createStatement()) {
          userStatement.execute(loadSql);
        }
        Assert.assertEquals(2, getMiniConversionCount() - conversionsBefore);
        assertOneTreeValue(statement, device, 1, 42);
        Assert.assertFalse(tsFile.exists());
        Assert.assertFalse(
            new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
      } finally {
        statement.execute("drop user " + user);
      }
    }
  }

  @Test
  public void testInvalidTreeDevicePathFailsWithAndWithoutMiniConversion() throws Exception {
    final long loadMemoryBefore = getLoadTsFileUsedMemory();
    for (final boolean nullNode : new boolean[] {false, true}) {
      final StringArrayDeviceID invalidDevice =
          new StringArrayDeviceID(
              nullNode
                  ? new String[] {"root.load_p120", null, "invalid_device"}
                  : new String[] {"root", "load_p120", ""});
      final File invalidFile = new File(tmpDir, "invalid-path-" + nullNode + ".tsfile");
      writeInvalidPathTsFile(invalidFile, invalidDevice);
      writeTreeDeletion(invalidFile, INVALID_DATABASE + ".valid_device", "value", 1, 1);

      for (final boolean miniConversion : new boolean[] {false, true}) {
        final File inputDir =
            new File(tmpDir, (miniConversion ? "invalid-mini-" : "invalid-normal-") + nullNode);
        Assert.assertTrue(inputDir.mkdir());
        final File inputFile = new File(inputDir, "invalid-path.tsfile");
        Files.copy(invalidFile.toPath(), inputFile.toPath());
        Files.copy(
            new File(invalidFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).toPath(),
            new File(inputFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).toPath());

        final long conversionsBefore = getMiniConversionCount();
        try (Connection connection = EnvFactory.getEnv().getConnection();
            Statement statement = connection.createStatement()) {
          deleteDatabase(statement, INVALID_DATABASE);
          statement.execute("create database " + INVALID_DATABASE);
          statement.execute(
              "create aligned timeseries "
                  + INVALID_DATABASE
                  + ".valid_device(quality INT32, value DOUBLE)");
          final String attributes =
              miniConversion
                  ? "'verify'='false','auto-create-schema'='false','on-success'='delete',"
                      + "'tablet-conversion-threshold'='"
                      + inputFile.length()
                      + "'"
                  : "'verify'='false','auto-create-schema'='false','on-success'='delete'";
          try {
            statement.execute(
                String.format("load \"%s\" with (%s)", inputFile.getAbsolutePath(), attributes));
            Assert.fail("A TsFile with an empty Tree device path must fail LOAD");
          } catch (SQLException expected) {
            Assert.assertTrue(
                "The error must identify the illegal empty node in the TsFile device path",
                expected
                    .getMessage()
                    .contains(
                        new IllegalPathException(new PartialPath(invalidDevice).getFullPath())
                            .getMessage()));
            Assert.assertEquals(
                TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), expected.getErrorCode());
          }
          Assert.assertEquals(
              "Only the mini-file case should enter tablet-conversion analysis",
              miniConversion ? 1 : 0,
              getMiniConversionCount() - conversionsBefore);

          Assert.assertTrue("Failed LOAD must retain its source TsFile", inputFile.exists());
          Assert.assertTrue(
              "Failed LOAD must retain the source mods",
              new File(inputFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
          try (ResultSet resultSet =
              statement.executeQuery("show timeseries " + INVALID_DATABASE + ".**")) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(INVALID_DATABASE + ".valid_device.quality", resultSet.getString(1));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(INVALID_DATABASE + ".valid_device.value", resultSet.getString(1));
            Assert.assertFalse(
                "Invalid device path created a visible timeseries", resultSet.next());
          }
          try (ResultSet resultSet =
              statement.executeQuery(
                  "select time, quality, value from " + INVALID_DATABASE + ".valid_device")) {
            if (miniConversion) {
              Assert.assertTrue(
                  "Mini conversion must retain the successfully converted valid prefix",
                  resultSet.next());
              Assert.assertEquals(1, resultSet.getLong("time"));
              Assert.assertEquals(7, resultSet.getInt("quality"));
              Assert.assertNull(
                  "Source mods must also apply to a visible valid prefix",
                  resultSet.getObject("value"));
              Assert.assertFalse(resultSet.next());
            } else {
              Assert.assertFalse(
                  "Normal analysis failure must not execute the LOAD data phase", resultSet.next());
            }
          }
        }
      }
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      deleteDatabase(statement, INVALID_DATABASE);
      statement.execute("create database " + INVALID_DATABASE);
      statement.execute("create timeseries " + INVALID_DATABASE + ".valid_after.value INT64");
      final File validFile = new File(tmpDir, "valid-after-invalid.tsfile");
      writeTreeFile(validFile, INVALID_DATABASE + ".valid_after", new long[] {1}, new long[] {42});
      statement.execute(
          String.format(
              "load \"%s\" with ('verify'='false','auto-create-schema'='false','on-success'='none')",
              validFile.getAbsolutePath()));
      assertOneTreeValue(statement, INVALID_DATABASE + ".valid_after", 1, 42);
    }
    assertLoadTsFileMemoryReturnsTo(loadMemoryBefore);
  }

  private String buildLoadSql(
      final File file,
      final long tabletConversionThreshold,
      final String databaseLevel,
      final String onSuccess) {
    final String thresholdAttribute =
        tabletConversionThreshold < 0
            ? ""
            : ", 'tablet-conversion-threshold'='" + tabletConversionThreshold + "'";
    return String.format(
        "load \"%s\" with ('database-level'='%s','auto-create-schema'='false',"
            + "'on-success'='%s'%s)",
        file.getAbsolutePath(), databaseLevel, onSuccess, thresholdAttribute);
  }

  private long getMiniConversionCount() {
    long count = 0;
    for (final String content : EnvFactory.getEnv().getMetricPrometheusReporterContents(null)) {
      if (content == null) {
        continue;
      }
      for (final String line : content.split("\\n")) {
        final int labelStart = line.indexOf('{');
        if (labelStart < 0
            || !line.substring(0, labelStart).endsWith("_count")
            || !line.contains("load_time_cost")
            || !line.contains("name=\"analysis_cast_tablets\"")) {
          continue;
        }
        count += Math.round(Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1).trim()));
      }
    }
    return count;
  }

  private long getLoadTsFileUsedMemory() {
    long usedMemory = 0;
    boolean foundMetric = false;
    for (final String content : EnvFactory.getEnv().getMetricPrometheusReporterContents(null)) {
      if (content == null) {
        continue;
      }
      for (final String line : content.split("\\n")) {
        final int labelStart = line.indexOf('{');
        if (labelStart < 0
            || !line.substring(0, labelStart).equals("load_mem")
            || !line.contains("name=\"LoadTsFileUsedMemory\"")) {
          continue;
        }
        foundMetric = true;
        usedMemory += Math.round(Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)));
      }
    }
    Assert.assertTrue(
        "LoadTsFileUsedMemory must be exposed by the configured reporter", foundMetric);
    return usedMemory;
  }

  private void assertLoadTsFileMemoryReturnsTo(final long expectedMemory)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000L;
    long actualMemory = getLoadTsFileUsedMemory();
    while (actualMemory != expectedMemory && System.currentTimeMillis() < deadline) {
      Thread.sleep(100L);
      actualMemory = getLoadTsFileUsedMemory();
    }
    Assert.assertEquals(
        "LOAD memory usage should return to its prior level", expectedMemory, actualMemory);
  }

  private void assertThresholdFileContents(final Statement statement, final String device)
      throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery("select time, value from " + device + " order by time")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(10000, resultSet.getLong("time"));
      Assert.assertEquals(10, resultSet.getLong("value"));
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(20000, resultSet.getLong("time"));
      Assert.assertEquals(20, resultSet.getLong("value"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private void assertTreeSeriesValues(
      final Statement statement,
      final String device,
      final int expectedCount,
      final long startValue)
      throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery("select time, value from " + device + " order by time")) {
      for (int i = 0; i < expectedCount; i++) {
        Assert.assertTrue(
            "A normal-path row was lost during statement reconstruction", resultSet.next());
        Assert.assertEquals(100L * (i + 1), resultSet.getLong("time"));
        Assert.assertEquals(startValue + i, resultSet.getLong("value"));
      }
      Assert.assertFalse(resultSet.next());
    }
  }

  private void assertTsFileOrder(final File directory, final String... expectedFileNames) {
    final File[] files = directory.listFiles((dir, name) -> name.endsWith(".tsfile"));
    Assert.assertNotNull(files);
    Arrays.sort(files, Comparator.comparing(File::getName));
    Assert.assertEquals(
        Arrays.asList(expectedFileNames),
        Arrays.stream(files).map(File::getName).collect(Collectors.toList()));
  }

  private void assertOneTreeValue(
      final Statement statement,
      final String device,
      final long expectedTime,
      final long expectedValue)
      throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery("select time, value from " + device + " order by time")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expectedTime, resultSet.getLong("time"));
      Assert.assertEquals(expectedValue, resultSet.getLong("value"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private void writeTreeFile(
      final File file, final String device, final long[] timestamps, final long[] values)
      throws Exception {
    Assert.assertEquals(timestamps.length, values.length);
    final MeasurementSchema schema = new MeasurementSchema("value", TSDataType.INT64);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(device), Collections.singletonList(schema));
      final Tablet tablet = new Tablet(device, Collections.singletonList(schema));
      for (int i = 0; i < timestamps.length; i++) {
        final int row = tablet.getRowSize();
        tablet.addTimestamp(row, timestamps[i]);
        tablet.addValue("value", row, values[i]);
      }
      writer.writeTree(tablet);
    }
  }

  private void writeTypedTreeFile(
      final File file,
      final String device,
      final TSDataType dataType,
      final long[] timestamps,
      final Object[] values)
      throws Exception {
    Assert.assertEquals(timestamps.length, values.length);
    final MeasurementSchema schema = new MeasurementSchema("value", dataType);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(device), Collections.singletonList(schema));
      final Tablet tablet = new Tablet(device, Collections.singletonList(schema));
      for (int i = 0; i < timestamps.length; i++) {
        final int row = tablet.getRowSize();
        tablet.addTimestamp(row, timestamps[i]);
        tablet.addValue("value", row, values[i]);
      }
      writer.writeTree(tablet);
    }
  }

  private void writeTreeDeletion(
      final File file,
      final String device,
      final String measurement,
      final long startTime,
      final long endTime)
      throws IOException, IllegalPathException {
    try (ModificationFile modifications =
        new ModificationFile(
            new File(file.getAbsolutePath() + ModificationFile.FILE_SUFFIX), false)) {
      modifications.write(
          new TreeDeletionEntry(new MeasurementPath(device, measurement), startTime, endTime));
    }
  }

  private void writeInvalidPathTsFile(final File file, final StringArrayDeviceID invalidDevice)
      throws Exception {
    final StringArrayDeviceID validDevice =
        new StringArrayDeviceID(new String[] {"root", "load_p120", "valid_device"});
    try (TsFileIOWriter writer = new TsFileIOWriter(file)) {
      writer.setSchema(new Schema());
      writeInvalidPathChunkGroup(writer, validDevice, 1, 7, 42.0);
      writeInvalidPathChunkGroup(writer, invalidDevice, 2, 1, 1.0);
      writer.endFile();
    }
  }

  private void writeInvalidPathChunkGroup(
      final TsFileIOWriter writer,
      final StringArrayDeviceID device,
      final long timestamp,
      final int quality,
      final double value)
      throws IOException {
    writer.startChunkGroup(device);
    final AlignedChunkWriterImpl chunkWriter =
        new AlignedChunkWriterImpl(
            List.of(
                new MeasurementSchema("quality", TSDataType.INT32),
                new MeasurementSchema("value", TSDataType.DOUBLE)));
    chunkWriter.getTimeChunkWriter().write(timestamp);
    chunkWriter.getValueChunkWriterByIndex(0).write(timestamp, quality, false);
    chunkWriter.getValueChunkWriterByIndex(1).write(timestamp, value, false);
    chunkWriter.writeToFileWriter(writer);
    writer.endChunkGroup();
  }

  private void deleteDatabase(final Statement statement, final String database) {
    try {
      statement.execute("delete database " + database);
    } catch (SQLException ignored) {
      // The database may not have been created by the test.
    }
  }

  private long[] buildTimes(final int count, final long timeGap) {
    final long[] times = new long[count];
    for (int i = 0; i < count; i++) {
      times[i] = timeGap * (i + 1);
    }
    return times;
  }

  private long[] buildValues(final int count, final long startValue) {
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = startValue + i;
    }
    return values;
  }

  private void deleteRecursively(final File file) throws IOException {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Files.deleteIfExists(file.toPath());
  }
}
