/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.db.it.utils.StandardObjectTableModelTsFileGenerator;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeTsFileSinkObjectIT extends AbstractPipeTableModelDualManualIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPipeTsFileSinkObjectIT.class);

  /** Same table name as {@link StandardObjectTableModelTsFileGenerator}. */
  private static final String OBJECT_TABLE_NAME = "factory_metrics";

  private static final int OBJECT_MULTI_WEEK_DEVICE_COUNT = 5;
  private static final int OBJECT_INCREMENTAL_TSFILE_COUNT = 100;

  private static final long HOUR_MS = 3600 * 1000L;
  private static final long DAY_MS = 24L * HOUR_MS;
  private static final long WEEK_MS = 7L * DAY_MS;
  private static final long EXPORT_SCAN_SLEEP_MS = 500L;

  /** Base time aligned so each device sits in a distinct week bucket. */
  private static final long OBJECT_BASE_TIME = 1600000000000L;

  private String targetDir;

  /** Directory for internally generated TsFiles before LOAD on sender. */
  private String sourceTsDir;

  @Override
  @Before
  public void setUp() {
    super.setUp();

    try {
      targetDir =
          Files.createTempDirectory("pipe_tsfile_sink_object_it").toAbsolutePath().toString();
      sourceTsDir =
          Files.createTempDirectory("pipe_tsfile_sink_object_it_src").toAbsolutePath().toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temp directory for targetDir", e);
    }
  }

  @After
  public void cleanupTargetDir() {
    if (targetDir != null) {
      deleteDirectoryQuietly(Paths.get(targetDir));
    }
    if (sourceTsDir != null) {
      deleteDirectoryQuietly(Paths.get(sourceTsDir));
    }
  }

  /** Object payload bytes are derived from the row timestamp for deterministic verification. */
  private static byte[] generateDynamicObjectContent(long timestamp) {
    String payload = "Industrial_Grade_Payload_Verification_For_Timestamp_" + timestamp;
    return payload.getBytes();
  }

  @Test
  public void testPipeTsFileLocalSinkWithObjectLocalMode() throws Exception {
    try (ITableSession session = senderEnv.getTableSessionConnection()) {

      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
      session.executeNonQueryStatement("USE db1");
      session.executeNonQueryStatement(
          "CREATE TABLE IF NOT EXISTS t1 (id STRING TAG, file OBJECT FIELD)");

      insertObjectData(session, "t1", 1, 900);
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      session.executeNonQueryStatement(
          String.format(
              "CREATE PIPE p1 "
                  + "WITH SOURCE ("
                  + "'source.capture.table'='true', "
                  + "'source.database-name'='db1', "
                  + "'source.table-name'='t1', "
                  + "'source.inclusion'='data.insert', "
                  + "'source.history.enable'='true', "
                  + "'source.realtime.enable'='true' "
                  + ") "
                  + "WITH SINK ("
                  + "'sink'='tsfile-local-sink', "
                  + "'sink.local.target-path'='%s', "
                  + "'sink.batch.max-delay-seconds'='1', "
                  + "'sink.batch.size-bytes'='1048576'"
                  + ")",
              targetDir));

      waitForAndVerifyExportedObjects(900, 1, 900, 0, 901, 2400);

      insertObjectData(session, "t1", 901, 2400);
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      waitForAndVerifyExportedObjects(900, 1, 900, 1500, 901, 2400);

      session.executeNonQueryStatement("DROP PIPE p1");
    }
  }

  /**
   * One internally built TsFile with five devices; each device only has points inside a single
   * calendar week, and the five devices use five distinct weeks. Data is LOADed on the sender,
   * exported by Pipe (tsfile-local-sink), sender database is dropped, then the exported TsFiles are
   * LOADed back and OBJECT payloads are checked against the generator format.
   */
  @Test
  public void testPipeTsFileLocalSinkObjectFiveDevicesMultiWeekGeneratedTsFileLoadRoundTrip()
      throws Exception {
    final File tsFile = new File(sourceTsDir, "five_devices_multi_week.tsfile");
    final List<List<Long>> expectedTimesPerDevice = new ArrayList<>();

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {
      for (int i = 0; i < OBJECT_MULTI_WEEK_DEVICE_COUNT; i++) {
        final String deviceId = String.format("device_%02d", i + 1);
        final long weekStart = OBJECT_BASE_TIME + (long) i * WEEK_MS;
        final long weekEnd = weekStart + 6 * DAY_MS;
        generator.writeDeviceData(OBJECT_TABLE_NAME, deviceId, weekStart, weekEnd, DAY_MS);
        expectedTimesPerDevice.add(generateExpectedTimes(weekStart, weekEnd, DAY_MS));
      }
    }

    try (ITableSession session = senderEnv.getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      session.executeNonQueryStatement(
          String.format(
              "CREATE PIPE p_multi_week_obj "
                  + "WITH SOURCE ("
                  + "'source.capture.table'='true', "
                  + "'source.database-name'='db1', "
                  + "'source.table-name'='%s', "
                  + "'source.inclusion'='data.insert', "
                  + "'source.history.enable'='true', "
                  + "'source.realtime.enable'='true' "
                  + ") "
                  + "WITH SINK ("
                  + "'sink'='tsfile-local-sink', "
                  + "'sink.local.target-path'='%s', "
                  + "'sink.batch.max-delay-seconds'='1', "
                  + "'sink.batch.size-bytes'='1048576'"
                  + ")",
              OBJECT_TABLE_NAME, targetDir));

      waitForAtLeastOneExportedTsFile(new File(targetDir), 60_000);
      session.executeNonQueryStatement("DROP PIPE p_multi_week_obj");

      session.executeNonQueryStatement("DROP DATABASE db1");

      final List<File> exportedTsFiles = new ArrayList<>();
      findTsFiles(new File(targetDir), exportedTsFiles);
      Assert.assertFalse("Pipe should export at least one .tsfile", exportedTsFiles.isEmpty());

      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
      session.executeNonQueryStatement("USE \"db1\"");
      for (File f : exportedTsFiles) {
        session.executeNonQueryStatement(String.format("LOAD '%s'", f.getAbsolutePath()));
      }
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      for (int i = 0; i < OBJECT_MULTI_WEEK_DEVICE_COUNT; i++) {
        final String deviceId = String.format("device_%02d", i + 1);
        assertDeviceObjectBytesMatchGenerator(
            session, OBJECT_TABLE_NAME, deviceId, expectedTimesPerDevice.get(i));
      }
    }
  }

  @Test
  public void testPipeTsFileLocalSinkObjectHundredGeneratedTsFilesLoadToReceiverWithoutMods()
      throws Exception {
    executeIncrementalExportLoadRoundTrip(false);
  }

  @Test
  public void testPipeTsFileLocalSinkObjectHundredGeneratedTsFilesLoadToReceiverWithMods()
      throws Exception {
    executeIncrementalExportLoadRoundTrip(true);
  }

  private void executeIncrementalExportLoadRoundTrip(final boolean withMods) throws Exception {
    final String database = "db1";
    final String pipeName =
        withMods
            ? "p_incremental_generated_obj_with_mods"
            : "p_incremental_generated_obj_without_mods";
    final Map<String, List<Long>> expectedTimesByDevice = new LinkedHashMap<>();
    final List<File> sourceTsFiles = new ArrayList<>();

    for (int i = 0; i < OBJECT_INCREMENTAL_TSFILE_COUNT; i++) {
      final String deviceId = String.format("device_%03d", i + 1);
      final File tsFile =
          new File(
              sourceTsDir,
              withMods
                  ? String.format("incremental_object_with_mods_%03d.tsfile", i + 1)
                  : String.format("incremental_object_without_mods_%03d.tsfile", i + 1));
      final long startTime = OBJECT_BASE_TIME + (long) i * DAY_MS;
      final long endTime = startTime + HOUR_MS;

      try (StandardObjectTableModelTsFileGenerator generator =
          new StandardObjectTableModelTsFileGenerator(tsFile)) {
        generator.writeDeviceData(OBJECT_TABLE_NAME, deviceId, startTime, endTime, HOUR_MS);
        if (withMods) {
          generator.generateDeletion(OBJECT_TABLE_NAME, deviceId, endTime, endTime);
          expectedTimesByDevice.put(
              deviceId, generateExpectedTimes(startTime, endTime, HOUR_MS, endTime, endTime));
        } else {
          expectedTimesByDevice.put(deviceId, generateExpectedTimes(startTime, endTime, HOUR_MS));
        }
      }
      sourceTsFiles.add(tsFile);
    }

    try (ITableSession sender = senderEnv.getTableSessionConnection()) {
      sender.executeNonQueryStatement(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
      sender.executeNonQueryStatement(String.format("USE \"%s\"", database));
      for (File tsFile : sourceTsFiles) {
        sender.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));
      }
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      sender.executeNonQueryStatement(
          String.format(
              "CREATE PIPE %s "
                  + "WITH SOURCE ("
                  + "'source.capture.table'='true', "
                  + "'source.database-name'='%s', "
                  + "'source.table-name'='%s', "
                  + "'source.inclusion'='data.insert', "
                  + "'source.mods.enable'='%s', "
                  + "'source.history.enable'='true', "
                  + "'source.realtime.enable'='false' "
                  + ") "
                  + "WITH SINK ("
                  + "'sink'='tsfile-local-sink', "
                  + "'sink.local.target-path'='%s', "
                  + "'sink.batch.max-delay-seconds'='1', "
                  + "'sink.batch.size-bytes'='1048576'"
                  + ")",
              pipeName, database, OBJECT_TABLE_NAME, withMods, targetDir));
    }

    try {
      try (ITableSession receiver = receiverEnv.getTableSessionConnection()) {
        receiver.executeNonQueryStatement(
            String.format("CREATE DATABASE IF NOT EXISTS %s", database));
        receiver.executeNonQueryStatement(String.format("USE \"%s\"", database));
        loadExportedTsFilesIncrementallyUntilExpectedOnReceiver(
            receiver, new File(targetDir), database, expectedTimesByDevice, 180_000L);
      }
    } finally {
      try (ITableSession sender = senderEnv.getTableSessionConnection()) {
        sender.executeNonQueryStatement("DROP PIPE " + pipeName);
      }
    }

    if (!withMods) {
      final List<File> modsFiles = new ArrayList<>();
      findFilesBySuffix(new File(targetDir), ModificationFile.FILE_SUFFIX, modsFiles);
      Assert.assertTrue(
          "No companion mods file should be exported for the pure object TsFiles.",
          modsFiles.isEmpty());
    }
  }

  private void loadExportedTsFilesIncrementallyUntilExpectedOnReceiver(
      final ITableSession receiver,
      final File exportRoot,
      final String database,
      final Map<String, List<Long>> expectedTimesByDevice,
      final long timeoutMs)
      throws Exception {
    final long expectedTotalRows = calculateExpectedRowCount(expectedTimesByDevice);
    final long deadline = System.currentTimeMillis() + timeoutMs;
    final Set<String> loadedTsFilePaths = new HashSet<>();

    long previousDiscoveredTsFileCount = -1;
    Throwable lastValidationFailure = null;

    while (System.currentTimeMillis() < deadline) {
      final List<File> exportedTsFiles = new ArrayList<>();
      findTsFiles(exportRoot, exportedTsFiles);
      exportedTsFiles.sort(Comparator.comparing(File::getAbsolutePath));

      boolean loadedAnyThisRound = false;
      for (File tsFile : exportedTsFiles) {
        final String tsFilePath = tsFile.getAbsolutePath();
        if (loadedTsFilePaths.contains(tsFilePath)) {
          continue;
        }

        try {
          receiver.executeNonQueryStatement(
              String.format("LOAD '%s' WITH ('database-name'='%s')", tsFilePath, database));
          loadedTsFilePaths.add(tsFilePath);
          loadedAnyThisRound = true;
        } catch (Exception e) {
          LOGGER.info("Receiver LOAD will retry later for exported TsFile {}", tsFilePath, e);
        }
      }

      if (loadedAnyThisRound) {
        TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
      }

      final long actualRowCount = queryRowCountOrNegative(receiver, OBJECT_TABLE_NAME);
      if (actualRowCount == expectedTotalRows) {
        try {
          for (Map.Entry<String, List<Long>> entry : expectedTimesByDevice.entrySet()) {
            assertDeviceObjectBytesMatchGenerator(
                receiver, OBJECT_TABLE_NAME, entry.getKey(), entry.getValue());
          }
          return;
        } catch (Throwable t) {
          lastValidationFailure = t;
        }
      }

      final long discoveredTsFileCount = exportedTsFiles.size();
      if (!loadedAnyThisRound && discoveredTsFileCount == previousDiscoveredTsFileCount) {
        Thread.sleep(EXPORT_SCAN_SLEEP_MS);
      }
      previousDiscoveredTsFileCount = discoveredTsFileCount;
    }

    final List<File> exportedTsFiles = new ArrayList<>();
    findTsFiles(exportRoot, exportedTsFiles);
    exportedTsFiles.sort(Comparator.comparing(File::getAbsolutePath));

    final List<String> pendingTsFiles = new ArrayList<>();
    for (File tsFile : exportedTsFiles) {
      final String tsFilePath = tsFile.getAbsolutePath();
      if (!loadedTsFilePaths.contains(tsFilePath)) {
        pendingTsFiles.add(tsFilePath);
      }
    }

    final long actualRowCount = queryRowCountOrNegative(receiver, OBJECT_TABLE_NAME);
    final String baseMessage =
        String.format(
            "Timeout waiting receiver data to match expected rows. expected=%d, actual=%d, "
                + "loadedTsFiles=%d, discoveredTsFiles=%d, pendingTsFiles=%s",
            expectedTotalRows,
            actualRowCount,
            loadedTsFilePaths.size(),
            exportedTsFiles.size(),
            pendingTsFiles);
    if (lastValidationFailure != null) {
      throw new AssertionError(baseMessage, lastValidationFailure);
    }
    Assert.fail(baseMessage);
  }

  private static long calculateExpectedRowCount(
      final Map<String, List<Long>> expectedTimesByDevice) {
    long totalRowCount = 0;
    for (List<Long> expectedTimes : expectedTimesByDevice.values()) {
      totalRowCount += expectedTimes.size();
    }
    return totalRowCount;
  }

  private static long queryRowCountOrNegative(final ITableSession session, final String table) {
    try {
      return queryRowCount(session, table);
    } catch (Exception e) {
      return -1;
    }
  }

  private static List<Long> generateExpectedTimes(
      final long startTime, final long endTime, final long interval) {
    final List<Long> times = new ArrayList<>();
    for (long t = startTime; t <= endTime; t += interval) {
      times.add(t);
    }
    return times;
  }

  private static List<Long> generateExpectedTimes(
      final long startTime,
      final long endTime,
      final long interval,
      final long deleteStartTime,
      final long deleteEndTime) {
    final List<Long> times = new ArrayList<>();
    for (long t = startTime; t <= endTime; t += interval) {
      if (t >= deleteStartTime && t <= deleteEndTime) {
        continue;
      }
      times.add(t);
    }
    return times;
  }

  /**
   * Polls until at least one {@code .tsfile} appears under {@code root} (recursive) or {@code
   * timeoutMs} elapses.
   */
  private static void waitForAtLeastOneExportedTsFile(final File root, final long timeoutMs)
      throws Exception {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final List<File> found = new ArrayList<>();
      findTsFiles(root, found);
      if (!found.isEmpty()) {
        Thread.sleep(1500);
        return;
      }
      Thread.sleep(1000);
    }
    Assert.fail("Timeout waiting for exported .tsfile under " + root.getAbsolutePath());
  }

  private static void assertDeviceObjectBytesMatchGenerator(
      final ITableSession session,
      final String tableName,
      final String deviceId,
      final List<Long> expectedTimes)
      throws Exception {
    final String query =
        String.format(
            "SELECT time, READ_OBJECT(sensor_obj) FROM %s WHERE id='%s' ORDER BY time ASC",
            tableName, deviceId);

    try (SessionDataSet dataSet = session.executeQueryStatement(query)) {
      final SessionDataSet.DataIterator iterator = dataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        Assert.assertTrue("More rows than expected for " + deviceId, count < expectedTimes.size());
        final long actualTime = iterator.getLong(1);
        final Binary binary = iterator.getBlob(2);
        final byte[] actualBytes = binary.getValues();
        final long expectedTime = expectedTimes.get(count);
        Assert.assertEquals("Time mismatch at index " + count, expectedTime, actualTime);
        final byte[] expectedBytes =
            String.format("AutoGenerated|Table=%s|ID=%s|Time=%d", tableName, deviceId, expectedTime)
                .getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(
            "Object byte content mismatch at time " + actualTime, expectedBytes, actualBytes);
        count++;
      }
      Assert.assertEquals("Total row count mismatch for " + deviceId, expectedTimes.size(), count);
    }
  }

  private void insertObjectData(ITableSession session, String tableName, long startTs, long endTs)
      throws Exception {
    List<String> columnNames = Arrays.asList("id", "file");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.STRING, TSDataType.OBJECT);
    List<ColumnCategory> columnCategories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    Tablet tablet = new Tablet(tableName, columnNames, dataTypes, columnCategories, 100);

    for (long ts = startTs; ts <= endTs; ts++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, ts);
      tablet.addValue(rowIndex, 0, "device1");

      byte[] dynamicObjectBytes = generateDynamicObjectContent(ts);
      tablet.addValue(rowIndex, 1, true, 0, dynamicObjectBytes);

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insert(tablet);
        tablet.reset();
      }
    }

    if (tablet.getRowSize() > 0) {
      session.insert(tablet);
    }
  }

  /**
   * Reads exported .tsfile(s), resolves OBJECT paths against the per-export bundle directory (same
   * basename as the .tsfile), and checks payload bytes against {@link
   * #generateDynamicObjectContent(long)}.
   */
  private void waitForAndVerifyExportedObjects(
      int expectedHistoryCount,
      long historyStartTs,
      long historyEndTs,
      int expectedRealtimeCount,
      long realtimeStartTs,
      long realtimeEndTs)
      throws Exception {
    int expectedTotal = expectedHistoryCount + expectedRealtimeCount;
    int historyFound = 0;
    int realtimeFound = 0;

    for (int i = 0; i < 60; i++) {
      historyFound = 0;
      realtimeFound = 0;

      List<File> tsfiles = new ArrayList<>();
      findTsFiles(new File(targetDir), tsfiles);

      for (File tsfile : tsfiles) {
        String bundleName = tsfile.getName().replace(".tsfile", "");
        File objectBundleRoot = new File(targetDir, bundleName);

        try (ITsFileReader reader = new TsFileReaderBuilder().file(tsfile).build();
            ResultSet resultSet =
                reader.query("t1", Arrays.asList("file"), Long.MIN_VALUE, Long.MAX_VALUE)) {
          while (resultSet.next()) {
            if (resultSet.isNull("file")) {
              continue;
            }

            final long ts = resultSet.getLong(1);
            final Binary val = new Binary(resultSet.getBinary("file"));

            final Pair<Long, String> sizeAndPath =
                ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(val);
            final long declaredSize = sizeAndPath.getLeft();
            final String relativePath = sizeAndPath.getRight();

            final File objFile = new File(objectBundleRoot, relativePath);
            Assert.assertTrue(
                "Exported Object file missing: " + objFile.getAbsolutePath(), objFile.exists());

            final byte[] actualBytes = Files.readAllBytes(objFile.toPath());
            Assert.assertEquals(
                "File size mismatch at timestamp " + ts, declaredSize, actualBytes.length);

            final byte[] expectedBytes = generateDynamicObjectContent(ts);
            Assert.assertArrayEquals(
                "Data corruption detected at timestamp: " + ts, expectedBytes, actualBytes);

            if (ts >= historyStartTs && ts <= historyEndTs) {
              historyFound++;
            } else if (ts >= realtimeStartTs && ts <= realtimeEndTs) {
              realtimeFound++;
            } else {
              Assert.fail(
                  String.format(
                      "Unexpected timestamp %d found. Expected History [%d, %d] or Realtime [%d, %d]",
                      ts, historyStartTs, historyEndTs, realtimeStartTs, realtimeEndTs));
            }
          }
        }
      }

      if (historyFound + realtimeFound >= expectedTotal) {
        break;
      }
      Thread.sleep(1000);
    }

    LOGGER.info(
        "Verification complete. History objects: {}, realtime objects: {}",
        historyFound,
        realtimeFound);
    Assert.assertEquals(
        "History Object count mismatch after Pipe sync", expectedHistoryCount, historyFound);
    Assert.assertEquals(
        "Realtime Object count mismatch after Pipe sync", expectedRealtimeCount, realtimeFound);
  }

  private static void waitForExportedTsFilesWithMods(final File root, final long timeoutMs)
      throws Exception {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final List<File> tsfiles = new ArrayList<>();
      findTsFiles(root, tsfiles);
      if (!tsfiles.isEmpty()) {
        boolean allMods = true;
        for (File tf : tsfiles) {
          final File mod = new File(tf.getParent(), tf.getName() + ModificationFile.FILE_SUFFIX);
          if (!mod.isFile()) {
            allMods = false;
            break;
          }
        }
        if (allMods) {
          Thread.sleep(2000);
          return;
        }
      }
      Thread.sleep(800);
    }
    Assert.fail(
        "Timeout waiting for .tsfile and companion "
            + ModificationFile.FILE_SUFFIX
            + " under "
            + root.getAbsolutePath());
  }

  private static void loadAllTsFilesUnderDir(
      final ITableSession session, final File root, final String database) throws Exception {
    final List<File> tsfiles = new ArrayList<>();
    findTsFiles(root, tsfiles);
    Assert.assertFalse(tsfiles.isEmpty());
    tsfiles.sort(Comparator.comparing(File::getAbsolutePath));
    for (File f : tsfiles) {
      session.executeNonQueryStatement(
          String.format("LOAD '%s' WITH ('database-name'='%s')", f.getAbsolutePath(), database));
    }
  }

  private static long queryRowCount(final ITableSession session, final String table)
      throws Exception {
    try (SessionDataSet ds = session.executeQueryStatement("SELECT COUNT(*) FROM " + table)) {
      final SessionDataSet.DataIterator it = ds.iterator();
      Assert.assertTrue(it.next());
      return it.getLong(1);
    }
  }

  private static void findTsFiles(File dir, List<File> tsfiles) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File f : files) {
      if (f.isDirectory()) {
        findTsFiles(f, tsfiles);
      } else if (f.getName().endsWith(".tsfile")) {
        tsfiles.add(f);
      }
    }
  }

  private static void findFilesBySuffix(File dir, String suffix, List<File> matchedFiles) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File f : files) {
      if (f.isDirectory()) {
        findFilesBySuffix(f, suffix, matchedFiles);
      } else if (f.getName().endsWith(suffix)) {
        matchedFiles.add(f);
      }
    }
  }

  private static void deleteDirectoryQuietly(Path dirPath) {
    if (!Files.exists(dirPath)) {
      return;
    }
    try {
      Files.walk(dirPath)
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  LOGGER.warn("Failed to delete path {}", path, e);
                }
              });
    } catch (IOException e) {
      LOGGER.warn("Failed to cleanup temp directory {}", dirPath, e);
    }
  }
}
