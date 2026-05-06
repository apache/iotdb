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

import org.apache.iotdb.db.it.utils.StandardObjectTableModelTsFileGenerator;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
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
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeTableModelObjectImportExportIT extends AbstractPipeTableModelDualManualIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBPipeTableModelObjectImportExportIT.class);

  private static final String TEST_DATABASE = "db1";

  private static final String OBJECT_TABLE_NAME = "factory_metrics";

  private static final int OBJECT_MULTI_WEEK_DEVICE_COUNT = 5;

  private static final int SOURCE_SINK_INSERT_ROWS = 100;

  private static final int SOURCE_SINK_DELETE_FROM_FIRST_OFFSET = 20;
  private static final int SOURCE_SINK_DELETE_LEN = 25;

  private static final long EXPECTED_ROWS_AFTER_DELETE =
      SOURCE_SINK_INSERT_ROWS - SOURCE_SINK_DELETE_LEN;

  private static final long SENDER_DELETE_LO = 1L + SOURCE_SINK_DELETE_FROM_FIRST_OFFSET;

  private static final long SENDER_DELETE_HI = SENDER_DELETE_LO + SOURCE_SINK_DELETE_LEN - 1;

  private static final long HOUR_MS = 3600 * 1000L;
  private static final long DAY_MS = 24L * HOUR_MS;
  private static final long WEEK_MS = 7L * DAY_MS;

  private static final long OBJECT_BASE_TIME = 1600000000000L;

  private String targetDir;
  private String sourceTsDir;

  private enum TableSourceScope {
    ALL,
    DATABASE_ONLY,
    DATABASE_AND_TABLE
  }

  @Override
  @Before
  public void setUp() {
    super.setUp();

    try {
      targetDir =
          Files.createTempDirectory("pipe_table_tsfile_source_sink_it").toAbsolutePath().toString();
      sourceTsDir =
          Files.createTempDirectory("pipe_table_tsfile_source_sink_it_src")
              .toAbsolutePath()
              .toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temp directory", e);
    }
  }

  @After
  public void cleanUpTempDirectories() {
    if (targetDir != null) {
      deleteRecursivelyQuietly(Paths.get(targetDir));
    }
    if (sourceTsDir != null) {
      deleteRecursivelyQuietly(Paths.get(sourceTsDir));
    }
  }

  private static byte[] buildExpectedObjectPayload(final long timestamp) {
    String payload = "Industrial_Grade_Payload_Verification_For_Timestamp_" + timestamp;
    return payload.getBytes();
  }

  @Test
  public void testHistoricalExportWithDeletesForAllScope() throws Exception {
    verifyHistoricalObjectExportWithDeletes(
        "p_scope_all_hist_mods",
        TableSourceScope.ALL,
        false,
        true,
        false,
        EXPECTED_ROWS_AFTER_DELETE);
  }

  @Test
  public void testHistoricalExportWithDeletesForDatabaseScope() throws Exception {
    verifyHistoricalObjectExportWithDeletes(
        "p_scope_db_hist_mods",
        TableSourceScope.DATABASE_ONLY,
        false,
        true,
        false,
        EXPECTED_ROWS_AFTER_DELETE);
  }

  @Test
  public void testHistoricalExportWithDeletesForDatabaseAndTableScope() throws Exception {
    verifyHistoricalObjectExportWithDeletes(
        "p_scope_dbt_hist_mods",
        TableSourceScope.DATABASE_AND_TABLE,
        false,
        true,
        false,
        EXPECTED_ROWS_AFTER_DELETE);
  }

  @Test
  public void testHistoricalExportWithoutDeletesForDatabaseAndTableScope() throws Exception {
    verifyHistoricalObjectExportWithoutDeletes(
        "p_scope_dbt_hist_nomods", TableSourceScope.DATABASE_AND_TABLE, true, false);
  }

  @Test
  public void testHistoricalExportWithoutDeletesForAllScope() throws Exception {
    verifyHistoricalObjectExportWithoutDeletes(
        "p_scope_all_hist_nomods", TableSourceScope.ALL, true, false);
  }

  @Test
  public void testHistoricalExportWithoutDeletesForDatabaseScope() throws Exception {
    verifyHistoricalObjectExportWithoutDeletes(
        "p_scope_db_hist_nomods", TableSourceScope.DATABASE_ONLY, true, false);
  }

  @Test
  public void testObjectRoundTripFromMultiWeekGeneratedTsFile() throws Exception {
    final File tsFile = new File(sourceTsDir, "five_devices_multi_week.tsfile");
    final List<List<Long>> expectedTimesPerDevice = new ArrayList<>();

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {
      for (int i = 0; i < OBJECT_MULTI_WEEK_DEVICE_COUNT; i++) {
        final String deviceId = String.format("device_%02d", i + 1);
        final long weekStart = OBJECT_BASE_TIME + (long) i * WEEK_MS;
        final long weekEnd = weekStart + 6 * DAY_MS;
        generator.writeDeviceData(OBJECT_TABLE_NAME, deviceId, weekStart, weekEnd, DAY_MS);
        expectedTimesPerDevice.add(buildExpectedTimestamps(weekStart, weekEnd, DAY_MS));
      }
    }

    try (ITableSession sender = senderEnv.getTableSessionConnection()) {
      sender.executeNonQueryStatement(
          String.format("CREATE DATABASE IF NOT EXISTS %s", TEST_DATABASE));
      useDatabase(sender, TEST_DATABASE);
      sender.executeNonQueryStatement(
          String.format(
              "LOAD '%s' WITH ('database-name'='%s')", tsFile.getAbsolutePath(), TEST_DATABASE));
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      sender.executeNonQueryStatement(
          String.format(
              "CREATE PIPE p_src_sink_multi_week WITH SOURCE ("
                  + "'source.capture.table'='true', "
                  + "'source.database-name'='%s', "
                  + "'source.table-name'='%s', "
                  + "'source.inclusion'='data.insert', "
                  + "'source.history.enable'='true', "
                  + "'source.realtime.enable'='false' "
                  + ") WITH SINK ("
                  + "%s"
                  + ")",
              TEST_DATABASE, OBJECT_TABLE_NAME, buildTsFileLocalSinkClause()));

      waitForExportedTsFile(new File(targetDir), 60_000);
      sender.executeNonQueryStatement("DROP PIPE p_src_sink_multi_week");
      sender.executeNonQueryStatement(String.format("DROP DATABASE %s", TEST_DATABASE));
    }

    final List<File> exportedTsFiles = new ArrayList<>();
    collectTsFilesRecursively(new File(targetDir), exportedTsFiles);
    Assert.assertFalse(exportedTsFiles.isEmpty());

    try (ITableSession receiver = receiverEnv.getTableSessionConnection()) {
      receiver.executeNonQueryStatement(
          String.format("CREATE DATABASE IF NOT EXISTS %s", TEST_DATABASE));
      useDatabase(receiver, TEST_DATABASE);
      for (File f : exportedTsFiles) {
        receiver.executeNonQueryStatement(
            String.format(
                "LOAD '%s' WITH ('database-name'='%s')", f.getAbsolutePath(), TEST_DATABASE));
      }
      TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");

      for (int i = 0; i < OBJECT_MULTI_WEEK_DEVICE_COUNT; i++) {
        final String deviceId = String.format("device_%02d", i + 1);
        assertGeneratedObjectPayloadsByDevice(
            receiver, OBJECT_TABLE_NAME, deviceId, expectedTimesPerDevice.get(i));
      }
    }
  }

  private void verifyHistoricalObjectExportWithDeletes(
      final String pipeName,
      final TableSourceScope scope,
      final boolean realtimeFirst,
      final boolean historyEnable,
      final boolean realtimeEnable,
      final long expectedRowsAfterLoad)
      throws Exception {
    try (ITableSession sender = senderEnv.getTableSessionConnection()) {
      createSenderObjectTable(sender);

      if (realtimeFirst) {
        sender.executeNonQueryStatement(
            String.format(
                "CREATE PIPE %s WITH SOURCE (%s) WITH SINK (%s)",
                pipeName,
                buildObjectSourceClause(scope, true, historyEnable, realtimeEnable, true),
                buildTsFileLocalSinkClause()));
        Thread.sleep(2000);
      }

      insertObjectRowsWithContiguousTimestamps(sender, "t1", 1L, SOURCE_SINK_INSERT_ROWS);
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
      final long delLo = 1L + SOURCE_SINK_DELETE_FROM_FIRST_OFFSET;
      final long delHi = delLo + SOURCE_SINK_DELETE_LEN - 1;
      sender.executeNonQueryStatement(
          String.format("DELETE FROM t1 WHERE time >= %d AND time <= %d", delLo, delHi));
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      if (!realtimeFirst) {
        sender.executeNonQueryStatement(
            String.format(
                "CREATE PIPE %s WITH SOURCE (%s) WITH SINK (%s)",
                pipeName,
                buildObjectSourceClause(scope, true, historyEnable, realtimeEnable, true),
                buildTsFileLocalSinkClause()));
      }

      waitForExportedTsFilesWithMods(new File(targetDir), 120_000);
      sender.executeNonQueryStatement("DROP PIPE IF EXISTS " + pipeName);
      sender.executeNonQueryStatement("DROP TABLE IF EXISTS t1");
    }

    try (ITableSession receiver = receiverEnv.getTableSessionConnection()) {
      createReceiverDatabaseForLoad(receiver);
      loadExportedTsFiles(receiver, new File(targetDir));
      TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
      Assert.assertEquals(expectedRowsAfterLoad, queryTableRowCount(receiver, "t1"));
      assertNoRowsExistInDeletedTimeRange(receiver, SENDER_DELETE_LO, SENDER_DELETE_HI);
      assertObjectColumnMatchesExpectedPayload(
          receiver, buildExpectedTimestampsAfterSenderDelete(1L, SOURCE_SINK_INSERT_ROWS));
    }
  }

  private void verifyHistoricalObjectExportWithoutDeletes(
      final String pipeName,
      final TableSourceScope scope,
      final boolean historyEnable,
      final boolean realtimeEnable)
      throws Exception {
    try (ITableSession sender = senderEnv.getTableSessionConnection()) {
      createSenderObjectTable(sender);

      if (realtimeEnable && !historyEnable) {
        sender.executeNonQueryStatement(
            String.format(
                "CREATE PIPE %s WITH SOURCE (%s) WITH SINK (%s)",
                pipeName,
                buildObjectSourceClause(scope, false, historyEnable, realtimeEnable, true),
                buildTsFileLocalSinkClause()));
        Thread.sleep(2000);
      }

      insertObjectRowsWithContiguousTimestamps(sender, "t1", 1L, SOURCE_SINK_INSERT_ROWS);
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      if (!realtimeEnable || historyEnable) {
        sender.executeNonQueryStatement(
            String.format(
                "CREATE PIPE %s WITH SOURCE (%s) WITH SINK (%s)",
                pipeName,
                buildObjectSourceClause(scope, false, historyEnable, realtimeEnable, true),
                buildTsFileLocalSinkClause()));
      }

      waitForExportedTsFile(new File(targetDir), 120_000);
      sender.executeNonQueryStatement("DROP PIPE IF EXISTS " + pipeName);
      sender.executeNonQueryStatement("DROP TABLE IF EXISTS t1");
    }

    try (ITableSession receiver = receiverEnv.getTableSessionConnection()) {
      createReceiverDatabaseForLoad(receiver);
      loadExportedTsFiles(receiver, new File(targetDir));
      TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
      Assert.assertEquals(SOURCE_SINK_INSERT_ROWS, queryTableRowCount(receiver, "t1"));
      assertObjectColumnMatchesExpectedPayload(
          receiver, buildContiguousTimestamps(1L, SOURCE_SINK_INSERT_ROWS));
    }
  }

  private static List<Long> buildContiguousTimestamps(
      final long startInclusive, final int rowCount) {
    final List<Long> out = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      out.add(startInclusive + i);
    }
    return out;
  }

  private static List<Long> buildExpectedTimestampsAfterSenderDelete(
      final long firstTimeInclusive, final int rowCount) {
    final List<Long> out = new ArrayList<>();
    final long end = firstTimeInclusive + rowCount - 1;
    for (long t = firstTimeInclusive; t <= end; t++) {
      if (t < SENDER_DELETE_LO || t > SENDER_DELETE_HI) {
        out.add(t);
      }
    }
    return out;
  }

  private static void assertNoRowsExistInDeletedTimeRange(
      final ITableSession session, final long loInclusive, final long hiInclusive)
      throws Exception {
    useDatabase(session, TEST_DATABASE);
    final String sql =
        String.format(
            "SELECT COUNT(*) FROM t1 WHERE time >= %d AND time <= %d", loInclusive, hiInclusive);
    try (SessionDataSet ds = session.executeQueryStatement(sql)) {
      final SessionDataSet.DataIterator it = ds.iterator();
      Assert.assertTrue(it.next());
      Assert.assertEquals(
          "Rows in sender-deleted time window must not exist on receiver after LOAD",
          0L,
          it.getLong(1));
    }
  }

  private static void assertObjectColumnMatchesExpectedPayload(
      final ITableSession session, final List<Long> expectedTimesAsc) throws Exception {
    useDatabase(session, TEST_DATABASE);
    try (SessionDataSet ds =
        session.executeQueryStatement("SELECT time, READ_OBJECT(file) FROM t1 ORDER BY time ASC")) {
      final SessionDataSet.DataIterator it = ds.iterator();
      int idx = 0;
      while (it.next()) {
        Assert.assertTrue("More rows than expected", idx < expectedTimesAsc.size());
        final long expectedTs = expectedTimesAsc.get(idx);
        Assert.assertEquals("time column", expectedTs, it.getLong(1));
        final byte[] actual = it.getBlob(2).getValues();
        Assert.assertArrayEquals(
            "OBJECT column (file) mismatch at time " + expectedTs,
            buildExpectedObjectPayload(expectedTs),
            actual);
        idx++;
      }
      Assert.assertEquals(expectedTimesAsc.size(), idx);
    }
  }

  private void createSenderObjectTable(final ITableSession sender) throws Exception {
    sender.executeNonQueryStatement(
        String.format("CREATE DATABASE IF NOT EXISTS %s", TEST_DATABASE));
    useDatabase(sender, TEST_DATABASE);
    sender.executeNonQueryStatement(
        "CREATE TABLE IF NOT EXISTS t1 (id STRING TAG, file OBJECT FIELD)");
  }

  private void createReceiverDatabaseForLoad(final ITableSession receiver) throws Exception {
    receiver.executeNonQueryStatement(
        String.format("CREATE DATABASE IF NOT EXISTS %s", TEST_DATABASE));
    useDatabase(receiver, TEST_DATABASE);
  }

  private static void useDatabase(final ITableSession session, final String database)
      throws Exception {
    session.executeNonQueryStatement(String.format("USE %s", database));
  }

  private String buildObjectSourceClause(
      final TableSourceScope scope,
      final boolean modsEnable,
      final boolean historyEnable,
      final boolean realtimeEnable,
      final boolean captureTable) {
    final StringBuilder sb = new StringBuilder();
    sb.append("'source.capture.table'='").append(captureTable).append("'");
    switch (scope) {
      case DATABASE_AND_TABLE:
        sb.append(", 'source.database-name'='")
            .append(TEST_DATABASE)
            .append("', 'source.table-name'='t1'");
        break;
      case DATABASE_ONLY:
        sb.append(", 'source.database-name'='").append(TEST_DATABASE).append("'");
        break;
      case ALL:
      default:
        break;
    }
    sb.append(", 'source.inclusion'='data.insert");
    if (modsEnable) {
      sb.append(",data.delete");
    }
    sb.append("'");
    if (modsEnable) {
      sb.append(", 'source.mods.enable'='true'");
    }
    sb.append(", 'source.history.enable'='").append(historyEnable).append("'");
    sb.append(", 'source.realtime.enable'='").append(realtimeEnable).append("'");
    return sb.toString();
  }

  private String buildTsFileLocalSinkClause() {
    return String.format(
        "'sink'='tsfile-local-sink', 'sink.local.target-path'='%s', "
            + "'sink.batch.max-delay-seconds'='1', 'sink.batch.size-bytes'='10485760'",
        targetDir);
  }

  private static List<Long> buildExpectedTimestamps(
      final long startTime, final long endTime, final long interval) {
    final List<Long> times = new ArrayList<>();
    for (long t = startTime; t <= endTime; t += interval) {
      times.add(t);
    }
    return times;
  }

  private static void waitForExportedTsFile(final File root, final long timeoutMs)
      throws Exception {
    waitForStableExportedTsFiles(
        root, timeoutMs, "Timeout waiting for exported .tsfile under " + root.getAbsolutePath());
  }

  private static void assertGeneratedObjectPayloadsByDevice(
      final ITableSession session,
      final String tableName,
      final String deviceId,
      final List<Long> expectedTimes)
      throws Exception {
    useDatabase(session, TEST_DATABASE);
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

  private void insertObjectRowsWithContiguousTimestamps(
      final ITableSession session, final String tableName, final long startTs, final int rowCount)
      throws Exception {
    final long endTs = startTs + rowCount - 1;
    insertObjectRowsInTimeRange(session, tableName, startTs, endTs);
  }

  private void insertObjectRowsInTimeRange(
      final ITableSession session, final String tableName, final long startTs, final long endTs)
      throws Exception {
    List<String> columnNames = Arrays.asList("id", "file");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.STRING, TSDataType.OBJECT);
    List<ColumnCategory> columnCategories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    Tablet tablet = new Tablet(tableName, columnNames, dataTypes, columnCategories, 100);

    for (long ts = startTs; ts <= endTs; ts++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, ts);
      tablet.addValue(rowIndex, 0, "device1");

      byte[] dynamicObjectBytes = buildExpectedObjectPayload(ts);
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

  private static void waitForExportedTsFilesWithMods(final File root, final long timeoutMs)
      throws Exception {
    waitForStableExportedTsFiles(
        root, timeoutMs, "Timeout waiting for exported .tsfile under " + root.getAbsolutePath());
  }

  private static void waitForStableExportedTsFiles(
      final File root, final long timeoutMs, final String timeoutMessage) throws Exception {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    ExportedTsFileSnapshot previousReadySnapshot = null;
    while (System.currentTimeMillis() < deadline) {
      final ExportedTsFileSnapshot currentSnapshot = ExportedTsFileSnapshot.capture(root);
      if (currentSnapshot.isReady()) {
        if (currentSnapshot.equals(previousReadySnapshot)) {
          return;
        }
        previousReadySnapshot = currentSnapshot;
      } else {
        previousReadySnapshot = null;
      }
      Thread.sleep(1000);
    }
    Assert.fail(timeoutMessage);
  }

  private static final class ExportedTsFileSnapshot {

    private final List<String> fingerprints;
    private final boolean ready;

    private ExportedTsFileSnapshot(final List<String> fingerprints, final boolean ready) {
      this.fingerprints = fingerprints;
      this.ready = ready;
    }

    private static ExportedTsFileSnapshot capture(final File root) {
      final List<File> tsfiles = new ArrayList<>();
      collectTsFilesRecursively(root, tsfiles);
      if (tsfiles.isEmpty()) {
        return new ExportedTsFileSnapshot(new ArrayList<>(), false);
      }

      tsfiles.sort(Comparator.comparing(File::getAbsolutePath));
      final List<String> fingerprints = new ArrayList<>();
      for (final File tsFile : tsfiles) {
        fingerprints.add(buildFileFingerprint(tsFile));
      }
      return new ExportedTsFileSnapshot(fingerprints, true);
    }

    private boolean isReady() {
      return ready;
    }

    private static String buildFileFingerprint(final File file) {
      return file.getAbsolutePath() + ":" + file.length() + ":" + file.lastModified();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ExportedTsFileSnapshot)) {
        return false;
      }
      final ExportedTsFileSnapshot that = (ExportedTsFileSnapshot) obj;
      return ready == that.ready && fingerprints.equals(that.fingerprints);
    }

    @Override
    public int hashCode() {
      int result = fingerprints.hashCode();
      result = 31 * result + Boolean.hashCode(ready);
      return result;
    }
  }

  private static void loadExportedTsFiles(final ITableSession session, final File root)
      throws Exception {
    useDatabase(session, TEST_DATABASE);
    final List<File> tsfiles = new ArrayList<>();
    collectTsFilesRecursively(root, tsfiles);
    Assert.assertFalse(tsfiles.isEmpty());
    tsfiles.sort(Comparator.comparing(File::getAbsolutePath));
    for (File f : tsfiles) {
      session.executeNonQueryStatement(
          String.format(
              "LOAD '%s' WITH ('database-name'='%s')", f.getAbsolutePath(), TEST_DATABASE));
    }
  }

  private static long queryTableRowCount(final ITableSession session, final String table)
      throws Exception {
    useDatabase(session, TEST_DATABASE);
    try (SessionDataSet ds = session.executeQueryStatement("SELECT COUNT(*) FROM " + table)) {
      final SessionDataSet.DataIterator it = ds.iterator();
      Assert.assertTrue(it.next());
      return it.getLong(1);
    }
  }

  private static void collectTsFilesRecursively(File dir, List<File> tsfiles) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File f : files) {
      if (f.isDirectory()) {
        collectTsFilesRecursively(f, tsfiles);
      } else if (f.getName().endsWith(".tsfile")) {
        tsfiles.add(f);
      }
    }
  }

  private static void deleteRecursivelyQuietly(Path dirPath) {
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
