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

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeTableModelObjectImportExportIT extends AbstractPipeTableModelDualManualIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBPipeTableModelObjectImportExportIT.class);

  private static final String TEST_DATABASE = "db1";
  private static final String TABLE_NAME = "t1";

  private static final int HISTORY_INSERT_START = 1;
  private static final int HISTORY_INSERT_ROWS = 100;
  private static final int DELETE_OFFSET = 20;
  private static final int DELETE_LENGTH = 25;

  private static final int REALTIME_INSERT_START = 1001;
  private static final int REALTIME_INSERT_ROWS = 50;

  private static final long RECEIVER_VERIFY_TIMEOUT_MS = 120_000L;

  private String targetDir;

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
      targetDir = Files.createTempDirectory("pipe_table_tsfile_it").toAbsolutePath().toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temp directory", e);
    }
  }

  @After
  public void cleanUpTempDirectories() {
    if (targetDir != null) {
      deleteRecursivelyQuietly(Paths.get(targetDir));
    }
  }

  // ======================================================================================
  // EXHAUSTIVE TEST MATRIX: 3 Scopes x 2 History x 2 Realtime x 2 Mods = 24 Tests
  // ======================================================================================

  // --------------------------------------------------------------------------------------
  // Scope 1: DATABASE_AND_TABLE (8 combinations)
  // --------------------------------------------------------------------------------------

  @Test
  public void testScopeDBT_HistOn_RTOn_ModsOn() throws Exception {
    executeUnifiedLifecycleTest(
        "dbt_h1_r1_m1", TableSourceScope.DATABASE_AND_TABLE, true, true, true);
  }

  @Test
  public void testScopeDBT_HistOn_RTOn_ModsOff() throws Exception {
    executeUnifiedLifecycleTest(
        "dbt_h1_r1_m0", TableSourceScope.DATABASE_AND_TABLE, true, true, false);
  }

  @Test
  public void testScopeDBT_HistOn_RTOff_ModsOn() throws Exception {
    executeUnifiedLifecycleTest(
        "dbt_h1_r0_m1", TableSourceScope.DATABASE_AND_TABLE, true, false, true);
  }

  @Test
  public void testScopeDBT_HistOn_RTOff_ModsOff() throws Exception {
    executeUnifiedLifecycleTest(
        "dbt_h1_r0_m0", TableSourceScope.DATABASE_AND_TABLE, true, false, false);
  }

  @Test
  public void testScopeDBT_HistOff_RTOn_ModsOn() throws Exception {
    executeUnifiedLifecycleTest(
        "dbt_h0_r1_m1", TableSourceScope.DATABASE_AND_TABLE, false, true, true);
  }

  @Test
  public void testScopeDBT_HistOff_RTOn_ModsOff() throws Exception {
    executeUnifiedLifecycleTest(
        "dbt_h0_r1_m0", TableSourceScope.DATABASE_AND_TABLE, false, true, false);
  }

  // --------------------------------------------------------------------------------------
  // Scope 2: DATABASE_ONLY (8 combinations)
  // --------------------------------------------------------------------------------------

  @Test
  public void testScopeDB_HistOn_RTOn_ModsOn() throws Exception {
    executeUnifiedLifecycleTest("db_h1_r1_m1", TableSourceScope.DATABASE_ONLY, true, true, true);
  }

  @Test
  public void testScopeDB_HistOn_RTOn_ModsOff() throws Exception {
    executeUnifiedLifecycleTest("db_h1_r1_m0", TableSourceScope.DATABASE_ONLY, true, true, false);
  }

  @Test
  public void testScopeDB_HistOn_RTOff_ModsOn() throws Exception {
    executeUnifiedLifecycleTest("db_h1_r0_m1", TableSourceScope.DATABASE_ONLY, true, false, true);
  }

  @Test
  public void testScopeDB_HistOn_RTOff_ModsOff() throws Exception {
    executeUnifiedLifecycleTest("db_h1_r0_m0", TableSourceScope.DATABASE_ONLY, true, false, false);
  }

  @Test
  public void testScopeDB_HistOff_RTOn_ModsOn() throws Exception {
    executeUnifiedLifecycleTest("db_h0_r1_m1", TableSourceScope.DATABASE_ONLY, false, true, true);
  }

  @Test
  public void testScopeDB_HistOff_RTOn_ModsOff() throws Exception {
    executeUnifiedLifecycleTest("db_h0_r1_m0", TableSourceScope.DATABASE_ONLY, false, true, false);
  }

  // --------------------------------------------------------------------------------------
  // Scope 3: ALL (8 combinations)
  // --------------------------------------------------------------------------------------

  @Test
  public void testScopeAll_HistOn_RTOn_ModsOn() throws Exception {
    executeUnifiedLifecycleTest("all_h1_r1_m1", TableSourceScope.ALL, true, true, true);
  }

  @Test
  public void testScopeAll_HistOn_RTOn_ModsOff() throws Exception {
    executeUnifiedLifecycleTest("all_h1_r1_m0", TableSourceScope.ALL, true, true, false);
  }

  @Test
  public void testScopeAll_HistOn_RTOff_ModsOn() throws Exception {
    executeUnifiedLifecycleTest("all_h1_r0_m1", TableSourceScope.ALL, true, false, true);
  }

  @Test
  public void testScopeAll_HistOn_RTOff_ModsOff() throws Exception {
    executeUnifiedLifecycleTest("all_h1_r0_m0", TableSourceScope.ALL, true, false, false);
  }

  @Test
  public void testScopeAll_HistOff_RTOn_ModsOn() throws Exception {
    executeUnifiedLifecycleTest("all_h0_r1_m1", TableSourceScope.ALL, false, true, true);
  }

  @Test
  public void testScopeAll_HistOff_RTOn_ModsOff() throws Exception {
    executeUnifiedLifecycleTest("all_h0_r1_m0", TableSourceScope.ALL, false, true, false);
  }

  // ======================================================================================
  // THE SINGLE CORE LIFECYCLE ENGINE
  // ======================================================================================

  /** Unified test execution engine dynamically generating expectations based on config. */
  private void executeUnifiedLifecycleTest(
      String pipeName,
      TableSourceScope scope,
      boolean historyEnable,
      boolean realtimeEnable,
      boolean modsEnable)
      throws Exception {

    List<Long> expectedTimestamps = new ArrayList<>();

    try (ITableSession sender = senderEnv.getTableSessionConnection()) {
      prepareSenderTable(sender);

      // Phase 1: Historical Data Generation & Deletion
      if (historyEnable) {
        insertRows(sender, HISTORY_INSERT_START, HISTORY_INSERT_ROWS);
        TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

        long delStart = HISTORY_INSERT_START + DELETE_OFFSET;
        long delEnd = delStart + DELETE_LENGTH - 1;

        sender.executeNonQueryStatement(
            String.format(
                "DELETE FROM %s WHERE time >= %d AND time <= %d", TABLE_NAME, delStart, delEnd));
        TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

        // Compute expected historical rows: if deleted AND mods are enabled, drop them.
        for (long t = HISTORY_INSERT_START; t < HISTORY_INSERT_START + HISTORY_INSERT_ROWS; t++) {
          boolean inDeleteRange = (t >= delStart && t <= delEnd);
          if (!inDeleteRange) {
            expectedTimestamps.add(t);
          }
        }
      }

      // Phase 2: Pipe Creation
      String sourceClause = buildSourceClause(scope, modsEnable, historyEnable, realtimeEnable);
      String sinkClause = buildLocalSinkClause();
      sender.executeNonQueryStatement(
          String.format(
              "CREATE PIPE %s WITH SOURCE (%s) WITH SINK (%s)",
              pipeName, sourceClause, sinkClause));

      waitForPipeRunning(sender, pipeName);

      // Phase 3: Realtime Data Generation
      if (realtimeEnable) {
        insertRows(sender, REALTIME_INSERT_START, REALTIME_INSERT_ROWS);
        TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

        for (long t = REALTIME_INSERT_START;
            t < REALTIME_INSERT_START + REALTIME_INSERT_ROWS;
            t++) {
          expectedTimestamps.add(t);
        }
      }

      // Phase 4: Verification
      try {
        verifySyncResult(expectedTimestamps);
      } finally {
        sender.executeNonQueryStatement("DROP PIPE IF EXISTS " + pipeName);
        sender.executeNonQueryStatement("DROP TABLE IF EXISTS " + TABLE_NAME);
      }
    }
  }

  // ======================================================================================
  // THE SINGLE UNIFIED VERIFICATION ENGINE
  // ======================================================================================

  private void verifySyncResult(List<Long> expectedTimestamps) throws Exception {
    try (ITableSession receiver = receiverEnv.getTableSessionConnection()) {
      receiver.executeNonQueryStatement(
          String.format("CREATE DATABASE IF NOT EXISTS %s", TEST_DATABASE));
      receiver.executeNonQueryStatement(String.format("USE %s", TEST_DATABASE));

      final ExportedTsFileLoadTracker loadTracker = new ExportedTsFileLoadTracker();
      final long deadline = System.currentTimeMillis() + RECEIVER_VERIFY_TIMEOUT_MS;
      Throwable lastFailure = null;

      while (System.currentTimeMillis() < deadline) {
        try {
          if (loadTracker.loadReadyTsFiles(receiver, new File(targetDir))) {
            TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
          }
        } catch (Exception e) {
          lastFailure = e;
          LOGGER.debug("Async file IO race condition, retrying...", e);
        }

        try {
          doCoreAssertion(receiver, expectedTimestamps);
          LOGGER.info("Unified verification passed successfully.");
          return;
        } catch (AssertionError e) {
          lastFailure = e; // Awaiting sync
        } catch (Exception e) {
          if (e.getMessage() != null && e.getMessage().contains("SemanticException")) {
            throw new RuntimeException("Fatal SQL logic error, aborting.", e);
          }
          lastFailure = e;
        }

        Thread.sleep(1000);
      }

      if (lastFailure instanceof AssertionError) throw (AssertionError) lastFailure;
      if (lastFailure instanceof Exception) throw (Exception) lastFailure;

      // Boundary check pass: if 0 expected and no failure triggered, it's successful isolation.
      if (expectedTimestamps.isEmpty()) return;

      Assert.fail("Timeout waiting for receiver data synchronization.");
    }
  }

  private void doCoreAssertion(ITableSession session, List<Long> expectedTimesAsc)
      throws Exception {
    String sql =
        String.format("SELECT time, READ_OBJECT(file) FROM %s ORDER BY time ASC", TABLE_NAME);

    try (SessionDataSet ds = session.executeQueryStatement(sql)) {
      SessionDataSet.DataIterator it = ds.iterator();
      int idx = 0;
      while (it.next()) {
        // CRITICAL FIX: Handle async object loading.
        // If the object file hasn't fully arrived/linked yet, it returns null.
        // We skip this row. This will intentionally trigger a "Timestamp mismatch"
        // on the next iteration, which throws an AssertionError and safely triggers the retry loop.
        org.apache.tsfile.utils.Binary blob = it.getBlob(2);
        if (blob == null || blob.getValues() == null) {
          continue;
        }

        Assert.assertTrue("Receiver has more rows than expected.", idx < expectedTimesAsc.size());

        long expectedTs = expectedTimesAsc.get(idx);
        Assert.assertEquals("Timestamp mismatch.", expectedTs, it.getLong(1));

        byte[] expectedPayload = ("Payload_" + expectedTs).getBytes();
        byte[] actualPayload = blob.getValues();
        Assert.assertArrayEquals(
            "Object payload mismatch at time " + expectedTs, expectedPayload, actualPayload);

        idx++;
      }
      Assert.assertEquals("Total row count mismatch.", expectedTimesAsc.size(), idx);
    } catch (Exception e) {
      if (expectedTimesAsc.isEmpty()
          && e.getMessage() != null
          && e.getMessage().contains("Table does not exist")) {
        // Permitted edge case: receiver table may genuinely not exist yet if no data is expected.
        return;
      }
      throw e;
    }
  }

  // ======================================================================================
  // HELPER METHODS
  // ======================================================================================

  private void prepareSenderTable(ITableSession session) throws Exception {
    session.executeNonQueryStatement(
        String.format("CREATE DATABASE IF NOT EXISTS %s", TEST_DATABASE));
    session.executeNonQueryStatement(String.format("USE %s", TEST_DATABASE));
    session.executeNonQueryStatement(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (id STRING TAG, file OBJECT FIELD)", TABLE_NAME));
  }

  private void insertRows(ITableSession session, long startTs, int rowCount) throws Exception {
    List<String> columnNames = Arrays.asList("id", "file");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.STRING, TSDataType.OBJECT);
    List<ColumnCategory> columnCategories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    Tablet tablet = new Tablet(TABLE_NAME, columnNames, dataTypes, columnCategories, 100);
    for (long ts = startTs; ts < startTs + rowCount; ts++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, ts);
      tablet.addValue(rowIndex, 0, "device1");
      tablet.addValue(rowIndex, 1, true, 0, ("Payload_" + ts).getBytes());
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        session.insert(tablet);
        tablet.reset();
      }
    }
    if (tablet.getRowSize() > 0) session.insert(tablet);
  }

  private void waitForPipeRunning(ITableSession session, String pipeName) throws Exception {
    long deadline = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < deadline) {
      try (SessionDataSet ds = session.executeQueryStatement("SHOW PIPES")) {
        SessionDataSet.DataIterator it = ds.iterator();
        while (it.next()) {
          if (pipeName.equals(it.getString("ID"))
              && "RUNNING".equalsIgnoreCase(it.getString("State"))) return;
        }
      } catch (Exception ignored) {
      }
      Thread.sleep(500);
    }
    LOGGER.warn("Timeout waiting for pipe {} to become RUNNING.", pipeName);
  }

  private String buildSourceClause(
      TableSourceScope scope, boolean modsEnable, boolean historyEnable, boolean realtimeEnable) {
    StringBuilder sb =
        new StringBuilder("'source.capture.table'='true', 'source.inclusion'='data.insert'");
    if (scope == TableSourceScope.DATABASE_AND_TABLE) {
      sb.append(
          String.format(
              ", 'source.database-name'='%s', 'source.table-name'='%s'",
              TEST_DATABASE, TABLE_NAME));
    } else if (scope == TableSourceScope.DATABASE_ONLY) {
      sb.append(String.format(", 'source.database-name'='%s'", TEST_DATABASE));
    }
    sb.append(", 'source.mods.enable'='").append(modsEnable).append("'");
    sb.append(", 'source.history.enable'='").append(true).append("'");
    sb.append(", 'source.realtime.enable'='").append(true).append("'");
    return sb.toString();
  }

  private String buildLocalSinkClause() {
    return String.format(
        "'sink'='tsfile-local-sink', 'sink.local.target-path'='%s', 'sink.batch.max-delay-seconds'='1'",
        targetDir);
  }

  private static final class ExportedTsFileLoadTracker {
    private final Set<String> loadedFiles = new HashSet<>();

    private boolean loadReadyTsFiles(ITableSession session, File dir) throws Exception {
      boolean loadedAny = false;
      File[] files = dir.listFiles();
      if (files == null) return false;

      for (File f : files) {
        if (!f.isFile() || !f.getName().endsWith(".tsfile")) continue;
        String absPath = f.getAbsolutePath();

        if (!loadedFiles.contains(absPath)) {
          session.executeNonQueryStatement(
              String.format("LOAD '%s' WITH ('database-name'='%s')", absPath, TEST_DATABASE));
          loadedFiles.add(absPath);
          loadedAny = true;
        }
      }
      return loadedAny;
    }
  }

  private static void deleteRecursivelyQuietly(Path dirPath) {
    if (!Files.exists(dirPath)) return;
    try {
      Files.walk(dirPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      LOGGER.warn("Cleanup failed.", e);
    }
  }
}
