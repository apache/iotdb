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

package org.apache.iotdb.db.it;

import org.apache.iotdb.db.it.utils.StandardObjectTableModelTsFileGenerator;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadObjectTableModelTsFileIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBLoadObjectTableModelTsFileIT.class);
  private static final String DATABASE = "test_db";
  private static final String TABLE_NAME = "factory_metrics";

  private static final long HOUR_MS = 3600 * 1000L;
  private static final long DAY_MS = 24L * HOUR_MS;
  private static final long WEEK_MS = 7L * DAY_MS;
  private static final long BASE_TIME = 1600000000000L;

  private File tmpDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load_object_table").toUri());
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    deleteDatabase();
    if (!deleteRecursively(tmpDir)) {
      LOGGER.error("Can not delete tmp dir for loading object tsfile.");
    }
  }

  @Test
  public void testLoadWithinOneWeekNoChunkSplit() throws Exception {
    final File tsFile = new File(tmpDir, "table-model-1-0-0-0.tsfile");

    List<Long> expectedDevice1Times;
    List<Long> expectedDevice2Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      generator.writeDeviceData(TABLE_NAME, "device_01", 1000L, 50000L, 1000L);
      generator.writeDeviceData(TABLE_NAME, "device_02", 1000L, 50000L, 1000L);

      expectedDevice1Times = generateExpectedTimes(1000L, 50000L, 1000L);
      expectedDevice2Times = generateExpectedTimes(1000L, 50000L, 1000L);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedDevice1Times);
      assertDeviceDataContent(session, "device_02", expectedDevice2Times);
    }
  }

  @Test
  public void testLoadSpanningMultipleWeeksWithChunkSplit() throws Exception {
    final File tsFile = new File(tmpDir, "table-model-weekly.tsfile");
    List<Long> expectedDevice1Times, expectedDevice2Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      // Device 1: 6 days (within 1 week) -> No Chunk Split
      generator.writeDeviceData(TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 6 * DAY_MS, DAY_MS);
      // Device 2: 5 weeks (spans > 1 week) -> Trigger Chunk Split
      generator.writeDeviceData(
          TABLE_NAME, "device_02", BASE_TIME, BASE_TIME + 5 * WEEK_MS, WEEK_MS);

      expectedDevice1Times = generateExpectedTimes(BASE_TIME, BASE_TIME + 6 * DAY_MS, DAY_MS);
      expectedDevice2Times = generateExpectedTimes(BASE_TIME, BASE_TIME + 5 * WEEK_MS, WEEK_MS);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedDevice1Times);
      assertDeviceDataContent(session, "device_02", expectedDevice2Times);
    }
  }

  @Test
  public void testLoadMultipleFilesWithinOneWeekNoTSFileSplit() throws Exception {
    final File tsFile1 = new File(tmpDir, "table-model-day1.tsfile");
    final File tsFile2 = new File(tmpDir, "table-model-day3.tsfile");

    List<Long> expectedTimes = new ArrayList<>();

    try (StandardObjectTableModelTsFileGenerator gen1 =
        new StandardObjectTableModelTsFileGenerator(tsFile1)) {
      gen1.writeDeviceData(TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 12 * HOUR_MS, HOUR_MS);
      expectedTimes.addAll(generateExpectedTimes(BASE_TIME, BASE_TIME + 12 * HOUR_MS, HOUR_MS));
    }

    try (StandardObjectTableModelTsFileGenerator gen2 =
        new StandardObjectTableModelTsFileGenerator(tsFile2)) {
      long day3StartTime = BASE_TIME + 2 * DAY_MS;
      gen2.writeDeviceData(
          TABLE_NAME, "device_01", day3StartTime, day3StartTime + 12 * HOUR_MS, HOUR_MS);
      expectedTimes.addAll(
          generateExpectedTimes(day3StartTime, day3StartTime + 12 * HOUR_MS, HOUR_MS));
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile1.getAbsolutePath()));
      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile2.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedTimes);
    }
  }

  @Test
  public void testLoadMultipleDevicesEachWithinOneWeekNoChunkSplit() throws Exception {
    final File tsFile = new File(tmpDir, "table-model-multi-device.tsfile");
    List<Long> expectedD1Times, expectedD2Times, expectedD3Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      generator.writeDeviceData(
          TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 10 * HOUR_MS, HOUR_MS);

      long week2Start = BASE_TIME + WEEK_MS;
      generator.writeDeviceData(
          TABLE_NAME, "device_02", week2Start, week2Start + 10 * HOUR_MS, HOUR_MS);

      long week3Start = BASE_TIME + 2 * WEEK_MS;
      generator.writeDeviceData(
          TABLE_NAME, "device_03", week3Start, week3Start + 10 * HOUR_MS, HOUR_MS);

      expectedD1Times = generateExpectedTimes(BASE_TIME, BASE_TIME + 10 * HOUR_MS, HOUR_MS);
      expectedD2Times = generateExpectedTimes(week2Start, week2Start + 10 * HOUR_MS, HOUR_MS);
      expectedD3Times = generateExpectedTimes(week3Start, week3Start + 10 * HOUR_MS, HOUR_MS);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedD1Times);
      assertDeviceDataContent(session, "device_02", expectedD2Times);
      assertDeviceDataContent(session, "device_03", expectedD3Times);
    }
  }

  @Test
  public void testLoadWithinOneWeekNoChunkSplitWithMods() throws Exception {
    final File tsFile = new File(tmpDir, "table-model-1-0-0-0.tsfile");

    List<Long> expectedDevice1Times;
    List<Long> expectedDevice2Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      generator.writeDeviceData(TABLE_NAME, "device_01", 1000L, 50000L, 1000L);
      generator.writeDeviceData(TABLE_NAME, "device_02", 1000L, 50000L, 1000L);

      generator.generateDeletion(TABLE_NAME, "device_01", 10000L, 20000L);

      expectedDevice1Times = generateExpectedTimes(1000L, 50000L, 1000L, 10000L, 20000L);
      expectedDevice2Times = generateExpectedTimes(1000L, 50000L, 1000L);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedDevice1Times);
      assertDeviceDataContent(session, "device_02", expectedDevice2Times);
    }
  }

  @Test
  public void testLoadSpanningMultipleWeeksWithChunkSplitWithMods() throws Exception {
    final File tsFile = new File(tmpDir, "table-model-weekly.tsfile");
    List<Long> expectedDevice1Times, expectedDevice2Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      generator.writeDeviceData(TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 6 * DAY_MS, DAY_MS);
      generator.writeDeviceData(
          TABLE_NAME, "device_02", BASE_TIME, BASE_TIME + 5 * WEEK_MS, WEEK_MS);

      long delTime = BASE_TIME + 2 * WEEK_MS;
      generator.generateDeletion(TABLE_NAME, "device_02", delTime, delTime);

      expectedDevice1Times = generateExpectedTimes(BASE_TIME, BASE_TIME + 6 * DAY_MS, DAY_MS);
      expectedDevice2Times =
          generateExpectedTimes(BASE_TIME, BASE_TIME + 5 * WEEK_MS, WEEK_MS, delTime, delTime);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedDevice1Times);
      assertDeviceDataContent(session, "device_02", expectedDevice2Times);
    }
  }

  @Test
  public void testLoadMultipleFilesWithinOneWeekNoTsFileSplitWithMods() throws Exception {
    final File tsFile1 = new File(tmpDir, "table-model-day1.tsfile");
    final File tsFile2 = new File(tmpDir, "table-model-day3.tsfile");

    List<Long> expectedTimes = new ArrayList<>();

    try (StandardObjectTableModelTsFileGenerator gen1 =
        new StandardObjectTableModelTsFileGenerator(tsFile1)) {
      gen1.writeDeviceData(TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 12 * HOUR_MS, HOUR_MS);

      gen1.generateDeletion(TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + HOUR_MS);
      expectedTimes.addAll(
          generateExpectedTimes(
              BASE_TIME, BASE_TIME + 12 * HOUR_MS, HOUR_MS, BASE_TIME, BASE_TIME + HOUR_MS));
    }

    try (StandardObjectTableModelTsFileGenerator gen2 =
        new StandardObjectTableModelTsFileGenerator(tsFile2)) {
      long day3StartTime = BASE_TIME + 2 * DAY_MS;
      gen2.writeDeviceData(
          TABLE_NAME, "device_01", day3StartTime, day3StartTime + 12 * HOUR_MS, HOUR_MS);

      long endDelTime = day3StartTime + 2 * HOUR_MS;
      gen2.generateDeletion(TABLE_NAME, "device_01", endDelTime);
      expectedTimes.addAll(
          generateExpectedTimes(
              day3StartTime, day3StartTime + 12 * HOUR_MS, HOUR_MS, Long.MIN_VALUE, endDelTime));
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile1.getAbsolutePath()));
      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile2.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedTimes);
    }
  }

  @Test
  public void testLoadMultipleDevicesEachWithinOneWeekNoChunkSplitWithFullDeletion()
      throws Exception {
    final File tsFile = new File(tmpDir, "table-model-multi-device.tsfile");
    List<Long> expectedD1Times, expectedD2Times, expectedD3Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      generator.writeDeviceData(
          TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 10 * HOUR_MS, HOUR_MS);

      long week2Start = BASE_TIME + WEEK_MS;
      generator.writeDeviceData(
          TABLE_NAME, "device_02", week2Start, week2Start + 10 * HOUR_MS, HOUR_MS);

      long week3Start = BASE_TIME + 2 * WEEK_MS;
      generator.writeDeviceData(
          TABLE_NAME, "device_03", week3Start, week3Start + 10 * HOUR_MS, HOUR_MS);

      generator.generateDeletion(TABLE_NAME, "device_02", Long.MAX_VALUE);

      expectedD1Times = generateExpectedTimes(BASE_TIME, BASE_TIME + 10 * HOUR_MS, HOUR_MS);
      expectedD2Times = new ArrayList<>();
      expectedD3Times = generateExpectedTimes(week3Start, week3Start + 10 * HOUR_MS, HOUR_MS);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedD1Times);
      assertDeviceDataContent(session, "device_02", expectedD2Times);
      assertDeviceDataContent(session, "device_03", expectedD3Times);
    }
  }

  @Test
  public void testLoadSingleDeviceTriggerTsFileToTabletBranch() throws Exception {
    final File tsFile = new File(tmpDir, "table-model-tsfile-to-tablet.tsfile");
    List<Long> expectedDevice1Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      generator.writeDeviceData(
          TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 99 * WEEK_MS, WEEK_MS);

      expectedDevice1Times = generateExpectedTimes(BASE_TIME, BASE_TIME + 99 * WEEK_MS, WEEK_MS);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedDevice1Times);
    }
  }

  @Test
  public void testLoadSingleDeviceTriggerTsFileToTabletBranchWithMods() throws Exception {
    final File tsFile = new File(tmpDir, "table-model-tsfile-to-tablet-mods.tsfile");
    List<Long> expectedDevice1Times;

    try (StandardObjectTableModelTsFileGenerator generator =
        new StandardObjectTableModelTsFileGenerator(tsFile)) {

      generator.writeDeviceData(
          TABLE_NAME, "device_01", BASE_TIME, BASE_TIME + 99 * WEEK_MS, WEEK_MS);

      long delStart = BASE_TIME + 10 * WEEK_MS;
      long delEnd = BASE_TIME + 20 * WEEK_MS;
      generator.generateDeletion(TABLE_NAME, "device_01", delStart, delEnd);

      expectedDevice1Times =
          generateExpectedTimes(BASE_TIME, BASE_TIME + 99 * WEEK_MS, WEEK_MS, delStart, delEnd);
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      initDatabase(session);

      session.executeNonQueryStatement(String.format("LOAD '%s'", tsFile.getAbsolutePath()));

      assertDeviceDataContent(session, "device_01", expectedDevice1Times);
    }
  }

  /** Generates a list of expected times excluding the specified deleted time range. */
  private List<Long> generateExpectedTimes(
      long startTime, long endTime, long interval, long delStart, long delEnd) {
    List<Long> times = new ArrayList<>();
    for (long t = startTime; t <= endTime; t += interval) {
      if (t >= delStart && t <= delEnd) {
        continue; // Exclude deleted points
      }
      times.add(t);
    }
    return times;
  }

  // --- Auxiliary Methods ---

  /**
   * Verifies the loaded object data by fetching native Binary via ITableSession and asserting byte
   * array content directly.
   */
  private void assertDeviceDataContent(
      ITableSession session, String deviceId, List<Long> expectedTimes) throws Exception {
    String query =
        String.format(
            "SELECT time, READ_OBJECT(sensor_obj) FROM %s WHERE id='%s' ", TABLE_NAME, deviceId);

    try (SessionDataSet dataSet = session.executeQueryStatement(query)) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      int count = 0;

      while (iterator.next()) {
        Assert.assertTrue(
            "More data returned than expected for " + deviceId, count < expectedTimes.size());

        // Use 1-based index (1: time, 2: READ_OBJECT(sensor_obj))
        long actualTime = iterator.getLong(1);
        Binary binary = iterator.getBlob(2);
        byte[] actualBytes = binary.getValues();
        long expectedTime = expectedTimes.get(count);

        Assert.assertEquals("Time mismatch at index " + count, expectedTime, actualTime);

        // Dynamically compute the expected content matching the Generator
        byte[] expectedBytes =
            String.format(
                    "AutoGenerated|Table=%s|ID=%s|Time=%d", TABLE_NAME, deviceId, expectedTime)
                .getBytes(StandardCharsets.UTF_8);

        Assert.assertArrayEquals(
            "Object byte content mismatch at time " + actualTime, expectedBytes, actualBytes);

        count++;
      }
      Assert.assertEquals("Total row count mismatch for " + deviceId, expectedTimes.size(), count);
    }
  }

  private List<Long> generateExpectedTimes(long startTime, long endTime, long interval) {
    List<Long> times = new ArrayList<>();
    for (long t = startTime; t <= endTime; t += interval) {
      times.add(t);
    }
    return times;
  }

  private void initDatabase(ITableSession session) throws Exception {
    session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + DATABASE);
    session.executeNonQueryStatement("USE \"" + DATABASE + "\"");
  }

  private void deleteDatabase() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("DROP DATABASE " + DATABASE);
    } catch (final Exception ignored) {
    }
  }

  private boolean deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return true;
    }
    if (file.isDirectory()) {
      final File[] children = file.listFiles();
      if (children != null) {
        for (final File child : children) {
          if (!deleteRecursively(child)) {
            return false;
          }
        }
      }
    }
    return file.delete();
  }
}
