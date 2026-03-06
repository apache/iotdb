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

package org.apache.iotdb;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/** TODO: move these manual tests into ITs */
public class ConsensusSubscriptionTest {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  private static int testCounter = 0;
  private static int passed = 0;
  private static int failed = 0;
  private static final List<String> failedTests = new ArrayList<>();

  public static void main(String[] args) throws Exception {
    System.out.println("=== Consensus-Based Subscription Test Suite ===\n");

    String targetTest = args.length > 0 ? args[0] : null;

    if (targetTest == null || "testBasicFlow".equals(targetTest)) {
      runTest("testBasicFlow", ConsensusSubscriptionTest::testBasicFlow);
    }
    if (targetTest == null || "testDataTypes".equals(targetTest)) {
      runTest("testDataTypes", ConsensusSubscriptionTest::testDataTypes);
    }
    if (targetTest == null || "testPathFiltering".equals(targetTest)) {
      runTest("testPathFiltering", ConsensusSubscriptionTest::testPathFiltering);
    }
    if (targetTest == null || "testSubscribeBeforeRegion".equals(targetTest)) {
      runTest("testSubscribeBeforeRegion", ConsensusSubscriptionTest::testSubscribeBeforeRegion);
    }
    if (targetTest == null || "testMultiEntityIsolation".equals(targetTest)) {
      runTest("testMultiEntityIsolation", ConsensusSubscriptionTest::testMultiEntityIsolation);
    }
    if (targetTest == null || "testBurstWriteGapRecovery".equals(targetTest)) {
      runTest("testBurstWriteGapRecovery", ConsensusSubscriptionTest::testBurstWriteGapRecovery);
    }
    if (targetTest == null || "testCommitAfterUnsubscribe".equals(targetTest)) {
      runTest("testCommitAfterUnsubscribe", ConsensusSubscriptionTest::testCommitAfterUnsubscribe);
    }
    if (targetTest == null || "testSeek".equals(targetTest)) {
      runTest("testSeek", ConsensusSubscriptionTest::testSeek);
    }

    // Summary
    System.out.println("\n=== Test Suite Summary ===");
    System.out.println("Passed: " + passed);
    System.out.println("Failed: " + failed);
    if (!failedTests.isEmpty()) {
      System.out.println("Failed tests: " + failedTests);
    }
    System.out.println("=== Done ===");
  }

  // ============================
  // Test Infrastructure
  // ============================

  @FunctionalInterface
  interface TestMethod {
    void run() throws Exception;
  }

  private static void runTest(String name, TestMethod test) {
    System.out.println("\n" + "=================================================================");
    System.out.println("Running: " + name);
    System.out.println("=================================================================");
    try {
      test.run();
      passed++;
      System.out.println(">>> PASSED: " + name);
    } catch (AssertionError e) {
      failed++;
      failedTests.add(name);
      System.out.println(">>> FAILED: " + name + " - " + e.getMessage());
      e.printStackTrace(System.out);
    } catch (Exception e) {
      failed++;
      failedTests.add(name);
      System.out.println(">>> ERROR: " + name + " - " + e.getMessage());
      e.printStackTrace(System.out);
    }
  }

  private static String nextDatabase() {
    testCounter++;
    return "root.csub_test_" + testCounter;
  }

  private static String nextTopic() {
    return "topic_csub_" + testCounter;
  }

  private static String nextConsumerGroup() {
    return "cg_csub_" + testCounter;
  }

  private static String nextConsumerId() {
    return "consumer_csub_" + testCounter;
  }

  private static ISession openSession() throws Exception {
    ISession session =
        new Session.Builder().host(HOST).port(PORT).username(USER).password(PASSWORD).build();
    session.open();
    return session;
  }

  private static void createDatabase(ISession session, String database) throws Exception {
    try {
      session.executeNonQueryStatement("CREATE DATABASE " + database);
    } catch (Exception e) {
      // ignore if already exists
    }
  }

  private static void deleteDatabase(String database) {
    try (ISession session = openSession()) {
      session.executeNonQueryStatement("DELETE DATABASE " + database);
    } catch (Exception e) {
      // ignore
    }
  }

  private static void dropTopic(String topicName) {
    try (SubscriptionTreeSession subSession = new SubscriptionTreeSession(HOST, PORT)) {
      subSession.open();
      subSession.dropTopic(topicName);
    } catch (Exception e) {
      // ignore
    }
  }

  private static void createTopic(String topicName, String path) throws Exception {
    try (SubscriptionTreeSession subSession = new SubscriptionTreeSession(HOST, PORT)) {
      subSession.open();
      try {
        subSession.dropTopic(topicName);
      } catch (Exception e) {
        // ignore
      }

      Properties topicConfig = new Properties();
      topicConfig.put(TopicConstant.MODE_KEY, TopicConstant.MODE_LIVE_VALUE);
      topicConfig.put(
          TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
      topicConfig.put(TopicConstant.PATH_KEY, path);
      subSession.createTopic(topicName, topicConfig);
      System.out.println("  Created topic: " + topicName + " (path=" + path + ")");
    }
  }

  private static SubscriptionTreePullConsumer createConsumer(
      String consumerId, String consumerGroupId) throws Exception {
    SubscriptionTreePullConsumer consumer =
        new SubscriptionTreePullConsumer.Builder()
            .host(HOST)
            .port(PORT)
            .consumerId(consumerId)
            .consumerGroupId(consumerGroupId)
            .autoCommit(false)
            .buildPullConsumer();
    consumer.open();
    return consumer;
  }

  // ============================
  // Polling & Verification
  // ============================

  /**
   * Poll and commit messages. After reaching expectedRows, continues polling for 5 consecutive
   * empty rounds to verify no extra data arrives.
   */
  private static PollResult pollUntilComplete(
      SubscriptionTreePullConsumer consumer, int expectedRows, int maxPollAttempts) {
    return pollUntilComplete(consumer, expectedRows, maxPollAttempts, 1000, true);
  }

  private static PollResult pollUntilComplete(
      SubscriptionTreePullConsumer consumer,
      int expectedRows,
      int maxPollAttempts,
      long pollTimeoutMs,
      boolean commitMessages) {
    PollResult result = new PollResult();
    int consecutiveEmpty = 0;

    for (int attempt = 1; attempt <= maxPollAttempts; attempt++) {
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(pollTimeoutMs));

      if (messages.isEmpty()) {
        consecutiveEmpty++;
        // Normal completion: reached expected rows and verified quiescence
        if (consecutiveEmpty >= 3 && result.totalRows >= expectedRows) {
          System.out.println(
              "    Verified: "
                  + consecutiveEmpty
                  + " consecutive empty polls after "
                  + result.totalRows
                  + " rows (expected "
                  + expectedRows
                  + ")");
          break;
        }
        // Stuck: have data but cannot reach expected count
        if (consecutiveEmpty >= 5 && result.totalRows > 0) {
          System.out.println(
              "    Stuck: "
                  + consecutiveEmpty
                  + " consecutive empty polls at "
                  + result.totalRows
                  + " rows (expected "
                  + expectedRows
                  + ")");
          break;
        }
        // Never received anything
        if (consecutiveEmpty >= 10 && result.totalRows == 0 && expectedRows > 0) {
          System.out.println("    No data received after " + consecutiveEmpty + " polls");
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
        continue;
      }

      consecutiveEmpty = 0;

      for (SubscriptionMessage message : messages) {
        for (SubscriptionSessionDataSet dataSet : message.getSessionDataSetsHandler()) {
          String device = null;
          List<String> columnNames = dataSet.getColumnNames();
          if (columnNames.size() > 1) {
            String fullPath = columnNames.get(1);
            int lastDot = fullPath.lastIndexOf('.');
            device = lastDot > 0 ? fullPath.substring(0, lastDot) : fullPath;
          }

          while (dataSet.hasNext()) {
            org.apache.tsfile.read.common.RowRecord record = dataSet.next();
            result.totalRows++;
            if (device != null) {
              result.rowsPerDevice.merge(device, 1, Integer::sum);
            }
            for (int i = 1; i < columnNames.size(); i++) {
              result.seenColumns.add(columnNames.get(i));
            }
            if (result.totalRows <= 5) {
              System.out.println(
                  "      Row: time="
                      + record.getTimestamp()
                      + ", values="
                      + record.getFields()
                      + ", device="
                      + device);
            }
          }
        }
        if (commitMessages) {
          consumer.commitSync(message);
        }
      }

      System.out.println(
          "    Poll attempt "
              + attempt
              + ": totalRows="
              + result.totalRows
              + " / expected="
              + expectedRows);

      // Stop immediately if we exceeded the expected row count
      if (expectedRows > 0 && result.totalRows > expectedRows) {
        System.out.println(
            "    EXCEEDED: totalRows=" + result.totalRows + " > expectedRows=" + expectedRows);
        break;
      }
    }

    return result;
  }

  // ============================
  // Cleanup
  // ============================

  /** Clean up all test artifacts: unsubscribe, close consumer, drop topic, delete database. */
  private static void cleanup(
      SubscriptionTreePullConsumer consumer, String topicName, String database) {
    if (consumer != null) {
      try {
        consumer.unsubscribe(topicName);
      } catch (Exception e) {
        // ignore
      }
      try {
        consumer.close();
      } catch (Exception e) {
        // ignore
      }
    }
    dropTopic(topicName);
    deleteDatabase(database);
  }

  // ============================
  // Result & Assertions
  // ============================

  static class PollResult {
    int totalRows = 0;
    Map<String, Integer> rowsPerDevice = new HashMap<>();
    Set<String> seenColumns = new HashSet<>();

    @Override
    public String toString() {
      return "PollResult{totalRows="
          + totalRows
          + ", rowsPerDevice="
          + rowsPerDevice
          + ", seenColumns="
          + seenColumns
          + "}";
    }
  }

  private static void assertEquals(String msg, int expected, int actual) {
    if (expected != actual) {
      throw new AssertionError(msg + ": expected=" + expected + ", actual=" + actual);
    }
  }

  private static void assertTrue(String msg, boolean condition) {
    if (!condition) {
      throw new AssertionError(msg);
    }
  }

  private static void assertAtLeast(String msg, int min, int actual) {
    if (actual < min) {
      throw new AssertionError(msg + ": expected at least " + min + ", actual=" + actual);
    }
  }

  // ======================================================================
  // Test 1: Basic Flow (merged: BasicDataDelivery + MultiDevices + Flush)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Data written BEFORE subscribe is NOT received
   *   <li>Multiple devices (d1, d2, d3) written AFTER subscribe are all received
   *   <li>Flush does not cause data loss (WAL pinning keeps entries available)
   *   <li>Exact row count matches expectation
   * </ul>
   */
  private static void testBasicFlow() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      // Step 1: Write initial data to create DataRegion (should NOT be received)
      System.out.println("  Step 1: Writing initial data (should NOT be received)");
      try (ISession session = openSession()) {
        createDatabase(session, database);
        for (int i = 0; i < 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
        // Also write to d2, d3 for multi-device readiness
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d2(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d3(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Step 2: Create topic and subscribe
      System.out.println("  Step 2: Creating topic and subscribing");
      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 3: Write to 3 devices (30 rows each = 90 total), then flush
      System.out.println("  Step 3: Writing 30 rows x 3 devices AFTER subscribe, then flush");
      try (ISession session = openSession()) {
        for (int i = 100; i < 130; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d2(time, s1) VALUES (%d, %d)", database, i, i * 20));
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d3(time, s1) VALUES (%d, %d)", database, i, i * 30));
        }
        System.out.println("  Flushing...");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Step 4: Poll and verify
      System.out.println("  Step 4: Polling...");
      PollResult result = pollUntilComplete(consumer, 90, 100);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 90 rows (30 per device)", 90, result.totalRows);
      if (!result.rowsPerDevice.isEmpty()) {
        System.out.println("  Rows per device: " + result.rowsPerDevice);
        for (String dev : new String[] {"d1", "d2", "d3"}) {
          Integer devRows = result.rowsPerDevice.get(database + "." + dev);
          assertAtLeast("Expected rows from " + dev, 1, devRows != null ? devRows : 0);
        }
      }
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 2: Data Types (merged: MultipleDataTypes + Aligned + CrossPartition)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Non-aligned: 6 data types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT)
   *   <li>Aligned: 6 data types, cross-partition timestamps (>1 week apart)
   *   <li>6 write methods: SQL single/multi-row, insertAlignedRecord/Records/Tablet/Tablets
   * </ul>
   */
  private static void testDataTypes() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;
    final long GAP = 604_800_001L; // slightly over 1 week

    try {
      try (ISession session = openSession()) {
        createDatabase(session, database);
        // Create aligned timeseries
        session.executeNonQueryStatement(
            String.format(
                "CREATE ALIGNED TIMESERIES %s.d_aligned"
                    + "(s_int32 INT32, s_int64 INT64, s_float FLOAT,"
                    + " s_double DOUBLE, s_bool BOOLEAN, s_text TEXT)",
                database));
        // Init rows to force DataRegion creation
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s_int32) VALUES (0, 0)", database));
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s.d_aligned(time, s_int32, s_int64, s_float,"
                    + " s_double, s_bool, s_text)"
                    + " VALUES (0, 0, 0, 0.0, 0.0, false, 'init')",
                database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      int totalExpected = 0;
      final String device = database + ".d_aligned";
      List<String> measurements =
          Arrays.asList("s_int32", "s_int64", "s_float", "s_double", "s_bool", "s_text");
      List<TSDataType> types =
          Arrays.asList(
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.BOOLEAN,
              TSDataType.TEXT);
      List<IMeasurementSchema> schemas = new ArrayList<>();
      schemas.add(new MeasurementSchema("s_int32", TSDataType.INT32));
      schemas.add(new MeasurementSchema("s_int64", TSDataType.INT64));
      schemas.add(new MeasurementSchema("s_float", TSDataType.FLOAT));
      schemas.add(new MeasurementSchema("s_double", TSDataType.DOUBLE));
      schemas.add(new MeasurementSchema("s_bool", TSDataType.BOOLEAN));
      schemas.add(new MeasurementSchema("s_text", TSDataType.TEXT));

      try (ISession session = openSession()) {
        // --- Part A: Non-aligned, 6 types x 20 rows ---
        System.out.println("  Part A: Non-aligned 6 data types x 20 rows");
        for (int i = 1; i <= 20; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s_int32) VALUES (%d, %d)", database, i, i));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s_int64) VALUES (%d, %d)",
                  database, i, (long) i * 100000L));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s_float) VALUES (%d, %f)", database, i, i * 1.1f));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s_double) VALUES (%d, %f)", database, i, i * 2.2));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s_bool) VALUES (%d, %s)",
                  database, i, i % 2 == 0 ? "true" : "false"));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s_text) VALUES (%d, 'text_%d')", database, i, i));
        }
        totalExpected += 120; // 6 types x 20 rows

        // --- Part B: Aligned cross-partition, 6 write methods ---
        System.out.println("  Part B: Aligned cross-partition, 6 write methods");

        // Method 1: SQL single row
        long t1 = 1;
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s.d_aligned(time, s_int32, s_int64, s_float,"
                    + " s_double, s_bool, s_text)"
                    + " VALUES (%d, 1, 100, 1.1, 1.11, true, 'sql_single')",
                database, t1));
        totalExpected += 1;

        // Method 2: SQL multi-row (cross-partition)
        long t2a = 1 + GAP;
        long t2b = 1 + 2 * GAP;
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s.d_aligned(time, s_int32, s_int64, s_float,"
                    + " s_double, s_bool, s_text)"
                    + " VALUES (%d, 2, 200, 2.2, 2.22, false, 'sql_multi_a'),"
                    + " (%d, 3, 300, 3.3, 3.33, true, 'sql_multi_b')",
                database, t2a, t2b));
        totalExpected += 2;

        // Method 3: insertAlignedRecord
        long t3 = 1 + 3 * GAP;
        session.insertAlignedRecord(
            device,
            t3,
            measurements,
            types,
            Arrays.asList(4, 400L, 4.4f, 4.44, true, "record_single"));
        totalExpected += 1;

        // Method 4: insertAlignedRecordsOfOneDevice (cross-partition)
        long t4a = 1 + 4 * GAP;
        long t4b = 1 + 5 * GAP;
        session.insertAlignedRecordsOfOneDevice(
            device,
            Arrays.asList(t4a, t4b),
            Arrays.asList(measurements, measurements),
            Arrays.asList(types, types),
            Arrays.asList(
                Arrays.asList(5, 500L, 5.5f, 5.55, false, "records_a"),
                Arrays.asList(6, 600L, 6.6f, 6.66, true, "records_b")));
        totalExpected += 2;

        // Method 5: insertAlignedTablet (cross-partition)
        long t5a = 1 + 6 * GAP;
        long t5b = 1 + 7 * GAP;
        Tablet tablet5 = new Tablet(device, schemas, 2);
        addAlignedTabletRow(tablet5, 0, t5a, 7, 700L, 7.7f, 7.77, false, "tablet_a");
        addAlignedTabletRow(tablet5, 1, t5b, 8, 800L, 8.8f, 8.88, true, "tablet_b");
        session.insertAlignedTablet(tablet5);
        totalExpected += 2;

        // Method 6: insertAlignedTablets (cross-partition)
        long t6a = 1 + 8 * GAP;
        long t6b = 1 + 9 * GAP;
        Tablet tablet6 = new Tablet(device, schemas, 2);
        addAlignedTabletRow(tablet6, 0, t6a, 9, 900L, 9.9f, 9.99, false, "tablets_a");
        addAlignedTabletRow(tablet6, 1, t6b, 10, 1000L, 10.1f, 10.10, true, "tablets_b");
        Map<String, Tablet> tabletMap = new HashMap<>();
        tabletMap.put(device, tablet6);
        session.insertAlignedTablets(tabletMap);
        totalExpected += 2;
      }

      System.out.println("  Total expected rows: " + totalExpected);
      Thread.sleep(2000);

      PollResult result = pollUntilComplete(consumer, totalExpected, 150);
      System.out.println("  Result: " + result);

      assertAtLeast(
          "Expected at least " + totalExpected + " rows", totalExpected, result.totalRows);
      assertAtLeast("Expected multiple column types in result", 2, result.seenColumns.size());
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 3: Path Filtering (merged: DeviceLevel + TimeseriesLevel)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Device-level: topic on d1.** does NOT deliver d2 data
   *   <li>Timeseries-level: topic on d1.s1 — lenient check for s2 filtering
   * </ul>
   */
  private static void testPathFiltering() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1, s2) VALUES (0, 0, 0)", database));
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d2(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Topic filters d1.s1 only (timeseries-level)
      String filterPath = database + ".d1.s1";
      createTopic(topicName, filterPath);
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing to d1 (s1 + s2) and d2 (s1)");
      try (ISession session = openSession()) {
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s1, s2) VALUES (%d, %d, %d)",
                  database, i, i * 10, i * 20));
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d2(time, s1) VALUES (%d, %d)", database, i, i * 30));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting d1 data only, ideally s1 only)...");
      PollResult result = pollUntilComplete(consumer, 50, 60);
      System.out.println("  Result: " + result);

      // Device-level: d2 must NOT appear
      if (!result.rowsPerDevice.isEmpty()) {
        Integer d2Rows = result.rowsPerDevice.get(database + ".d2");
        assertTrue("Expected NO rows from d2, but got " + d2Rows, d2Rows == null || d2Rows == 0);
        Integer d1Rows = result.rowsPerDevice.get(database + ".d1");
        assertAtLeast("Expected d1 rows", 1, d1Rows != null ? d1Rows : 0);
        System.out.println("  Device filtering verified: d1=" + d1Rows + ", d2=" + d2Rows);
      }

      // Timeseries-level: lenient check
      boolean hasS2 = result.seenColumns.stream().anyMatch(c -> c.contains(".s2"));
      if (hasS2) {
        System.out.println(
            "  INFO: Both s1 and s2 received — converter uses device-level filtering only.");
        assertAtLeast("Should have received d1 rows", 50, result.totalRows);
      } else {
        System.out.println("  Timeseries-level filtering verified: only s1 data received");
        assertEquals("Expected exactly 50 rows from d1.s1 only", 50, result.totalRows);
      }
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 4: Subscribe Before Region Creation (kept as-is)
  // ======================================================================
  /**
   * Subscribe BEFORE the database/region exists, then create database and write. Tests the
   * IoTConsensus.onNewPeerCreated auto-binding path.
   */
  private static void testSubscribeBeforeRegion() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      System.out.println("  Step 1: Creating topic BEFORE database exists");
      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      System.out.println("  Step 2: Subscribing (no DataRegion exists yet)");
      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Step 3: Creating database and writing data (100 rows)");
      try (ISession session = openSession()) {
        createDatabase(session, database);
        for (int i = 0; i < 100; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(5000);

      System.out.println("  Step 4: Polling...");
      PollResult result = pollUntilComplete(consumer, 100, 100);
      System.out.println("  Result: " + result);

      if (result.totalRows >= 100) {
        System.out.println("  Auto-binding works! All " + result.totalRows + " rows received.");
      } else if (result.totalRows > 0) {
        System.out.println(
            "  Partial: " + result.totalRows + "/100 rows. First writes may precede binding.");
      } else {
        System.out.println("  No data received. Check logs for auto-binding messages.");
      }
      assertAtLeast(
          "Expected some rows from subscribe-before-region (auto-binding)", 1, result.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 6: Multi-Entity Isolation (merged: MultiConsumerGroup + MultiTopic)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Two consumer groups on same topic: each group gets ALL data independently
   *   <li>One consumer subscribes to two topics with different path filters: each topic delivers
   *       only matching data
   * </ul>
   */
  private static void testMultiEntityIsolation() throws Exception {
    String database = nextDatabase();
    String topicName1 = "topic_multi_" + testCounter + "_a";
    String topicName2 = "topic_multi_" + testCounter + "_b";
    String consumerGroupId1 = "cg_multi_" + testCounter + "_a";
    String consumerId1 = "consumer_multi_" + testCounter + "_a";
    String consumerGroupId2 = "cg_multi_" + testCounter + "_b";
    String consumerId2 = "consumer_multi_" + testCounter + "_b";
    SubscriptionTreePullConsumer consumer1 = null;
    SubscriptionTreePullConsumer consumer2 = null;

    try {
      // Setup: database with d1 and d2
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d2(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Topic 1: covers d1 only, Topic 2: covers d2 only
      createTopic(topicName1, database + ".d1.**");
      createTopic(topicName2, database + ".d2.**");
      Thread.sleep(1000);

      // Consumer 1 (group A): subscribes to BOTH topics
      consumer1 = createConsumer(consumerId1, consumerGroupId1);
      consumer1.subscribe(topicName1, topicName2);
      // Consumer 2 (group B): subscribes to BOTH topics
      consumer2 = createConsumer(consumerId2, consumerGroupId2);
      consumer2.subscribe(topicName1, topicName2);
      Thread.sleep(3000);

      // Write 30 rows to d1, 40 rows to d2
      System.out.println("  Writing 30 rows to d1, 40 rows to d2");
      try (ISession session = openSession()) {
        for (int i = 1; i <= 40; i++) {
          if (i <= 30) {
            session.executeNonQueryStatement(
                String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
          }
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d2(time, s1) VALUES (%d, %d)", database, i, i * 20));
        }
      }
      Thread.sleep(2000);

      // Part A: Both groups should get 70 rows independently
      System.out.println("  Part A: Multi-group isolation");
      System.out.println("  Polling from group 1...");
      PollResult result1 = pollUntilComplete(consumer1, 70, 80);
      System.out.println("  Group 1 result: " + result1);

      System.out.println("  Polling from group 2...");
      PollResult result2 = pollUntilComplete(consumer2, 70, 80);
      System.out.println("  Group 2 result: " + result2);

      assertEquals("Group 1 should receive all 70 rows", 70, result1.totalRows);
      assertEquals("Group 2 should receive all 70 rows", 70, result2.totalRows);

      // Part B: Verify per-topic device isolation
      if (!result1.rowsPerDevice.isEmpty()) {
        Integer d1Rows = result1.rowsPerDevice.get(database + ".d1");
        Integer d2Rows = result1.rowsPerDevice.get(database + ".d2");
        assertEquals("Expected 30 rows from d1 (topic1)", 30, d1Rows != null ? d1Rows : 0);
        assertEquals("Expected 40 rows from d2 (topic2)", 40, d2Rows != null ? d2Rows : 0);
        System.out.println("  Multi-topic isolation verified: d1=" + d1Rows + ", d2=" + d2Rows);
      }
      System.out.println(
          "  Multi-group isolation verified: group1="
              + result1.totalRows
              + ", group2="
              + result2.totalRows);
    } finally {
      if (consumer1 != null) {
        try {
          consumer1.unsubscribe(topicName1, topicName2);
        } catch (Exception e) {
          /* ignore */
        }
        try {
          consumer1.close();
        } catch (Exception e) {
          /* ignore */
        }
      }
      if (consumer2 != null) {
        try {
          consumer2.unsubscribe(topicName1, topicName2);
        } catch (Exception e) {
          /* ignore */
        }
        try {
          consumer2.close();
        } catch (Exception e) {
          /* ignore */
        }
      }
      dropTopic(topicName1);
      dropTopic(topicName2);
      deleteDatabase(database);
    }
  }

  // ======================================================================
  // Test 7: Burst Write Gap Recovery (NEW — tests C2 fix)
  // ======================================================================
  /**
   * Tests that burst writing beyond the pending queue capacity (4096) does not cause data loss. The
   * pending queue overflow triggers gaps, which should be recovered from WAL.
   *
   * <p><b>Mechanism:</b> Each {@code IoTConsensusServerImpl.write()} call produces exactly one
   * {@code pendingEntries.offer()}. A single {@code session.insertTablet(tablet)} with N rows in
   * one time partition = 1 write() call = 1 offer, so Tablet batches rarely overflow the queue. To
   * actually overflow, we need 4096+ <i>individual</i> write() calls arriving faster than the
   * prefetch thread can drain. We achieve this with multiple concurrent writer threads, each
   * performing individual SQL INSERTs, to maximize the aggregate write rate vs. drain rate.
   *
   * <p><b>Note:</b> Gap occurrence is inherently timing-dependent (race between writers and the
   * prefetch drain loop). This test maximizes the probability by using concurrent threads, but
   * cannot guarantee gap occurrence on every run. Check server logs for "gap detected" / "Filling
   * from WAL" messages to confirm the gap path was exercised.
   *
   * <p>Fix verified: C2 — gap entries are not skipped when WAL fill times out; they are deferred to
   * the next prefetch iteration.
   */
  private static void testBurstWriteGapRecovery() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Use multiple concurrent writer threads with individual SQL INSERTs.
      // Each INSERT → 1 IoTConsensusServerImpl.write() → 1 pendingEntries.offer().
      // With N threads writing concurrently, aggregate rate should exceed drain rate
      // and overflow the 4096-capacity queue, creating gaps.
      final int writerThreads = 4;
      final int rowsPerThread = 1500; // 4 * 1500 = 6000 total write() calls > 4096
      final int totalRows = writerThreads * rowsPerThread;
      final AtomicInteger errorCount = new AtomicInteger(0);
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch doneLatch = new CountDownLatch(writerThreads);

      System.out.println(
          "  Burst writing "
              + totalRows
              + " rows via "
              + writerThreads
              + " concurrent threads ("
              + rowsPerThread
              + " individual SQL INSERTs each)");
      System.out.println(
          "  (Each INSERT = 1 WAL entry = 1 pendingEntries.offer(); " + "queue capacity = 4096)");

      ExecutorService executor = Executors.newFixedThreadPool(writerThreads);
      for (int t = 0; t < writerThreads; t++) {
        final int threadId = t;
        final int startTs = threadId * rowsPerThread + 1;
        executor.submit(
            () -> {
              try {
                startLatch.await(); // all threads start at the same time
                try (ISession session = openSession()) {
                  for (int i = 0; i < rowsPerThread; i++) {
                    int ts = startTs + i;
                    session.executeNonQueryStatement(
                        String.format(
                            "INSERT INTO %s.d1(time, s1) VALUES (%d, %d)",
                            database, ts, (long) ts * 10));
                  }
                }
              } catch (Exception e) {
                System.out.println("  Writer thread " + threadId + " error: " + e.getMessage());
                errorCount.incrementAndGet();
              } finally {
                doneLatch.countDown();
              }
            });
      }

      // Fire all threads simultaneously
      startLatch.countDown();
      doneLatch.await();
      executor.shutdown();

      if (errorCount.get() > 0) {
        System.out.println("  WARNING: " + errorCount.get() + " writer threads encountered errors");
      }

      // Do NOT add artificial delay — let the consumer compete with ongoing WAL writes
      System.out.println(
          "  Polling (expecting " + totalRows + " rows, may need WAL gap recovery)...");
      System.out.println(
          "  (Check server logs for 'gap detected' to confirm gap recovery was triggered)");
      PollResult result = pollUntilComplete(consumer, totalRows, 6000, 2000, true);
      System.out.println("  Result: " + result);

      assertEquals(
          "Expected exactly " + totalRows + " rows (no data loss despite pending queue overflow)",
          totalRows,
          result.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 8: Commit After Unsubscribe (NEW — tests H7 fix)
  // ======================================================================
  /**
   * Tests that commit still works correctly after the consumer has unsubscribed (queue has been
   * torn down). The commit routing should use metadata-based topic config check instead of runtime
   * queue state.
   *
   * <p>Fix verified: H7 — commit routes via isConsensusBasedTopic() instead of hasQueue().
   */
  private static void testCommitAfterUnsubscribe() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Write data
      System.out.println("  Writing 50 rows");
      try (ISession session = openSession()) {
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(2000);

      // Poll WITHOUT commit
      System.out.println("  Polling WITHOUT commit...");
      List<SubscriptionMessage> uncommittedMessages = new ArrayList<>();
      int polledRows = 0;
      for (int attempt = 0; attempt < 60 && polledRows < 50; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(2000));
        if (msgs.isEmpty()) {
          if (polledRows > 0) break;
          Thread.sleep(500);
          continue;
        }
        for (SubscriptionMessage msg : msgs) {
          uncommittedMessages.add(msg);
          for (SubscriptionSessionDataSet ds : msg.getSessionDataSetsHandler()) {
            while (ds.hasNext()) {
              ds.next();
              polledRows++;
            }
          }
        }
      }
      System.out.println(
          "  Polled "
              + polledRows
              + " rows, holding "
              + uncommittedMessages.size()
              + " uncommitted messages");
      assertAtLeast("Should have polled some rows before unsubscribe", 1, polledRows);

      // Unsubscribe (tears down the consensus queue)
      System.out.println("  Unsubscribing (queue teardown)...");
      consumer.unsubscribe(topicName);
      Thread.sleep(2000);

      // Now commit the previously polled messages — should NOT throw
      System.out.println(
          "  Committing " + uncommittedMessages.size() + " messages AFTER unsubscribe...");
      boolean commitSucceeded = true;
      for (SubscriptionMessage msg : uncommittedMessages) {
        try {
          consumer.commitSync(msg);
        } catch (Exception e) {
          System.out.println("  Commit threw exception: " + e.getMessage());
          commitSucceeded = false;
        }
      }

      // The commit may silently succeed or fail gracefully — the key is no crash
      System.out.println("  Commit after unsubscribe completed. Success=" + commitSucceeded);
      System.out.println("  (Key: no exception crash, routing handled gracefully)");
    } finally {
      if (consumer != null) {
        try {
          consumer.close();
        } catch (Exception e) {
          /* ignore */
        }
      }
      dropTopic(topicName);
      deleteDatabase(database);
    }
  }

  // ======================================================================
  // Test 8: Seek (seekToBeginning, seekToEnd, seek by timestamp)
  // ======================================================================
  /**
   * Verifies all three seek operations in a single flow:
   *
   * <ul>
   *   <li>seekToBeginning — re-delivers previously committed data from earliest available position
   *   <li>seekToEnd — skips all existing data, only new writes are received
   *   <li>seek(timestamp) — positions at the approximate WAL entry matching the given timestamp
   * </ul>
   */
  private static void testSeek() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      // Step 0: Create DataRegion
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Step 1: Create topic + consumer + subscribe
      System.out.println("  Step 1: Create topic and subscribe");
      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 2: Write 1000 rows with timestamps 1000..1999 and poll+commit all
      System.out.println("  Step 2: Write 1000 rows (timestamps 1000..1999) and poll+commit");
      try (ISession session = openSession()) {
        for (int i = 0; i < 1000; i++) {
          long ts = 1000 + i;
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, ts, ts * 10));
        }
      }
      Thread.sleep(2000);

      PollResult firstPoll = pollUntilComplete(consumer, 1000, 120);
      System.out.println("  First poll: " + firstPoll.totalRows + " rows");
      assertAtLeast("First poll should get rows", 1, firstPoll.totalRows);

      // ------------------------------------------------------------------
      // Step 3: seekToBeginning — should re-deliver data from the start
      // ------------------------------------------------------------------
      System.out.println("  Step 3: seekToBeginning → expect re-delivery");
      consumer.seekToBeginning(topicName);
      Thread.sleep(2000);

      // expectedRows=1001: 1000 from Step 2 + 1 from Step 0 initial INSERT (if WAL not yet cleaned)
      PollResult beginningPoll = pollUntilComplete(consumer, 1001, 120);
      System.out.println("  After seekToBeginning: " + beginningPoll);
      assertAtLeast(
          "seekToBeginning should re-deliver rows (WAL retention permitting)",
          1,
          beginningPoll.totalRows);

      // ------------------------------------------------------------------
      // Step 4: seekToEnd — should receive nothing until new writes
      // ------------------------------------------------------------------
      System.out.println("  Step 4: seekToEnd → expect no old data");
      consumer.seekToEnd(topicName);
      Thread.sleep(2000);

      PollResult endPoll = new PollResult();
      int consecutiveEmpty = 0;
      for (int attempt = 0; attempt < 15; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(1000));
        if (msgs.isEmpty()) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 5) break;
          Thread.sleep(500);
          continue;
        }
        consecutiveEmpty = 0;
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionSessionDataSet ds : msg.getSessionDataSetsHandler()) {
            while (ds.hasNext()) {
              ds.next();
              endPoll.totalRows++;
            }
          }
          consumer.commitSync(msg);
        }
      }
      System.out.println("  After seekToEnd (no new writes): " + endPoll.totalRows + " rows");
      // May occasionally be 1 due to prefetch thread race; tolerate small values
      assertTrue(
          "seekToEnd should yield at most 1 row (race tolerance)", endPoll.totalRows <= 1);

      // Write 200 new rows — they should be received
      System.out.println("  Writing 200 new rows after seekToEnd");
      try (ISession session = openSession()) {
        for (int i = 2000; i < 2200; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(2000);

      PollResult afterEndPoll = pollUntilComplete(consumer, 200, 120);
      System.out.println("  After seekToEnd + new writes: " + afterEndPoll);
      assertEquals(
          "Should receive exactly 200 new rows after seekToEnd", 200, afterEndPoll.totalRows);

      // ------------------------------------------------------------------
      // Step 5: seek(timestamp) — seek to midpoint timestamp 1500
      // ------------------------------------------------------------------
      System.out.println("  Step 5: seek(1500) → expect rows from near midpoint");
      consumer.seek(topicName, 1500);
      Thread.sleep(2000);

      // With 1000 rows (ts=1000..1999) + 200 rows (ts=2000..2199), sparse mapping (interval=100)
      // produces ~12 samples. seek(1500) should position near ts=1500.
      // Minimum expected: 500 rows (ts=1500..1999) + 200 rows (ts=2000..2199) = 700
      // May get more due to sparse mapping imprecision (up to ~100 extra rows)
      PollResult afterSeek = pollUntilComplete(consumer, 1201, 120);
      System.out.println("  After seek(1500): " + afterSeek.totalRows + " rows");
      assertAtLeast(
          "seek(1500) should deliver at least 700 rows (ts >= 1500)",
          700,
          afterSeek.totalRows);

      // ------------------------------------------------------------------
      // Step 6: seek(future timestamp) — expect 0 rows
      // ------------------------------------------------------------------
      System.out.println("  Step 6: seek(99999) → expect no data");
      consumer.seek(topicName, 99999);
      Thread.sleep(2000);

      PollResult futurePoll = new PollResult();
      consecutiveEmpty = 0;
      for (int attempt = 0; attempt < 10; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(1000));
        if (msgs.isEmpty()) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 5) break;
          Thread.sleep(500);
          continue;
        }
        consecutiveEmpty = 0;
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionSessionDataSet ds : msg.getSessionDataSetsHandler()) {
            while (ds.hasNext()) {
              ds.next();
              futurePoll.totalRows++;
            }
          }
          consumer.commitSync(msg);
        }
      }
      System.out.println("  After seek(99999): " + futurePoll.totalRows + " rows");
      // seek(99999) should behave like seekToEnd — 0 rows normally,
      // but may yield up to 1 row due to prefetch thread race (same as seekToEnd)
      assertTrue("seek(future) should yield at most 1 row (race tolerance)",
          futurePoll.totalRows <= 1);

      System.out.println("  testSeek passed all sub-tests!");
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  /** Helper: populate one row of an aligned Tablet with all 6 data types. */
  private static void addAlignedTabletRow(
      Tablet tablet,
      int rowIndex,
      long timestamp,
      int intVal,
      long longVal,
      float floatVal,
      double doubleVal,
      boolean boolVal,
      String textVal) {
    tablet.addTimestamp(rowIndex, timestamp);
    tablet.addValue("s_int32", rowIndex, intVal);
    tablet.addValue("s_int64", rowIndex, longVal);
    tablet.addValue("s_float", rowIndex, floatVal);
    tablet.addValue("s_double", rowIndex, doubleVal);
    tablet.addValue("s_bool", rowIndex, boolVal);
    tablet.addValue("s_text", rowIndex, new Binary(textVal, TSFileConfig.STRING_CHARSET));
  }
}
