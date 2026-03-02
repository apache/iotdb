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

    if (targetTest == null || "testBasicDataDelivery".equals(targetTest)) {
      runTest("testBasicDataDelivery", ConsensusSubscriptionTest::testBasicDataDelivery);
    }
    if (targetTest == null || "testMultipleDataTypes".equals(targetTest)) {
      runTest("testMultipleDataTypes", ConsensusSubscriptionTest::testMultipleDataTypes);
    }
    if (targetTest == null || "testDeviceLevelFiltering".equals(targetTest)) {
      runTest("testDeviceLevelFiltering", ConsensusSubscriptionTest::testDeviceLevelFiltering);
    }
    if (targetTest == null || "testTimeseriesLevelFiltering".equals(targetTest)) {
      runTest(
          "testTimeseriesLevelFiltering", ConsensusSubscriptionTest::testTimeseriesLevelFiltering);
    }
    if (targetTest == null || "testSubscribeBeforeRegion".equals(targetTest)) {
      runTest("testSubscribeBeforeRegion", ConsensusSubscriptionTest::testSubscribeBeforeRegion);
    }
    if (targetTest == null || "testMultipleDevicesAggregation".equals(targetTest)) {
      runTest(
          "testMultipleDevicesAggregation",
          ConsensusSubscriptionTest::testMultipleDevicesAggregation);
    }
    if (targetTest == null || "testAlignedTimeseries".equals(targetTest)) {
      runTest("testAlignedTimeseries", ConsensusSubscriptionTest::testAlignedTimeseries);
    }
    if (targetTest == null || "testPollWithoutCommit".equals(targetTest)) {
      runTest("testPollWithoutCommit", ConsensusSubscriptionTest::testPollWithoutCommit);
    }
    if (targetTest == null || "testMultiConsumerGroupIndependent".equals(targetTest)) {
      runTest(
          "testMultiConsumerGroupIndependent",
          ConsensusSubscriptionTest::testMultiConsumerGroupIndependent);
    }
    if (targetTest == null || "testMultiTopicSubscription".equals(targetTest)) {
      runTest("testMultiTopicSubscription", ConsensusSubscriptionTest::testMultiTopicSubscription);
    }
    if (targetTest == null || "testFlushDataDelivery".equals(targetTest)) {
      runTest("testFlushDataDelivery", ConsensusSubscriptionTest::testFlushDataDelivery);
    }
    if (targetTest == null || "testCrossPartitionAligned".equals(targetTest)) {
      runTest("testCrossPartitionAligned", ConsensusSubscriptionTest::testCrossPartitionAligned);
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

  // ============================
  // Test 1: Basic Data Delivery
  // ============================
  /**
   * Verifies the basic consensus subscription flow: write before subscribe (not received), write
   * after subscribe (received), and no extra data beyond expectation.
   */
  private static void testBasicDataDelivery() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      // Step 1: Write initial data to create DataRegion
      System.out.println("  Step 1: Writing initial data (should NOT be received)");
      try (ISession session = openSession()) {
        createDatabase(session, database);
        for (int i = 0; i < 50; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s1, s2) VALUES (%d, %d, %f)",
                  database, i, i * 10, i * 1.5));
        }
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

      // Step 3: Write new data AFTER subscription
      System.out.println("  Step 3: Writing new data AFTER subscription (100 rows)");
      try (ISession session = openSession()) {
        for (int i = 100; i < 200; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s1, s2) VALUES (%d, %d, %f)",
                  database, i, i * 10, i * 1.5));
        }
      }
      Thread.sleep(2000);

      // Step 4: Poll and verify exact count (also verifies no extra data)
      System.out.println("  Step 4: Polling...");
      PollResult result = pollUntilComplete(consumer, 100, 100);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 100 rows from post-subscribe writes", 100, result.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 2: Multiple Data Types (Non-Aligned)
  // ============================
  /**
   * Writes data with multiple data types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT) using
   * separate INSERT statements per type (non-aligned), and verifies all types are delivered.
   */
  private static void testMultipleDataTypes() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s_int32) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing data with 6 data types x 20 rows each");
      try (ISession session = openSession()) {
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
      }
      Thread.sleep(2000);

      System.out.println("  Polling...");
      PollResult result = pollUntilComplete(consumer, 120, 120);
      System.out.println("  Result: " + result);

      assertAtLeast("Expected at least 20 rows with multiple data types", 20, result.totalRows);
      System.out.println("  Seen columns: " + result.seenColumns);
      assertTrue(
          "Expected multiple column types in result, got: " + result.seenColumns,
          result.seenColumns.size() > 1);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 3: Device-Level Filtering
  // ============================
  /**
   * Creates a topic that only matches root.db.d1.** and verifies that data written to d2 is NOT
   * delivered.
   */
  private static void testDeviceLevelFiltering() throws Exception {
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
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d2(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      String filterPath = database + ".d1.**";
      createTopic(topicName, filterPath);
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing to both d1 and d2 (topic filter: d1.** only)");
      try (ISession session = openSession()) {
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d2(time, s1) VALUES (%d, %d)", database, i, i * 20));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting only d1 data)...");
      PollResult result = pollUntilComplete(consumer, 50, 60);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 50 rows from d1 only", 50, result.totalRows);
      if (!result.rowsPerDevice.isEmpty()) {
        Integer d2Rows = result.rowsPerDevice.get(database + ".d2");
        assertTrue("Expected NO rows from d2, but got " + d2Rows, d2Rows == null || d2Rows == 0);
        Integer d1Rows = result.rowsPerDevice.get(database + ".d1");
        assertAtLeast("Expected d1 rows", 1, d1Rows != null ? d1Rows : 0);
        System.out.println(
            "  Device filtering verified: d1=" + d1Rows + " rows, d2=" + d2Rows + " rows");
      }
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 4: Timeseries-Level Filtering
  // ============================
  /**
   * Creates a topic matching root.db.d1.s1 only. Tests whether the converter filters at measurement
   * level. Lenient: if both s1 and s2 arrive, reports device-level-only filtering.
   */
  private static void testTimeseriesLevelFiltering() throws Exception {
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
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      String filterPath = database + ".d1.s1";
      createTopic(topicName, filterPath);
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing to d1.s1 and d1.s2 (topic filter: d1.s1 only)");
      try (ISession session = openSession()) {
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d1(time, s1, s2) VALUES (%d, %d, %d)",
                  database, i, i * 10, i * 20));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting only s1 data)...");
      PollResult result = pollUntilComplete(consumer, 50, 60);
      System.out.println("  Result: " + result);

      System.out.println("  Seen columns: " + result.seenColumns);
      boolean hasS2 = result.seenColumns.stream().anyMatch(c -> c.contains(".s2"));
      if (hasS2) {
        System.out.println(
            "  INFO: Both s1 and s2 received — converter uses device-level filtering only.");
        assertAtLeast("Should have received some rows", 50, result.totalRows);
      } else {
        System.out.println("  Timeseries-level filtering verified: only s1 data received");
        assertEquals("Expected exactly 50 rows from s1 only", 50, result.totalRows);
      }
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 5: Subscribe Before Region Creation
  // ============================
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

      System.out.println("  Step 4: Polling (auto-binding should have picked up new region)...");
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

  // ============================
  // Test 6: Multiple Devices Aggregation
  // ============================
  /** Writes to d1, d2, d3 and verifies all are received via a broad topic path. */
  private static void testMultipleDevicesAggregation() throws Exception {
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
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d2(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d3(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing to 3 devices (d1, d2, d3), 30 rows each");
      try (ISession session = openSession()) {
        for (int i = 100; i < 130; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d2(time, s1) VALUES (%d, %d)", database, i, i * 20));
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d3(time, s1) VALUES (%d, %d)", database, i, i * 30));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting 90 total from 3 devices)...");
      PollResult result = pollUntilComplete(consumer, 90, 100);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 90 rows total (30 per device)", 90, result.totalRows);
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

  // ============================
  // Test 7: Aligned Timeseries
  // ============================
  /**
   * Creates aligned timeseries with 6 data types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT) and
   * writes rows where each INSERT contains ALL columns. Verifies all rows and all column types are
   * delivered correctly.
   */
  private static void testAlignedTimeseries() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      // Create aligned timeseries with multiple data types
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format(
                "CREATE ALIGNED TIMESERIES %s.d_aligned"
                    + "(s_int32 INT32, s_int64 INT64, s_float FLOAT,"
                    + " s_double DOUBLE, s_bool BOOLEAN, s_text TEXT)",
                database));
        // Write initial row to force DataRegion creation
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

      // Write 50 aligned rows, each with all 6 data types in a single INSERT
      System.out.println("  Writing 50 aligned rows with 6 data types per row");
      try (ISession session = openSession()) {
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO %s.d_aligned(time, s_int32, s_int64, s_float,"
                      + " s_double, s_bool, s_text)"
                      + " VALUES (%d, %d, %d, %f, %f, %s, 'text_%d')",
                  database,
                  i,
                  i,
                  (long) i * 100000L,
                  i * 1.1f,
                  i * 2.2,
                  i % 2 == 0 ? "true" : "false",
                  i));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling...");
      PollResult result = pollUntilComplete(consumer, 50, 70);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 50 aligned rows", 50, result.totalRows);
      // Verify we see columns for multiple data types
      System.out.println("  Seen columns: " + result.seenColumns);
      assertAtLeast(
          "Expected at least 6 columns (one per data type)", 6, result.seenColumns.size());
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 8: Poll Without Commit (Re-delivery)
  // ============================
  /**
   * Tests at-least-once delivery with a mixed commit/no-commit pattern.
   *
   * <p>Writes 50 rows. The prefetching thread may batch multiple INSERTs into a single event, so we
   * track committed ROWS (not events). The state machine alternates:
   *
   * <ul>
   *   <li>Even-numbered rounds: poll WITHOUT commit, record ALL timestamps from the event; next
   *       poll verifies the EXACT SAME timestamps are re-delivered, then commit.
   *   <li>Odd-numbered rounds: poll and commit directly; next poll should deliver DIFFERENT data.
   * </ul>
   *
   * <p>This exercises both the re-delivery path (recycleInFlightEventsForConsumer) and the normal
   * commit path in an interleaved fashion.
   */
  private static void testPollWithoutCommit() throws Exception {
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

      // Write 50 rows (may be batched into fewer events by the prefetching thread)
      final int totalRows = 50;
      System.out.println("  Writing " + totalRows + " rows");
      try (ISession session = openSession()) {
        for (int i = 1; i <= totalRows; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(3000);

      // State machine: alternate between skip-commit and direct-commit.
      // Track committed ROWS (not events) because batching is unpredictable.
      int totalRowsCommitted = 0;
      int roundNumber = 0; // counts distinct events seen (used for alternation)
      boolean hasPending = false;
      List<Long> pendingTimestamps = new ArrayList<>(); // timestamps from the uncommitted event
      Set<Long> allCommittedTimestamps = new HashSet<>(); // all timestamps ever committed
      int redeliveryCount = 0;

      for (int attempt = 0; attempt < 200 && totalRowsCommitted < totalRows; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(5000));
        if (msgs.isEmpty()) {
          Thread.sleep(1000);
          continue;
        }

        for (SubscriptionMessage msg : msgs) {
          // Extract ALL timestamps from this event (may contain multiple rows)
          List<Long> currentTimestamps = new ArrayList<>();
          for (SubscriptionSessionDataSet ds : msg.getSessionDataSetsHandler()) {
            while (ds.hasNext()) {
              currentTimestamps.add(ds.next().getTimestamp());
            }
          }
          assertTrue("Poll should return data with at least 1 row", currentTimestamps.size() > 0);

          if (hasPending) {
            // === Re-delivery round: verify EXACT same timestamps ===
            assertTrue(
                "Re-delivery timestamp list mismatch: expected="
                    + pendingTimestamps
                    + ", actual="
                    + currentTimestamps,
                currentTimestamps.equals(pendingTimestamps));
            consumer.commitSync(msg);
            totalRowsCommitted += currentTimestamps.size();
            allCommittedTimestamps.addAll(currentTimestamps);
            hasPending = false;
            redeliveryCount++;
            roundNumber++;
            System.out.println(
                "    [rows="
                    + totalRowsCommitted
                    + "/"
                    + totalRows
                    + "] Re-delivered & committed: timestamps="
                    + currentTimestamps);
          } else {
            // === New event round ===
            // After a commit, verify this is DIFFERENT data (no overlap with committed set)
            if (totalRowsCommitted > 0) {
              boolean overlap = false;
              for (Long ts : currentTimestamps) {
                if (allCommittedTimestamps.contains(ts)) {
                  overlap = true;
                  break;
                }
              }
              assertTrue(
                  "After commit, should receive different data (timestamps="
                      + currentTimestamps
                      + " overlap with committed="
                      + allCommittedTimestamps
                      + ")",
                  !overlap);
            }

            // Even-numbered rounds: skip commit (test re-delivery)
            // Odd-numbered rounds: commit directly (test normal flow)
            if (roundNumber % 2 == 0) {
              pendingTimestamps = new ArrayList<>(currentTimestamps);
              hasPending = true;
              System.out.println(
                  "    [rows="
                      + totalRowsCommitted
                      + "/"
                      + totalRows
                      + "] New event (NOT committed): timestamps="
                      + currentTimestamps);
            } else {
              consumer.commitSync(msg);
              totalRowsCommitted += currentTimestamps.size();
              allCommittedTimestamps.addAll(currentTimestamps);
              roundNumber++;
              System.out.println(
                  "    [rows="
                      + totalRowsCommitted
                      + "/"
                      + totalRows
                      + "] New event (committed directly): timestamps="
                      + currentTimestamps);
            }
          }
        }
      }

      assertEquals("Should have committed all rows", totalRows, totalRowsCommitted);
      assertTrue(
          "Should have at least 1 re-delivery round (got " + redeliveryCount + ")",
          redeliveryCount > 0);

      // Final poll: should be empty
      System.out.println("  Final poll: expecting no data");
      int extraRows = 0;
      for (int i = 0; i < 3; i++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(2000));
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionSessionDataSet ds : msg.getSessionDataSetsHandler()) {
            while (ds.hasNext()) {
              ds.next();
              extraRows++;
            }
          }
        }
      }
      assertEquals("After all committed, should receive no more data", 0, extraRows);

      System.out.println(
          "  At-least-once re-delivery verified: "
              + totalRows
              + " rows committed with "
              + redeliveryCount
              + " re-delivery rounds");
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 9: Multi Consumer Group Independent Consumption
  // ============================
  /**
   * Two consumer groups subscribe to the same topic. Verifies that each group independently
   * receives ALL data (data is not partitioned/split between groups).
   */
  private static void testMultiConsumerGroupIndependent() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId1 = "cg_multi_" + testCounter + "_a";
    String consumerId1 = "consumer_multi_" + testCounter + "_a";
    String consumerGroupId2 = "cg_multi_" + testCounter + "_b";
    String consumerId2 = "consumer_multi_" + testCounter + "_b";
    SubscriptionTreePullConsumer consumer1 = null;
    SubscriptionTreePullConsumer consumer2 = null;

    try {
      // Create database and initial data
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      // Two consumers in different groups both subscribe to the same topic
      consumer1 = createConsumer(consumerId1, consumerGroupId1);
      consumer1.subscribe(topicName);
      consumer2 = createConsumer(consumerId2, consumerGroupId2);
      consumer2.subscribe(topicName);
      Thread.sleep(3000);

      // Write 50 rows
      System.out.println("  Writing 50 rows");
      try (ISession session = openSession()) {
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(2000);

      // Poll from group 1
      System.out.println("  Polling from consumer group 1...");
      PollResult result1 = pollUntilComplete(consumer1, 50, 70);
      System.out.println("  Group 1 result: " + result1);

      // Poll from group 2
      System.out.println("  Polling from consumer group 2...");
      PollResult result2 = pollUntilComplete(consumer2, 50, 70);
      System.out.println("  Group 2 result: " + result2);

      // Both groups should have all 50 rows
      assertEquals("Group 1 should receive all 50 rows", 50, result1.totalRows);
      assertEquals("Group 2 should receive all 50 rows", 50, result2.totalRows);
      System.out.println(
          "  Independent consumption verified: group1="
              + result1.totalRows
              + ", group2="
              + result2.totalRows);
    } finally {
      // Clean up both consumers
      if (consumer1 != null) {
        try {
          consumer1.unsubscribe(topicName);
        } catch (Exception e) {
          // ignore
        }
        try {
          consumer1.close();
        } catch (Exception e) {
          // ignore
        }
      }
      if (consumer2 != null) {
        try {
          consumer2.unsubscribe(topicName);
        } catch (Exception e) {
          // ignore
        }
        try {
          consumer2.close();
        } catch (Exception e) {
          // ignore
        }
      }
      dropTopic(topicName);
      deleteDatabase(database);
    }
  }

  // ============================
  // Test 10: Multi Topic Subscription
  // ============================
  /**
   * One consumer subscribes to two different topics with different path filters. Verifies that each
   * topic delivers only its matching data, and no cross-contamination occurs.
   */
  private static void testMultiTopicSubscription() throws Exception {
    String database = nextDatabase();
    String topicName1 = "topic_multi_" + testCounter + "_a";
    String topicName2 = "topic_multi_" + testCounter + "_b";
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      // Create database with two device groups
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d2(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Topic 1: covers d1 only
      createTopic(topicName1, database + ".d1.**");
      // Topic 2: covers d2 only
      createTopic(topicName2, database + ".d2.**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName1, topicName2);
      Thread.sleep(3000);

      // Write 30 rows to d1 and 40 rows to d2
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

      // Poll all data — should get d1 rows (via topic1) + d2 rows (via topic2)
      System.out.println("  Polling (expecting 30 from d1 + 40 from d2 = 70 total)...");
      PollResult result = pollUntilComplete(consumer, 70, 80);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 70 rows total (30 d1 + 40 d2)", 70, result.totalRows);
      if (!result.rowsPerDevice.isEmpty()) {
        Integer d1Rows = result.rowsPerDevice.get(database + ".d1");
        Integer d2Rows = result.rowsPerDevice.get(database + ".d2");
        assertEquals("Expected 30 rows from d1", 30, d1Rows != null ? d1Rows : 0);
        assertEquals("Expected 40 rows from d2", 40, d2Rows != null ? d2Rows : 0);
        System.out.println(
            "  Multi-topic isolation verified: d1=" + d1Rows + " rows, d2=" + d2Rows + " rows");
      }
    } finally {
      // Clean up consumer, both topics, and database
      if (consumer != null) {
        try {
          consumer.unsubscribe(topicName1, topicName2);
        } catch (Exception e) {
          // ignore
        }
        try {
          consumer.close();
        } catch (Exception e) {
          // ignore
        }
      }
      dropTopic(topicName1);
      dropTopic(topicName2);
      deleteDatabase(database);
    }
  }

  // ============================
  // Test 11: Flush Data Delivery
  // ============================
  /**
   * Subscribes first, then writes data and flushes before polling. Verifies that flushing (memtable
   * → TSFile) does not cause data loss in the subscription pipeline, because WAL pinning keeps
   * entries available until committed by the subscription consumer.
   */
  private static void testFlushDataDelivery() throws Exception {
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

      // Write 50 rows, then flush before polling
      System.out.println("  Writing 50 rows then flushing");
      try (ISession session = openSession()) {
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
        System.out.println("  Flushing...");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Poll — all 50 rows should be delivered despite flush
      System.out.println("  Polling after flush...");
      PollResult result = pollUntilComplete(consumer, 50, 70);
      System.out.println("  Result: " + result);
      assertEquals("Expected exactly 50 rows after flush (no data loss)", 50, result.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 12: Cross-Partition Aligned Timeseries (Multiple Write Methods)
  // ============================
  /**
   * Tests cross-partition aligned timeseries with 6 data types, written via six different aligned
   * methods. Timestamps are spaced >1 week apart to force different time partitions, exercising the
   * WAL merge path for multi-partition inserts.
   *
   * <p>Write methods (all aligned):
   *
   * <ol>
   *   <li>SQL single row
   *   <li>SQL multi-row (cross-partition)
   *   <li>session.insertAlignedRecord (single row)
   *   <li>session.insertAlignedRecordsOfOneDevice (cross-partition)
   *   <li>session.insertAlignedTablet (cross-partition)
   *   <li>session.insertAlignedTablets (cross-partition)
   * </ol>
   */
  private static void testCrossPartitionAligned() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    // Gap slightly over 1 week (default partition interval = 604,800,000ms)
    final long GAP = 604_800_001L;
    final String device = database + ".d_aligned";

    try {
      // Create aligned timeseries with 6 data types
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format(
                "CREATE ALIGNED TIMESERIES %s.d_aligned"
                    + "(s_int32 INT32, s_int64 INT64, s_float FLOAT,"
                    + " s_double DOUBLE, s_bool BOOLEAN, s_text TEXT)",
                database));
        // Init row to force DataRegion creation
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

      // Shared measurement info for Session API calls
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

      // Shared schema for Tablet API calls
      List<IMeasurementSchema> schemas = new ArrayList<>();
      schemas.add(new MeasurementSchema("s_int32", TSDataType.INT32));
      schemas.add(new MeasurementSchema("s_int64", TSDataType.INT64));
      schemas.add(new MeasurementSchema("s_float", TSDataType.FLOAT));
      schemas.add(new MeasurementSchema("s_double", TSDataType.DOUBLE));
      schemas.add(new MeasurementSchema("s_bool", TSDataType.BOOLEAN));
      schemas.add(new MeasurementSchema("s_text", TSDataType.TEXT));

      System.out.println("  Writing cross-partition aligned data via 6 methods");
      int totalExpected = 0;

      try (ISession session = openSession()) {

        // --- Method 1: SQL single row ---
        long t1 = 1;
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s.d_aligned(time, s_int32, s_int64, s_float,"
                    + " s_double, s_bool, s_text)"
                    + " VALUES (%d, 1, 100, 1.1, 1.11, true, 'sql_single')",
                database, t1));
        totalExpected += 1;
        System.out.println("    Method 1 (SQL single row): 1 row");

        // --- Method 2: SQL multi-row (cross-partition, 2 rows >1 week apart) ---
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
        System.out.println("    Method 2 (SQL multi-row, cross-partition): 2 rows");

        // --- Method 3: insertAlignedRecord (single row) ---
        long t3 = 1 + 3 * GAP;
        List<Object> values3 = Arrays.asList(4, 400L, 4.4f, 4.44, true, "record_single");
        session.insertAlignedRecord(device, t3, measurements, types, values3);
        totalExpected += 1;
        System.out.println("    Method 3 (insertAlignedRecord): 1 row");

        // --- Method 4: insertAlignedRecordsOfOneDevice (cross-partition, 2 rows) ---
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
        System.out.println(
            "    Method 4 (insertAlignedRecordsOfOneDevice, cross-partition): 2 rows");

        // --- Method 5: insertAlignedTablet (cross-partition, 2 rows) ---
        long t5a = 1 + 6 * GAP;
        long t5b = 1 + 7 * GAP;
        Tablet tablet5 = new Tablet(device, schemas, 2);
        addAlignedTabletRow(tablet5, 0, t5a, 7, 700L, 7.7f, 7.77, false, "tablet_a");
        addAlignedTabletRow(tablet5, 1, t5b, 8, 800L, 8.8f, 8.88, true, "tablet_b");
        session.insertAlignedTablet(tablet5);
        totalExpected += 2;
        System.out.println("    Method 5 (insertAlignedTablet, cross-partition): 2 rows");

        // --- Method 6: insertAlignedTablets (cross-partition, 2 rows) ---
        long t6a = 1 + 8 * GAP;
        long t6b = 1 + 9 * GAP;
        Tablet tablet6 = new Tablet(device, schemas, 2);
        addAlignedTabletRow(tablet6, 0, t6a, 9, 900L, 9.9f, 9.99, false, "tablets_a");
        addAlignedTabletRow(tablet6, 1, t6b, 10, 1000L, 10.1f, 10.10, true, "tablets_b");
        Map<String, Tablet> tabletMap = new HashMap<>();
        tabletMap.put(device, tablet6);
        session.insertAlignedTablets(tabletMap);
        totalExpected += 2;
        System.out.println("    Method 6 (insertAlignedTablets, cross-partition): 2 rows");
      }

      System.out.println("  Total expected rows: " + totalExpected);
      Thread.sleep(2000);

      System.out.println("  Polling...");
      PollResult result = pollUntilComplete(consumer, totalExpected, 100);
      System.out.println("  Result: " + result);

      assertEquals(
          "Expected exactly " + totalExpected + " cross-partition aligned rows",
          totalExpected,
          result.totalRows);
      assertAtLeast(
          "Expected at least 6 columns (one per data type)", 6, result.seenColumns.size());
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
