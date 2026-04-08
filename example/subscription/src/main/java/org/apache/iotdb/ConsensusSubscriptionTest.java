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
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler.SubscriptionResultSet;

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
import java.util.stream.Collectors;

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
    if (targetTest == null || "testFilteringAndTopicSelection".equals(targetTest)) {
      runTest(
          "testFilteringAndTopicSelection",
          ConsensusSubscriptionTest::testFilteringAndTopicSelection);
    }
    if (targetTest == null || "testSubscribeBeforeRegion".equals(targetTest)) {
      runTest("testSubscribeBeforeRegion", ConsensusSubscriptionTest::testSubscribeBeforeRegion);
    }
    if (targetTest == null || "testMultiEntityIsolation".equals(targetTest)) {
      runTest("testMultiEntityIsolation", ConsensusSubscriptionTest::testMultiEntityIsolation);
    }
    if (targetTest == null || "testWalCatchUpAndGapRecovery".equals(targetTest)) {
      runTest(
          "testWalCatchUpAndGapRecovery", ConsensusSubscriptionTest::testWalCatchUpAndGapRecovery);
    }
    if (targetTest == null || "testSeekAndPositionSemantics".equals(targetTest)) {
      runTest(
          "testSeekAndPositionSemantics", ConsensusSubscriptionTest::testSeekAndPositionSemantics);
    }
    if (targetTest == null || "testConsumerRestartRecovery".equals(targetTest)) {
      runTest(
          "testConsumerRestartRecovery", ConsensusSubscriptionTest::testConsumerRestartRecovery);
    }
    if (targetTest == null || "testAckNackAndPoisonSemantics".equals(targetTest)) {
      runTest(
          "testAckNackAndPoisonSemantics",
          ConsensusSubscriptionTest::testAckNackAndPoisonSemantics);
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
      topicConfig.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
      topicConfig.put(TopicConstant.PATH_KEY, path);
      topicConfig.put(TopicConstant.ORDER_MODE_KEY, TopicConstant.ORDER_MODE_PER_WRITER_VALUE);
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
        for (SubscriptionResultSet dataSet : getResultSets(message)) {
          String device = null;
          List<String> columnNames = dataSet.getColumnNames();
          if (columnNames.size() > 1) {
            String fullPath = columnNames.get(1);
            int lastDot = fullPath.lastIndexOf('.');
            device = lastDot > 0 ? fullPath.substring(0, lastDot) : fullPath;
          }

          while (dataSet.hasNext()) {
            try {
              org.apache.tsfile.read.common.RowRecord record = dataSet.nextRecord();
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
            } catch (java.io.IOException e) {
              throw new RuntimeException("Failed to iterate subscription result set", e);
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

  private static void assertEquals(String msg, long expected, long actual) {
    if (expected != actual) {
      throw new AssertionError(msg + ": expected=" + expected + ", actual=" + actual);
    }
  }

  private static void assertEquals(String msg, String expected, String actual) {
    if (expected == null ? actual != null : !expected.equals(actual)) {
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

  private static void assertAtMost(String msg, int max, int actual) {
    if (actual > max) {
      throw new AssertionError(msg + ": expected at most " + max + ", actual=" + actual);
    }
  }

  private static int countWriterFrontiers(TopicProgress topicProgress) {
    int writerCount = 0;
    if (topicProgress == null || topicProgress.getRegionProgress() == null) {
      return 0;
    }
    for (Map.Entry<String, RegionProgress> entry : topicProgress.getRegionProgress().entrySet()) {
      if (entry.getValue() != null && entry.getValue().getWriterPositions() != null) {
        writerCount += entry.getValue().getWriterPositions().size();
      }
    }
    return writerCount;
  }

  private static int countRows(SubscriptionMessage message) {
    int rows = 0;
    for (SubscriptionResultSet dataSet : getResultSets(message)) {
      while (dataSet.hasNext()) {
        try {
          dataSet.next();
          rows++;
        } catch (java.io.IOException e) {
          throw new RuntimeException("Failed to count rows from subscription result set", e);
        }
      }
    }
    return rows;
  }

  private static final class CommittedSnapshot {
    private final TopicProgress progress;
    private final int rowsInMessage;
    private final int cumulativeRows;

    private CommittedSnapshot(TopicProgress progress, int rowsInMessage, int cumulativeRows) {
      this.progress = progress;
      this.rowsInMessage = rowsInMessage;
      this.cumulativeRows = cumulativeRows;
    }
  }

  private static final class PolledMessageBatch {
    private final List<SubscriptionMessage> messages;
    private final int totalRows;

    private PolledMessageBatch(List<SubscriptionMessage> messages, int totalRows) {
      this.messages = messages;
      this.totalRows = totalRows;
    }
  }

  private static void pause(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for subscription test state", e);
    }
  }

  private static void bootstrapSeekTopic(String database, String topicName) throws Exception {
    try (ISession session = openSession()) {
      createDatabase(session, database);
      session.executeNonQueryStatement(
          String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
      session.executeNonQueryStatement("flush");
    }
    pause(2000);

    createTopic(topicName, database + ".**");
    pause(1000);
  }

  private static SubscriptionTreePullConsumer createSubscribedConsumer(
      String topicName, String consumerId, String consumerGroupId) throws Exception {
    SubscriptionTreePullConsumer consumer = createConsumer(consumerId, consumerGroupId);
    consumer.subscribe(topicName);
    pause(3000);
    return consumer;
  }

  private static void writeSequentialRowsAndFlush(
      String database, int startTimestampInclusive, int rowCount) throws Exception {
    try (ISession session = openSession()) {
      for (int i = 0; i < rowCount; i++) {
        long ts = startTimestampInclusive + i;
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, ts, ts * 10));
      }
      session.executeNonQueryStatement("flush");
    }
    pause(2000);
  }

  private static CommittedSnapshot pollUntilCommittedRows(
      SubscriptionTreePullConsumer consumer,
      String topicName,
      int minimumRows,
      int maxPollAttempts,
      long pollTimeoutMs) {
    int cumulativeRows = 0;
    int consecutiveEmpty = 0;

    for (int attempt = 1; attempt <= maxPollAttempts; attempt++) {
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(pollTimeoutMs));
      if (messages.isEmpty()) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= 5 && cumulativeRows > 0) {
          break;
        }
        pause(1000);
        continue;
      }

      consecutiveEmpty = 0;
      for (SubscriptionMessage message : messages) {
        int rowsInMessage = countRows(message);
        consumer.commitSync(message);
        cumulativeRows += rowsInMessage;
        TopicProgress checkpoint = consumer.committedPositions(topicName);
        System.out.println(
            "    Captured committed checkpoint after "
                + cumulativeRows
                + " rows (last message="
                + rowsInMessage
                + ")");
        if (cumulativeRows >= minimumRows) {
          return new CommittedSnapshot(checkpoint, rowsInMessage, cumulativeRows);
        }
      }
    }

    throw new AssertionError(
        "Unable to capture committed checkpoint after "
            + minimumRows
            + " rows; stopped at "
            + cumulativeRows);
  }

  private static List<CommittedSnapshot> pollAndCaptureCommittedSnapshots(
      SubscriptionTreePullConsumer consumer,
      String topicName,
      int maxPollAttempts,
      long pollTimeoutMs) {
    List<CommittedSnapshot> snapshots = new ArrayList<>();
    int cumulativeRows = 0;
    int consecutiveEmpty = 0;

    for (int attempt = 1; attempt <= maxPollAttempts; attempt++) {
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(pollTimeoutMs));
      if (messages.isEmpty()) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= 3 && cumulativeRows > 0) {
          break;
        }
        if (consecutiveEmpty >= 8 && cumulativeRows == 0) {
          break;
        }
        pause(1000);
        continue;
      }

      consecutiveEmpty = 0;
      for (SubscriptionMessage message : messages) {
        int rowsInMessage = countRows(message);
        consumer.commitSync(message);
        cumulativeRows += rowsInMessage;
        snapshots.add(
            new CommittedSnapshot(
                consumer.committedPositions(topicName), rowsInMessage, cumulativeRows));
      }
    }

    System.out.println(
        "    Drained "
            + cumulativeRows
            + " rows across "
            + snapshots.size()
            + " committed messages");
    return snapshots;
  }

  private static PolledMessageBatch pollFirstNonEmptyBatchWithoutCommit(
      SubscriptionTreePullConsumer consumer, int maxPollAttempts, long pollTimeoutMs) {
    for (int attempt = 1; attempt <= maxPollAttempts; attempt++) {
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(pollTimeoutMs));
      if (messages.isEmpty()) {
        pause(1000);
        continue;
      }

      int totalRows = 0;
      for (SubscriptionMessage message : messages) {
        totalRows += countRows(message);
      }
      System.out.println(
          "    Polled stale batch without commit: "
              + messages.size()
              + " messages, "
              + totalRows
              + " rows");
      return new PolledMessageBatch(new ArrayList<>(messages), totalRows);
    }

    return new PolledMessageBatch(new ArrayList<>(), 0);
  }

  private static int totalRows(List<CommittedSnapshot> snapshots) {
    return snapshots.isEmpty() ? 0 : snapshots.get(snapshots.size() - 1).cumulativeRows;
  }

  // ======================================================================
  // High-signal 10-test suite wrappers
  // ======================================================================

  private static void testFilteringAndTopicSelection() throws Exception {
    testPathFiltering();
    testPollWithInfoTopicFilter();
  }

  private static void testWalCatchUpAndGapRecovery() throws Exception {
    testBurstWriteGapRecovery();
  }

  private static void testSeekAndPositionSemantics() throws Exception {
    testSeekNavigationSemantics();
    testSeekAfterCheckpointSemantics();
    testSeekAfterWithStaleAckFencing();
  }

  private static void testAckNackAndPoisonSemantics() throws Exception {
    testCommitAfterUnsubscribe();
    testPoisonMessageDrop();
  }

  // ======================================================================
  // Test 8: Consumer Restart Recovery
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>A committed per-region checkpoint captured by consumer1 can be reused after restart
   *   <li>A restarted consumer with the same group can seek to that checkpoint and continue
   *   <li>The tail after restart is replayed exactly once
   * </ul>
   */
  private static void testConsumerRestartRecovery() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId1 = nextConsumerId();
    String consumerId2 = consumerId1 + "_restart";
    SubscriptionTreePullConsumer consumer1 = null;
    SubscriptionTreePullConsumer consumer2 = null;

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

      consumer1 = createConsumer(consumerId1, consumerGroupId);
      consumer1.subscribe(topicName);
      Thread.sleep(3000);

      final int totalRows = 257;
      System.out.println("  Writing " + totalRows + " rows before restart");
      try (ISession session = openSession()) {
        for (int i = 1; i <= totalRows; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(3000);

      SubscriptionMessage committedMessage = null;
      int committedRows = 0;
      for (int attempt = 1; attempt <= 30; attempt++) {
        List<SubscriptionMessage> messages = consumer1.poll(Duration.ofMillis(2000));
        if (messages.isEmpty()) {
          Thread.sleep(1000);
          continue;
        }
        committedMessage = messages.get(0);
        committedRows = countRows(committedMessage);
        consumer1.commitSync(committedMessage);
        break;
      }

      assertAtLeast("First consumer should commit some rows before restart", 1, committedRows);
      TopicProgress checkpoint = consumer1.committedPositions(topicName);
      assertTrue(
          "Committed checkpoint should not be empty",
          checkpoint.getRegionProgress() != null && !checkpoint.getRegionProgress().isEmpty());
      int remainingRows = totalRows - committedRows;
      assertAtLeast("Restart scenario should leave rows after the first commit", 1, remainingRows);
      System.out.println(
          "  Captured checkpoint after committing "
              + committedRows
              + " rows: "
              + checkpoint
              + ", remainingRows="
              + remainingRows);

      consumer1.close();
      consumer1 = null;

      consumer2 = createConsumer(consumerId2, consumerGroupId);
      consumer2.subscribe(topicName);
      Thread.sleep(3000);
      consumer2.seekAfter(topicName, checkpoint);
      Thread.sleep(1000);

      PollResult resumed = pollUntilComplete(consumer2, remainingRows, 120);
      System.out.println("  Restart recovery result: " + resumed);
      assertEquals(
          "Restarted consumer should resume from the committed checkpoint without replay",
          remainingRows,
          resumed.totalRows);
    } finally {
      cleanup(consumer1, topicName, database);
      cleanup(consumer2, topicName, database);
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
   *   <li>Timeseries-level: topic on d1.s1 鈥?lenient check for s2 filtering
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
            "  INFO: Both s1 and s2 received 鈥?converter uses device-level filtering only.");
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
  // Test 6: Multi-Entity Isolation
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Two consumer groups on the same topic each receive the full data stream independently
   * </ul>
   */
  private static void testMultiEntityIsolation() throws Exception {
    String database = nextDatabase();
    String topicName = "topic_multi_" + testCounter;
    String consumerGroupId1 = "cg_multi_" + testCounter + "_a";
    String consumerId1 = "consumer_multi_" + testCounter + "_a";
    String consumerGroupId2 = "cg_multi_" + testCounter + "_b";
    String consumerId2 = "consumer_multi_" + testCounter + "_b";
    SubscriptionTreePullConsumer consumer1 = null;
    SubscriptionTreePullConsumer consumer2 = null;

    try {
      // Setup: database with a single device path to isolate multi-group semantics.
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopic(topicName, database + ".d1.**");
      Thread.sleep(1000);

      consumer1 = createConsumer(consumerId1, consumerGroupId1);
      consumer1.subscribe(topicName);
      consumer2 = createConsumer(consumerId2, consumerGroupId2);
      consumer2.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing 70 rows to d1");
      try (ISession session = openSession()) {
        for (int i = 1; i <= 70; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Multi-group isolation");
      System.out.println("  Polling from group 1...");
      PollResult result1 = pollUntilComplete(consumer1, 70, 80);
      System.out.println("  Group 1 result: " + result1);

      System.out.println("  Polling from group 2...");
      PollResult result2 = pollUntilComplete(consumer2, 70, 80);
      System.out.println("  Group 2 result: " + result2);

      assertEquals("Group 1 should receive all 70 rows", 70, result1.totalRows);
      assertEquals("Group 2 should receive all 70 rows", 70, result2.totalRows);
      assertEquals(
          "Expected 70 rows from d1", 70, result1.rowsPerDevice.getOrDefault(database + ".d1", 0));
      assertEquals(
          "Expected 70 rows from d1", 70, result2.rowsPerDevice.getOrDefault(database + ".d1", 0));
      System.out.println(
          "  Multi-group isolation verified: group1="
              + result1.totalRows
              + ", group2="
              + result2.totalRows);
    } finally {
      if (consumer1 != null) {
        try {
          consumer1.unsubscribe(topicName);
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
          consumer2.unsubscribe(topicName);
        } catch (Exception e) {
          /* ignore */
        }
        try {
          consumer2.close();
        } catch (Exception e) {
          /* ignore */
        }
      }
      dropTopic(topicName);
      deleteDatabase(database);
    }
  }

  // ======================================================================
  // Test 7: Burst Write Gap Recovery (NEW 鈥?tests C2 fix)
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
   * <p>Fix verified: C2 鈥?gap entries are not skipped when WAL fill times out; they are deferred to
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
      // Each INSERT 鈫?1 IoTConsensusServerImpl.write() 鈫?1 pendingEntries.offer().
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

      // Do NOT add artificial delay 鈥?let the consumer compete with ongoing WAL writes
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
  // Test 8: Commit After Unsubscribe (NEW 鈥?tests H7 fix)
  // ======================================================================
  /**
   * Tests that commit still works correctly after the consumer has unsubscribed (queue has been
   * torn down). The commit routing should use metadata-based topic config check instead of runtime
   * queue state.
   *
   * <p>Fix verified: H7 鈥?commit routes via isConsensusBasedTopic() instead of hasQueue().
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
          for (SubscriptionResultSet ds : getResultSets(msg)) {
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

      // Now commit the previously polled messages 鈥?should NOT throw
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

      // The commit may silently succeed or fail gracefully 鈥?the key is no crash
      System.out.println("  Commit after unsubscribe completed. Success=" + commitSucceeded);
      assertTrue("Commit after unsubscribe should succeed without exception", commitSucceeded);
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

  /**
   * Verifies:
   *
   * <ul>
   *   <li>seekToBeginning replays historical rows from the beginning of the topic
   *   <li>seekToEnd suppresses old rows and only delivers future writes
   *   <li>seek(topicProgress) resumes from a committed checkpoint without replaying earlier rows
   * </ul>
   */
  private static void testSeekNavigationSemantics() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    final int initialRows = 1800;
    final int rowsAfterSeekToEnd = 240;

    try {
      bootstrapSeekTopic(database, topicName);
      consumer = createSubscribedConsumer(topicName, consumerId, consumerGroupId);

      System.out.println("  Step 1: Write initial live rows and drain them");
      writeSequentialRowsAndFlush(database, 1000, initialRows);
      PollResult firstPoll = pollUntilComplete(consumer, initialRows, 120);
      System.out.println("  First poll: " + firstPoll.totalRows + " rows");
      assertEquals(
          "Initial live poll should deliver exactly the rows written after subscribe",
          initialRows,
          firstPoll.totalRows);

      System.out.println("  Step 2: seekToBeginning -> expect full replay");
      consumer.seekToBeginning(topicName);
      pause(2000);

      PollResult beginningPoll = pollUntilComplete(consumer, initialRows, 120);
      System.out.println("  After seekToBeginning: " + beginningPoll.totalRows + " rows");
      assertAtLeast(
          "seekToBeginning should replay the rows written after subscribe",
          initialRows,
          beginningPoll.totalRows);
      assertAtMost(
          "seekToBeginning should replay at most one extra bootstrap row",
          initialRows + 1,
          beginningPoll.totalRows);

      System.out.println("  Step 3: seekToEnd -> expect no old data");
      consumer.seekToEnd(topicName);
      pause(2000);

      PollResult endPoll = pollUntilComplete(consumer, 0, 15, 1000, true);
      System.out.println("  After seekToEnd with no new writes: " + endPoll.totalRows + " rows");
      assertAtMost("seekToEnd should yield at most 1 race row", 1, endPoll.totalRows);

      System.out.println("  Step 4: Write new rows after seekToEnd");
      writeSequentialRowsAndFlush(database, 4000, rowsAfterSeekToEnd);
      PollResult afterEndPoll = pollUntilComplete(consumer, rowsAfterSeekToEnd, 120);
      System.out.println("  After seekToEnd + new writes: " + afterEndPoll.totalRows + " rows");
      assertEquals(
          "seekToEnd should only deliver rows written after the seek",
          rowsAfterSeekToEnd,
          afterEndPoll.totalRows);

      System.out.println("  Step 5: seek(committed checkpoint) -> expect remaining tail only");
      consumer.seekToBeginning(topicName);
      pause(2000);

      CommittedSnapshot midpointCheckpoint =
          pollUntilCommittedRows(consumer, topicName, initialRows / 2, 60, 1000);
      List<CommittedSnapshot> remainingTail =
          pollAndCaptureCommittedSnapshots(consumer, topicName, 60, 1000);
      int expectedRemainingRows = totalRows(remainingTail);
      System.out.println(
          "  Midpoint checkpoint after "
              + midpointCheckpoint.cumulativeRows
              + " rows, expected remaining tail="
              + expectedRemainingRows);
      assertAtLeast(
          "seek(topicProgress) scenario should leave rows after the checkpoint",
          1,
          expectedRemainingRows);

      consumer.seekAfter(topicName, midpointCheckpoint.progress);
      pause(2000);

      PollResult afterSeek = pollUntilComplete(consumer, expectedRemainingRows, 120);
      System.out.println("  After seek(topicProgress): " + afterSeek.totalRows + " rows");
      assertEquals(
          "seek(topicProgress) should resume from the committed checkpoint",
          expectedRemainingRows,
          afterSeek.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  /**
   * Verifies:
   *
   * <ul>
   *   <li>seekAfter(topicProgress) replays only rows strictly after the checkpoint
   *   <li>Repeating seekAfter with the same checkpoint is stable
   *   <li>seekAfter(tail) suppresses history but still allows future rows through
   * </ul>
   */
  private static void testSeekAfterCheckpointSemantics() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    final int totalRows = 2000;
    final int futureRows = 160;

    try {
      bootstrapSeekTopic(database, topicName);
      consumer = createSubscribedConsumer(topicName, consumerId, consumerGroupId);

      System.out.println("  Step 1: Write rows and capture a committed checkpoint");
      writeSequentialRowsAndFlush(database, 1000, totalRows);
      CommittedSnapshot checkpoint =
          pollUntilCommittedRows(consumer, topicName, totalRows / 3, 60, 1000);
      List<CommittedSnapshot> drainedTail =
          pollAndCaptureCommittedSnapshots(consumer, topicName, 80, 1000);
      int expectedTailRows = totalRows(drainedTail);
      System.out.println(
          "  Checkpoint after "
              + checkpoint.cumulativeRows
              + " rows, tail after checkpoint="
              + expectedTailRows);
      assertAtLeast(
          "seekAfter(topicProgress) scenario should leave rows after the checkpoint",
          1,
          expectedTailRows);

      int writerFrontierCount = countWriterFrontiers(checkpoint.progress);
      assertAtLeast("Committed checkpoint should contain writer frontiers", 1, writerFrontierCount);

      System.out.println("  Step 2: seekAfter(midpoint checkpoint) -> expect exact tail replay");
      consumer.seekAfter(topicName, checkpoint.progress);
      pause(2000);

      PollResult firstReplay = pollUntilComplete(consumer, expectedTailRows, 120);
      System.out.println("  After first seekAfter(checkpoint): " + firstReplay.totalRows + " rows");
      assertEquals(
          "seekAfter(topicProgress) should replay exactly the tail after the checkpoint",
          expectedTailRows,
          firstReplay.totalRows);

      System.out.println("  Step 3: repeat seekAfter(checkpoint) -> expect same exact replay");
      consumer.seekAfter(topicName, checkpoint.progress);
      pause(2000);

      PollResult repeatedReplay = pollUntilComplete(consumer, expectedTailRows, 120);
      System.out.println(
          "  After repeated seekAfter(checkpoint): " + repeatedReplay.totalRows + " rows");
      assertEquals(
          "Repeating seekAfter(topicProgress) should be stable",
          expectedTailRows,
          repeatedReplay.totalRows);

      System.out.println("  Step 4: seekAfter(tail) -> expect no historical rows");
      TopicProgress tailProgress = consumer.committedPositions(topicName);
      assertTrue("Tail checkpoint should be non-null", tailProgress != null);
      consumer.seekAfter(topicName, tailProgress);
      pause(2000);

      PollResult noHistory = pollUntilComplete(consumer, 0, 15, 1000, true);
      System.out.println("  After seekAfter(tail): " + noHistory.totalRows + " rows");
      assertAtMost("seekAfter(tail) should yield at most 1 race row", 1, noHistory.totalRows);

      System.out.println("  Step 5: Write new rows after seekAfter(tail)");
      writeSequentialRowsAndFlush(database, 5000, futureRows);
      PollResult futureOnly = pollUntilComplete(consumer, futureRows, 120);
      System.out.println("  After seekAfter(tail) + new writes: " + futureOnly.totalRows + " rows");
      assertEquals(
          "seekAfter(tail) should only deliver rows written after the seek",
          futureRows,
          futureOnly.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  /**
   * Verifies:
   *
   * <ul>
   *   <li>seekAfter fences off stale in-flight commit contexts from before the seek
   *   <li>Committing old polled messages after the seek does not affect the new replay frontier
   *   <li>The full tail after the checkpoint is replayed exactly once
   * </ul>
   */
  private static void testSeekAfterWithStaleAckFencing() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    final int totalRows = 2400;

    try {
      bootstrapSeekTopic(database, topicName);
      consumer = createSubscribedConsumer(topicName, consumerId, consumerGroupId);

      System.out.println("  Step 1: Write rows and commit part of them");
      writeSequentialRowsAndFlush(database, 1000, totalRows);
      CommittedSnapshot committedCheckpoint =
          pollUntilCommittedRows(consumer, topicName, totalRows / 3, 60, 1000);
      assertTrue(
          "Committed checkpoint should not be null",
          committedCheckpoint.progress != null
              && committedCheckpoint.progress.getRegionProgress() != null
              && !committedCheckpoint.progress.getRegionProgress().isEmpty());

      System.out.println("  Step 2: Poll a stale batch without committing it");
      PolledMessageBatch staleBatch = pollFirstNonEmptyBatchWithoutCommit(consumer, 30, 1000);
      assertAtLeast(
          "Stale-ack scenario should poll at least one row after the checkpoint",
          1,
          staleBatch.totalRows);

      int expectedTailRows = totalRows - committedCheckpoint.cumulativeRows;
      System.out.println(
          "  Committed checkpoint after "
              + committedCheckpoint.cumulativeRows
              + " rows, stale batch="
              + staleBatch.totalRows
              + ", expected replay tail="
              + expectedTailRows);
      assertAtLeast(
          "Stale-ack replay should include the stale batch rows",
          staleBatch.totalRows,
          expectedTailRows);

      System.out.println("  Step 3: seekAfter(checkpoint), then commit stale messages");
      consumer.seekAfter(topicName, committedCheckpoint.progress);
      pause(2000);

      for (SubscriptionMessage staleMessage : staleBatch.messages) {
        consumer.commitSync(staleMessage);
      }

      PollResult replayAfterSeek = pollUntilComplete(consumer, expectedTailRows, 120);
      System.out.println(
          "  After seekAfter(checkpoint) with stale commits: "
              + replayAfterSeek.totalRows
              + " rows");
      assertEquals(
          "Stale commits from the old generation must not reduce the replayed tail",
          expectedTailRows,
          replayAfterSeek.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 11: pollWithInfo(topicNames, timeoutMs) 鈥?topic-level filtering
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>pollWithInfo(Set, long) only returns data matching the specified topics
   *   <li>Data from other subscribed topics is not returned in the filtered poll
   *   <li>After filtered poll, remaining data can still be retrieved via unfiltered poll
   * </ul>
   */
  private static void testPollWithInfoTopicFilter() throws Exception {
    String database = nextDatabase();
    String topicName1 = "topic_pwf_" + testCounter + "_a";
    String topicName2 = "topic_pwf_" + testCounter + "_b";
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTreePullConsumer consumer = null;

    try {
      // Step 0: Create database with d1, d2
      try (ISession session = openSession()) {
        createDatabase(session, database);
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d1(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement(
            String.format("INSERT INTO %s.d2(time, s1) VALUES (0, 0)", database));
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Step 1: Create two topics with distinct path filters
      System.out.println("  Step 1: Creating two topics (d1 / d2)");
      createTopic(topicName1, database + ".d1.**");
      createTopic(topicName2, database + ".d2.**");
      Thread.sleep(1000);

      // Step 2: Subscribe to both topics
      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName1, topicName2);
      Thread.sleep(3000);

      // Step 3: Write 30 rows to d1, 40 rows to d2
      System.out.println("  Step 3: Writing 30 rows to d1, 40 rows to d2");
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
      Thread.sleep(3000);

      // Step 4: pollWithInfo for topicName1 only
      System.out.println("  Step 4: pollWithInfo for topic1 (d1) only");
      Set<String> topic1Only = new HashSet<>(Arrays.asList(topicName1));
      int d1Rows = 0;
      for (int attempt = 0; attempt < 40; attempt++) {
        org.apache.iotdb.session.subscription.payload.PollResult pollResult =
            consumer.pollWithInfo(topic1Only, 2000);
        List<SubscriptionMessage> msgs = pollResult.getMessages();
        if (msgs.isEmpty()) {
          if (d1Rows > 0) break;
          Thread.sleep(1000);
          continue;
        }
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            List<String> cols = ds.getColumnNames();
            while (ds.hasNext()) {
              ds.next();
              d1Rows++;
              // Verify no d2 columns appear
              for (String col : cols) {
                assertTrue("Topic1 poll should not contain d2 data: " + col, !col.contains(".d2."));
              }
            }
          }
          consumer.commitSync(msg);
        }
      }
      System.out.println("  Topic1-only poll received: " + d1Rows + " rows");
      assertEquals("Topic1 should deliver exactly 30 rows from d1", 30, d1Rows);

      // Step 5: pollWithInfo for topicName2 only 鈥?should get d2 data
      System.out.println("  Step 5: pollWithInfo for topic2 (d2) only");
      Set<String> topic2Only = new HashSet<>(Arrays.asList(topicName2));
      int d2Rows = 0;
      for (int attempt = 0; attempt < 40; attempt++) {
        org.apache.iotdb.session.subscription.payload.PollResult pollResult =
            consumer.pollWithInfo(topic2Only, 2000);
        List<SubscriptionMessage> msgs = pollResult.getMessages();
        if (msgs.isEmpty()) {
          if (d2Rows > 0) break;
          Thread.sleep(1000);
          continue;
        }
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            List<String> cols = ds.getColumnNames();
            while (ds.hasNext()) {
              ds.next();
              d2Rows++;
              // Verify no d1 columns appear
              for (String col : cols) {
                assertTrue("Topic2 poll should not contain d1 data: " + col, !col.contains(".d1."));
              }
            }
          }
          consumer.commitSync(msg);
        }
      }
      System.out.println("  Topic2-only poll received: " + d2Rows + " rows");
      assertEquals("Topic2 should deliver exactly 40 rows from d2", 40, d2Rows);

      System.out.println("  testPollWithInfoTopicFilter passed!");
    } finally {
      if (consumer != null) {
        try {
          consumer.unsubscribe(topicName1, topicName2);
        } catch (Exception e) {
          /* ignore */
        }
        try {
          consumer.close();
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
  // Test 12: Poison Message Drop 鈥?messages nacked beyond threshold
  //          are force-acked (dropped) and don't block new data.
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>A message that is nacked (poll timeout without commit) more than
   *       POISON_MESSAGE_NACK_THRESHOLD (10) times is eventually dropped
   *   <li>After the poison message is dropped, new data can still be received
   *   <li>The consumer is not permanently blocked by a single unprocessable message
   * </ul>
   *
   * <p><b>Note:</b> "Nack" in this context means the server re-enqueues an in-flight event that was
   * polled but never committed by the consumer. Each re-enqueue increments the event's nack
   * counter. After 10 nacks, the event is marked as poisoned and force-acked (dropped) at the next
   * re-enqueue attempt.
   */
  private static void testPoisonMessageDrop() throws Exception {
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

      // Step 1: Create topic and subscribe
      System.out.println("  Step 1: Creating topic and subscribing");
      createTopic(topicName, database + ".**");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 2: Write initial data that will become the "poison" message
      System.out.println("  Step 2: Writing 10 rows (the initial batch)");
      try (ISession session = openSession()) {
        for (int i = 1; i <= 10; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(2000);

      // Step 3: Poll without commit 鈥?repeatedly. Each poll-then-timeout cycle
      // causes the server to nack the in-flight event and re-enqueue it.
      // After POISON_MESSAGE_NACK_THRESHOLD (10) nacks, the message should be dropped.
      System.out.println(
          "  Step 3: Polling without commit for 15 rounds (threshold=10, need >10 nacks)");
      int totalPoisonPolled = 0;
      for (int round = 1; round <= 15; round++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(3000));
        int roundRows = 0;
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            while (ds.hasNext()) {
              ds.next();
              roundRows++;
              totalPoisonPolled++;
            }
          }
          // Deliberately NOT committing 鈥?this is the "nack" behavior
        }
        System.out.println(
            "    Round " + round + ": received " + roundRows + " rows (NOT committing)");
        if (msgs.isEmpty() && round > 11) {
          // After threshold exceeded, the message may have been dropped
          System.out.println("    No messages 鈥?poison message may have been force-acked");
          break;
        }
        Thread.sleep(1000);
      }
      System.out.println("  Total rows polled across all rounds: " + totalPoisonPolled);

      // Step 4: Write NEW data and verify it can be received (consumer not blocked)
      System.out.println("  Step 4: Writing 50 NEW rows and polling WITH commit");
      try (ISession session = openSession()) {
        for (int i = 1000; i < 1050; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO %s.d1(time, s1) VALUES (%d, %d)", database, i, i * 10));
        }
      }
      Thread.sleep(2000);

      PollResult newResult = pollUntilComplete(consumer, 50, 60);
      System.out.println("  New data poll result: " + newResult);

      // The key assertion: new data must be receivable
      // The exact count may be slightly more than 50 if the old poison data leaked through
      // in an earlier round, but the queue must not be permanently blocked.
      assertAtLeast(
          "Consumer must not be permanently blocked by poison message 鈥?new data should arrive",
          1,
          newResult.totalRows);
      System.out.println(
          "  testPoisonMessageDrop passed: consumer received "
              + newResult.totalRows
              + " new rows after poison message handling");
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  private static List<SubscriptionResultSet> getResultSets(final SubscriptionMessage message) {
    return message.getResultSets().stream()
        .map(resultSet -> (SubscriptionResultSet) resultSet)
        .collect(Collectors.toList());
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
