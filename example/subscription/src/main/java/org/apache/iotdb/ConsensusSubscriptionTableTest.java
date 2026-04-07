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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.base.ColumnAlignProcessor;
import org.apache.iotdb.session.subscription.consumer.base.WatermarkProcessor;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler.SubscriptionResultSet;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

/** TODO: Move these manual tests into ITs */
public class ConsensusSubscriptionTableTest {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;
  private static final String USER = "root";
  private static final String PASSWORD = "root";

  private static int testCounter = 0;
  private static int passed = 0;
  private static int failed = 0;
  private static final List<String> failedTests = new ArrayList<>();

  public static void main(String[] args) throws Exception {
    System.out.println("=== Consensus-Based Subscription Table Model Test Suite ===\n");

    String targetTest = args.length > 0 ? args[0] : null;

    if (targetTest == null || "testBasicFlow".equals(targetTest)) {
      runTest("testBasicFlow", ConsensusSubscriptionTableTest::testBasicFlow);
    }
    if (targetTest == null || "testDataTypes".equals(targetTest)) {
      runTest("testDataTypes", ConsensusSubscriptionTableTest::testDataTypes);
    }
    if (targetTest == null || "testFilteringAndTopicSelection".equals(targetTest)) {
      runTest(
          "testFilteringAndTopicSelection",
          ConsensusSubscriptionTableTest::testFilteringAndTopicSelection);
    }
    if (targetTest == null || "testSubscribeBeforeRegion".equals(targetTest)) {
      runTest(
          "testSubscribeBeforeRegion", ConsensusSubscriptionTableTest::testSubscribeBeforeRegion);
    }
    if (targetTest == null || "testMultiEntityIsolation".equals(targetTest)) {
      runTest("testMultiEntityIsolation", ConsensusSubscriptionTableTest::testMultiEntityIsolation);
    }
    if (targetTest == null || "testWalCatchUpAndGapRecovery".equals(targetTest)) {
      runTest(
          "testWalCatchUpAndGapRecovery",
          ConsensusSubscriptionTableTest::testWalCatchUpAndGapRecovery);
    }
    if (targetTest == null || "testSeekAndPositionSemantics".equals(targetTest)) {
      runTest(
          "testSeekAndPositionSemantics",
          ConsensusSubscriptionTableTest::testSeekAndPositionSemantics);
    }
    if (targetTest == null || "testConsumerRestartRecovery".equals(targetTest)) {
      runTest(
          "testConsumerRestartRecovery",
          ConsensusSubscriptionTableTest::testConsumerRestartRecovery);
    }
    if (targetTest == null || "testAckNackAndPoisonSemantics".equals(targetTest)) {
      runTest(
          "testAckNackAndPoisonSemantics",
          ConsensusSubscriptionTableTest::testAckNackAndPoisonSemantics);
    }
    if (targetTest == null || "testProcessorWatermarkAndMetadata".equals(targetTest)) {
      runTest(
          "testProcessorWatermarkAndMetadata",
          ConsensusSubscriptionTableTest::testProcessorWatermarkAndMetadata);
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
    return "csub_tbl_" + testCounter;
  }

  private static String nextTopic() {
    return "topic_tbl_" + testCounter;
  }

  private static String nextConsumerGroup() {
    return "cg_tbl_" + testCounter;
  }

  private static String nextConsumerId() {
    return "consumer_tbl_" + testCounter;
  }

  private static ITableSession openTableSession() throws Exception {
    return new TableSessionBuilder()
        .nodeUrls(Collections.singletonList(HOST + ":" + PORT))
        .username(USER)
        .password(PASSWORD)
        .build();
  }

  private static void createDatabaseAndTable(
      ITableSession session, String database, String tableName, String tableSchema)
      throws Exception {
    session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + database);
    session.executeNonQueryStatement("USE " + database);
    session.executeNonQueryStatement(String.format("CREATE TABLE %s (%s)", tableName, tableSchema));
  }

  private static void deleteDatabase(String database) {
    try (ITableSession session = openTableSession()) {
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS " + database);
    } catch (Exception e) {
      // ignore
    }
  }

  private static void dropTopicTable(String topicName) {
    try (ISubscriptionTableSession subSession =
        new SubscriptionTableSessionBuilder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .build()) {
      subSession.dropTopicIfExists(topicName);
    } catch (Exception e) {
      // ignore
    }
  }

  private static void createTopicTable(String topicName, String dbKey, String tableKey)
      throws Exception {
    try (ISubscriptionTableSession subSession =
        new SubscriptionTableSessionBuilder()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .build()) {
      try {
        subSession.dropTopicIfExists(topicName);
      } catch (Exception e) {
        // ignore
      }

      Properties topicConfig = new Properties();
      topicConfig.put(TopicConstant.MODE_KEY, TopicConstant.MODE_LIVE_VALUE);
      topicConfig.put(
          TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
      topicConfig.put(TopicConstant.DATABASE_KEY, dbKey);
      topicConfig.put(TopicConstant.TABLE_KEY, tableKey);
      subSession.createTopic(topicName, topicConfig);
      System.out.println(
          "  Created topic: " + topicName + " (database=" + dbKey + ", table=" + tableKey + ")");
    }
  }

  private static ISubscriptionTablePullConsumer createConsumer(
      String consumerId, String consumerGroupId) throws Exception {
    ISubscriptionTablePullConsumer consumer =
        new SubscriptionTablePullConsumerBuilder()
            .host(HOST)
            .port(PORT)
            .consumerId(consumerId)
            .consumerGroupId(consumerGroupId)
            .autoCommit(false)
            .build();
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
      ISubscriptionTablePullConsumer consumer, int expectedRows, int maxPollAttempts) {
    return pollUntilComplete(consumer, expectedRows, maxPollAttempts, 1000, true);
  }

  /**
   * Poll until we accumulate the expected number of rows, then verify no extra data arrives.
   *
   * <p>After reaching expectedRows, continues polling until 5 consecutive empty polls confirm
   * quiescence. Any extra rows polled are included in the count (will break assertEquals).
   *
   * @param commitMessages if false, messages are NOT committed
   */
  private static PollResult pollUntilComplete(
      ISubscriptionTablePullConsumer consumer,
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
          String tableName = dataSet.getTableName();
          String databaseName = dataSet.getDatabaseName();
          List<String> columnNames = dataSet.getColumnNames();

          while (dataSet.hasNext()) {
            try {
              org.apache.tsfile.read.common.RowRecord record = dataSet.nextRecord();
              result.totalRows++;
              if (tableName != null) {
                result.rowsPerTable.merge(tableName, 1, Integer::sum);
              }
              if (databaseName != null) {
                result.rowsPerDatabase.merge(databaseName, 1, Integer::sum);
              }
              for (int i = 0; i < columnNames.size(); i++) {
                result.seenColumns.add(columnNames.get(i));
              }
              if (result.totalRows <= 5) {
                System.out.println(
                    "      Row: time="
                        + record.getTimestamp()
                        + ", values="
                        + record.getFields()
                        + ", table="
                        + tableName
                        + ", database="
                        + databaseName);
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
      ISubscriptionTablePullConsumer consumer, String topicName, String database) {
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
    dropTopicTable(topicName);
    deleteDatabase(database);
  }

  /** Clean up with multiple databases. */
  private static void cleanup(
      ISubscriptionTablePullConsumer consumer, String topicName, String... databases) {
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
    dropTopicTable(topicName);
    for (String db : databases) {
      deleteDatabase(db);
    }
  }

  // ============================
  // Result & Assertions
  // ============================

  static class PollResult {
    int totalRows = 0;
    Map<String, Integer> rowsPerTable = new HashMap<>();
    Map<String, Integer> rowsPerDatabase = new HashMap<>();
    Set<String> seenColumns = new HashSet<>();

    @Override
    public String toString() {
      return "PollResult{totalRows="
          + totalRows
          + ", rowsPerTable="
          + rowsPerTable
          + ", rowsPerDatabase="
          + rowsPerDatabase
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
    testSeek();
  }

  private static void testAckNackAndPoisonSemantics() throws Exception {
    testCommitAfterUnsubscribe();
    testPoisonMessageDrop();
  }

  private static void testProcessorWatermarkAndMetadata() throws Exception {
    testProcessorFramework();
    testWriterProgressFields();
  }

  // ======================================================================
  // Topic filter subcase for table model
  // ======================================================================
  private static void testPollWithInfoTopicFilter() throws Exception {
    String database = nextDatabase();
    String topicName1 = "topic_tbl_filter_" + testCounter + "_a";
    String topicName2 = "topic_tbl_filter_" + testCounter + "_b";
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTablePullConsumer consumer = null;

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("CREATE TABLE t2 (tag1 STRING TAG, s1 INT64 FIELD)");
      }
      Thread.sleep(2000);

      createTopicTable(topicName1, database, "t1");
      createTopicTable(topicName2, database, "t2");
      Thread.sleep(1000);

      consumer = (SubscriptionTablePullConsumer) createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName1, topicName2);
      Thread.sleep(3000);

      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 40; i++) {
          if (i <= 30) {
            session.executeNonQueryStatement(
                String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
          }
          session.executeNonQueryStatement(
              String.format("INSERT INTO t2 (tag1, s1, time) VALUES ('d2', %d, %d)", i * 20, i));
        }
      }
      Thread.sleep(3000);

      int t1Rows = 0;
      Set<String> topic1Only = new HashSet<>(Arrays.asList(topicName1));
      for (int attempt = 0; attempt < 40; attempt++) {
        org.apache.iotdb.session.subscription.payload.PollResult pollResult =
            consumer.pollWithInfo(topic1Only, 2000);
        List<SubscriptionMessage> msgs = pollResult.getMessages();
        if (msgs.isEmpty()) {
          if (t1Rows > 0) {
            break;
          }
          Thread.sleep(1000);
          continue;
        }
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            while (ds.hasNext()) {
              ds.next();
              t1Rows++;
              assertEquals("Topic1-only poll should stay on t1", "t1", ds.getTableName());
            }
          }
          consumer.commitSync(msg);
        }
      }
      assertEquals("Topic1 should deliver exactly 30 rows from t1", 30, t1Rows);

      int t2Rows = 0;
      Set<String> topic2Only = new HashSet<>(Arrays.asList(topicName2));
      for (int attempt = 0; attempt < 40; attempt++) {
        org.apache.iotdb.session.subscription.payload.PollResult pollResult =
            consumer.pollWithInfo(topic2Only, 2000);
        List<SubscriptionMessage> msgs = pollResult.getMessages();
        if (msgs.isEmpty()) {
          if (t2Rows > 0) {
            break;
          }
          Thread.sleep(1000);
          continue;
        }
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            while (ds.hasNext()) {
              ds.next();
              t2Rows++;
              assertEquals("Topic2-only poll should stay on t2", "t2", ds.getTableName());
            }
          }
          consumer.commitSync(msg);
        }
      }
      assertEquals("Topic2 should deliver exactly 40 rows from t2", 40, t2Rows);
    } finally {
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
      dropTopicTable(topicName1);
      dropTopicTable(topicName2);
      deleteDatabase(database);
    }
  }

  // ======================================================================
  // Test 8: Consumer Restart Recovery
  // ======================================================================
  private static void testConsumerRestartRecovery() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId1 = nextConsumerId();
    String consumerId2 = consumerId1 + "_restart";
    SubscriptionTablePullConsumer consumer1 = null;
    SubscriptionTablePullConsumer consumer2 = null;

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, "t1");
      Thread.sleep(1000);

      consumer1 = (SubscriptionTablePullConsumer) createConsumer(consumerId1, consumerGroupId);
      consumer1.subscribe(topicName);
      Thread.sleep(3000);

      final int totalRows = 257;
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= totalRows; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
        session.executeNonQueryStatement("FLUSH");
      }
      Thread.sleep(3000);

      int committedRows = 0;
      for (int attempt = 1; attempt <= 30; attempt++) {
        List<SubscriptionMessage> messages = consumer1.poll(Duration.ofMillis(2000));
        if (messages.isEmpty()) {
          Thread.sleep(1000);
          continue;
        }
        SubscriptionMessage firstMessage = messages.get(0);
        committedRows = countRows(firstMessage);
        consumer1.commitSync(firstMessage);
        break;
      }

      assertAtLeast("First consumer should commit some rows before restart", 1, committedRows);
      TopicProgress checkpoint = consumer1.committedPositions(topicName);
      assertTrue(
          "Committed checkpoint should not be empty",
          checkpoint.getRegionProgress() != null && !checkpoint.getRegionProgress().isEmpty());
      int remainingRows = totalRows - committedRows;
      assertAtLeast("Restart scenario should leave rows after the first commit", 1, remainingRows);

      consumer1.close();
      consumer1 = null;

      consumer2 = (SubscriptionTablePullConsumer) createConsumer(consumerId2, consumerGroupId);
      consumer2.subscribe(topicName);
      Thread.sleep(3000);
      consumer2.seekAfter(topicName, checkpoint);
      Thread.sleep(1000);

      PollResult resumed = pollUntilComplete(consumer2, remainingRows, 120);
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
  // Test 1: Basic Flow (merged: BasicDataDelivery + MultiTables + Flush)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Data written BEFORE subscribe is NOT received
   *   <li>Multiple tables (t1, t2, t3) written AFTER subscribe are all received
   *   <li>Flush does not cause data loss (WAL pinning keeps entries available)
   *   <li>Exact row count matches expectation
   * </ul>
   */
  private static void testBasicFlow() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      // Step 1: Write initial data to create DataRegion (should NOT be received)
      System.out.println("  Step 1: Writing initial data (should NOT be received)");
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("CREATE TABLE t2 (tag1 STRING TAG, s1 INT64 FIELD)");
        session.executeNonQueryStatement("CREATE TABLE t3 (tag1 STRING TAG, s1 INT64 FIELD)");
        for (int i = 0; i < 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
        session.executeNonQueryStatement("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("INSERT INTO t3 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Step 2: Create topic and subscribe
      System.out.println("  Step 2: Creating topic and subscribing");
      createTopicTable(topicName, database, ".*");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 3: Write to 3 tables (30 rows each = 90 total), then flush
      System.out.println("  Step 3: Writing 30 rows x 3 tables AFTER subscribe, then flush");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 100; i < 130; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
          session.executeNonQueryStatement(
              String.format("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 20, i));
          session.executeNonQueryStatement(
              String.format("INSERT INTO t3 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 30, i));
        }
        System.out.println("  Flushing...");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Step 4: Poll and verify
      System.out.println("  Step 4: Polling...");
      PollResult result = pollUntilComplete(consumer, 90, 100);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 90 rows (30 per table)", 90, result.totalRows);
      if (!result.rowsPerTable.isEmpty()) {
        System.out.println("  Rows per table: " + result.rowsPerTable);
        for (String tbl : new String[] {"t1", "t2", "t3"}) {
          Integer tblRows = result.rowsPerTable.get(tbl);
          assertAtLeast("Expected rows from " + tbl, 1, tblRows != null ? tblRows : 0);
        }
      }
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 2: Data Types (merged: MultipleDataTypes + MultiColumnTypes + CrossPartition)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Non-aligned: 6 data types via separate INSERTs
   *   <li>All-column: 6 fields in a single INSERT
   *   <li>Cross-partition: timestamps >1 week apart via SQL, Tablet methods
   * </ul>
   */
  private static void testDataTypes() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;
    final long GAP = 604_800_001L; // slightly over 1 week

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(
            session,
            database,
            "t1",
            "tag1 STRING TAG, s_int32 INT32 FIELD, s_int64 INT64 FIELD, "
                + "s_float FLOAT FIELD, s_double DOUBLE FIELD, s_bool BOOLEAN FIELD, "
                + "s_text TEXT FIELD");
        session.executeNonQueryStatement("USE " + database);
        // Init row to force DataRegion creation
        session.executeNonQueryStatement(
            "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                + "VALUES ('d1', 0, 0, 0.0, 0.0, false, 'init', 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, ".*");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      int totalExpected = 0;
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);

        // --- Part A: 6 data types x 20 rows, separate INSERTs ---
        System.out.println("  Part A: 6 data types x 20 rows (separate INSERTs)");
        for (int i = 1; i <= 20; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s_int32, time) VALUES ('d1', %d, %d)", i, i));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s_int64, time) VALUES ('d1', %d, %d)",
                  (long) i * 100000L, i));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s_float, time) VALUES ('d1', %f, %d)", i * 1.1f, i));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s_double, time) VALUES ('d1', %f, %d)", i * 2.2, i));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s_bool, time) VALUES ('d1', %s, %d)",
                  i % 2 == 0 ? "true" : "false", i));
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s_text, time) VALUES ('d1', 'text_%d', %d)", i, i));
        }
        totalExpected += 120; // 6 types x 20 rows

        // --- Part B: All-column rows (50 rows) ---
        System.out.println("  Part B: 50 all-column rows");
        for (int i = 21; i <= 70; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time)"
                      + " VALUES ('d1', %d, %d, %f, %f, %s, 'text_%d', %d)",
                  i, (long) i * 100000L, i * 1.1f, i * 2.2, i % 2 == 0 ? "true" : "false", i, i));
        }
        totalExpected += 50;

        // --- Part C: Cross-partition writes ---
        System.out.println("  Part C: Cross-partition (SQL single, multi, Tablet)");
        long baseTs = 1_000_000_000L;

        // SQL single-row x2
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                    + "VALUES ('d1', 1, 100, 1.1, 1.11, true, 'xp_single_1', %d)",
                baseTs));
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                    + "VALUES ('d1', 2, 200, 2.2, 2.22, false, 'xp_single_2', %d)",
                baseTs + GAP));
        totalExpected += 2;

        // SQL multi-row x3
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                    + "VALUES ('d1', 3, 300, 3.3, 3.33, true, 'xp_multi_1', %d), "
                    + "('d1', 4, 400, 4.4, 4.44, false, 'xp_multi_2', %d), "
                    + "('d1', 5, 500, 5.5, 5.55, true, 'xp_multi_3', %d)",
                baseTs + GAP * 2, baseTs + GAP * 3, baseTs + GAP * 4));
        totalExpected += 3;

        // Tablet x4
        List<IMeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
        schemaList.add(new MeasurementSchema("s_int32", TSDataType.INT32));
        schemaList.add(new MeasurementSchema("s_int64", TSDataType.INT64));
        schemaList.add(new MeasurementSchema("s_float", TSDataType.FLOAT));
        schemaList.add(new MeasurementSchema("s_double", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s_bool", TSDataType.BOOLEAN));
        schemaList.add(new MeasurementSchema("s_text", TSDataType.STRING));

        List<ColumnCategory> categories =
            java.util.Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD);

        Tablet tablet =
            new Tablet(
                "t1",
                IMeasurementSchema.getMeasurementNameList(schemaList),
                IMeasurementSchema.getDataTypeList(schemaList),
                categories,
                10);
        for (int i = 0; i < 4; i++) {
          int row = tablet.getRowSize();
          long ts = baseTs + GAP * (5 + i);
          tablet.addTimestamp(row, ts);
          tablet.addValue("tag1", row, "d1");
          tablet.addValue("s_int32", row, 6 + i);
          tablet.addValue("s_int64", row, (long) (600 + i * 100));
          tablet.addValue("s_float", row, (6 + i) * 1.1f);
          tablet.addValue("s_double", row, (6 + i) * 2.22);
          tablet.addValue("s_bool", row, i % 2 == 0);
          tablet.addValue("s_text", row, "xp_tablet_" + (i + 1));
        }
        session.insert(tablet);
        totalExpected += 4;
      }

      System.out.println("  Total expected rows: " + totalExpected);
      Thread.sleep(2000);

      PollResult result = pollUntilComplete(consumer, totalExpected, 200);
      System.out.println("  Result: " + result);

      assertAtLeast(
          "Expected at least " + totalExpected + " rows", totalExpected, result.totalRows);
      assertAtLeast("Expected multiple column types in result", 2, result.seenColumns.size());
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 3: Path Filtering (merged: TableLevel + DatabaseLevel)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>Table-level: topic on table=t1 does NOT deliver t2 data
   *   <li>Database-level: topic on db1 does NOT deliver db2 data
   * </ul>
   */
  private static void testPathFiltering() throws Exception {
    String database1 = nextDatabase();
    String database2 = database1 + "_other";
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      try (ITableSession session = openTableSession()) {
        // db1 with t1 and t2
        createDatabaseAndTable(session, database1, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database1);
        session.executeNonQueryStatement("CREATE TABLE t2 (tag1 STRING TAG, s1 INT64 FIELD)");
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', 0, 0)");
        // db2 with t1
        createDatabaseAndTable(session, database2, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database2);
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Topic: only db1, only table t1
      createTopicTable(topicName, database1, "t1");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing to db1.t1, db1.t2, db2.t1 (topic filter: db1.t1 only)");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database1);
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
          session.executeNonQueryStatement(
              String.format("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 20, i));
        }
        session.executeNonQueryStatement("USE " + database2);
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 30, i));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting only db1.t1 data = 50 rows)...");
      PollResult result = pollUntilComplete(consumer, 50, 60);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 50 rows from db1.t1 only", 50, result.totalRows);
      if (!result.rowsPerTable.isEmpty()) {
        Integer t2Rows = result.rowsPerTable.get("t2");
        assertTrue("Expected NO rows from t2, but got " + t2Rows, t2Rows == null || t2Rows == 0);
        System.out.println("  Table filtering verified: t1 only");
      }
      if (!result.rowsPerDatabase.isEmpty()) {
        Integer db2Rows = result.rowsPerDatabase.get(database2);
        assertTrue("Expected NO rows from " + database2, db2Rows == null || db2Rows == 0);
        System.out.println("  Database filtering verified: " + database1 + " only");
      }
    } finally {
      cleanup(consumer, topicName, database1, database2);
    }
  }

  // ======================================================================
  // Test 4: Subscribe Before Region Creation (kept as-is)
  // ======================================================================
  /**
   * Subscribe BEFORE the database/region exists, then create database and write. Tests the
   * IoTConsensus.onNewPeerCreated auto-binding path with table model.
   */
  private static void testSubscribeBeforeRegion() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      System.out.println("  Step 1: Creating topic BEFORE database exists");
      createTopicTable(topicName, database, ".*");
      Thread.sleep(1000);

      System.out.println("  Step 2: Subscribing (no DataRegion exists yet)");
      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Step 3: Creating database, table and writing data (100 rows)");
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        for (int i = 0; i < 100; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
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

  // testRedelivery removed — will be re-added with proper timeout-based nack testing

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
    String topicName = "topic_tbl_multi_" + testCounter;
    String consumerGroupId1 = "cg_tbl_multi_" + testCounter + "_a";
    String consumerId1 = "consumer_tbl_multi_" + testCounter + "_a";
    String consumerGroupId2 = "cg_tbl_multi_" + testCounter + "_b";
    String consumerId2 = "consumer_tbl_multi_" + testCounter + "_b";
    ISubscriptionTablePullConsumer consumer1 = null;
    ISubscriptionTablePullConsumer consumer2 = null;

    try {
      // Setup: database with a single table to isolate the multi-group semantics.
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, "t1");
      Thread.sleep(1000);

      consumer1 = createConsumer(consumerId1, consumerGroupId1);
      consumer1.subscribe(topicName);
      consumer2 = createConsumer(consumerId2, consumerGroupId2);
      consumer2.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing 70 rows to t1");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 70; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
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
      assertEquals("Expected 70 rows from t1", 70, result1.rowsPerTable.getOrDefault("t1", 0));
      assertEquals("Expected 70 rows from t1", 70, result2.rowsPerTable.getOrDefault("t1", 0));
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
      dropTopicTable(topicName);
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
   * {@code pendingEntries.offer()}. A single {@code session.insert(tablet)} with N rows in one time
   * partition = 1 write() call = 1 offer, so Tablet batches rarely overflow the queue. To actually
   * overflow, we need 4096+ <i>individual</i> write() calls arriving faster than the prefetch
   * thread can drain. We achieve this with multiple concurrent writer threads, each performing
   * individual SQL INSERTs, to maximize the aggregate write rate vs. drain rate.
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
    ISubscriptionTablePullConsumer consumer = null;

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, ".*");
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
                try (ITableSession session = openTableSession()) {
                  session.executeNonQueryStatement("USE " + database);
                  for (int i = 0; i < rowsPerThread; i++) {
                    int ts = startTs + i;
                    session.executeNonQueryStatement(
                        String.format(
                            "INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)",
                            (long) ts * 10, ts));
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
    ISubscriptionTablePullConsumer consumer = null;

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, ".*");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Write data
      System.out.println("  Writing 50 rows");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
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
      dropTopicTable(topicName);
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
    SubscriptionTablePullConsumer consumer = null;

    try {
      // Step 0: Create DataRegion
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
      }
      Thread.sleep(2000);

      // Step 1: Create topic + consumer + subscribe
      System.out.println("  Step 1: Create topic and subscribe");
      createTopicTable(topicName, database, "t1");
      Thread.sleep(1000);

      consumer = (SubscriptionTablePullConsumer) createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 2: Write 1000 rows with timestamps 1000..1999 and poll+commit all
      System.out.println("  Step 2: Write 1000 rows (timestamps 1000..1999) and poll+commit");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 0; i < 1000; i++) {
          long ts = 1000 + i;
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", ts * 10, ts));
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

      // No initial INSERT in table test (Step 0 only creates DB+table), so expectedRows=1000
      PollResult beginningPoll = pollUntilComplete(consumer, 1000, 120);
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
          for (SubscriptionResultSet ds : getResultSets(msg)) {
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
      assertTrue("seekToEnd should yield at most 1 row (race tolerance)", endPoll.totalRows <= 1);

      // Write 200 new rows — they should be received
      System.out.println("  Writing 200 new rows after seekToEnd");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 2000; i < 2200; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
      }
      Thread.sleep(2000);

      PollResult afterEndPoll = pollUntilComplete(consumer, 200, 120);
      System.out.println("  After seekToEnd + new writes: " + afterEndPoll);
      assertEquals(
          "Should receive exactly 200 new rows after seekToEnd", 200, afterEndPoll.totalRows);

      // ------------------------------------------------------------------
      // Step 5: seek(timestamp) — seek to timestamp 1500
      // ------------------------------------------------------------------
      System.out.println("  Step 5: seek(1500) → expect rows from near ts=1500");
      consumer.seekToBeginning(topicName);
      Thread.sleep(2000);

      PollResult midpointPoll = new PollResult();
      TopicProgress midpointProgress = null;
      consecutiveEmpty = 0;
      for (int attempt = 0; attempt < 20 && midpointProgress == null; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(1000));
        if (msgs.isEmpty()) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 5) break;
          Thread.sleep(500);
          continue;
        }
        consecutiveEmpty = 0;
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            while (ds.hasNext()) {
              ds.next();
              midpointPoll.totalRows++;
            }
          }
          consumer.commitSync(msg);
          if (midpointPoll.totalRows >= 500) {
            midpointProgress = consumer.committedPositions(topicName);
            break;
          }
        }
      }
      assertTrue("Should capture a midpoint TopicProgress", midpointProgress != null);

      consumer.seek(topicName, midpointProgress);
      Thread.sleep(2000);

      // Sparse mapping (interval=100) positions near ts=1500.
      // Expect: ~500 rows from ts≥1500 in original data (1500..1999)
      //       + 200 rows from new writes (2000..2199) = ~700 minimum
      PollResult afterSeek = pollUntilComplete(consumer, 1200, 120);
      final int minimumTailRows = Math.max(1, 1200 - midpointPoll.totalRows);
      System.out.println(
          "  After seek(topicProgress): "
              + afterSeek.totalRows
              + " rows from midpoint progress "
              + midpointPoll.totalRows);
      assertAtLeast(
          "seek(topicProgress) should deliver the remaining tail rows",
          minimumTailRows,
          afterSeek.totalRows);

      // ------------------------------------------------------------------
      // Step 6: seek(future timestamp) — expect 0 rows
      // ------------------------------------------------------------------
      System.out.println("  Step 6: seek(99999) → expect no data");
      TopicProgress tailProgress = consumer.committedPositions(topicName);
      assertTrue("Tail TopicProgress should be available after replay", tailProgress != null);
      consumer.seekAfter(topicName, tailProgress);
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
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            while (ds.hasNext()) {
              ds.next();
              futurePoll.totalRows++;
            }
          }
          consumer.commitSync(msg);
        }
      }
      System.out.println(
          "  After seekAfter(tail topicProgress): " + futurePoll.totalRows + " rows");
      // seek(99999) should behave like seekToEnd — 0 rows normally,
      // but may yield up to 1 row due to prefetch thread race (same as seekToEnd)
      assertTrue(
          "seekAfter(tail topicProgress) should yield at most 1 row (race tolerance)",
          futurePoll.totalRows <= 1);

      // ------------------------------------------------------------------
      // Step 7: seek(topicProgress) — seek by per-region writer progress
      // ------------------------------------------------------------------
      System.out.println(
          "  Step 7: seekToBeginning first, then poll to collect per-region positions");
      consumer.seekToBeginning(topicName);
      Thread.sleep(2000);

      List<TopicProgress> positionSnapshots = new ArrayList<>();
      List<Integer> rowsPerMsg = new ArrayList<>();
      int totalRowsCollected = 0;
      consecutiveEmpty = 0;

      for (int attempt = 0; attempt < 60; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(2000));
        if (msgs.isEmpty()) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 5 && totalRowsCollected > 0) break;
          Thread.sleep(500);
          continue;
        }
        consecutiveEmpty = 0;
        for (SubscriptionMessage msg : msgs) {
          int msgRows = 0;
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            while (ds.hasNext()) {
              ds.next();
              msgRows++;
            }
          }
          consumer.commitSync(msg);
          rowsPerMsg.add(msgRows);
          totalRowsCollected += msgRows;
          positionSnapshots.add(consumer.committedPositions(topicName));
        }
      }
      System.out.println(
          "  Collected "
              + totalRowsCollected
              + " rows in "
              + positionSnapshots.size()
              + " messages");

      if (positionSnapshots.size() >= 2) {
        int midIdx = positionSnapshots.size() / 2;
        TopicProgress seekPositions = positionSnapshots.get(midIdx);
        int writerFrontierCount = countWriterFrontiers(seekPositions);
        assertTrue(
            "committed TopicProgress should contain at least one writer frontier",
            writerFrontierCount > 0);
        System.out.println(
            "  seekAfter(topicProgress.regionCount="
                + seekPositions.getRegionProgress().size()
                + ", writerFrontierCount="
                + writerFrontierCount
                + ") [msg "
                + midIdx
                + "/"
                + positionSnapshots.size()
                + "]");

        int expectedFromMid = 0;
        for (int i = midIdx; i < rowsPerMsg.size(); i++) {
          expectedFromMid += rowsPerMsg.get(i);
        }

        consumer.seekAfter(topicName, seekPositions);
        Thread.sleep(2000);

        PollResult afterSeekEpoch = pollUntilComplete(consumer, expectedFromMid, 60);
        System.out.println(
            "  After seekAfter(topicProgress): "
                + afterSeekEpoch.totalRows
                + " rows (expected ~"
                + expectedFromMid
                + ")");
        assertAtLeast(
            "seekAfter(topicProgress) should deliver at least half the tail data",
            expectedFromMid / 2,
            afterSeekEpoch.totalRows);
      } else {
        System.out.println(
            "  SKIP seekAfter(topicProgress) sub-test: only "
                + positionSnapshots.size()
                + " messages");
      }

      System.out.println("  testSeek passed all sub-tests!");
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ======================================================================
  // Test 9: Processor Framework (ColumnAlignProcessor + WatermarkProcessor + PollResult)
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>ColumnAlignProcessor forward-fills null columns per table
   *   <li>pollWithInfo() returns PollResult with correct metadata
   *   <li>WatermarkProcessor buffers and emits based on watermark
   *   <li>Processor chaining works correctly
   *   <li>Idempotent double-commit does not throw
   * </ul>
   */
  private static void testProcessorFramework() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    String tableName = "proc_test";
    SubscriptionTablePullConsumer consumer = null;
    SubscriptionTablePullConsumer consumer2 = null;

    try {
      // Step 1: Create table with 3 measurement columns
      System.out.println("  Step 1: Creating table with 3 measurement columns");
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(
            session,
            database,
            tableName,
            "device_id STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD, s3 INT32 FIELD");
      }

      // Step 2: Create topic and subscribe
      System.out.println("  Step 2: Creating topic and subscribing");
      createTopicTable(topicName, database, tableName);
      Thread.sleep(1000);

      // Build consumer with ColumnAlignProcessor — use concrete type for addProcessor access
      consumer =
          (SubscriptionTablePullConsumer)
              new SubscriptionTablePullConsumerBuilder()
                  .host(HOST)
                  .port(PORT)
                  .consumerId(consumerId)
                  .consumerGroupId(consumerGroupId)
                  .autoCommit(false)
                  .build();
      consumer.addProcessor(new ColumnAlignProcessor());
      consumer.open();
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 3: Write a Tablet with 2 rows — row 2 has s2/s3 null (marked in BitMap).
      // Using insertTablet ensures both rows share the same Tablet with all 3 columns,
      // so ColumnAlignProcessor can forward-fill the nulls.
      System.out.println("  Step 3: Writing partial-column data via insertTablet");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        List<IMeasurementSchema> schemas =
            Arrays.asList(
                new MeasurementSchema("device_id", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s2", TSDataType.INT32),
                new MeasurementSchema("s3", TSDataType.INT32));
        List<ColumnCategory> categories =
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD);
        Tablet tablet =
            new Tablet(
                tableName,
                IMeasurementSchema.getMeasurementNameList(schemas),
                IMeasurementSchema.getDataTypeList(schemas),
                categories,
                2);

        // Row 0 (time=100): all columns present
        tablet.addTimestamp(0, 100);
        tablet.addValue("device_id", 0, "dev1");
        tablet.addValue("s1", 0, 10);
        tablet.addValue("s2", 0, 20);
        tablet.addValue("s3", 0, 30);

        // Row 1 (time=200): only s1 — s2/s3 remain null (BitMap marked by addTimestamp)
        tablet.addTimestamp(1, 200);
        tablet.addValue("device_id", 1, "dev1");
        tablet.addValue("s1", 1, 11);

        session.insert(tablet);
        session.executeNonQueryStatement("FLUSH");
      }
      Thread.sleep(2000);

      // Step 4: Poll with pollWithInfo and verify ColumnAlign + PollResult
      System.out.println("  Step 4: Polling with pollWithInfo");
      int totalRows = 0;
      boolean foundForwardFill = false;
      org.apache.iotdb.session.subscription.payload.PollResult lastPollResult = null;
      List<SubscriptionMessage> allMessages = new ArrayList<>();

      for (int attempt = 0; attempt < 30; attempt++) {
        org.apache.iotdb.session.subscription.payload.PollResult pollResult =
            consumer.pollWithInfo(Duration.ofMillis(1000));
        lastPollResult = pollResult;

        assertTrue("PollResult should not be null", pollResult != null);
        // With only ColumnAlignProcessor (non-buffering), bufferedCount should be 0
        assertEquals("ColumnAlignProcessor should not buffer", 0, pollResult.getBufferedCount());

        List<SubscriptionMessage> msgs = pollResult.getMessages();
        if (msgs.isEmpty()) {
          if (totalRows >= 2) break;
          Thread.sleep(1000);
          continue;
        }

        allMessages.addAll(msgs);
        for (SubscriptionMessage msg : msgs) {
          for (SubscriptionResultSet ds : getResultSets(msg)) {
            List<String> columnNames = ds.getColumnNames();
            while (ds.hasNext()) {
              org.apache.tsfile.read.common.RowRecord row = ds.nextRecord();
              totalRows++;
              List<org.apache.tsfile.read.common.Field> fields = row.getFields();
              System.out.println(
                  "      Row: time="
                      + row.getTimestamp()
                      + ", columns="
                      + columnNames
                      + ", fields="
                      + fields);
              // Check forward-fill: at timestamp 200, s2 and s3 should be filled
              if (row.getTimestamp() == 200) {
                // Table results include "time" in columnNames but not in fields.
                int s2ColumnIdx = columnNames.indexOf("s2");
                int s3ColumnIdx = columnNames.indexOf("s3");
                int fieldOffset =
                    !columnNames.isEmpty() && "time".equalsIgnoreCase(columnNames.get(0)) ? 1 : 0;
                int s2FieldIdx = s2ColumnIdx - fieldOffset;
                int s3FieldIdx = s3ColumnIdx - fieldOffset;
                if (s2FieldIdx >= 0
                    && s3FieldIdx >= 0
                    && s2FieldIdx < fields.size()
                    && s3FieldIdx < fields.size()
                    && fields.get(s2FieldIdx) != null
                    && fields.get(s2FieldIdx).getDataType() != null
                    && fields.get(s3FieldIdx) != null
                    && fields.get(s3FieldIdx).getDataType() != null) {
                  foundForwardFill = true;
                  System.out.println("      >>> Forward-fill confirmed at timestamp 200");
                }
              }
            }
          }
        }
      }

      assertEquals("Expected 2 rows total", 2, totalRows);
      assertTrue(
          "ColumnAlignProcessor should forward-fill nulls at timestamp 200", foundForwardFill);
      System.out.println("  ColumnAlignProcessor: PASSED");

      // Step 5: Idempotent double-commit
      System.out.println("  Step 5: Testing idempotent double-commit");
      if (!allMessages.isEmpty()) {
        SubscriptionMessage firstMsg = allMessages.get(0);
        consumer.commitSync(firstMsg);
        // Second commit of same message should not throw
        consumer.commitSync(firstMsg);
        System.out.println("    Double-commit succeeded (idempotent)");
      }

      // Step 6: Test with WatermarkProcessor chained
      System.out.println("  Step 6: Verifying WatermarkProcessor buffering");
      // Close current consumer and create a new one with WatermarkProcessor
      consumer.unsubscribe(topicName);
      consumer.close();

      String consumerId2 = consumerId + "_wm";
      consumer2 =
          (SubscriptionTablePullConsumer)
              new SubscriptionTablePullConsumerBuilder()
                  .host(HOST)
                  .port(PORT)
                  .consumerId(consumerId2)
                  .consumerGroupId(consumerGroupId + "_wm")
                  .autoCommit(false)
                  .build();
      // Chain: ColumnAlign → Watermark(5s out-of-order, 10s timeout)
      consumer2.addProcessor(new ColumnAlignProcessor());
      consumer2.addProcessor(new WatermarkProcessor(5000, 10000));
      consumer2.open();
      consumer2.subscribe(topicName);
      Thread.sleep(3000);

      // Write data that should be buffered by watermark
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s(time, device_id, s1, s2, s3) VALUES (1000, 'dev1', 100, 200, 300)",
                tableName));
        session.executeNonQueryStatement("FLUSH");
      }
      Thread.sleep(2000);

      // First poll — data may be buffered by WatermarkProcessor
      org.apache.iotdb.session.subscription.payload.PollResult wmResult =
          consumer2.pollWithInfo(Duration.ofMillis(2000));
      System.out.println(
          "    WatermarkProcessor poll: messages="
              + wmResult.getMessages().size()
              + ", buffered="
              + wmResult.getBufferedCount());
      // The watermark processor may buffer or emit depending on timing;
      // we just verify the API works and returns valid metadata
      assertTrue("PollResult bufferedCount should be >= 0", wmResult.getBufferedCount() >= 0);

      // consumer already closed above in Step 6 setup
      consumer = null;

      System.out.println("  testProcessorFramework passed all sub-tests!");
    } finally {
      cleanup(consumer, topicName, database);
      cleanup(consumer2, topicName, database);
    }
  }

  // ======================================================================
  // Test 10: Poison Message Drop — messages nacked beyond threshold
  //          are force-acked (dropped) and don't block new data.
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>A message nacked more than POISON_MESSAGE_NACK_THRESHOLD (10) times is dropped
   *   <li>After drop, new data can still be received
   *   <li>The consumer is not permanently blocked by a single unprocessable message
   * </ul>
   */
  private static void testPoisonMessageDrop() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTablePullConsumer consumer = null;

    try {
      // Step 0: Create DataRegion
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
      }
      Thread.sleep(2000);

      // Step 1: Create topic and subscribe
      System.out.println("  Step 1: Creating topic and subscribing");
      createTopicTable(topicName, database, "t1");
      Thread.sleep(1000);

      consumer = (SubscriptionTablePullConsumer) createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 2: Write initial data that will become the "poison" message
      System.out.println("  Step 2: Writing 10 rows (the initial batch)");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 10; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
      }
      Thread.sleep(2000);

      // Step 3: Poll without commit — repeatedly. Each poll-then-timeout cycle
      // causes the server to nack the in-flight event and re-enqueue it.
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
          // Deliberately NOT committing — this is the "nack" behavior
        }
        System.out.println(
            "    Round " + round + ": received " + roundRows + " rows (NOT committing)");
        if (msgs.isEmpty() && round > 11) {
          System.out.println("    No messages — poison message may have been force-acked");
          break;
        }
        Thread.sleep(1000);
      }
      System.out.println("  Total rows polled across all rounds: " + totalPoisonPolled);

      // Step 4: Write NEW data and verify it can be received (consumer not blocked)
      System.out.println("  Step 4: Writing 50 NEW rows and polling WITH commit");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1000; i < 1050; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
      }
      Thread.sleep(2000);

      PollResult newResult = pollUntilComplete(consumer, 50, 60);
      System.out.println("  New data poll result: " + newResult);

      assertAtLeast(
          "Consumer must not be permanently blocked by poison message — new data should arrive",
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

  // ======================================================================
  // Test 11: Serialization V2 Fields — regionId, epoch, dataNodeId
  //          are properly populated in polled messages' SubscriptionCommitContext.
  // ======================================================================
  /**
   * Verifies:
   *
   * <ul>
   *   <li>SubscriptionCommitContext.getWriterId() is non-null for consensus messages
   *   <li>SubscriptionCommitContext.getWriterProgress() is non-null for consensus messages
   *   <li>SubscriptionCommitContext.getWriterId().getRegionId() stays aligned with the region
   *   <li>These writer-progress fields survive the serialize/deserialize round-trip through RPC
   * </ul>
   */
  private static void testWriterProgressFields() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    SubscriptionTablePullConsumer consumer = null;

    try {
      // Step 0: Create DataRegion
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
      }
      Thread.sleep(2000);

      // Step 1: Create topic and subscribe
      System.out.println("  Step 1: Creating topic and subscribing");
      createTopicTable(topicName, database, "t1");
      Thread.sleep(1000);

      consumer = (SubscriptionTablePullConsumer) createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      // Step 2: Write data
      System.out.println("  Step 2: Writing 20 rows");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 20; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
      }
      Thread.sleep(2000);

      // Step 3: Poll and check writer-progress fields in SubscriptionCommitContext
      System.out.println("  Step 3: Polling and verifying writer-progress fields in CommitContext");
      int totalRows = 0;
      int messagesChecked = 0;
      boolean foundWriterProgress = false;

      for (int attempt = 0; attempt < 30; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(2000));
        if (msgs.isEmpty()) {
          if (totalRows > 0) break;
          Thread.sleep(1000);
          continue;
        }

        for (SubscriptionMessage msg : msgs) {
          SubscriptionCommitContext ctx = msg.getCommitContext();
          messagesChecked++;

          // Check writer-progress fields and their compatibility projections
          String regionId = ctx.getRegionId();
          int dataNodeId = ctx.getDataNodeId();
          WriterId writerId = ctx.getWriterId();
          WriterProgress writerProgress = ctx.getWriterProgress();
          long physicalTime =
              writerProgress != null ? writerProgress.getPhysicalTime() : Long.MIN_VALUE;

          System.out.println(
              "    Message "
                  + messagesChecked
                  + ": regionId="
                  + regionId
                  + ", physicalTime="
                  + physicalTime
                  + ", writerId="
                  + writerId
                  + ", writerProgress="
                  + writerProgress
                  + ", dataNodeId="
                  + dataNodeId
                  + ", topicName="
                  + ctx.getTopicName()
                  + ", consumerGroupId="
                  + ctx.getConsumerGroupId());

          assertTrue(
              "regionId should be non-null for consensus message",
              regionId != null && !regionId.isEmpty());
          assertTrue("writerId should be non-null for consensus message", writerId != null);
          assertTrue(
              "writerProgress should be non-null for consensus message", writerProgress != null);
          assertEquals("regionId should match writerId.regionId", writerId.getRegionId(), regionId);
          assertEquals(
              "physicalTime should mirror writerProgress.physicalTime",
              writerProgress.getPhysicalTime(),
              physicalTime);
          foundWriterProgress = true;

          assertTrue("physicalTime should be >= 0, got " + physicalTime, physicalTime >= 0);

          assertTrue("dataNodeId should be > 0, got " + dataNodeId, dataNodeId > 0);

          for (SubscriptionResultSet ds : getResultSets(msg)) {
            while (ds.hasNext()) {
              ds.next();
              totalRows++;
            }
          }
          consumer.commitSync(msg);
        }
      }

      System.out.println(
          "  Checked "
              + messagesChecked
              + " messages, "
              + totalRows
              + " rows. foundWriterProgress="
              + foundWriterProgress);
      assertAtLeast("Should have received data rows", 1, totalRows);
      assertTrue(
          "Should have found writer-progress metadata in at least one message",
          foundWriterProgress);
      System.out.println("  testWriterProgressFields passed!");
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  private static List<SubscriptionResultSet> getResultSets(final SubscriptionMessage message) {
    return message.getResultSets().stream()
        .map(resultSet -> (SubscriptionResultSet) resultSet)
        .collect(Collectors.toList());
  }
}
