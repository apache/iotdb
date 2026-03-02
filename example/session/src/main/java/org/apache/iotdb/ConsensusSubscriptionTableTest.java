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
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

    if (targetTest == null || "testBasicDataDelivery".equals(targetTest)) {
      runTest("testBasicDataDelivery", ConsensusSubscriptionTableTest::testBasicDataDelivery);
    }
    if (targetTest == null || "testMultipleDataTypes".equals(targetTest)) {
      runTest("testMultipleDataTypes", ConsensusSubscriptionTableTest::testMultipleDataTypes);
    }
    if (targetTest == null || "testTableLevelFiltering".equals(targetTest)) {
      runTest("testTableLevelFiltering", ConsensusSubscriptionTableTest::testTableLevelFiltering);
    }
    if (targetTest == null || "testDatabaseLevelFiltering".equals(targetTest)) {
      runTest(
          "testDatabaseLevelFiltering", ConsensusSubscriptionTableTest::testDatabaseLevelFiltering);
    }
    if (targetTest == null || "testSubscribeBeforeRegion".equals(targetTest)) {
      runTest(
          "testSubscribeBeforeRegion", ConsensusSubscriptionTableTest::testSubscribeBeforeRegion);
    }
    if (targetTest == null || "testMultipleTablesAggregation".equals(targetTest)) {
      runTest(
          "testMultipleTablesAggregation",
          ConsensusSubscriptionTableTest::testMultipleTablesAggregation);
    }
    if (targetTest == null || "testMultiColumnTypes".equals(targetTest)) {
      runTest("testMultiColumnTypes", ConsensusSubscriptionTableTest::testMultiColumnTypes);
    }
    if (targetTest == null || "testPollWithoutCommit".equals(targetTest)) {
      runTest("testPollWithoutCommit", ConsensusSubscriptionTableTest::testPollWithoutCommit);
    }
    if (targetTest == null || "testMultiConsumerGroupIndependent".equals(targetTest)) {
      runTest(
          "testMultiConsumerGroupIndependent",
          ConsensusSubscriptionTableTest::testMultiConsumerGroupIndependent);
    }
    if (targetTest == null || "testMultiTopicSubscription".equals(targetTest)) {
      runTest(
          "testMultiTopicSubscription", ConsensusSubscriptionTableTest::testMultiTopicSubscription);
    }
    if (targetTest == null || "testFlushDataDelivery".equals(targetTest)) {
      runTest("testFlushDataDelivery", ConsensusSubscriptionTableTest::testFlushDataDelivery);
    }
    if (targetTest == null || "testCrossPartitionMultiWrite".equals(targetTest)) {
      runTest(
          "testCrossPartitionMultiWrite",
          ConsensusSubscriptionTableTest::testCrossPartitionMultiWrite);
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
        for (SubscriptionSessionDataSet dataSet : message.getSessionDataSetsHandler()) {
          String tableName = dataSet.getTableName();
          String databaseName = dataSet.getDatabaseName();
          List<String> columnNames = dataSet.getColumnNames();

          while (dataSet.hasNext()) {
            org.apache.tsfile.read.common.RowRecord record = dataSet.next();
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
   * Verifies the basic consensus subscription flow with table model: write before subscribe (not
   * received), write after subscribe (received), and no extra data beyond expectation.
   */
  private static void testBasicDataDelivery() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      // Step 1: Write initial data to create DataRegion
      System.out.println("  Step 1: Writing initial data (should NOT be received)");
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(
            session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD");
        session.executeNonQueryStatement("USE " + database);
        for (int i = 0; i < 50; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s1, s2, time) VALUES ('d1', %d, %f, %d)",
                  i * 10, i * 1.5, i));
        }
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

      // Step 3: Write new data AFTER subscription
      System.out.println("  Step 3: Writing new data AFTER subscription (100 rows)");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 100; i < 200; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s1, s2, time) VALUES ('d1', %d, %f, %d)",
                  i * 10, i * 1.5, i));
        }
      }
      Thread.sleep(2000);

      // Step 4: Poll and verify exact count
      System.out.println("  Step 4: Polling...");
      PollResult result = pollUntilComplete(consumer, 100, 100);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 100 rows from post-subscribe writes", 100, result.totalRows);
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 2: Multiple Data Types
  // ============================
  /**
   * Writes data with multiple data types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT) using
   * separate INSERT statements per type (one field per INSERT), and verifies all types are
   * delivered.
   */
  private static void testMultipleDataTypes() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

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
        // Write initial row to create DataRegion
        session.executeNonQueryStatement(
            "INSERT INTO t1 (tag1, s_int32, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, ".*");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing data with 6 data types x 20 rows each");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
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
  // Test 3: Table-Level Filtering
  // ============================
  /**
   * Creates a topic that only matches table "t1" via TABLE_KEY. Verifies that data written to t2 is
   * NOT delivered.
   */
  private static void testTableLevelFiltering() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("CREATE TABLE t2 (tag1 STRING TAG, s1 INT64 FIELD)");
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Topic matches only table t1
      createTopicTable(topicName, database, "t1");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing to both t1 and t2 (topic filter: t1 only)");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
          session.executeNonQueryStatement(
              String.format("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 20, i));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting only t1 data)...");
      PollResult result = pollUntilComplete(consumer, 50, 60);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 50 rows from t1 only", 50, result.totalRows);
      if (!result.rowsPerTable.isEmpty()) {
        Integer t2Rows = result.rowsPerTable.get("t2");
        assertTrue("Expected NO rows from t2, but got " + t2Rows, t2Rows == null || t2Rows == 0);
        Integer t1Rows = result.rowsPerTable.get("t1");
        assertAtLeast("Expected t1 rows", 1, t1Rows != null ? t1Rows : 0);
        System.out.println(
            "  Table filtering verified: t1=" + t1Rows + " rows, t2=" + t2Rows + " rows");
      }
    } finally {
      cleanup(consumer, topicName, database);
    }
  }

  // ============================
  // Test 4: Database-Level Filtering
  // ============================
  /**
   * Creates a topic that only matches database db1 via DATABASE_KEY. Verifies that data written to
   * db2 is NOT delivered.
   */
  private static void testDatabaseLevelFiltering() throws Exception {
    String database1 = nextDatabase();
    String database2 = database1 + "_other";
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database1, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        createDatabaseAndTable(session, database2, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database1);
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("USE " + database2);
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Topic matches only database1
      createTopicTable(topicName, database1, ".*");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println(
          "  Writing to both "
              + database1
              + " and "
              + database2
              + " (topic filter: "
              + database1
              + " only)");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database1);
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
        session.executeNonQueryStatement("USE " + database2);
        for (int i = 100; i < 150; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 20, i));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting only " + database1 + " data)...");
      PollResult result = pollUntilComplete(consumer, 50, 60);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 50 rows from " + database1 + " only", 50, result.totalRows);
      if (!result.rowsPerDatabase.isEmpty()) {
        Integer db2Rows = result.rowsPerDatabase.get(database2);
        assertTrue(
            "Expected NO rows from " + database2 + ", but got " + db2Rows,
            db2Rows == null || db2Rows == 0);
        Integer db1Rows = result.rowsPerDatabase.get(database1);
        assertAtLeast("Expected " + database1 + " rows", 1, db1Rows != null ? db1Rows : 0);
        System.out.println(
            "  Database filtering verified: "
                + database1
                + "="
                + db1Rows
                + " rows, "
                + database2
                + "="
                + db2Rows
                + " rows");
      }
    } finally {
      cleanup(consumer, topicName, database1, database2);
    }
  }

  // ============================
  // Test 5: Subscribe Before Region Creation
  // ============================
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
  // Test 6: Multiple Tables Aggregation
  // ============================
  /** Writes to t1, t2, t3 and verifies all are received via a broad topic TABLE_KEY. */
  private static void testMultipleTablesAggregation() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("CREATE TABLE t2 (tag1 STRING TAG, s1 INT64 FIELD)");
        session.executeNonQueryStatement("CREATE TABLE t3 (tag1 STRING TAG, s1 INT64 FIELD)");
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("INSERT INTO t3 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, ".*");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName);
      Thread.sleep(3000);

      System.out.println("  Writing to 3 tables (t1, t2, t3), 30 rows each");
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
      }
      Thread.sleep(2000);

      System.out.println("  Polling (expecting 90 total from 3 tables)...");
      PollResult result = pollUntilComplete(consumer, 90, 100);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 90 rows total (30 per table)", 90, result.totalRows);
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

  // ============================
  // Test 7: Multi Column Types (Table Model Equivalent of Aligned Timeseries)
  // ============================
  /**
   * Creates a table with 6 different FIELD types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT) and
   * writes rows where each INSERT contains ALL columns. Verifies all rows and all column types are
   * delivered correctly. This is the table model equivalent of the aligned timeseries test.
   */
  private static void testMultiColumnTypes() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      // Create table with multiple field types
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(
            session,
            database,
            "t1",
            "tag1 STRING TAG, s_int32 INT32 FIELD, s_int64 INT64 FIELD, "
                + "s_float FLOAT FIELD, s_double DOUBLE FIELD, s_bool BOOLEAN FIELD, "
                + "s_text TEXT FIELD");
        session.executeNonQueryStatement("USE " + database);
        // Write initial row to force DataRegion creation
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

      // Write 50 rows, each with all 6 data types in a single INSERT
      System.out.println("  Writing 50 rows with 6 data types per row");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format(
                  "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time)"
                      + " VALUES ('d1', %d, %d, %f, %f, %s, 'text_%d', %d)",
                  i, (long) i * 100000L, i * 1.1f, i * 2.2, i % 2 == 0 ? "true" : "false", i, i));
        }
      }
      Thread.sleep(2000);

      System.out.println("  Polling...");
      PollResult result = pollUntilComplete(consumer, 50, 70);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 50 rows with all field types", 50, result.totalRows);
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

      // Write 50 rows
      final int totalRows = 50;
      System.out.println("  Writing " + totalRows + " rows");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= totalRows; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
        }
      }
      Thread.sleep(3000);

      // State machine: alternate between skip-commit and direct-commit.
      int totalRowsCommitted = 0;
      int roundNumber = 0;
      boolean hasPending = false;
      List<Long> pendingTimestamps = new ArrayList<>();
      Set<Long> allCommittedTimestamps = new HashSet<>();
      int redeliveryCount = 0;

      for (int attempt = 0; attempt < 200 && totalRowsCommitted < totalRows; attempt++) {
        List<SubscriptionMessage> msgs = consumer.poll(Duration.ofMillis(5000));
        if (msgs.isEmpty()) {
          Thread.sleep(1000);
          continue;
        }

        for (SubscriptionMessage msg : msgs) {
          // Extract ALL timestamps from this event
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
    String consumerGroupId1 = "cg_tbl_multi_" + testCounter + "_a";
    String consumerId1 = "consumer_tbl_multi_" + testCounter + "_a";
    String consumerGroupId2 = "cg_tbl_multi_" + testCounter + "_b";
    String consumerId2 = "consumer_tbl_multi_" + testCounter + "_b";
    ISubscriptionTablePullConsumer consumer1 = null;
    ISubscriptionTablePullConsumer consumer2 = null;

    try {
      // Create database and initial data
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      createTopicTable(topicName, database, ".*");
      Thread.sleep(1000);

      // Two consumers in different groups both subscribe to the same topic
      consumer1 = createConsumer(consumerId1, consumerGroupId1);
      consumer1.subscribe(topicName);
      consumer2 = createConsumer(consumerId2, consumerGroupId2);
      consumer2.subscribe(topicName);
      Thread.sleep(3000);

      // Write 50 rows
      System.out.println("  Writing 50 rows");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
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
      dropTopicTable(topicName);
      deleteDatabase(database);
    }
  }

  // ============================
  // Test 10: Multi Topic Subscription
  // ============================
  /**
   * One consumer subscribes to two different topics with different TABLE_KEY filters. Verifies that
   * each topic delivers only its matching data, and no cross-contamination occurs.
   */
  private static void testMultiTopicSubscription() throws Exception {
    String database = nextDatabase();
    String topicName1 = "topic_tbl_multi_" + testCounter + "_a";
    String topicName2 = "topic_tbl_multi_" + testCounter + "_b";
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    try {
      // Create database with two tables
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, "t1", "tag1 STRING TAG, s1 INT64 FIELD");
        session.executeNonQueryStatement("USE " + database);
        session.executeNonQueryStatement("CREATE TABLE t2 (tag1 STRING TAG, s1 INT64 FIELD)");
        session.executeNonQueryStatement("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', 0, 0)");
        session.executeNonQueryStatement("flush");
      }
      Thread.sleep(2000);

      // Topic 1: covers t1 only
      createTopicTable(topicName1, database, "t1");
      // Topic 2: covers t2 only
      createTopicTable(topicName2, database, "t2");
      Thread.sleep(1000);

      consumer = createConsumer(consumerId, consumerGroupId);
      consumer.subscribe(topicName1, topicName2);
      Thread.sleep(3000);

      // Write 30 rows to t1 and 40 rows to t2
      System.out.println("  Writing 30 rows to t1, 40 rows to t2");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 40; i++) {
          if (i <= 30) {
            session.executeNonQueryStatement(
                String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
          }
          session.executeNonQueryStatement(
              String.format("INSERT INTO t2 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 20, i));
        }
      }
      Thread.sleep(2000);

      // Poll all data — should get t1 rows (via topic1) + t2 rows (via topic2)
      System.out.println("  Polling (expecting 30 from t1 + 40 from t2 = 70 total)...");
      PollResult result = pollUntilComplete(consumer, 70, 80);
      System.out.println("  Result: " + result);

      assertEquals("Expected exactly 70 rows total (30 t1 + 40 t2)", 70, result.totalRows);
      if (!result.rowsPerTable.isEmpty()) {
        Integer t1Rows = result.rowsPerTable.get("t1");
        Integer t2Rows = result.rowsPerTable.get("t2");
        assertEquals("Expected 30 rows from t1", 30, t1Rows != null ? t1Rows : 0);
        assertEquals("Expected 40 rows from t2", 40, t2Rows != null ? t2Rows : 0);
        System.out.println(
            "  Multi-topic isolation verified: t1=" + t1Rows + " rows, t2=" + t2Rows + " rows");
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
      dropTopicTable(topicName1);
      dropTopicTable(topicName2);
      deleteDatabase(database);
    }
  }

  // ============================
  // Test 12: Cross-Partition Multi-Write
  // ============================
  /**
   * Tests that cross-partition writes via all table model write methods are correctly delivered.
   *
   * <p>Uses timestamps spaced >1 week apart (default partition interval = 604,800,000ms) to force
   * cross-partition distribution. Exercises three write paths:
   *
   * <ul>
   *   <li>Method 1: SQL single-row INSERT (2 rows, separate partitions)
   *   <li>Method 2: SQL multi-row INSERT (3 rows spanning 3 partitions in one statement)
   *   <li>Method 3: session.insert(Tablet) with 4 rows spanning 4 partitions
   * </ul>
   *
   * <p>The table has 6 FIELD columns (INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT) plus 1 TAG. Total
   * expected rows: 2 + 3 + 4 = 9.
   *
   * <p>This test verifies that when a SQL multi-row INSERT or Tablet write spans multiple time
   * partitions (causing the plan node to be split into sub-nodes for each partition), all sub-nodes
   * are correctly converted by the consensus subscription pipeline.
   */
  private static void testCrossPartitionMultiWrite() throws Exception {
    String database = nextDatabase();
    String topicName = nextTopic();
    String consumerGroupId = nextConsumerGroup();
    String consumerId = nextConsumerId();
    ISubscriptionTablePullConsumer consumer = null;

    // Gap > default time partition interval (7 days = 604,800,000ms)
    final long GAP = 604_800_001L;
    final String TABLE = "t1";
    final String SCHEMA =
        "tag1 STRING TAG, s_int32 INT32 FIELD, s_int64 INT64 FIELD, "
            + "s_float FLOAT FIELD, s_double DOUBLE FIELD, s_bool BOOLEAN FIELD, "
            + "s_text TEXT FIELD";

    try {
      // Create database and table, write init row to force DataRegion creation
      try (ITableSession session = openTableSession()) {
        createDatabaseAndTable(session, database, TABLE, SCHEMA);
        session.executeNonQueryStatement("USE " + database);
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

      System.out.println("  Writing cross-partition data via 3 methods...");

      // --- Method 1: SQL single-row INSERT (2 rows, each in its own partition) ---
      long baseTs = 1_000_000_000L;
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        long ts1 = baseTs;
        long ts2 = baseTs + GAP;
        System.out.println("    Method 1: SQL single-row x2 (ts=" + ts1 + ", " + ts2 + ")");
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                    + "VALUES ('d1', 1, 100, 1.1, 1.11, true, 'sql_single_1', %d)",
                ts1));
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                    + "VALUES ('d1', 2, 200, 2.2, 2.22, false, 'sql_single_2', %d)",
                ts2));
      }

      // --- Method 2: SQL multi-row INSERT (3 rows spanning 3 different partitions) ---
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        long t1 = baseTs + GAP * 2;
        long t2 = baseTs + GAP * 3;
        long t3 = baseTs + GAP * 4;
        System.out.println(
            "    Method 2: SQL multi-row x3 (ts=" + t1 + ", " + t2 + ", " + t3 + ")");
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO t1 (tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                    + "VALUES ('d1', 3, 300, 3.3, 3.33, true, 'sql_multi_1', %d), "
                    + "('d1', 4, 400, 4.4, 4.44, false, 'sql_multi_2', %d), "
                    + "('d1', 5, 500, 5.5, 5.55, true, 'sql_multi_3', %d)",
                t1, t2, t3));
      }

      // --- Method 3: session.insert(Tablet) with 4 rows spanning 4 partitions ---
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);

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
                TABLE,
                IMeasurementSchema.getMeasurementNameList(schemaList),
                IMeasurementSchema.getDataTypeList(schemaList),
                categories,
                10);

        for (int i = 0; i < 4; i++) {
          int row = tablet.getRowSize();
          long ts = baseTs + GAP * (5 + i); // partitions 5, 6, 7, 8
          tablet.addTimestamp(row, ts);
          tablet.addValue("tag1", row, "d1");
          tablet.addValue("s_int32", row, 6 + i);
          tablet.addValue("s_int64", row, (long) (600 + i * 100));
          tablet.addValue("s_float", row, (6 + i) * 1.1f);
          tablet.addValue("s_double", row, (6 + i) * 2.22);
          tablet.addValue("s_bool", row, i % 2 == 0);
          tablet.addValue("s_text", row, "tablet_" + (i + 1));
        }
        System.out.println(
            "    Method 3: Tablet x4 (ts=" + (baseTs + GAP * 5) + ".." + (baseTs + GAP * 8) + ")");
        session.insert(tablet);
      }

      Thread.sleep(2000);

      // Poll — expect 9 rows total (2 + 3 + 4)
      final int expectedRows = 9;
      System.out.println("  Polling (expecting " + expectedRows + " rows)...");
      PollResult result = pollUntilComplete(consumer, expectedRows, 80);
      System.out.println("  Result: " + result);

      assertEquals(
          "Expected exactly " + expectedRows + " cross-partition rows",
          expectedRows,
          result.totalRows);
      // Verify we see all 6 FIELD columns plus tag
      assertAtLeast(
          "Expected at least 6 data columns in cross-partition result",
          6,
          result.seenColumns.size());
    } finally {
      cleanup(consumer, topicName, database);
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

      // Write 50 rows, then flush before polling
      System.out.println("  Writing 50 rows then flushing");
      try (ITableSession session = openTableSession()) {
        session.executeNonQueryStatement("USE " + database);
        for (int i = 1; i <= 50; i++) {
          session.executeNonQueryStatement(
              String.format("INSERT INTO t1 (tag1, s1, time) VALUES ('d1', %d, %d)", i * 10, i));
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
}
