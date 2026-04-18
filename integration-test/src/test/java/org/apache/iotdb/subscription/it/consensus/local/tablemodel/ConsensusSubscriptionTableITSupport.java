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

package org.apache.iotdb.subscription.it.consensus.local.tablemodel;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.PollResult;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler;

import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.junit.Assert;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

final class ConsensusSubscriptionTableITSupport {

  static final String DEFAULT_TABLE_SCHEMA = "tag1 STRING TAG, s1 INT64 FIELD";

  private static final AtomicInteger IDENTIFIER = new AtomicInteger(0);
  private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final int QUIET_ROUNDS_AFTER_DATA = 3;
  private static final int QUIET_ROUNDS_WITHOUT_DATA = 8;

  private ConsensusSubscriptionTableITSupport() {
    throw new IllegalStateException("Utility class");
  }

  static TestIdentifiers newIdentifiers(final String prefix) {
    final int id = IDENTIFIER.incrementAndGet();
    final String normalized =
        prefix.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
    return new TestIdentifiers(
        "consensus_tbl_it_" + normalized + "_" + id,
        "topic_consensus_tbl_it_" + normalized + "_" + id,
        "cg_consensus_tbl_it_" + normalized + "_" + id,
        "c_consensus_tbl_it_" + normalized + "_" + id);
  }

  static void createDatabase(final String database) throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("create database " + database);
    }
  }

  static void createDatabaseAndTable(
      final String database, final String tableName, final String schema) throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("create database " + database);
      session.executeNonQueryStatement("use " + database);
      session.executeNonQueryStatement(String.format("create table %s (%s)", tableName, schema));
    }
  }

  static void createTable(final String database, final String tableName, final String schema)
      throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      session.executeNonQueryStatement(String.format("create table %s (%s)", tableName, schema));
    }
  }

  static String bootstrapDatabaseAndTable(
      final String database, final String tableName, final String schema) throws Exception {
    createDatabaseAndTable(database, tableName, schema);
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      session.executeNonQueryStatement(
          String.format(
              "insert into %s(tag1, s1, time) values ('bootstrap', %d, %d)", tableName, 0L, 0L));
      session.executeNonQueryStatement("flush");
    }
    return rowKey(database, tableName, 0L);
  }

  static void createConsensusTopic(
      final String topicName, final String databasePattern, final String tablePattern)
      throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.open();
      session.dropTopicIfExists(topicName);

      final Properties config = new Properties();
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
      config.put(TopicConstant.DATABASE_KEY, databasePattern);
      config.put(TopicConstant.TABLE_KEY, tablePattern);
      session.createTopic(topicName, config);
    }
  }

  static SubscriptionTablePullConsumer createConsumer(
      final String consumerId, final String consumerGroupId) throws Exception {
    final SubscriptionTablePullConsumer consumer =
        (SubscriptionTablePullConsumer)
            new SubscriptionTablePullConsumerBuilder()
                .host(EnvFactory.getEnv().getIP())
                .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
                .consumerId(consumerId)
                .consumerGroupId(consumerGroupId)
                .autoCommit(false)
                .build();
    consumer.open();
    return consumer;
  }

  static Set<String> insertRows(
      final String database,
      final String tableName,
      final long startTimestampInclusive,
      final int rowCount,
      final boolean flush)
      throws Exception {
    return insertRows(database, tableName, startTimestampInclusive, rowCount, 10L, flush);
  }

  static Set<String> insertRows(
      final String database,
      final String tableName,
      final long startTimestampInclusive,
      final int rowCount,
      final long valueMultiplier,
      final boolean flush)
      throws Exception {
    final Set<String> rowKeys = new LinkedHashSet<>();

    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      for (int row = 0; row < rowCount; row++) {
        final long timestamp = startTimestampInclusive + row;
        session.executeNonQueryStatement(
            String.format(
                "insert into %s(tag1, s1, time) values ('%s', %d, %d)",
                tableName, tableName + "_tag", timestamp * valueMultiplier, timestamp));
        rowKeys.add(rowKey(database, tableName, timestamp));
      }
      if (flush) {
        session.executeNonQueryStatement("flush");
      }
    }

    return rowKeys;
  }

  static ConsumedRecords pollAndCommitUntilAtLeast(
      final SubscriptionTablePullConsumer consumer,
      final int expectedUniqueRows,
      final int maxPollRounds)
      throws Exception {
    return pollAndCommitUntilAtLeast(
        consumer, expectedUniqueRows, maxPollRounds, DEFAULT_POLL_TIMEOUT);
  }

  static ConsumedRecords pollAndCommitUntilAtLeast(
      final SubscriptionTablePullConsumer consumer,
      final int expectedUniqueRows,
      final int maxPollRounds,
      final Duration pollTimeout)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    int emptyRounds = 0;

    for (int round = 0; round < maxPollRounds; round++) {
      final List<SubscriptionMessage> messages = consumer.poll(pollTimeout);
      if (messages.isEmpty()) {
        emptyRounds++;
        if (consumed.getUniqueRowCount() >= expectedUniqueRows
            && emptyRounds >= QUIET_ROUNDS_AFTER_DATA) {
          break;
        }
        if (consumed.getUniqueRowCount() == 0
            && expectedUniqueRows == 0
            && emptyRounds >= QUIET_ROUNDS_WITHOUT_DATA) {
          break;
        }
        continue;
      }

      emptyRounds = 0;
      consumed.merge(consumeMessages(messages));
      consumer.commitSync(messages);
    }

    return consumed;
  }

  static ConsumedRecords pollWithInfoAndCommitUntilAtLeast(
      final SubscriptionTablePullConsumer consumer,
      final Set<String> topicNames,
      final int expectedUniqueRows,
      final int maxPollRounds)
      throws Exception {
    return pollWithInfoAndCommitUntilAtLeast(
        consumer, topicNames, expectedUniqueRows, maxPollRounds, DEFAULT_POLL_TIMEOUT);
  }

  static ConsumedRecords pollWithInfoAndCommitUntilAtLeast(
      final SubscriptionTablePullConsumer consumer,
      final Set<String> topicNames,
      final int expectedUniqueRows,
      final int maxPollRounds,
      final Duration pollTimeout)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    int emptyRounds = 0;

    for (int round = 0; round < maxPollRounds; round++) {
      final PollResult pollResult = consumer.pollWithInfo(topicNames, pollTimeout.toMillis());
      final List<SubscriptionMessage> messages = pollResult.getMessages();
      if (messages.isEmpty()) {
        emptyRounds++;
        if (consumed.getUniqueRowCount() >= expectedUniqueRows
            && emptyRounds >= QUIET_ROUNDS_AFTER_DATA) {
          break;
        }
        continue;
      }

      emptyRounds = 0;
      consumed.merge(consumeMessages(messages));
      consumer.commitSync(messages);
    }

    return consumed;
  }

  static void assertExactRowKeys(
      final Set<String> expectedRowKeys, final ConsumedRecords consumed) {
    Assert.assertTrue(
        "Unexpected duplicate row keys: " + consumed.getDuplicateRowKeys(),
        consumed.getDuplicateRowKeys().isEmpty());
    Assert.assertEquals(expectedRowKeys, consumed.getRowKeys());
    Assert.assertEquals(expectedRowKeys.size(), consumed.getRowCount());
  }

  static void assertNoMoreMessages(
      final SubscriptionTablePullConsumer consumer, final int rounds, final Duration pollTimeout)
      throws Exception {
    for (int i = 0; i < rounds; i++) {
      Assert.assertTrue(
          "Unexpected extra subscription messages after quiescence",
          consumer.poll(pollTimeout).isEmpty());
    }
  }

  static void cleanup(
      final SubscriptionTablePullConsumer consumer,
      final String topicName,
      final String... databases) {
    cleanup(consumer, Collections.singleton(topicName), databases);
  }

  static void cleanup(
      final SubscriptionTablePullConsumer consumer,
      final Set<String> topicNames,
      final String... databases) {
    if (consumer != null) {
      try {
        consumer.unsubscribe(topicNames);
      } catch (final Exception ignored) {
        // ignored on cleanup
      }
      try {
        consumer.close();
      } catch (final Exception ignored) {
        // ignored on cleanup
      }
    }

    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.open();
      for (final String topicName : topicNames) {
        session.dropTopicIfExists(topicName);
      }
    } catch (final Exception ignored) {
      // ignored on cleanup
    }

    for (final String database : databases) {
      try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("drop database if exists " + database);
      } catch (final Exception ignored) {
        // ignored on cleanup
      }
    }
  }

  static void pause(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for subscription state", e);
    }
  }

  static String rowKey(final String database, final String tableName, final long timestamp) {
    return database + "." + tableName + "#" + timestamp;
  }

  private static ConsumedRecords consumeMessages(final List<SubscriptionMessage> messages)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    for (final SubscriptionMessage message : messages) {
      for (final ResultSet resultSet : message.getResultSets()) {
        final SubscriptionRecordHandler.SubscriptionResultSet subscriptionResultSet =
            (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
        consumed.getSeenColumns().addAll(subscriptionResultSet.getColumnNames());
        final String databaseName = subscriptionResultSet.getDatabaseName();
        final String tableName = subscriptionResultSet.getTableName();
        while (subscriptionResultSet.hasNext()) {
          final RowRecord record = subscriptionResultSet.nextRecord();
          consumed.addRow(databaseName, tableName, record.getTimestamp());
        }
      }
    }
    return consumed;
  }

  static final class TestIdentifiers {

    private final String database;
    private final String topic;
    private final String consumerGroupId;
    private final String consumerId;

    private TestIdentifiers(
        final String database,
        final String topic,
        final String consumerGroupId,
        final String consumerId) {
      this.database = database;
      this.topic = topic;
      this.consumerGroupId = consumerGroupId;
      this.consumerId = consumerId;
    }

    String getDatabase() {
      return database;
    }

    String getTopic() {
      return topic;
    }

    String getConsumerGroupId() {
      return consumerGroupId;
    }

    String getConsumerId() {
      return consumerId;
    }

    String database(final String suffix) {
      return database + "_" + suffix;
    }

    String topic(final String suffix) {
      return topic + "_" + suffix;
    }
  }

  static final class ConsumedRecords {

    private final Set<String> rowKeys = new LinkedHashSet<>();
    private final Set<String> duplicateRowKeys = new LinkedHashSet<>();
    private final Set<String> seenColumns = new LinkedHashSet<>();
    private final Map<String, Integer> rowsPerTable = new LinkedHashMap<>();
    private final Map<String, Integer> rowsPerDatabase = new LinkedHashMap<>();
    private int rowCount;

    void addRow(final String databaseName, final String tableName, final long timestamp) {
      rowCount++;
      final String rowKey = rowKey(databaseName, tableName, timestamp);
      if (!rowKeys.add(rowKey)) {
        duplicateRowKeys.add(rowKey);
      }
      rowsPerTable.merge(tableName, 1, Integer::sum);
      rowsPerDatabase.merge(databaseName, 1, Integer::sum);
    }

    void merge(final ConsumedRecords other) {
      rowCount += other.rowCount;
      rowKeys.addAll(other.rowKeys);
      duplicateRowKeys.addAll(other.duplicateRowKeys);
      seenColumns.addAll(other.seenColumns);
      other.rowsPerTable.forEach((table, count) -> rowsPerTable.merge(table, count, Integer::sum));
      other.rowsPerDatabase.forEach(
          (database, count) -> rowsPerDatabase.merge(database, count, Integer::sum));
    }

    Set<String> getRowKeys() {
      return rowKeys;
    }

    Set<String> getDuplicateRowKeys() {
      return duplicateRowKeys;
    }

    Set<String> getSeenColumns() {
      return seenColumns;
    }

    Map<String, Integer> getRowsPerTable() {
      return rowsPerTable;
    }

    Map<String, Integer> getRowsPerDatabase() {
      return rowsPerDatabase;
    }

    int getRowCount() {
      return rowCount;
    }

    int getUniqueRowCount() {
      return rowKeys.size();
    }

    @Override
    public String toString() {
      return "ConsumedRecords{rowCount="
          + rowCount
          + ", uniqueRowCount="
          + getUniqueRowCount()
          + ", rowsPerTable="
          + rowsPerTable
          + ", rowsPerDatabase="
          + rowsPerDatabase
          + ", duplicateRowKeys="
          + duplicateRowKeys
          + "}";
    }
  }
}
