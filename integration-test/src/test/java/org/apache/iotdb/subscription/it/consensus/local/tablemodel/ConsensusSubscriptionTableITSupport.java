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
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class ConsensusSubscriptionTableITSupport {

  static final String DEFAULT_TABLE_SCHEMA = "tag1 STRING TAG, s1 INT64 FIELD";

  private static final AtomicInteger IDENTIFIER = new AtomicInteger(0);
  private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofMinutes(2);
  private static final long ALTER_PROBE_INSERT_INTERVAL_MS = 1_000L;
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
    createConsensusTopic(topicName, databasePattern, tablePattern, null);
  }

  static void createConsensusTopic(
      final String topicName,
      final String databasePattern,
      final String tablePattern,
      final String columnFilter)
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
      if (columnFilter != null) {
        config.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
      }
      session.createTopic(topicName, config);
    }
  }

  static void alterConsensusTopicColumnFilter(final String topicName, final String columnFilter)
      throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.open();

      final Properties config = new Properties();
      config.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
      session.alterTopic(topicName, config);
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

  static Set<String> insertRows(
      final String database,
      final String tableName,
      final long startTimestampInclusive,
      final int rowCount,
      final boolean includeS2,
      final boolean includeS3,
      final boolean flush)
      throws Exception {
    final Set<String> rowKeys = new LinkedHashSet<>();

    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      for (int row = 0; row < rowCount; row++) {
        final long timestamp = startTimestampInclusive + row;
        final StringBuilder columns = new StringBuilder("tag1, s1");
        final StringBuilder values =
            new StringBuilder(
                String.format(
                    Locale.ROOT, "'%s', %d", tableName + "_tag_" + timestamp, timestamp * 10L));
        if (includeS2) {
          columns.append(", s2");
          values.append(String.format(Locale.ROOT, ", %.1f", timestamp + 0.5d));
        }
        if (includeS3) {
          columns.append(", s3");
          values.append(timestamp % 2 == 0 ? ", true" : ", false");
        }
        columns.append(", time");
        values.append(", ").append(timestamp);
        session.executeNonQueryStatement(
            String.format(
                Locale.ROOT, "insert into %s(%s) values (%s)", tableName, columns, values));
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
    final AtomicInteger emptyRounds = new AtomicInteger(0);
    awaitDrain(maxPollRounds, pollTimeout)
        .untilAsserted(
            () -> {
              pollAndCommitOnce(consumer, pollTimeout, consumed, emptyRounds);
              Assert.assertTrue(
                  atLeastTimeoutMessage(expectedUniqueRows, consumed),
                  hasDrainedAtLeast(consumed, expectedUniqueRows, emptyRounds.get()));
            });

    return consumed;
  }

  static ConsumedRecords pollAndCommitUntilContains(
      final SubscriptionTablePullConsumer consumer,
      final Set<String> expectedRowKeys,
      final int maxPollRounds)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    final AtomicInteger emptyRounds = new AtomicInteger(0);
    awaitDrain(maxPollRounds, DEFAULT_POLL_TIMEOUT)
        .untilAsserted(
            () -> {
              pollAndCommitOnce(consumer, DEFAULT_POLL_TIMEOUT, consumed, emptyRounds);
              Assert.assertTrue(
                  containsTimeoutMessage(expectedRowKeys, consumed),
                  hasDrainedExpectedKeys(consumed, expectedRowKeys, emptyRounds.get()));
            });

    return consumed;
  }

  static ConsumedRecords insertRowsAndPollUntilColumnSignature(
      final SubscriptionTablePullConsumer consumer,
      final String database,
      final String tableName,
      final long startTimestampInclusive,
      final int rowCountPerInsert,
      final boolean includeS2,
      final boolean includeS3,
      final String expectedColumnSignature,
      final int maxPollRounds)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    final AtomicBoolean expectedSignatureSeen = new AtomicBoolean(false);
    final AtomicInteger emptyRounds = new AtomicInteger(0);
    final AtomicLong nextTimestamp = new AtomicLong(startTimestampInclusive);
    final AtomicLong lastInsertTimeMs = new AtomicLong(0L);
    awaitDrain(maxPollRounds, DEFAULT_POLL_TIMEOUT)
        .untilAsserted(
            () -> {
              if (!expectedSignatureSeen.get()) {
                insertProbeRowsIfNecessary(
                    database,
                    tableName,
                    rowCountPerInsert,
                    includeS2,
                    includeS3,
                    nextTimestamp,
                    lastInsertTimeMs);
              }
              pollAndCommitOnce(consumer, DEFAULT_POLL_TIMEOUT, consumed, emptyRounds);
              if (consumed.getSeenColumnSignatures().contains(expectedColumnSignature)) {
                expectedSignatureSeen.set(true);
              }
              Assert.assertTrue(
                  columnSignatureTimeoutMessage(expectedColumnSignature, consumed),
                  expectedSignatureSeen.get() && emptyRounds.get() >= QUIET_ROUNDS_AFTER_DATA);
            });

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
    final AtomicInteger emptyRounds = new AtomicInteger(0);
    awaitDrain(maxPollRounds, pollTimeout)
        .untilAsserted(
            () -> {
              pollWithInfoAndCommitOnce(consumer, topicNames, pollTimeout, consumed, emptyRounds);
              Assert.assertTrue(
                  atLeastTimeoutMessage(expectedUniqueRows, consumed),
                  hasDrainedAtLeast(consumed, expectedUniqueRows, emptyRounds.get()));
            });

    return consumed;
  }

  static void assertExactRowKeys(
      final Set<String> expectedRowKeys, final ConsumedRecords consumed) {
    Assert.assertTrue(
        "Unexpected duplicate row keys: " + consumed.getDuplicateRowKeys(),
        consumed.getDuplicateRowKeys().isEmpty());
    Assert.assertEquals(
        rowKeyDiffMessage(expectedRowKeys, consumed), expectedRowKeys, consumed.getRowKeys());
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

  static String normalizeColumnSignature(final String... columns) {
    final List<String> normalized = new ArrayList<>(columns.length);
    for (final String column : columns) {
      normalized.add(column.toLowerCase(Locale.ROOT));
    }
    Collections.sort(normalized);
    return String.join(",", normalized);
  }

  private static ConsumedRecords consumeMessages(final List<SubscriptionMessage> messages)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    for (final SubscriptionMessage message : messages) {
      for (final ResultSet resultSet : message.getResultSets()) {
        final SubscriptionRecordHandler.SubscriptionResultSet subscriptionResultSet =
            (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
        final List<String> columnNames = subscriptionResultSet.getColumnNames();
        for (final String columnName : columnNames) {
          consumed.getSeenColumns().add(columnName.toLowerCase(Locale.ROOT));
        }
        consumed
            .getSeenColumnSignatures()
            .add(normalizeColumnSignature(columnNames.toArray(new String[0])));
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

  private static void pollAndCommitOnce(
      final SubscriptionTablePullConsumer consumer,
      final Duration pollTimeout,
      final ConsumedRecords consumed,
      final AtomicInteger emptyRounds)
      throws Exception {
    final List<SubscriptionMessage> messages = consumer.poll(pollTimeout);
    if (messages.isEmpty()) {
      emptyRounds.incrementAndGet();
      return;
    }

    emptyRounds.set(0);
    consumed.merge(consumeMessages(messages));
    consumer.commitSync(messages);
  }

  private static void pollWithInfoAndCommitOnce(
      final SubscriptionTablePullConsumer consumer,
      final Set<String> topicNames,
      final Duration pollTimeout,
      final ConsumedRecords consumed,
      final AtomicInteger emptyRounds)
      throws Exception {
    final PollResult pollResult = consumer.pollWithInfo(topicNames, pollTimeout.toMillis());
    final List<SubscriptionMessage> messages = pollResult.getMessages();
    if (messages.isEmpty()) {
      emptyRounds.incrementAndGet();
      return;
    }

    emptyRounds.set(0);
    consumed.merge(consumeMessages(messages));
    consumer.commitSync(messages);
  }

  private static void insertProbeRowsIfNecessary(
      final String database,
      final String tableName,
      final int rowCountPerInsert,
      final boolean includeS2,
      final boolean includeS3,
      final AtomicLong nextTimestamp,
      final AtomicLong lastInsertTimeMs)
      throws Exception {
    final long now = System.currentTimeMillis();
    final long lastInsert = lastInsertTimeMs.get();
    if (lastInsert > 0L && now - lastInsert < ALTER_PROBE_INSERT_INTERVAL_MS) {
      return;
    }

    insertRows(
        database,
        tableName,
        nextTimestamp.getAndAdd(rowCountPerInsert),
        rowCountPerInsert,
        includeS2,
        includeS3,
        true);
    lastInsertTimeMs.set(System.currentTimeMillis());
  }

  private static ConditionFactory awaitDrain(
      final int legacyMaxPollRounds, final Duration pollTimeout) {
    final Duration drainTimeout = drainTimeout(legacyMaxPollRounds, pollTimeout);
    return Awaitility.await()
        .pollInSameThread()
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(1, TimeUnit.MILLISECONDS)
        .atMost(drainTimeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static Duration drainTimeout(final int legacyMaxPollRounds, final Duration pollTimeout) {
    final long legacyTimeoutMillis =
        Math.max(0L, legacyMaxPollRounds) * Math.max(1L, pollTimeout.toMillis());
    final Duration legacyTimeout = Duration.ofMillis(legacyTimeoutMillis);
    return legacyTimeout.compareTo(DEFAULT_DRAIN_TIMEOUT) > 0
        ? legacyTimeout
        : DEFAULT_DRAIN_TIMEOUT;
  }

  private static boolean hasDrainedAtLeast(
      final ConsumedRecords consumed, final int expectedUniqueRows, final int emptyRounds) {
    if (expectedUniqueRows == 0) {
      return consumed.getUniqueRowCount() == 0 && emptyRounds >= QUIET_ROUNDS_WITHOUT_DATA;
    }
    return consumed.getUniqueRowCount() >= expectedUniqueRows
        && emptyRounds >= QUIET_ROUNDS_AFTER_DATA;
  }

  private static boolean hasDrainedExpectedKeys(
      final ConsumedRecords consumed, final Set<String> expectedRowKeys, final int emptyRounds) {
    return consumed.getRowKeys().containsAll(expectedRowKeys)
        && emptyRounds >= QUIET_ROUNDS_AFTER_DATA;
  }

  private static String atLeastTimeoutMessage(
      final int expectedUniqueRows, final ConsumedRecords consumed) {
    return "Expected at least "
        + expectedUniqueRows
        + " unique row keys before the subscription drain timeout, but collected "
        + consumed.getUniqueRowCount()
        + ". Consumed records: "
        + consumed;
  }

  private static String containsTimeoutMessage(
      final Set<String> expectedRowKeys, final ConsumedRecords consumed) {
    return "Expected row keys were not fully collected before the subscription drain timeout. "
        + rowKeyDiffMessage(expectedRowKeys, consumed);
  }

  private static String columnSignatureTimeoutMessage(
      final String expectedColumnSignature, final ConsumedRecords consumed) {
    return "Expected column signature "
        + expectedColumnSignature
        + " before the subscription drain timeout. Consumed records: "
        + consumed;
  }

  private static String rowKeyDiffMessage(
      final Set<String> expectedRowKeys, final ConsumedRecords consumed) {
    final Set<String> missingRowKeys = new LinkedHashSet<>(expectedRowKeys);
    missingRowKeys.removeAll(consumed.getRowKeys());
    final Set<String> unexpectedRowKeys = new LinkedHashSet<>(consumed.getRowKeys());
    unexpectedRowKeys.removeAll(expectedRowKeys);
    return "expected="
        + expectedRowKeys
        + ", actual="
        + consumed.getRowKeys()
        + ", missing="
        + missingRowKeys
        + ", unexpected="
        + unexpectedRowKeys
        + ", consumed="
        + consumed;
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

    String consumerGroup(final String suffix) {
      return consumerGroupId + "_" + suffix;
    }

    String consumer(final String suffix) {
      return consumerId + "_" + suffix;
    }
  }

  static final class ConsumedRecords {

    private final Set<String> rowKeys = new LinkedHashSet<>();
    private final Set<String> duplicateRowKeys = new LinkedHashSet<>();
    private final Set<String> seenColumns = new LinkedHashSet<>();
    private final Set<String> seenColumnSignatures = new LinkedHashSet<>();
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
      for (final String rowKey : other.rowKeys) {
        if (!rowKeys.add(rowKey)) {
          duplicateRowKeys.add(rowKey);
        }
      }
      duplicateRowKeys.addAll(other.duplicateRowKeys);
      seenColumns.addAll(other.seenColumns);
      seenColumnSignatures.addAll(other.seenColumnSignatures);
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

    Set<String> getSeenColumnSignatures() {
      return seenColumnSignatures;
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
          + ", seenColumns="
          + seenColumns
          + ", seenColumnSignatures="
          + seenColumnSignatures
          + ", duplicateRowKeys="
          + duplicateRowKeys
          + "}";
    }
  }
}
