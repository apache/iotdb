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

package org.apache.iotdb.subscription.it.consensus.local;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler;

import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.junit.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS;

final class ConsensusSubscriptionITSupport {

  private static final AtomicInteger IDENTIFIER = new AtomicInteger(0);
  private static final int QUIET_ROUNDS_AFTER_DATA = 3;
  private static final int QUIET_ROUNDS_WITHOUT_DATA = 8;

  private ConsensusSubscriptionITSupport() {
    throw new IllegalStateException("Utility class");
  }

  static TestIdentifiers newIdentifiers(final String prefix) {
    final int id = IDENTIFIER.incrementAndGet();
    final String normalized =
        prefix.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
    return new TestIdentifiers(
        "root.consensus_it_" + normalized + "_" + id,
        "topic_consensus_it_" + normalized + "_" + id,
        "cg_consensus_it_" + normalized + "_" + id,
        "c_consensus_it_" + normalized + "_" + id);
  }

  static String bootstrapDatabase(final String database) throws Exception {
    createDatabase(database);
    final String bootstrapDevice = database + ".bootstrap";
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("insert into %s(time, s1) values (%d, %d)", bootstrapDevice, 0L, 0L));
      session.executeNonQueryStatement("flush");
    }
    return rowKey(bootstrapDevice, 0L);
  }

  static void createDatabase(final String database) throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("create database " + database);
    }
  }

  static void createConsensusTopic(final String topicName, final String path) throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      session.dropTopicIfExists(topicName);

      final Properties config = new Properties();
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
      config.put(TopicConstant.PATH_KEY, path);
      config.put(TopicConstant.ORDER_MODE_KEY, TopicConstant.ORDER_MODE_PER_WRITER_VALUE);
      session.createTopic(topicName, config);
    }
  }

  static SubscriptionTreePullConsumer createConsumer(
      final String consumerId, final String consumerGroupId) throws Exception {
    final SubscriptionTreePullConsumer consumer =
        new SubscriptionTreePullConsumer.Builder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .consumerId(consumerId)
            .consumerGroupId(consumerGroupId)
            .autoCommit(false)
            .buildPullConsumer();
    consumer.open();
    return consumer;
  }

  static Set<String> insertRows(
      final String database,
      final List<String> devices,
      final long startTimestampInclusive,
      final int rowsPerDevice,
      final boolean flush)
      throws Exception {
    final Set<String> rowKeys = new LinkedHashSet<>();

    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int deviceIndex = 0; deviceIndex < devices.size(); deviceIndex++) {
        final String devicePath = database + "." + devices.get(deviceIndex);
        final long deviceBaseTimestamp = startTimestampInclusive + deviceIndex * 10_000L;
        for (int row = 0; row < rowsPerDevice; row++) {
          final long timestamp = deviceBaseTimestamp + row;
          session.executeNonQueryStatement(
              String.format(
                  "insert into %s(time, s1) values (%d, %d)",
                  devicePath, timestamp, timestamp * 10));
          rowKeys.add(rowKey(devicePath, timestamp));
        }
      }
      if (flush) {
        session.executeNonQueryStatement("flush");
      }
    }

    return rowKeys;
  }

  static Set<String> insertRows(
      final String database,
      final String device,
      final long startTimestampInclusive,
      final int rowCount,
      final boolean flush)
      throws Exception {
    return insertRows(
        database, Collections.singletonList(device), startTimestampInclusive, rowCount, flush);
  }

  static ConsumedRecords pollAndCommitUntilAtLeast(
      final SubscriptionTreePullConsumer consumer,
      final int expectedUniqueRows,
      final int maxPollRounds)
      throws Exception {
    return pollAndCommitUntilAtLeast(
        consumer, expectedUniqueRows, maxPollRounds, Duration.ofMillis(POLL_TIMEOUT_MS));
  }

  static ConsumedRecords pollAndCommitUntilAtLeast(
      final SubscriptionTreePullConsumer consumer,
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

  static ConsumedRecords drainAndCommitUntilQuiet(
      final SubscriptionTreePullConsumer consumer, final int maxPollRounds) throws Exception {
    return drainAndCommitUntilQuiet(consumer, maxPollRounds, Duration.ofMillis(POLL_TIMEOUT_MS));
  }

  static ConsumedRecords drainAndCommitUntilQuiet(
      final SubscriptionTreePullConsumer consumer,
      final int maxPollRounds,
      final Duration pollTimeout)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    int emptyRounds = 0;
    boolean sawData = false;

    for (int round = 0; round < maxPollRounds; round++) {
      final List<SubscriptionMessage> messages = consumer.poll(pollTimeout);
      if (messages.isEmpty()) {
        emptyRounds++;
        if ((sawData && emptyRounds >= QUIET_ROUNDS_AFTER_DATA)
            || (!sawData && emptyRounds >= QUIET_ROUNDS_WITHOUT_DATA)) {
          break;
        }
        continue;
      }

      sawData = true;
      emptyRounds = 0;
      consumed.merge(consumeMessages(messages));
      consumer.commitSync(messages);
    }

    return consumed;
  }

  static PolledMessageBatch pollFirstNonEmptyBatchWithoutCommit(
      final SubscriptionTreePullConsumer consumer, final int maxPollRounds) throws Exception {
    return pollFirstNonEmptyBatchWithoutCommit(
        consumer, maxPollRounds, Duration.ofMillis(POLL_TIMEOUT_MS));
  }

  static PolledMessageBatch pollFirstNonEmptyBatchWithoutCommit(
      final SubscriptionTreePullConsumer consumer,
      final int maxPollRounds,
      final Duration pollTimeout)
      throws Exception {
    for (int round = 0; round < maxPollRounds; round++) {
      final List<SubscriptionMessage> messages = consumer.poll(pollTimeout);
      if (messages.isEmpty()) {
        continue;
      }
      return new PolledMessageBatch(messages, consumeMessages(messages));
    }
    return new PolledMessageBatch(Collections.emptyList(), new ConsumedRecords());
  }

  static CommittedSnapshot pollUntilCommittedRows(
      final SubscriptionTreePullConsumer consumer,
      final String topicName,
      final int minimumRows,
      final int maxPollRounds)
      throws Exception {
    return pollUntilCommittedRows(
        consumer, topicName, minimumRows, maxPollRounds, Duration.ofMillis(POLL_TIMEOUT_MS));
  }

  static CommittedSnapshot pollUntilCommittedRows(
      final SubscriptionTreePullConsumer consumer,
      final String topicName,
      final int minimumRows,
      final int maxPollRounds,
      final Duration pollTimeout)
      throws Exception {
    final ConsumedRecords committed = new ConsumedRecords();
    int emptyRounds = 0;

    for (int round = 0; round < maxPollRounds; round++) {
      final List<SubscriptionMessage> messages = consumer.poll(pollTimeout);
      if (messages.isEmpty()) {
        emptyRounds++;
        if (committed.getUniqueRowCount() > 0 && emptyRounds >= QUIET_ROUNDS_WITHOUT_DATA) {
          break;
        }
        continue;
      }

      emptyRounds = 0;
      for (final SubscriptionMessage message : messages) {
        final ConsumedRecords batch = consumeMessages(Collections.singletonList(message));
        consumer.commitSync(message);
        committed.merge(batch);
        if (committed.getUniqueRowCount() >= minimumRows) {
          return new CommittedSnapshot(
              consumer.committedPositions(topicName), committed.copyRowKeys(), batch.getRowCount());
        }
      }
    }

    Assert.fail(
        "Unable to capture committed checkpoint after "
            + minimumRows
            + " rows, got "
            + committed.getUniqueRowCount());
    return null;
  }

  static GroupDrainResult drainConsumersWithoutDuplicates(
      final List<SubscriptionTreePullConsumer> consumers,
      final int expectedUniqueRows,
      final int maxPollRounds)
      throws Exception {
    final List<ConsumedRecords> perConsumer = new ArrayList<>(consumers.size());
    for (int i = 0; i < consumers.size(); i++) {
      perConsumer.add(new ConsumedRecords());
    }
    final ConsumedRecords union = new ConsumedRecords();
    int emptyRounds = 0;

    for (int round = 0; round < maxPollRounds; round++) {
      boolean sawData = false;
      for (int consumerIndex = 0; consumerIndex < consumers.size(); consumerIndex++) {
        final SubscriptionTreePullConsumer consumer = consumers.get(consumerIndex);
        final List<SubscriptionMessage> messages =
            consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
        if (messages.isEmpty()) {
          continue;
        }

        sawData = true;
        final ConsumedRecords batch = consumeMessages(messages);
        perConsumer.get(consumerIndex).merge(batch);
        union.merge(batch);
        consumer.commitSync(messages);
      }

      if (sawData) {
        emptyRounds = 0;
        continue;
      }

      emptyRounds++;
      if (union.getUniqueRowCount() >= expectedUniqueRows
          && emptyRounds >= QUIET_ROUNDS_AFTER_DATA) {
        break;
      }
      if (union.getUniqueRowCount() == 0
          && expectedUniqueRows == 0
          && emptyRounds >= QUIET_ROUNDS_WITHOUT_DATA) {
        break;
      }
    }

    return new GroupDrainResult(union, perConsumer);
  }

  static void assertExactRowKeys(
      final Set<String> expectedRowKeys, final ConsumedRecords consumed) {
    Assert.assertTrue(
        "Unexpected duplicate row keys: " + consumed.getDuplicateRowKeys(),
        consumed.getDuplicateRowKeys().isEmpty());
    Assert.assertEquals(expectedRowKeys, consumed.getRowKeys());
    Assert.assertEquals(expectedRowKeys.size(), consumed.getRowCount());
  }

  static void assertContainsExpectedRowKeys(
      final Set<String> expectedRowKeys, final ConsumedRecords consumed, final int maxExtraRows) {
    Assert.assertTrue(
        "Unexpected duplicate row keys: " + consumed.getDuplicateRowKeys(),
        consumed.getDuplicateRowKeys().isEmpty());
    Assert.assertTrue(
        "Replay should contain all expected rows. expected="
            + expectedRowKeys
            + ", actual="
            + consumed.getRowKeys(),
        consumed.getRowKeys().containsAll(expectedRowKeys));
    Assert.assertTrue(
        "Replay should contain at most " + maxExtraRows + " extra rows. actual=" + consumed,
        consumed.getUniqueRowCount() <= expectedRowKeys.size() + maxExtraRows);
  }

  static void assertNoMoreMessages(
      final SubscriptionTreePullConsumer consumer, final int rounds, final Duration pollTimeout)
      throws Exception {
    for (int i = 0; i < rounds; i++) {
      Assert.assertTrue(
          "Unexpected extra subscription messages after quiescence",
          consumer.poll(pollTimeout).isEmpty());
    }
  }

  static Set<String> subtract(final Set<String> minuend, final Set<String> subtrahend) {
    final Set<String> difference = new LinkedHashSet<>(minuend);
    difference.removeAll(subtrahend);
    return difference;
  }

  static Set<String> measurementPaths(final String devicePath, final String... measurements) {
    final Set<String> result = new LinkedHashSet<>();
    Arrays.stream(measurements).forEach(measurement -> result.add(devicePath + "." + measurement));
    return result;
  }

  static void cleanup(
      final SubscriptionTreePullConsumer consumer, final String topicName, final String database) {
    if (consumer != null) {
      try {
        consumer.unsubscribe(topicName);
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
    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      session.dropTopicIfExists(topicName);
    } catch (final Exception ignored) {
      // ignored on cleanup
    }

    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("drop database " + database);
    } catch (final Exception ignored) {
      // ignored on cleanup
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

  static String rowKey(final String devicePath, final long timestamp) {
    return devicePath + "#" + timestamp;
  }

  private static ConsumedRecords consumeMessages(final List<SubscriptionMessage> messages)
      throws Exception {
    final ConsumedRecords consumed = new ConsumedRecords();
    for (final SubscriptionMessage message : messages) {
      for (final ResultSet resultSet : message.getResultSets()) {
        final SubscriptionRecordHandler.SubscriptionResultSet subscriptionResultSet =
            (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
        final List<String> columnNames = subscriptionResultSet.getColumnNames();
        final String devicePath = extractDevicePath(columnNames);
        if (columnNames.size() > 1) {
          consumed.getSeenColumns().addAll(columnNames.subList(1, columnNames.size()));
        }
        while (subscriptionResultSet.hasNext()) {
          final RowRecord record = subscriptionResultSet.nextRecord();
          consumed.addRow(devicePath, record.getTimestamp());
        }
      }
    }
    return consumed;
  }

  private static String extractDevicePath(final List<String> columnNames) {
    if (columnNames.size() <= 1) {
      return "";
    }
    final String firstMeasurement = columnNames.get(1);
    final int lastDot = firstMeasurement.lastIndexOf('.');
    return lastDot > 0 ? firstMeasurement.substring(0, lastDot) : firstMeasurement;
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

    String consumer(final String suffix) {
      return consumerId + "_" + suffix;
    }

    String consumerGroup(final String suffix) {
      return consumerGroupId + "_" + suffix;
    }
  }

  static final class ConsumedRecords {

    private final Set<String> rowKeys = new LinkedHashSet<>();
    private final Set<String> duplicateRowKeys = new LinkedHashSet<>();
    private final Set<Long> timestamps = new LinkedHashSet<>();
    private final Set<String> seenColumns = new LinkedHashSet<>();
    private final Map<String, Integer> rowsPerDevice = new LinkedHashMap<>();
    private int rowCount;

    void addRow(final String devicePath, final long timestamp) {
      rowCount++;
      timestamps.add(timestamp);
      final String rowKey = rowKey(devicePath, timestamp);
      if (!rowKeys.add(rowKey)) {
        duplicateRowKeys.add(rowKey);
      }
      rowsPerDevice.merge(devicePath, 1, Integer::sum);
    }

    void merge(final ConsumedRecords other) {
      rowCount += other.rowCount;
      timestamps.addAll(other.timestamps);
      seenColumns.addAll(other.seenColumns);
      other.rowsPerDevice.forEach(
          (device, count) -> rowsPerDevice.merge(device, count, Integer::sum));
      for (final String rowKey : other.rowKeys) {
        if (!rowKeys.add(rowKey)) {
          duplicateRowKeys.add(rowKey);
        }
      }
      duplicateRowKeys.addAll(other.duplicateRowKeys);
    }

    int getRowCount() {
      return rowCount;
    }

    int getUniqueRowCount() {
      return rowKeys.size();
    }

    Set<String> getRowKeys() {
      return rowKeys;
    }

    Set<String> copyRowKeys() {
      return new LinkedHashSet<>(rowKeys);
    }

    Set<String> getDuplicateRowKeys() {
      return duplicateRowKeys;
    }

    Set<Long> getTimestamps() {
      return timestamps;
    }

    Set<String> getSeenColumns() {
      return seenColumns;
    }

    Map<String, Integer> getRowsPerDevice() {
      return rowsPerDevice;
    }

    @Override
    public String toString() {
      return "ConsumedRecords{"
          + "rowCount="
          + rowCount
          + ", uniqueRowCount="
          + rowKeys.size()
          + ", duplicateRowKeys="
          + duplicateRowKeys
          + ", rowKeys="
          + rowKeys
          + '}';
    }
  }

  static final class CommittedSnapshot {

    private final TopicProgress progress;
    private final Set<String> committedRowKeys;
    private final int rowsInLastCommittedMessage;

    private CommittedSnapshot(
        final TopicProgress progress,
        final Set<String> committedRowKeys,
        final int rowsInLastCommittedMessage) {
      this.progress = progress;
      this.committedRowKeys = Collections.unmodifiableSet(new LinkedHashSet<>(committedRowKeys));
      this.rowsInLastCommittedMessage = rowsInLastCommittedMessage;
    }

    TopicProgress getProgress() {
      return progress;
    }

    Set<String> getCommittedRowKeys() {
      return committedRowKeys;
    }

    int getCommittedRowCount() {
      return committedRowKeys.size();
    }

    int getRowsInLastCommittedMessage() {
      return rowsInLastCommittedMessage;
    }
  }

  static final class PolledMessageBatch {

    private final List<SubscriptionMessage> messages;
    private final ConsumedRecords consumedRecords;

    private PolledMessageBatch(
        final List<SubscriptionMessage> messages, final ConsumedRecords consumedRecords) {
      this.messages = new ArrayList<>(messages);
      this.consumedRecords = consumedRecords;
    }

    List<SubscriptionMessage> getMessages() {
      return messages;
    }

    ConsumedRecords getConsumedRecords() {
      return consumedRecords;
    }
  }

  static final class GroupDrainResult {

    private final ConsumedRecords union;
    private final List<ConsumedRecords> perConsumer;

    private GroupDrainResult(final ConsumedRecords union, final List<ConsumedRecords> perConsumer) {
      this.union = union;
      this.perConsumer = perConsumer;
    }

    ConsumedRecords getUnion() {
      return union;
    }

    List<ConsumedRecords> getPerConsumer() {
      return perConsumer;
    }
  }
}
