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

package org.apache.iotdb.subscription.it.dual.tablemodel;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionTableArchVerification;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;
import org.apache.iotdb.subscription.it.dual.AbstractSubscriptionDualIT;

import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionTableArchVerification.class})
public class IoTDBSubscriptionWritableViewIT extends AbstractSubscriptionDualIT {

  private static final String SOURCE_TABLE = "source_table";
  private static final String VIEW_TABLE = "writable_view";
  private static final String SOURCE_SCHEMA =
      "device_id STRING TAG, site STRING TAG, model STRING ATTRIBUTE, temperature INT32 FIELD, "
          + "humidity DOUBLE FIELD, hidden INT64 FIELD";

  @Override
  protected void setUpConfig() {
    super.setUpConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setEnforceStrongPassword(false)
        .setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(30);
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeMetaSyncerInitialSyncDelayMinutes(1)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setPipeMetaSyncerSyncIntervalMinutes(1)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
  }

  @Test
  public void testLiveWritableViewProjectsLogicalNamesAndAliasColumnFilter() throws Exception {
    final String database = databaseName("live");
    final String topicName = topicName("live");
    try {
      createWritableViewSchema(database);
      createTopic(
          topicName,
          database,
          VIEW_TABLE,
          TopicConstant.MODE_LIVE_VALUE,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          "column_name = \"temp\"");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerName("live"), consumerGroupName("live"))) {
        consumer.subscribe(topicName);
        insertSourceRow(database, 501L);
        insertViewRow(database, 502L);

        final ConsumedRows consumed =
            pollUntilTimestamps(consumer, new LinkedHashSet<>(Arrays.asList(501L, 502L)), false);

        Assert.assertEquals(
            new LinkedHashSet<>(Arrays.asList("dev", "area", "temp")), consumed.columnNames);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(database)), consumed.databaseNames);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(VIEW_TABLE)), consumed.tableNames);
        Assert.assertEquals(2, consumed.rowCount);
        Assert.assertFalse(consumed.columnNames.contains("device_id"));
        Assert.assertFalse(consumed.columnNames.contains("temperature"));
        Assert.assertFalse(consumed.columnNames.contains("humidity"));
        Assert.assertFalse(consumed.columnNames.contains("hidden"));
        Assert.assertFalse(consumed.columnNames.contains("label"));
      }
    } finally {
      cleanup(topicName, database, null);
    }
  }

  @Test
  public void testSnapshotWritableViewProjectsHistoricalSourceAndViewWrites() throws Exception {
    final String database = databaseName("snapshot");
    final String topicName = topicName("snapshot");
    try {
      createWritableViewSchema(database);
      insertSourceRow(database, 601L);
      insertViewRow(database, 602L);
      createTopic(
          topicName,
          database,
          VIEW_TABLE,
          TopicConstant.MODE_SNAPSHOT_VALUE,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          null);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerName("snapshot"), consumerGroupName("snapshot"))) {
        consumer.subscribe(topicName);

        final ConsumedRows consumed =
            pollUntilTimestamps(consumer, new LinkedHashSet<>(Arrays.asList(601L, 602L)), true);

        Assert.assertEquals(
            new LinkedHashSet<>(Arrays.asList("time", "dev", "area", "label", "temp", "humidity")),
            consumed.columnNames);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(database)), consumed.databaseNames);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(VIEW_TABLE)), consumed.tableNames);
        Assert.assertEquals(2, consumed.rowCount);
        Assert.assertFalse(consumed.columnNames.contains("hidden"));
      }
    } finally {
      cleanup(topicName, database, null);
    }
  }

  @Test
  public void testWritableViewTsFileTopicIsRejectedOnSubscribe() throws Exception {
    final String database = databaseName("tsfile");
    final String topicName = topicName("tsfile");
    try {
      createWritableViewSchema(database);
      createTopic(
          topicName,
          database,
          VIEW_TABLE,
          TopicConstant.MODE_LIVE_VALUE,
          TopicConstant.FORMAT_TS_FILE_VALUE,
          null);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerName("tsfile"), consumerGroupName("tsfile"))) {
        final Exception exception =
            Assert.assertThrows(Exception.class, () -> consumer.subscribe(topicName));
        Assert.assertTrue(exception.getMessage().contains("record format"));
      }
    } finally {
      cleanup(topicName, database, null);
    }
  }

  @Test
  public void testWritableViewRegexTopicIsRejectedOnSubscribe() throws Exception {
    final String database = databaseName("regex");
    final String topicName = topicName("regex");
    try {
      createWritableViewSchema(database);
      createTopic(
          topicName,
          database,
          "writable_.*",
          TopicConstant.MODE_LIVE_VALUE,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          null);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerName("regex"), consumerGroupName("regex"))) {
        final Exception exception =
            Assert.assertThrows(Exception.class, () -> consumer.subscribe(topicName));
        Assert.assertTrue(exception.getMessage().contains("exact database and table"));
      }
    } finally {
      cleanup(topicName, database, null);
    }
  }

  @Test
  public void testWritableViewSubscriptionUsesViewSelectPrivilege() throws Exception {
    final String database = databaseName("privilege");
    final String topicName = topicName("privilege");
    final String username = "view_sub_user_" + Math.abs(database.hashCode());
    final String password = "ViewSubPassword1!";
    try {
      createWritableViewSchema(database);
      createUserWithViewSelect(database, username, password);
      createTopic(
          topicName,
          database,
          VIEW_TABLE,
          TopicConstant.MODE_LIVE_VALUE,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          null);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(
              consumerName("privilege"), consumerGroupName("privilege"), username, password)) {
        consumer.subscribe(topicName);
        insertSourceRow(database, 701L);

        final ConsumedRows consumed =
            pollUntilTimestamps(consumer, new LinkedHashSet<>(Arrays.asList(701L)), true);

        Assert.assertEquals(1, consumed.rowCount);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(VIEW_TABLE)), consumed.tableNames);
      }
    } finally {
      cleanup(topicName, database, username);
    }
  }

  private static String databaseName(final String suffix) {
    return "subscription_writable_view_" + suffix;
  }

  private static String topicName(final String suffix) {
    return "topic_subscription_writable_view_" + suffix;
  }

  private static String consumerName(final String suffix) {
    return "consumer_subscription_writable_view_" + suffix;
  }

  private static String consumerGroupName(final String suffix) {
    return "group_subscription_writable_view_" + suffix;
  }

  private void createWritableViewSchema(final String database) throws Exception {
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + database);
      statement.execute("use " + database);
      statement.execute(String.format("create table %s (%s)", SOURCE_TABLE, SOURCE_SCHEMA));
      statement.execute(
          "create writable view "
              + VIEW_TABLE
              + " as select device_id as dev, site as area, model as label, "
              + "temperature as temp, humidity from "
              + SOURCE_TABLE);
    }
  }

  private void createTopic(
      final String topicName,
      final String database,
      final String table,
      final String mode,
      final String format,
      final String columnFilter)
      throws Exception {
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(senderEnv.getIP())
            .port(Integer.parseInt(senderEnv.getPort()))
            .build()) {
      session.open();
      session.dropTopicIfExists(topicName);
      final Properties config = new Properties();
      config.put(TopicConstant.MODE_KEY, mode);
      config.put(TopicConstant.FORMAT_KEY, format);
      config.put(TopicConstant.DATABASE_KEY, database);
      config.put(TopicConstant.TABLE_KEY, table);
      if (Objects.nonNull(columnFilter)) {
        config.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
      }
      session.createTopic(topicName, config);
    }
  }

  private ISubscriptionTablePullConsumer createConsumer(
      final String consumerId, final String consumerGroupId) throws Exception {
    return createConsumer(consumerId, consumerGroupId, null, null);
  }

  private ISubscriptionTablePullConsumer createConsumer(
      final String consumerId,
      final String consumerGroupId,
      final String username,
      final String password)
      throws Exception {
    final SubscriptionTablePullConsumerBuilder builder =
        new SubscriptionTablePullConsumerBuilder()
            .host(senderEnv.getIP())
            .port(Integer.parseInt(senderEnv.getPort()))
            .consumerId(consumerId)
            .consumerGroupId(consumerGroupId)
            .autoCommit(false);
    if (Objects.nonNull(username)) {
      builder.username(username).password(password);
    }
    final ISubscriptionTablePullConsumer consumer = builder.build();
    consumer.open();
    return consumer;
  }

  private void insertSourceRow(final String database, final long timestamp) throws Exception {
    executeInsert(
        database,
        String.format(
            Locale.ROOT,
            "insert into %s(device_id, site, model, temperature, humidity, hidden, time) "
                + "values ('device_%d', 'site_%d', 'model_%d', %d, %.1f, %d, %d)",
            SOURCE_TABLE,
            timestamp,
            timestamp,
            timestamp,
            timestamp,
            timestamp + 0.5d,
            timestamp * 10,
            timestamp));
  }

  private void insertViewRow(final String database, final long timestamp) throws Exception {
    executeInsert(
        database,
        String.format(
            Locale.ROOT,
            "insert into %s(dev, area, label, temp, humidity, time) "
                + "values ('device_%d', 'site_%d', 'model_%d', %d, %.1f, %d)",
            VIEW_TABLE,
            timestamp,
            timestamp,
            timestamp,
            timestamp,
            timestamp + 0.5d,
            timestamp));
  }

  private void executeInsert(final String database, final String sql) throws Exception {
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      statement.execute(sql);
      statement.execute("flush");
    }
  }

  private void createUserWithViewSelect(
      final String database, final String username, final String password) throws Exception {
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create user " + username + " '" + password + "'");
      statement.execute("use " + database);
      statement.execute("grant select on table " + VIEW_TABLE + " to user " + username);
    }
  }

  private static ConsumedRows pollUntilTimestamps(
      final ISubscriptionTablePullConsumer consumer,
      final Set<Long> expectedTimestamps,
      final boolean expectedTimeSelected)
      throws Exception {
    final ConsumedRows consumed = new ConsumedRows();
    int emptyRoundsAfterExpected = 0;
    for (int round = 0; round < 90 && emptyRoundsAfterExpected < 2; round++) {
      final List<SubscriptionMessage> messages =
          consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        if (consumed.timestamps.containsAll(expectedTimestamps)) {
          emptyRoundsAfterExpected++;
        }
        continue;
      }
      for (final SubscriptionMessage message : messages) {
        if (SubscriptionMessageType.WATERMARK.getType() == message.getMessageType()) {
          continue;
        }
        Assert.assertEquals(
            SubscriptionMessageType.RECORD_HANDLER.getType(), message.getMessageType());
        Assert.assertEquals(expectedTimeSelected, message.isTimeSelected());
        for (final ResultSet resultSet : message.getResultSets()) {
          final SubscriptionRecordHandler.SubscriptionResultSet subscriptionResultSet =
              (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
          consumed.databaseNames.add(subscriptionResultSet.getDatabaseName());
          consumed.tableNames.add(subscriptionResultSet.getTableName());
          subscriptionResultSet
              .getColumnNames()
              .forEach(columnName -> consumed.columnNames.add(columnName.toLowerCase(Locale.ROOT)));
          while (subscriptionResultSet.hasNext()) {
            final RowRecord rowRecord = subscriptionResultSet.nextRecord();
            consumed.timestamps.add(rowRecord.getTimestamp());
            consumed.rowCount++;
          }
        }
      }
      consumer.commitSync(messages);
    }

    Assert.assertEquals(expectedTimestamps, consumed.timestamps);
    return consumed;
  }

  private void cleanup(final String topicName, final String database, final String username) {
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(senderEnv.getIP())
            .port(Integer.parseInt(senderEnv.getPort()))
            .build()) {
      session.open();
      session.dropTopicIfExists(topicName);
    } catch (final Exception ignored) {
      // ignored on cleanup
    }
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("drop database if exists " + database);
      if (Objects.nonNull(username)) {
        statement.execute("drop user " + username);
      }
    } catch (final Exception ignored) {
      // ignored on cleanup
    }
  }

  private static final class ConsumedRows {
    private final Set<String> databaseNames = new LinkedHashSet<>();
    private final Set<String> tableNames = new LinkedHashSet<>();
    private final Set<String> columnNames = new LinkedHashSet<>();
    private final Set<Long> timestamps = new LinkedHashSet<>();
    private int rowCount;
  }
}
