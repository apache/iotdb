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

import org.apache.iotdb.db.it.utils.TestUtils;
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
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionTableArchVerification.class})
public class IoTDBSubscriptionColumnFilterIT extends AbstractSubscriptionDualIT {

  private static final String TABLE_NAME = "t_column_filter";
  private static final String TABLE_SCHEMA =
      "tag1 STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD, s3 BOOLEAN FIELD";
  private static final String COLUMN_FILTER = "column_name IN (\"time\", \"tag1\", \"s2\")";
  private static final String FIELD_CATEGORY_FILTER = "category = \"FIELD\"";
  private static final String FIELD_CATEGORY_REBIND_FILTER = "category IN (\"FIELD\")";
  private static final String MIXED_COLUMN_FILTER =
      "( CATEGORY = \"field\" AND datatype IN (\"int64\", \"double\") )"
          + " OR \"column_name\" REGEXP \"tag.*\"";
  private static final String COMPLEX_OPERATOR_FILTER =
      "datatype IS NOT NULL"
          + " AND column_name != \"s1\""
          + " AND column_name NOT IN (\"s1\", \"s3\")"
          + " AND column_name NOT LIKE \"unknown%\""
          + " AND column_name NOT REGEXP \"unknown.*\""
          + " AND (column_name LIKE \"s%\" OR column_name = \"tag1\")";
  private static final Set<String> EXPECTED_COLUMNS =
      new LinkedHashSet<>(Arrays.asList("time", "tag1", "s2"));
  private static final Set<String> EXPECTED_ALL_COLUMNS =
      new LinkedHashSet<>(Arrays.asList("time", "tag1", "s1", "s2", "s3"));
  private static final Set<String> EXPECTED_MIXED_COLUMNS =
      new LinkedHashSet<>(Arrays.asList("tag1", "s1", "s2"));
  private static final Set<String> EXPECTED_COMPLEX_OPERATOR_COLUMNS =
      new LinkedHashSet<>(Arrays.asList("tag1", "s2"));

  @Override
  protected void setUpConfig() {
    super.setUpConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
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
  public void testLiveRecordHandlerColumnFilter() throws Exception {
    final String database = databaseName("record");
    final String topicName = topicName("record");
    final String consumerId = consumerName("record");
    final String consumerGroupId = consumerGroupName("record");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          COLUMN_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 1, 4);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(1L, 2L, 3L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, true);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(EXPECTED_COLUMNS, stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("s1"));
        Assert.assertFalse(stats.columnNames.contains("s3"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerWithoutColumnFilterPreservesAllColumns() throws Exception {
    final String database = databaseName("default");
    final String topicName = topicName("default");
    final String consumerId = consumerName("default");
    final String consumerGroupId = consumerGroupName("default");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopicWithoutColumnFilter(
          topicName, database, TABLE_NAME, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 110, 113);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(110L, 111L, 112L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, true);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(EXPECTED_ALL_COLUMNS, stats.columnNames);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerColumnFilterKeepsHistoricalAndRealtimeConsistent()
      throws Exception {
    final String database = databaseName("live_both");
    final String topicName = topicName("live_both");
    final String consumerId = consumerName("live_both");
    final String consumerGroupId = consumerGroupName("live_both");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      insertRows(senderEnv, database, TABLE_NAME, 180, 183);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          COLUMN_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);

        final Set<Long> historicalTimestamps = new LinkedHashSet<>(Arrays.asList(180L, 181L, 182L));
        final ConsumedRecordStats historicalStats =
            pollRecordMessagesForTimestamps(consumer, historicalTimestamps, true);
        Assert.assertEquals(EXPECTED_COLUMNS, historicalStats.columnNames);

        insertRows(senderEnv, database, TABLE_NAME, 190, 193);

        final Set<Long> realtimeTimestamps = new LinkedHashSet<>(Arrays.asList(190L, 191L, 192L));
        final ConsumedRecordStats realtimeStats =
            pollRecordMessagesForTimestamps(consumer, realtimeTimestamps, true);
        Assert.assertEquals(EXPECTED_COLUMNS, realtimeStats.columnNames);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerColumnFilterAppliesToMultipleConsumerGroups() throws Exception {
    final String database = databaseName("multi_group");
    final String topicName = topicName("multi_group");
    final String firstConsumerId = consumerName("multi_group_a");
    final String firstConsumerGroupId = consumerGroupName("multi_group_a");
    final String secondConsumerId = consumerName("multi_group_b");
    final String secondConsumerGroupId = consumerGroupName("multi_group_b");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          COLUMN_FILTER);

      try (final ISubscriptionTablePullConsumer firstConsumer =
              createConsumer(firstConsumerId, firstConsumerGroupId);
          final ISubscriptionTablePullConsumer secondConsumer =
              createConsumer(secondConsumerId, secondConsumerGroupId)) {
        firstConsumer.subscribe(topicName);
        secondConsumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 170, 173);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(170L, 171L, 172L));
        final ConsumedRecordStats firstStats =
            pollRecordMessagesForTimestamps(firstConsumer, expectedTimestamps, true);
        final ConsumedRecordStats secondStats =
            pollRecordMessagesForTimestamps(secondConsumer, expectedTimestamps, true);

        Assert.assertEquals(EXPECTED_COLUMNS, firstStats.columnNames);
        Assert.assertEquals(EXPECTED_COLUMNS, secondStats.columnNames);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testSnapshotRecordHandlerColumnFilter() throws Exception {
    final String database = databaseName("snapshot");
    final String topicName = topicName("snapshot");
    final String consumerId = consumerName("snapshot");
    final String consumerGroupId = consumerGroupName("snapshot");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      insertRows(senderEnv, database, TABLE_NAME, 20, 23);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.MODE_SNAPSHOT_VALUE,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          COLUMN_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(20L, 21L, 22L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, true);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(EXPECTED_COLUMNS, stats.columnNames);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerMixedCaseInsensitiveColumnFilter() throws Exception {
    final String database = databaseName("mixed");
    final String topicName = topicName("mixed");
    final String consumerId = consumerName("mixed");
    final String consumerGroupId = consumerGroupName("mixed");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          MIXED_COLUMN_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 30, 33);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(30L, 31L, 32L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, false);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(EXPECTED_MIXED_COLUMNS, stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("time"));
        Assert.assertFalse(stats.columnNames.contains("s3"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerComplexOperatorColumnFilter() throws Exception {
    final String database = databaseName("operators");
    final String topicName = topicName("operators");
    final String consumerId = consumerName("operators");
    final String consumerGroupId = consumerGroupName("operators");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          COMPLEX_OPERATOR_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 100, 103);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(100L, 101L, 102L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, false);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(EXPECTED_COMPLEX_OPERATOR_COLUMNS, stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("time"));
        Assert.assertFalse(stats.columnNames.contains("s1"));
        Assert.assertFalse(stats.columnNames.contains("s3"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerColumnFilterDropsMatchedAttributeColumn() throws Exception {
    final String database = databaseName("attribute");
    final String topicName = topicName("attribute");
    final String consumerId = consumerName("attribute");
    final String consumerGroupId = consumerGroupName("attribute");
    final String schemaWithAttribute =
        "tag1 STRING TAG, attr1 STRING ATTRIBUTE, s1 INT64 FIELD, s2 DOUBLE FIELD";

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, schemaWithAttribute);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          "category = \"ATTRIBUTE\" OR column_name = \"s2\"");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRowsWithAttribute(senderEnv, database, TABLE_NAME, 160, 163);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(160L, 161L, 162L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, false);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("tag1", "s2")), stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("attr1"));
        Assert.assertFalse(stats.columnNames.contains("s1"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testCreateTopicWithTreeViewColumnFilterUsesViewFieldName() throws Exception {
    final String database = databaseName("view_topic");
    final String treeDatabase = database + "_tree";
    final String viewName = TABLE_NAME + "_view";
    final String topicName = topicName("view_topic");
    final String consumerId = consumerName("view_topic");
    final String consumerGroupId = consumerGroupName("view_topic");

    try {
      createTreeView(senderEnv, database, treeDatabase, viewName);
      createTopic(
          topicName,
          database,
          viewName,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          "column_name = \"s_view\"");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertTreeRows(senderEnv, treeDatabase, 400, 403);

        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(
                consumer, new LinkedHashSet<>(Arrays.asList(400L, 401L, 402L)), false);

        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("tag1", "s_src")), stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("s_view"));
        Assert.assertFalse(stats.columnNames.contains("s_other"));
      }
    } finally {
      cleanup(topicName, database);
      dropTreeDatabase(senderEnv, treeDatabase);
    }
  }

  @Test
  public void testLiveTsFileColumnFilter() throws Exception {
    final String database = databaseName("tsfile");
    final String topicName = topicName("tsfile");
    final String consumerId = consumerName("tsfile");
    final String consumerGroupId = consumerGroupName("tsfile");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createDatabaseAndTable(receiverEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName, database, TABLE_NAME, TopicConstant.FORMAT_TS_FILE_VALUE, COLUMN_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 10, 13);
        pollTsFileMessagesAndLoad(consumer, database, 3);
      }

      assertLoadedTsFileRows(database, 3);
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testSnapshotTsFileColumnFilter() throws Exception {
    final String database = databaseName("snapshot_tsfile");
    final String topicName = topicName("snapshot_tsfile");
    final String consumerId = consumerName("snapshot_tsfile");
    final String consumerGroupId = consumerGroupName("snapshot_tsfile");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createDatabaseAndTable(receiverEnv, database, TABLE_NAME, TABLE_SCHEMA);
      insertRows(senderEnv, database, TABLE_NAME, 200, 203);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.MODE_SNAPSHOT_VALUE,
          TopicConstant.FORMAT_TS_FILE_VALUE,
          COLUMN_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        pollTsFileMessagesAndLoad(consumer, database, 3);
      }

      assertLoadedTsFileRows(database, 3);
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveTsFileTrueColumnFilterPreservesAllColumns() throws Exception {
    final String database = databaseName("tsfile_true");
    final String topicName = topicName("tsfile_true");
    final String consumerId = consumerName("tsfile_true");
    final String consumerGroupId = consumerGroupName("tsfile_true");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createDatabaseAndTable(receiverEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(topicName, database, TABLE_NAME, TopicConstant.FORMAT_TS_FILE_VALUE, "true");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 120, 123);
        pollTsFileMessagesAndLoad(consumer, database, 3);
      }

      assertLoadedTsFileRowsWithAllColumns(database, 3);
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerAlterColumnFilterRebindsNewData() throws Exception {
    final String database = databaseName("alter");
    final String topicName = topicName("alter");
    final String consumerId = consumerName("alter");
    final String consumerGroupId = consumerGroupName("alter");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          "column_name = \"s1\"");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 40, 43);

        final Set<Long> beforeAlterTimestamps = new LinkedHashSet<>(Arrays.asList(40L, 41L, 42L));
        final ConsumedRecordStats beforeAlter =
            pollRecordMessagesForTimestamps(consumer, beforeAlterTimestamps, false);
        Assert.assertEquals(beforeAlterTimestamps, beforeAlter.timestamps);
        Assert.assertEquals(
            new LinkedHashSet<>(Arrays.asList("tag1", "s1")), beforeAlter.columnNames);

        alterTopicColumnFilter(topicName, COLUMN_FILTER);
        insertRows(senderEnv, database, TABLE_NAME, 50, 53);

        final Set<Long> afterAlterTimestamps = new LinkedHashSet<>(Arrays.asList(50L, 51L, 52L));
        final ConsumedRecordStats afterAlter =
            pollRecordMessagesForTimestamps(consumer, afterAlterTimestamps, true);
        Assert.assertEquals(afterAlterTimestamps, afterAlter.timestamps);
        Assert.assertEquals(EXPECTED_COLUMNS, afterAlter.columnNames);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerFalseColumnFilterSkipsRowsAndRebinds() throws Exception {
    final String database = databaseName("false");
    final String topicName = topicName("false");
    final String consumerId = consumerName("false");
    final String consumerGroupId = consumerGroupName("false");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName, database, TABLE_NAME, TopicConstant.FORMAT_RECORD_HANDLER_VALUE, "false");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 60, 63);

        assertNoUserRows(consumer, 5);

        alterTopicColumnFilter(topicName, COLUMN_FILTER);
        insertRows(senderEnv, database, TABLE_NAME, 70, 73);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(70L, 71L, 72L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, true);

        Assert.assertEquals(EXPECTED_COLUMNS, stats.columnNames);
        Assert.assertEquals(expectedTimestamps, stats.timestamps);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerColumnFilterSurvivesSenderRestart() throws Exception {
    final String database = databaseName("restart");
    final String topicName = topicName("restart");
    final String consumerId = consumerName("restart");
    final String consumerGroupId = consumerGroupName("restart");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          COLUMN_FILTER);

      TestUtils.restartCluster(senderEnv);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 130, 133);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(130L, 131L, 132L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, true);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(EXPECTED_COLUMNS, stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("s1"));
        Assert.assertFalse(stats.columnNames.contains("s3"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerRestartResnapshotsNewMatchingTable() throws Exception {
    final String database = databaseName("restart_new");
    final String topicName = topicName("restart_new");
    final String consumerId = consumerName("restart_new");
    final String consumerGroupId = consumerGroupName("restart_new");
    final String initialTableName = TABLE_NAME + "_restart_old";
    final String newTableName = TABLE_NAME + "_restart_new";

    try {
      createDatabaseAndTable(senderEnv, database, initialTableName, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME + "_restart_.*",
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          FIELD_CATEGORY_FILTER);
      createTable(senderEnv, database, newTableName, TABLE_SCHEMA);

      TestUtils.restartCluster(senderEnv);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, newTableName, 240, 243);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(240L, 241L, 242L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, false);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(
            new LinkedHashSet<>(Arrays.asList("tag1", "s1", "s2", "s3")), stats.columnNames);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(newTableName)), stats.tableNames);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerStrictSnapshotRequiresAlterAfterAddColumn() throws Exception {
    final String database = databaseName("strict");
    final String topicName = topicName("strict");
    final String consumerId = consumerName("strict");
    final String consumerGroupId = consumerGroupName("strict");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          FIELD_CATEGORY_FILTER);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        addColumn(senderEnv, database, TABLE_NAME, "s4 INT32 FIELD");
        insertRowsWithS4(senderEnv, database, TABLE_NAME, 80, 84);

        final Set<Long> beforeAlterTimestamps =
            new LinkedHashSet<>(Arrays.asList(80L, 81L, 82L, 83L));
        final ConsumedRecordStats beforeAlter =
            pollRecordMessagesForTimestamps(consumer, beforeAlterTimestamps, false);
        Assert.assertTrue(beforeAlter.columnNames.contains("tag1"));
        Assert.assertTrue(beforeAlter.columnNames.contains("s1"));
        Assert.assertTrue(beforeAlter.columnNames.contains("s2"));
        Assert.assertTrue(beforeAlter.columnNames.contains("s3"));
        Assert.assertFalse(beforeAlter.columnNames.contains("s4"));

        alterTopicColumnFilter(topicName, FIELD_CATEGORY_REBIND_FILTER);
        insertRowsWithS4(senderEnv, database, TABLE_NAME, 90, 94);

        final Set<Long> afterAlterTimestamps =
            new LinkedHashSet<>(Arrays.asList(90L, 91L, 92L, 93L));
        final ConsumedRecordStats afterAlter =
            pollRecordMessagesForTimestamps(consumer, afterAlterTimestamps, false);
        Assert.assertTrue(afterAlter.columnNames.contains("tag1"));
        Assert.assertTrue(afterAlter.columnNames.contains("s1"));
        Assert.assertTrue(afterAlter.columnNames.contains("s2"));
        Assert.assertTrue(afterAlter.columnNames.contains("s3"));
        Assert.assertTrue(afterAlter.columnNames.contains("s4"));
        Assert.assertEquals(afterAlterTimestamps, afterAlter.timestamps);
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerExpressionCoverageAndTagRetention() throws Exception {
    final String database = databaseName("expr");
    final String topicName = topicName("expr");
    final String consumerId = consumerName("expr");
    final String consumerGroupId = consumerGroupName("expr");
    final String expressionFilter =
        "database IS NOT NULL"
            + " AND table_name IS NOT NULL"
            + " AND (database IS NULL OR datatype IN (\"DOUBLE\", \"FLOAT\", \"INT64\"))"
            + " AND column_name NOT IN (\"s1\", \"s3\")"
            + " AND column_name NOT LIKE \"unknown%\""
            + " AND column_name NOT REGEXP \"unknown.*\"";

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          expressionFilter);

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 250, 253);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(250L, 251L, 252L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, false);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("tag1", "s2")), stats.columnNames);
        Assert.assertTrue(
            "TAG column must be retained when only FIELD matches",
            stats.columnNames.contains("tag1"));
        Assert.assertFalse(stats.columnNames.contains("time"));
        Assert.assertFalse(stats.columnNames.contains("s1"));
        Assert.assertFalse(stats.columnNames.contains("s3"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerColumnFilterMatchesCustomTimeColumn() throws Exception {
    final String database = databaseName("custom_time");
    final String topicName = topicName("custom_time");
    final String consumerId = consumerName("custom_time");
    final String consumerGroupId = consumerGroupName("custom_time");
    final String customTimeTableSchema =
        "event_time TIMESTAMP TIME, tag1 STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD";

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, customTimeTableSchema);
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          "column_name = \"event_time\" OR column_name = \"s2\"");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRowsWithCustomTimeColumn(senderEnv, database, TABLE_NAME, 140, 143);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(140L, 141L, 142L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, true);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(EXPECTED_COLUMNS, stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("s1"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerColumnFilterUsesAlteredDataTypeAtBindTime() throws Exception {
    final String database = databaseName("alter_type");
    final String topicName = topicName("alter_type");
    final String consumerId = consumerName("alter_type");
    final String consumerGroupId = consumerGroupName("alter_type");
    final String initialSchema =
        "tag1 STRING TAG, s1 INT32 FIELD, s2 DOUBLE FIELD, s3 BOOLEAN FIELD";

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, initialSchema);
      alterColumnType(senderEnv, database, TABLE_NAME, "s1", "INT64");
      createTopic(
          topicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          "datatype = \"INT64\"");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, TABLE_NAME, 150, 153);

        final Set<Long> expectedTimestamps = new LinkedHashSet<>(Arrays.asList(150L, 151L, 152L));
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestamps(consumer, expectedTimestamps, false);

        Assert.assertEquals(expectedTimestamps, stats.timestamps);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("tag1", "s1")), stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("time"));
        Assert.assertFalse(stats.columnNames.contains("s2"));
        Assert.assertFalse(stats.columnNames.contains("s3"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testLiveRecordHandlerColumnFilterAppliesTimeSelectionPerTable() throws Exception {
    final String database = databaseName("multi_time");
    final String topicName = topicName("multi_time");
    final String consumerId = consumerName("multi_time");
    final String consumerGroupId = consumerGroupName("multi_time");
    final String timeSelectedTableName = TABLE_NAME + "_time_selected";
    final String timeUnselectedTableName = TABLE_NAME + "_time_unselected";

    try {
      createDatabaseAndTable(senderEnv, database, timeSelectedTableName, TABLE_SCHEMA);
      createTable(senderEnv, database, timeUnselectedTableName, TABLE_SCHEMA);
      createTopic(
          topicName,
          database,
          TABLE_NAME + "_time_.*",
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          "(table_name = \""
              + timeSelectedTableName
              + "\" AND column_name = \"time\")"
              + " OR (table_name = \""
              + timeUnselectedTableName
              + "\" AND column_name = \"s2\")");

      try (final ISubscriptionTablePullConsumer consumer =
          createConsumer(consumerId, consumerGroupId)) {
        consumer.subscribe(topicName);
        insertRows(senderEnv, database, timeSelectedTableName, 210, 213);
        insertRows(senderEnv, database, timeUnselectedTableName, 220, 223);

        final Map<Long, Boolean> expectedTimeSelectedByTimestamp = new LinkedHashMap<>();
        expectedTimeSelectedByTimestamp.put(210L, true);
        expectedTimeSelectedByTimestamp.put(211L, true);
        expectedTimeSelectedByTimestamp.put(212L, true);
        expectedTimeSelectedByTimestamp.put(220L, false);
        expectedTimeSelectedByTimestamp.put(221L, false);
        expectedTimeSelectedByTimestamp.put(222L, false);
        final ConsumedRecordStats stats =
            pollRecordMessagesForTimestampTimeSelection(consumer, expectedTimeSelectedByTimestamp);

        Assert.assertEquals(expectedTimeSelectedByTimestamp.keySet(), stats.timestamps);
        Assert.assertEquals(expectedTimeSelectedByTimestamp, stats.timeSelectedByTimestamp);
        Assert.assertEquals(
            new LinkedHashSet<>(Arrays.asList("time", "tag1", "s2")), stats.columnNames);
        Assert.assertFalse(stats.columnNames.contains("s1"));
        Assert.assertFalse(stats.columnNames.contains("s3"));
      }
    } finally {
      cleanup(topicName, database);
    }
  }

  @Test
  public void testCreateAndAlterRejectInvalidColumnFilter() throws Exception {
    final String database = databaseName("invalid");
    final String invalidTopicName = topicName("invalid_create");
    final String validTopicName = topicName("invalid_alter");

    try {
      createDatabaseAndTable(senderEnv, database, TABLE_NAME, TABLE_SCHEMA);

      final Exception createException =
          Assert.assertThrows(
              Exception.class,
              () ->
                  createTopic(
                      invalidTopicName,
                      database,
                      TABLE_NAME,
                      TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
                      "temperature > 10"));
      Assert.assertFalse(String.valueOf(createException.getMessage()).contains("tag_"));

      createTopic(
          validTopicName,
          database,
          TABLE_NAME,
          TopicConstant.FORMAT_RECORD_HANDLER_VALUE,
          COLUMN_FILTER);
      final Exception alterException =
          Assert.assertThrows(
              Exception.class, () -> alterTopicColumnFilter(validTopicName, "owner = \"alice\""));
      Assert.assertFalse(String.valueOf(alterException.getMessage()).contains("tag_"));
    } finally {
      cleanup(invalidTopicName, database);
      cleanup(validTopicName, database);
    }
  }

  private String databaseName(final String suffix) {
    return "cf_" + suffix + "_" + testId();
  }

  private String topicName(final String suffix) {
    return "topic_cf_" + suffix + "_" + testId();
  }

  private String consumerName(final String suffix) {
    return "consumer_cf_" + suffix + "_" + testId();
  }

  private String consumerGroupName(final String suffix) {
    return "cg_cf_" + suffix + "_" + testId();
  }

  private String testId() {
    return Integer.toUnsignedString(testName.getDisplayName().hashCode(), 36);
  }

  private static void createDatabaseAndTable(
      final BaseEnv env, final String database, final String tableName, final String schema)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + database);
      statement.execute("use " + database);
      statement.execute(String.format("create table %s (%s)", tableName, schema));
    }
  }

  private static void createTable(
      final BaseEnv env, final String database, final String tableName, final String schema)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      statement.execute(String.format("create table %s (%s)", tableName, schema));
    }
  }

  private static void createTreeView(
      final BaseEnv env, final String database, final String treeDatabase, final String viewName)
      throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database root." + treeDatabase);
      statement.execute(
          "create timeseries root." + treeDatabase + ".d1.s_src with datatype=DOUBLE");
      statement.execute(
          "create timeseries root." + treeDatabase + ".d1.s_other with datatype=DOUBLE");
    }

    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + database);
      statement.execute("use " + database);
      statement.execute(
          String.format(
              Locale.ROOT,
              "create view %s(tag1 string tag, s_view double field from s_src, "
                  + "s_other double field from s_other) as root.%s.**",
              viewName,
              treeDatabase));
    }
  }

  private static void insertTreeRows(
      final BaseEnv env, final String treeDatabase, final int start, final int end)
      throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      for (int i = start; i < end; i++) {
        statement.execute(
            String.format(
                Locale.ROOT,
                "insert into root.%s.d1(timestamp,s_src,s_other) values(%d,%f,%f)",
                treeDatabase,
                i,
                i * 1.0,
                i * 10.0));
      }
    }
  }

  private void createTopic(
      final String topicName,
      final String database,
      final String tableName,
      final String format,
      final String columnFilter)
      throws Exception {
    createTopic(
        topicName, database, tableName, TopicConstant.MODE_LIVE_VALUE, format, columnFilter);
  }

  private void createTopic(
      final String topicName,
      final String database,
      final String tableName,
      final String mode,
      final String format,
      final String columnFilter)
      throws Exception {
    createTopic(topicName, database, tableName, mode, format, columnFilter, true);
  }

  private void createTopicWithoutColumnFilter(
      final String topicName, final String database, final String tableName, final String format)
      throws Exception {
    createTopic(topicName, database, tableName, TopicConstant.MODE_LIVE_VALUE, format, "", false);
  }

  private void createTopic(
      final String topicName,
      final String database,
      final String tableName,
      final String mode,
      final String format,
      final String columnFilter,
      final boolean includeColumnFilter)
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
      config.put(TopicConstant.TABLE_KEY, tableName);
      if (includeColumnFilter) {
        config.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
      }
      session.createTopic(topicName, config);
    }
  }

  private void alterTopicColumnFilter(final String topicName, final String columnFilter)
      throws Exception {
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(senderEnv.getIP())
            .port(Integer.parseInt(senderEnv.getPort()))
            .build()) {
      session.open();

      final Properties config = new Properties();
      config.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
      session.alterTopic(topicName, config);
    }
  }

  private ISubscriptionTablePullConsumer createConsumer(
      final String consumerId, final String consumerGroupId) throws Exception {
    final ISubscriptionTablePullConsumer consumer =
        new SubscriptionTablePullConsumerBuilder()
            .host(senderEnv.getIP())
            .port(Integer.parseInt(senderEnv.getPort()))
            .consumerId(consumerId)
            .consumerGroupId(consumerGroupId)
            .autoCommit(false)
            .build();
    consumer.open();
    return consumer;
  }

  private static void insertRows(
      final BaseEnv env,
      final String database,
      final String tableName,
      final int startInclusive,
      final int endExclusive)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      for (int i = startInclusive; i < endExclusive; i++) {
        statement.execute(
            String.format(
                Locale.ROOT,
                "insert into %s(tag1, s1, s2, s3, time) values ('tag_%d', %d, %.1f, %s, %d)",
                tableName,
                i,
                i * 10L,
                i + 0.5d,
                i % 2 == 0 ? "true" : "false",
                i));
      }
      statement.execute("flush");
    }
  }

  private static void insertRowsWithS4(
      final BaseEnv env,
      final String database,
      final String tableName,
      final int startInclusive,
      final int endExclusive)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      for (int i = startInclusive; i < endExclusive; i++) {
        statement.execute(
            String.format(
                Locale.ROOT,
                "insert into %s(tag1, s1, s2, s3, s4, time) "
                    + "values ('tag_%d', %d, %.1f, %s, %d, %d)",
                tableName,
                i,
                i * 10L,
                i + 0.5d,
                i % 2 == 0 ? "true" : "false",
                i,
                i));
      }
      statement.execute("flush");
    }
  }

  private static void insertRowsWithAttribute(
      final BaseEnv env,
      final String database,
      final String tableName,
      final int startInclusive,
      final int endExclusive)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      for (int i = startInclusive; i < endExclusive; i++) {
        statement.execute(
            String.format(
                Locale.ROOT,
                "insert into %s(tag1, attr1, s1, s2, time) "
                    + "values ('tag_%d', 'attr_%d', %d, %.1f, %d)",
                tableName,
                i,
                i,
                i * 10L,
                i + 0.5d,
                i));
      }
      statement.execute("flush");
    }
  }

  private static void insertRowsWithCustomTimeColumn(
      final BaseEnv env,
      final String database,
      final String tableName,
      final int startInclusive,
      final int endExclusive)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      for (int i = startInclusive; i < endExclusive; i++) {
        statement.execute(
            String.format(
                Locale.ROOT,
                "insert into %s(tag1, s1, s2, event_time) values ('tag_%d', %d, %.1f, %d)",
                tableName,
                i,
                i * 10L,
                i + 0.5d,
                i));
      }
      statement.execute("flush");
    }
  }

  private static void addColumn(
      final BaseEnv env, final String database, final String tableName, final String column)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      statement.execute(String.format("alter table %s add column %s", tableName, column));
    }
  }

  private static void alterColumnType(
      final BaseEnv env,
      final String database,
      final String tableName,
      final String columnName,
      final String dataType)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      statement.execute(
          String.format(
              "alter table %s alter column %s set data type %s", tableName, columnName, dataType));
    }
  }

  private static void assertNoUserRows(
      final ISubscriptionTablePullConsumer consumer, final int rounds) throws Exception {
    int rowCount = 0;
    for (int round = 0; round < rounds; round++) {
      final List<SubscriptionMessage> messages =
          consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        continue;
      }
      for (final SubscriptionMessage message : messages) {
        if (SubscriptionMessageType.WATERMARK.getType() == message.getMessageType()) {
          continue;
        }
        Assert.assertEquals(
            SubscriptionMessageType.RECORD_HANDLER.getType(), message.getMessageType());
        Assert.assertFalse(message.isTimeSelected());
        for (final ResultSet resultSet : message.getResultSets()) {
          final SubscriptionRecordHandler.SubscriptionResultSet subscriptionResultSet =
              (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
          while (subscriptionResultSet.hasNext()) {
            subscriptionResultSet.nextRecord();
            rowCount++;
          }
        }
      }
      consumer.commitSync(messages);
    }
    Assert.assertEquals(0, rowCount);
  }

  private static ConsumedRecordStats pollRecordMessages(
      final ISubscriptionTablePullConsumer consumer, final int expectedRows) throws Exception {
    return pollRecordMessages(consumer, expectedRows, true);
  }

  private static ConsumedRecordStats pollRecordMessages(
      final ISubscriptionTablePullConsumer consumer,
      final int expectedRows,
      final boolean expectedTimeSelected)
      throws Exception {
    final ConsumedRecordStats stats = new ConsumedRecordStats();
    for (int round = 0; round < 60 && stats.rowCount < expectedRows; round++) {
      final List<SubscriptionMessage> messages =
          consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        continue;
      }
      consumeRecordMessages(messages, stats, expectedTimeSelected);
      consumer.commitSync(messages);
    }
    Assert.assertEquals(stats.toString(), expectedRows, stats.rowCount);
    return stats;
  }

  private static ConsumedRecordStats pollRecordMessagesForTimestamps(
      final ISubscriptionTablePullConsumer consumer,
      final Set<Long> expectedTimestamps,
      final boolean expectedTimeSelected)
      throws Exception {
    final ConsumedRecordStats stats = new ConsumedRecordStats();
    int emptyRoundsAfterExpected = 0;
    for (int round = 0; round < 90 && emptyRoundsAfterExpected < 1; round++) {
      final List<SubscriptionMessage> messages =
          consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        if (stats.timestamps.containsAll(expectedTimestamps)) {
          emptyRoundsAfterExpected++;
        }
        continue;
      }
      consumeRecordMessages(messages, stats, expectedTimeSelected);
      consumer.commitSync(messages);
    }
    Assert.assertTrue(stats.toString(), stats.timestamps.containsAll(expectedTimestamps));

    final Set<Long> unexpectedTimestamps = new LinkedHashSet<>(stats.timestamps);
    unexpectedTimestamps.removeAll(expectedTimestamps);
    Assert.assertTrue(stats.toString(), unexpectedTimestamps.isEmpty());
    return stats;
  }

  private static ConsumedRecordStats pollRecordMessagesForTimestampTimeSelection(
      final ISubscriptionTablePullConsumer consumer, final Map<Long, Boolean> expectedTimeSelection)
      throws Exception {
    final ConsumedRecordStats stats = new ConsumedRecordStats();
    int emptyRoundsAfterExpected = 0;
    for (int round = 0; round < 90 && emptyRoundsAfterExpected < 1; round++) {
      final List<SubscriptionMessage> messages =
          consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        if (stats.timestamps.containsAll(expectedTimeSelection.keySet())) {
          emptyRoundsAfterExpected++;
        }
        continue;
      }
      consumeRecordMessages(messages, stats, null);
      consumer.commitSync(messages);
    }
    Assert.assertTrue(
        stats.toString(), stats.timestamps.containsAll(expectedTimeSelection.keySet()));

    final Set<Long> unexpectedTimestamps = new LinkedHashSet<>(stats.timestamps);
    unexpectedTimestamps.removeAll(expectedTimeSelection.keySet());
    Assert.assertTrue(stats.toString(), unexpectedTimestamps.isEmpty());
    return stats;
  }

  private static void consumeRecordMessages(
      final List<SubscriptionMessage> messages,
      final ConsumedRecordStats stats,
      final Boolean expectedTimeSelected)
      throws Exception {
    for (final SubscriptionMessage message : messages) {
      if (SubscriptionMessageType.WATERMARK.getType() == message.getMessageType()) {
        continue;
      }
      Assert.assertEquals(
          SubscriptionMessageType.RECORD_HANDLER.getType(), message.getMessageType());
      if (Objects.nonNull(expectedTimeSelected)) {
        Assert.assertEquals(expectedTimeSelected.booleanValue(), message.isTimeSelected());
      }
      for (final ResultSet resultSet : message.getResultSets()) {
        final SubscriptionRecordHandler.SubscriptionResultSet subscriptionResultSet =
            (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
        stats.tableNames.add(subscriptionResultSet.getTableName());
        subscriptionResultSet
            .getColumnNames()
            .forEach(columnName -> stats.columnNames.add(columnName.toLowerCase(Locale.ROOT)));
        while (subscriptionResultSet.hasNext()) {
          final RowRecord rowRecord = subscriptionResultSet.nextRecord();
          stats.timestamps.add(rowRecord.getTimestamp());
          stats.timeSelectedByTimestamp.put(
              rowRecord.getTimestamp(), subscriptionResultSet.isTimeSelected());
          stats.rowCount++;
        }
      }
    }
  }

  private void pollTsFileMessagesAndLoad(
      final ISubscriptionTablePullConsumer consumer, final String database, final int expectedRows)
      throws Exception {
    for (int round = 0; round < 60 && countRows(receiverEnv, database) < expectedRows; round++) {
      final List<SubscriptionMessage> messages =
          consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
      if (messages.isEmpty()) {
        continue;
      }
      for (final SubscriptionMessage message : messages) {
        if (SubscriptionMessageType.WATERMARK.getType() == message.getMessageType()) {
          continue;
        }
        Assert.assertEquals(SubscriptionMessageType.TS_FILE.getType(), message.getMessageType());
        Assert.assertTrue(message.isTimeSelected());
        final SubscriptionTsFileHandler tsFileHandler = message.getTsFile();
        Assert.assertEquals(database, Objects.requireNonNull(tsFileHandler.getDatabaseName()));
        loadTsFile(receiverEnv, database, tsFileHandler);
      }
      consumer.commitSync(messages);
    }
    Assert.assertEquals(expectedRows, countRows(receiverEnv, database));
  }

  private static void loadTsFile(
      final BaseEnv env, final String database, final SubscriptionTsFileHandler tsFileHandler)
      throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      statement.execute(String.format("load '%s'", tsFileHandler.getFile().getAbsolutePath()));
    }
  }

  private static int countRows(final BaseEnv env, final String database) throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      try (final java.sql.ResultSet resultSet =
          statement.executeQuery("select count(*) from " + TABLE_NAME)) {
        Assert.assertTrue(resultSet.next());
        return resultSet.getInt(1);
      }
    }
  }

  private void assertLoadedTsFileRows(final String database, final int expectedRows)
      throws Exception {
    try (final Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      try (final java.sql.ResultSet resultSet =
          statement.executeQuery(
              "select time, tag1, s1, s2, s3 from " + TABLE_NAME + " order by time")) {
        int rows = 0;
        while (resultSet.next()) {
          rows++;
          final long timestamp = resultSet.getLong("time");
          Assert.assertEquals("tag_" + timestamp, resultSet.getString("tag1"));
          Assert.assertNull(resultSet.getObject("s1"));
          Assert.assertEquals(timestamp + 0.5d, resultSet.getDouble("s2"), 0.001d);
          Assert.assertNull(resultSet.getObject("s3"));
        }
        Assert.assertEquals(expectedRows, rows);
      }
    }
  }

  private void assertLoadedTsFileRowsWithAllColumns(final String database, final int expectedRows)
      throws Exception {
    try (final Connection connection = receiverEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      try (final java.sql.ResultSet resultSet =
          statement.executeQuery(
              "select time, tag1, s1, s2, s3 from " + TABLE_NAME + " order by time")) {
        int rows = 0;
        while (resultSet.next()) {
          rows++;
          final long timestamp = resultSet.getLong("time");
          Assert.assertEquals("tag_" + timestamp, resultSet.getString("tag1"));
          Assert.assertEquals(timestamp * 10L, resultSet.getLong("s1"));
          Assert.assertEquals(timestamp + 0.5d, resultSet.getDouble("s2"), 0.001d);
          Assert.assertEquals(timestamp % 2 == 0, resultSet.getBoolean("s3"));
        }
        Assert.assertEquals(expectedRows, rows);
      }
    }
  }

  private void cleanup(final String topicName, final String database) {
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
    dropDatabase(senderEnv, database);
    dropDatabase(receiverEnv, database);
  }

  private static void dropDatabase(final BaseEnv env, final String database) {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("drop database if exists " + database);
    } catch (final Exception ignored) {
      // ignored on cleanup
    }
  }

  private static void dropTreeDatabase(final BaseEnv env, final String database) {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("delete database root." + database);
    } catch (final Exception ignored) {
      // ignored on cleanup
    }
  }

  private static final class ConsumedRecordStats {
    private final Set<String> columnNames = new LinkedHashSet<>();
    private final Set<String> tableNames = new LinkedHashSet<>();
    private final Set<Long> timestamps = new LinkedHashSet<>();
    private final Map<Long, Boolean> timeSelectedByTimestamp = new LinkedHashMap<>();
    private int rowCount;

    @Override
    public String toString() {
      return "ConsumedRecordStats{"
          + "columnNames="
          + columnNames
          + ", tableNames="
          + tableNames
          + ", timestamps="
          + timestamps
          + ", timeSelectedByTimestamp="
          + timeSelectedByTimestamp
          + ", rowCount="
          + rowCount
          + '}';
    }
  }
}
