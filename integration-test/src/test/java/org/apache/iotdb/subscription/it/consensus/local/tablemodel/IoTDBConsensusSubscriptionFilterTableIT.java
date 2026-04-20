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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.subscription.it.consensus.local.AbstractSubscriptionConsensusLocalIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionFilterTableIT extends AbstractSubscriptionConsensusLocalIT {

  @Test
  public void testDatabaseAndTableFiltering() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("table_filter_database_and_table");
    final String database1 = ids.database("db1");
    final String database2 = ids.database("db2");
    final String table1 = "t1";
    final String table2 = "t2";
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(
          database1, table1, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createTable(
          database1, table2, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(
          database2, table1, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);

      ConsensusSubscriptionTableITSupport.insertRows(database1, table1, 0L, 1, false);
      ConsensusSubscriptionTableITSupport.insertRows(database1, table2, 0L, 1, false);
      ConsensusSubscriptionTableITSupport.insertRows(database2, table1, 0L, 1, true);

      ConsensusSubscriptionTableITSupport.createConsensusTopic(ids.getTopic(), database1, table1);

      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final Set<String> expectedRowKeys =
          ConsensusSubscriptionTableITSupport.insertRows(database1, table1, 100L, 10, false);
      ConsensusSubscriptionTableITSupport.insertRows(database1, table2, 100L, 10, false);
      ConsensusSubscriptionTableITSupport.insertRows(database2, table1, 100L, 10, true);

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 50);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertFalse(consumed.getRowsPerTable().containsKey(table2));
      Assert.assertFalse(consumed.getRowsPerDatabase().containsKey(database2));
      Assert.assertEquals(
          expectedRowKeys.size(),
          consumed.getRowsPerDatabase().getOrDefault(database1, 0).intValue());
      ConsensusSubscriptionTableITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), database1, database2);
    }
  }

  @Test
  public void testPollWithInfoTopicFilter() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("table_filter_poll_with_info");
    final String database = ids.getDatabase();
    final String table1 = "t1";
    final String table2 = "t2";
    final String topic1 = ids.topic("t1");
    final String topic2 = ids.topic("t2");
    final Set<String> subscribedTopics = new LinkedHashSet<>(Arrays.asList(topic1, topic2));
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(
          database, table1, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createTable(
          database, table2, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.insertRows(database, table1, 0L, 1, false);
      ConsensusSubscriptionTableITSupport.insertRows(database, table2, 0L, 1, true);

      ConsensusSubscriptionTableITSupport.createConsensusTopic(topic1, database, table1);
      ConsensusSubscriptionTableITSupport.createConsensusTopic(topic2, database, table2);

      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(subscribedTopics);

      final Set<String> expectedRowsTopic1 =
          ConsensusSubscriptionTableITSupport.insertRows(database, table1, 100L, 12, false);
      final Set<String> expectedRowsTopic2 =
          ConsensusSubscriptionTableITSupport.insertRows(database, table2, 200L, 8, true);

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumedTopic1 =
          ConsensusSubscriptionTableITSupport.pollWithInfoAndCommitUntilAtLeast(
              consumer, Collections.singleton(topic1), expectedRowsTopic1.size(), 40);
      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumedTopic2 =
          ConsensusSubscriptionTableITSupport.pollWithInfoAndCommitUntilAtLeast(
              consumer, Collections.singleton(topic2), expectedRowsTopic2.size(), 40);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowsTopic1, consumedTopic1);
      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowsTopic2, consumedTopic2);
      Assert.assertEquals(
          expectedRowsTopic1.size(),
          consumedTopic1.getRowsPerTable().getOrDefault(table1, 0).intValue());
      Assert.assertEquals(
          expectedRowsTopic2.size(),
          consumedTopic2.getRowsPerTable().getOrDefault(table2, 0).intValue());
      ConsensusSubscriptionTableITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, subscribedTopics, database);
    }
  }

  @Test
  public void testColumnFiltering() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("table_filter_columns");
    final String database = ids.getDatabase();
    final String table = "t1";
    final String schema = "tag1 STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD, s3 BOOLEAN FIELD";
    final String expectedColumnSignature =
        ConsensusSubscriptionTableITSupport.normalizeColumnSignature("time", "tag1", "s2");
    final Set<String> expectedSeenColumns =
        new LinkedHashSet<>(Arrays.asList("time", "tag1", "s2"));
    final Set<String> expectedRowKeys = new LinkedHashSet<>();
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(database, table, schema);
      try (final ITableSession session =
          org.apache.iotdb.it.env.EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + database);
        session.executeNonQueryStatement(
            "insert into "
                + table
                + "(tag1, s1, s2, s3, time) values ('bootstrap', 0, 0.0, true, 0)");
        session.executeNonQueryStatement("flush");
      }

      ConsensusSubscriptionTableITSupport.createConsensusTopic(
          ids.getTopic(), database, table, "(tag1|s2)");

      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      try (final ITableSession session =
          org.apache.iotdb.it.env.EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + database);
        for (int i = 1; i <= 10; i++) {
          final long timestamp = 100L + i;
          session.executeNonQueryStatement(
              String.format(
                  Locale.ROOT,
                  "insert into %s(tag1, s1, s2, s3, time) values ('tag_%d', %d, %.1f, %s, %d)",
                  table,
                  i,
                  i * 10L,
                  i + 0.5d,
                  i % 2 == 0 ? "true" : "false",
                  timestamp));
          expectedRowKeys.add(
              ConsensusSubscriptionTableITSupport.rowKey(database, table, timestamp));
        }
        session.executeNonQueryStatement("flush");
      }

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 50);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertEquals(expectedSeenColumns, consumed.getSeenColumns());
      Assert.assertEquals(
          Collections.singleton(expectedColumnSignature), consumed.getSeenColumnSignatures());
      Assert.assertFalse(consumed.getSeenColumns().contains("s1"));
      Assert.assertFalse(consumed.getSeenColumns().contains("s3"));
      ConsensusSubscriptionTableITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), database);
    }
  }

  @Test
  public void testColumnFilteringWithNoMatchedColumnsReturnsNothing() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("table_filter_columns_no_match");
    final String database = ids.getDatabase();
    final String table = "t1";
    final String schema = "tag1 STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD";
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(database, table, schema);
      try (final ITableSession session =
          org.apache.iotdb.it.env.EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + database);
        session.executeNonQueryStatement(
            "insert into " + table + "(tag1, s1, s2, time) values ('bootstrap', 0, 0.0, 0)");
        session.executeNonQueryStatement("flush");
      }

      ConsensusSubscriptionTableITSupport.createConsensusTopic(
          ids.getTopic(), database, table, "not_exist");

      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      try (final ITableSession session =
          org.apache.iotdb.it.env.EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + database);
        for (int i = 1; i <= 5; i++) {
          session.executeNonQueryStatement(
              String.format(
                  Locale.ROOT,
                  "insert into %s(tag1, s1, s2, time) values ('tag_%d', %d, %.1f, %d)",
                  table,
                  i,
                  i * 10L,
                  i + 0.25d,
                  200L + i));
        }
        session.executeNonQueryStatement("flush");
      }

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(consumer, 0, 20);

      Assert.assertEquals(0, consumed.getRowCount());
      Assert.assertTrue(consumed.getRowKeys().isEmpty());
      Assert.assertTrue(consumed.getSeenColumns().isEmpty());
      Assert.assertTrue(consumed.getSeenColumnSignatures().isEmpty());
      ConsensusSubscriptionTableITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), database);
    }
  }
}
