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
import java.util.LinkedHashSet;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionBasicTableIT extends AbstractSubscriptionConsensusLocalIT {

  @Test
  public void testRealtimeOnlyAfterSubscribe() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers(
            "table_basic_realtime_only_after_subscribe");
    final String database = ids.getDatabase();
    final String table1 = "t1";
    final String table2 = "t2";
    final String table3 = "t3";
    SubscriptionTablePullConsumer consumer = null;

    try {
      final String bootstrapRowKey =
          ConsensusSubscriptionTableITSupport.bootstrapDatabaseAndTable(
              database, table1, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createTable(
          database, table2, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createTable(
          database, table3, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createConsensusTopic(ids.getTopic(), database, ".*");

      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final Set<String> expectedRowKeys = new LinkedHashSet<>();
      expectedRowKeys.addAll(
          ConsensusSubscriptionTableITSupport.insertRows(database, table1, 100L, 8, true));
      expectedRowKeys.addAll(
          ConsensusSubscriptionTableITSupport.insertRows(database, table2, 200L, 5, true));
      expectedRowKeys.addAll(
          ConsensusSubscriptionTableITSupport.insertRows(database, table3, 300L, 4, true));

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 40);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertFalse(consumed.getRowKeys().contains(bootstrapRowKey));
      Assert.assertEquals(8, consumed.getRowsPerTable().getOrDefault(table1, 0).intValue());
      Assert.assertEquals(5, consumed.getRowsPerTable().getOrDefault(table2, 0).intValue());
      Assert.assertEquals(4, consumed.getRowsPerTable().getOrDefault(table3, 0).intValue());
      ConsensusSubscriptionTableITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), database);
    }
  }

  @Test
  public void testWritableViewEmitsLogicalTableAndAliasColumns() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("table_writable_view");
    final String viewDatabase = ids.getDatabase();
    final String sourceTable = "source_table";
    final String viewTable = "writable_view";
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(
          viewDatabase,
          sourceTable,
          "device_id STRING TAG, value INT64 FIELD, hidden DOUBLE FIELD");
      try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + viewDatabase);
        session.executeNonQueryStatement(
            "create writable view "
                + viewTable
                + " as select device_id as device_alias, value as value_alias from "
                + sourceTable);
        session.executeNonQueryStatement(
            "insert into "
                + sourceTable
                + "(device_id, value, hidden, time) values ('bootstrap', 0, 0.0, 0)");
        session.executeNonQueryStatement("flush");
      }

      ConsensusSubscriptionTableITSupport.createConsensusTopic(
          ids.getTopic(), viewDatabase, viewTable, "column_name = \"value_alias\"");
      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + viewDatabase);
        session.executeNonQueryStatement(
            "insert into "
                + sourceTable
                + "(device_id, value, hidden, time) values ('source', 1010, 1.0, 101)");
        session.executeNonQueryStatement("use " + viewDatabase);
        session.executeNonQueryStatement(
            "insert into "
                + viewTable
                + "(device_alias, value_alias, time) values ('view', 1020, 102)");
        session.executeNonQueryStatement("flush");
      }

      final Set<String> expectedRowKeys = new LinkedHashSet<>();
      expectedRowKeys.add(
          ConsensusSubscriptionTableITSupport.rowKey(viewDatabase, viewTable, 101L));
      expectedRowKeys.add(
          ConsensusSubscriptionTableITSupport.rowKey(viewDatabase, viewTable, 102L));
      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilContains(
              consumer, expectedRowKeys, 40);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertEquals(
          new LinkedHashSet<>(Arrays.asList("device_alias", "value_alias")),
          consumed.getSeenColumns());
      Assert.assertEquals(2, consumed.getRowsPerTable().getOrDefault(viewTable, 0).intValue());
      Assert.assertFalse(consumed.getRowsPerTable().containsKey(sourceTable));
      Assert.assertFalse(consumed.getSeenColumns().contains("device_id"));
      Assert.assertFalse(consumed.getSeenColumns().contains("value"));
      Assert.assertFalse(consumed.getSeenColumns().contains("hidden"));
      ConsensusSubscriptionTableITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), viewDatabase);
    }
  }
}
