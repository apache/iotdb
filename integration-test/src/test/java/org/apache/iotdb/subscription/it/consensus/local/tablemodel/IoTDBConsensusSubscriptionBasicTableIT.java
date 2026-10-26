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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.subscription.it.consensus.local.AbstractSubscriptionConsensusLocalIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionBasicTableIT extends AbstractSubscriptionConsensusLocalIT {

  @Test
  public void testLastConsumerCloseRetainsProgress() throws Exception {
    for (final boolean unsubscribeFirst : new boolean[] {false, true}) {
      final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
          ConsensusSubscriptionTableITSupport.newIdentifiers("retained_last_close");
      SubscriptionTablePullConsumer consumer = null;
      try {
        ConsensusSubscriptionTableITSupport.bootstrapDatabaseAndTable(
            ids.getDatabase(), "t1", ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
        ConsensusSubscriptionTableITSupport.createConsensusTopic(
            ids.getTopic(), ids.getDatabase(), "t1");
        try (final ISubscriptionTableSession session =
            new SubscriptionTableSessionBuilder()
                .host(EnvFactory.getEnv().getIP())
                .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
                .build()) {
          session.open();
          final Properties config = new Properties();
          config.setProperty(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, "true");
          session.alterTopic(ids.getTopic(), config);
        }
        consumer =
            ConsensusSubscriptionTableITSupport.createConsumer(
                ids.getConsumerId(), ids.getConsumerGroupId());
        consumer.subscribe(ids.getTopic());
        final Set<String> firstRows =
            ConsensusSubscriptionTableITSupport.insertRows(ids.getDatabase(), "t1", 100L, 10, true);
        ConsensusSubscriptionTableITSupport.assertExactRowKeys(
            firstRows,
            ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
                consumer, firstRows.size(), 40));
        if (unsubscribeFirst) {
          consumer.unsubscribe(ids.getTopic());
        }
        consumer.close();
        consumer = null;

        final Set<String> offlineRows =
            ConsensusSubscriptionTableITSupport.insertRows(ids.getDatabase(), "t1", 200L, 10, true);
        consumer =
            ConsensusSubscriptionTableITSupport.createConsumer(
                ids.consumer("replacement"), ids.getConsumerGroupId());
        consumer.subscribe(ids.getTopic());
        ConsensusSubscriptionTableITSupport.assertExactRowKeys(
            offlineRows,
            ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
                consumer, offlineRows.size(), 40));
        ConsensusSubscriptionTableITSupport.assertNoMoreMessages(
            consumer, 3, Duration.ofMillis(500));
      } finally {
        ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
      }
    }
  }

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
}
