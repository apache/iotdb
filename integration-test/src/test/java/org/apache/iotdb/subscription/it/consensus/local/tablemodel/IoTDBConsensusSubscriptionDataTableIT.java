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
import java.util.Locale;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionDataTableIT extends AbstractSubscriptionConsensusLocalIT {

  private static final long TIME_PARTITION_GAP = 604_800_001L;
  private static final String TYPED_TABLE_SCHEMA =
      "tag1 STRING TAG, "
          + "s_int32 INT32 FIELD, "
          + "s_int64 INT64 FIELD, "
          + "s_float FLOAT FIELD, "
          + "s_double DOUBLE FIELD, "
          + "s_bool BOOLEAN FIELD, "
          + "s_text TEXT FIELD";

  @Test
  public void testTypedRowsAcrossTimePartitions() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers(
            "table_data_typed_rows_across_partitions");
    final String database = ids.getDatabase();
    final String tableName = "t1";
    final String bootstrapRowKey =
        ConsensusSubscriptionTableITSupport.rowKey(database, tableName, 0L);
    final Set<String> expectedColumns =
        new LinkedHashSet<>(
            Arrays.asList("tag1", "s_int32", "s_int64", "s_float", "s_double", "s_bool", "s_text"));
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(
          database, tableName, TYPED_TABLE_SCHEMA);
      try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + database);
        session.executeNonQueryStatement(
            "insert into "
                + tableName
                + "(tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                + "values ('bootstrap', 0, 0, 0.0, 0.0, true, 'bootstrap', 0)");
        session.executeNonQueryStatement("flush");
      }

      ConsensusSubscriptionTableITSupport.createConsensusTopic(ids.getTopic(), database, tableName);
      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final long[] timestamps = {
        100L,
        101L,
        102L,
        1_000_000_000L,
        1_000_000_000L + TIME_PARTITION_GAP,
        1_000_000_000L + TIME_PARTITION_GAP * 2
      };
      final Set<String> expectedRowKeys = new LinkedHashSet<>();

      try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + database);
        for (int i = 0; i < timestamps.length; i++) {
          final long timestamp = timestamps[i];
          session.executeNonQueryStatement(
              String.format(
                  Locale.ROOT,
                  "insert into %s(tag1, s_int32, s_int64, s_float, s_double, s_bool, s_text, time) "
                      + "values ('device_%d', %d, %d, %.1f, %.2f, %s, 'text_%d', %d)",
                  tableName,
                  i,
                  i + 1,
                  (i + 1L) * 100L,
                  (i + 1) * 1.1f,
                  (i + 1) * 2.22d,
                  i % 2 == 0,
                  i,
                  timestamp));
          expectedRowKeys.add(
              ConsensusSubscriptionTableITSupport.rowKey(database, tableName, timestamp));
        }
        session.executeNonQueryStatement("flush");
      }

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 60);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertFalse(consumed.getRowKeys().contains(bootstrapRowKey));
      Assert.assertEquals(
          expectedRowKeys.size(), consumed.getRowsPerTable().getOrDefault(tableName, 0).intValue());
      Assert.assertTrue(
          "Expected typed columns in consumed records, actual=" + consumed.getSeenColumns(),
          consumed.getSeenColumns().containsAll(expectedColumns));
      ConsensusSubscriptionTableITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), database);
    }
  }
}
