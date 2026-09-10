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

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;

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
public class IoTDBConsensusSubscriptionBasicIT extends AbstractSubscriptionConsensusLocalIT {

  @Test
  public void testRealtimeOnlyAfterSubscribe() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("basic_realtime_only_after_subscribe");
    SubscriptionTreePullConsumer consumer = null;

    try {
      final String bootstrapRowKey =
          ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final Set<String> expectedRowKeys =
          ConsensusSubscriptionITSupport.insertRows(
              ids.getDatabase(), Arrays.asList("d0", "d1"), 100L, 8, true);

      final ConsensusSubscriptionITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 40);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertFalse(consumed.getRowKeys().contains(bootstrapRowKey));
      ConsensusSubscriptionITSupport.assertNoMoreMessages(consumer, 3, Duration.ofMillis(500));
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }

  @Test
  public void testSubscribeBeforeRegionCreation() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("basic_subscribe_before_region");
    SubscriptionTreePullConsumer consumer = null;

    try {
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      ConsensusSubscriptionITSupport.createDatabase(ids.getDatabase());
      final Set<String> expectedRowKeys =
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 1L, 12, true);

      final ConsensusSubscriptionITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 50);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed);
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }

  @Test
  public void testRealtimeRowsSurviveFlush() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("basic_rows_survive_flush");
    SubscriptionTreePullConsumer consumer = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final Set<String> expectedRowKeys = new LinkedHashSet<>();
      expectedRowKeys.addAll(
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 100L, 6, true));
      expectedRowKeys.addAll(
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 200L, 6, true));
      expectedRowKeys.addAll(
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d1", 300L, 4, true));

      final ConsensusSubscriptionITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 50);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertTrue(
          "Expected rows from both devices after flush boundaries, actual=" + consumed,
          consumed
              .getRowsPerDevice()
              .keySet()
              .containsAll(Arrays.asList(ids.getDatabase() + ".d0", ids.getDatabase() + ".d1")));
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }
}
