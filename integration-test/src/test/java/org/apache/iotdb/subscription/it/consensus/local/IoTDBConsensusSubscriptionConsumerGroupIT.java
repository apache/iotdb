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
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionConsumerGroupIT
    extends AbstractSubscriptionConsensusLocalIT {

  @Test
  public void testDifferentConsumerGroupsReceiveIndependentFullData() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("consumer_group_different_groups");
    SubscriptionTreePullConsumer consumer1 = null;
    SubscriptionTreePullConsumer consumer2 = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer1 =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("g1"), ids.consumerGroup("g1"));
      consumer2 =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("g2"), ids.consumerGroup("g2"));
      consumer1.subscribe(ids.getTopic());
      consumer2.subscribe(ids.getTopic());

      final Set<String> expectedRowKeys =
          ConsensusSubscriptionITSupport.insertRows(
              ids.getDatabase(), Arrays.asList("d0", "d1"), 100L, 10, true);

      final ConsensusSubscriptionITSupport.ConsumedRecords consumed1 =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer1, expectedRowKeys.size(), 50);
      final ConsensusSubscriptionITSupport.ConsumedRecords consumed2 =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer2, expectedRowKeys.size(), 50);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed1);
      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed2);
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer1, ids.getTopic(), ids.getDatabase());
      ConsensusSubscriptionITSupport.cleanup(consumer2, ids.getTopic(), ids.getDatabase());
    }
  }

  @Test
  public void testTwoConsumersInSameGroupDoNotDuplicateRows() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("consumer_group_same_group_no_duplicate");
    SubscriptionTreePullConsumer consumer1 = null;
    SubscriptionTreePullConsumer consumer2 = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer1 =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("a"), ids.getConsumerGroupId());
      consumer2 =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("b"), ids.getConsumerGroupId());
      consumer1.subscribe(ids.getTopic());
      consumer2.subscribe(ids.getTopic());
      ConsensusSubscriptionITSupport.pause(1_000L);

      final Set<String> expectedRowKeys =
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 1_000L, 24, true);

      final ConsensusSubscriptionITSupport.GroupDrainResult result =
          ConsensusSubscriptionITSupport.drainConsumersWithoutDuplicates(
              Arrays.asList(consumer1, consumer2), expectedRowKeys.size(), 60);

      Assert.assertTrue(
          "Expected no duplicate rows across the same consumer group, union="
              + result.getUnion()
              + ", perConsumer="
              + result.getPerConsumer(),
          result.getUnion().getDuplicateRowKeys().isEmpty());
      Assert.assertEquals(expectedRowKeys, result.getUnion().getRowKeys());
      Assert.assertEquals(expectedRowKeys.size(), result.getUnion().getRowCount());
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer1, ids.getTopic(), ids.getDatabase());
      ConsensusSubscriptionITSupport.cleanup(consumer2, ids.getTopic(), ids.getDatabase());
    }
  }

  @Test
  public void testCommitAfterUnsubscribeDoesNotThrow() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("consumer_group_commit_after_unsubscribe");
    SubscriptionTreePullConsumer consumer = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 100L, 16, true);
      final ConsensusSubscriptionITSupport.PolledMessageBatch batch =
          ConsensusSubscriptionITSupport.pollFirstNonEmptyBatchWithoutCommit(consumer, 30);

      Assert.assertTrue(
          "Expected some rows to be polled before unsubscribe, batch=" + batch.getConsumedRecords(),
          batch.getConsumedRecords().getRowCount() > 0);

      consumer.unsubscribe(ids.getTopic());

      for (final SubscriptionMessage message : batch.getMessages()) {
        consumer.commitSync(message);
      }
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }
}
