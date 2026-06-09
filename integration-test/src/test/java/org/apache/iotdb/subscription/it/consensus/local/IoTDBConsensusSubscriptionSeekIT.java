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
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionSeekIT extends AbstractSubscriptionConsensusLocalIT {

  private static final Duration SEEK_POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final int TAIL_ROW_COUNT = 256;
  private static final int CHECKPOINT_MINIMUM_ROWS = 96;

  @Test
  public void testSeekToBeginningReplaysRows() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("seek_to_beginning");
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
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 1_000L, 32, true);

      final ConsensusSubscriptionITSupport.ConsumedRecords initialDrain =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 40, SEEK_POLL_TIMEOUT);
      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, initialDrain);

      consumer.seekToBeginning(ids.getTopic());
      ConsensusSubscriptionITSupport.pause(1_000L);

      final ConsensusSubscriptionITSupport.ConsumedRecords replay =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 40, SEEK_POLL_TIMEOUT);

      ConsensusSubscriptionITSupport.assertContainsExpectedRowKeys(expectedRowKeys, replay, 1);
      if (replay.getUniqueRowCount() == expectedRowKeys.size() + 1) {
        Assert.assertTrue(
            "Only the bootstrap row is allowed as an extra replayed row, replay=" + replay,
            replay.getRowKeys().contains(bootstrapRowKey));
      }
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }

  @Test
  public void testSeekAfterCheckpointReplaysExactTail() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("seek_after_checkpoint");
    SubscriptionTreePullConsumer consumer = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final Set<String> allRowKeys =
          new java.util.LinkedHashSet<>(
              ConsensusSubscriptionITSupport.insertRows(
                  ids.getDatabase(), "d0", 1_000L, TAIL_ROW_COUNT, true));

      final ConsensusSubscriptionITSupport.CommittedSnapshot checkpoint =
          ConsensusSubscriptionITSupport.pollUntilCommittedRows(
              consumer, ids.getTopic(), CHECKPOINT_MINIMUM_ROWS, 40, SEEK_POLL_TIMEOUT);
      TopicProgress checkpointProgress = checkpoint.getProgress();
      Assert.assertNotNull(checkpointProgress);

      Set<String> expectedTail =
          ConsensusSubscriptionITSupport.subtract(allRowKeys, checkpoint.getCommittedRowKeys());
      if (expectedTail.isEmpty()) {
        allRowKeys.addAll(
            ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 2_000L, 64, true));
      }

      ConsensusSubscriptionITSupport.drainAndCommitUntilQuiet(consumer, 40, SEEK_POLL_TIMEOUT);
      expectedTail =
          ConsensusSubscriptionITSupport.subtract(allRowKeys, checkpoint.getCommittedRowKeys());
      Assert.assertFalse("Expected a non-empty replay tail", expectedTail.isEmpty());

      consumer.seekAfter(ids.getTopic(), checkpointProgress);
      ConsensusSubscriptionITSupport.pause(1_000L);

      final ConsensusSubscriptionITSupport.ConsumedRecords replay =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedTail.size(), 50, SEEK_POLL_TIMEOUT);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedTail, replay);
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }

  @Test
  public void testSeekAfterFencesStaleCommitContexts() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("seek_after_fences_stale_contexts");
    SubscriptionTreePullConsumer consumer = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final Set<String> allRowKeys =
          new java.util.LinkedHashSet<>(
              ConsensusSubscriptionITSupport.insertRows(
                  ids.getDatabase(), "d0", 1_000L, TAIL_ROW_COUNT, true));

      final ConsensusSubscriptionITSupport.CommittedSnapshot checkpoint =
          ConsensusSubscriptionITSupport.pollUntilCommittedRows(
              consumer, ids.getTopic(), CHECKPOINT_MINIMUM_ROWS, 40, SEEK_POLL_TIMEOUT);
      final TopicProgress checkpointProgress = checkpoint.getProgress();
      Assert.assertNotNull(checkpointProgress);

      Set<String> expectedTail =
          ConsensusSubscriptionITSupport.subtract(allRowKeys, checkpoint.getCommittedRowKeys());

      ConsensusSubscriptionITSupport.PolledMessageBatch staleBatch =
          ConsensusSubscriptionITSupport.pollFirstNonEmptyBatchWithoutCommit(
              consumer, 5, SEEK_POLL_TIMEOUT);
      if (staleBatch.getConsumedRecords().getRowCount() == 0) {
        allRowKeys.addAll(
            ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 2_000L, 64, true));
        expectedTail =
            ConsensusSubscriptionITSupport.subtract(allRowKeys, checkpoint.getCommittedRowKeys());
        staleBatch =
            ConsensusSubscriptionITSupport.pollFirstNonEmptyBatchWithoutCommit(
                consumer, 10, SEEK_POLL_TIMEOUT);
      }

      Assert.assertTrue(
          "Expected a stale batch after checkpoint, batch=" + staleBatch.getConsumedRecords(),
          staleBatch.getConsumedRecords().getRowCount() > 0);

      consumer.seekAfter(ids.getTopic(), checkpointProgress);
      ConsensusSubscriptionITSupport.pause(1_000L);

      for (final SubscriptionMessage staleMessage : staleBatch.getMessages()) {
        consumer.commitSync(staleMessage);
      }

      final ConsensusSubscriptionITSupport.ConsumedRecords replay =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedTail.size(), 60, SEEK_POLL_TIMEOUT);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedTail, replay);
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }
}
