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

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedHashSet;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionRecoveryIT extends AbstractSubscriptionConsensusLocalIT {

  @Test
  public void testConsumerRestartResumesFromCommittedCheckpoint() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("recovery_consumer_restart");
    SubscriptionTreePullConsumer consumer1 = null;
    SubscriptionTreePullConsumer consumer2 = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer1 =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("first"), ids.getConsumerGroupId());
      consumer1.subscribe(ids.getTopic());

      final Set<String> allRowKeys =
          new LinkedHashSet<>(
              ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 1_000L, 64, true));

      final ConsensusSubscriptionITSupport.CommittedSnapshot checkpoint =
          ConsensusSubscriptionITSupport.pollUntilCommittedRows(consumer1, ids.getTopic(), 16, 40);
      final TopicProgress checkpointProgress = checkpoint.getProgress();
      Assert.assertNotNull(checkpointProgress);

      Set<String> remainingRowKeys =
          ConsensusSubscriptionITSupport.subtract(allRowKeys, checkpoint.getCommittedRowKeys());

      consumer1.close();
      consumer1 = null;

      if (remainingRowKeys.isEmpty()) {
        allRowKeys.addAll(
            ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 2_000L, 16, true));
        remainingRowKeys =
            ConsensusSubscriptionITSupport.subtract(allRowKeys, checkpoint.getCommittedRowKeys());
      }

      Assert.assertFalse(
          "Expected rows to remain after the committed checkpoint", remainingRowKeys.isEmpty());

      consumer2 =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.consumer("restart"), ids.getConsumerGroupId());
      consumer2.subscribe(ids.getTopic());
      consumer2.seekAfter(ids.getTopic(), checkpointProgress);
      ConsensusSubscriptionITSupport.pause(1_000L);

      final ConsensusSubscriptionITSupport.ConsumedRecords replay =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer2, remainingRowKeys.size(), 60);

      ConsensusSubscriptionITSupport.assertExactRowKeys(remainingRowKeys, replay);
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer1, ids.getTopic(), ids.getDatabase());
      ConsensusSubscriptionITSupport.cleanup(consumer2, ids.getTopic(), ids.getDatabase());
    }
  }
}
