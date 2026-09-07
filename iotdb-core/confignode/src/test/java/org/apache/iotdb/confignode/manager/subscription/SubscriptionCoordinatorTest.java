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

package org.apache.iotdb.confignode.manager.subscription;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTopicReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SubscriptionCoordinatorTest {

  @Test
  public void testTopicAlterationLockSerializesOnlyTheSameTopic() throws Exception {
    final SubscriptionCoordinator coordinator =
        new SubscriptionCoordinator(Mockito.mock(ConfigManager.class), new SubscriptionInfo());
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch sameTopicAttemptStarted = new CountDownLatch(1);
    final CountDownLatch sameTopicLockAcquired = new CountDownLatch(1);
    boolean topic1LockHeld = true;

    coordinator.lockTopicAlteration("topic1", false);
    try {
      final Future<?> sameTopicAlteration =
          executor.submit(
              () -> {
                sameTopicAttemptStarted.countDown();
                coordinator.lockTopicAlteration("topic1", false);
                try {
                  sameTopicLockAcquired.countDown();
                } finally {
                  coordinator.unlockTopicAlteration("topic1", false);
                }
              });

      Assert.assertTrue(sameTopicAttemptStarted.await(5, TimeUnit.SECONDS));
      Assert.assertFalse(sameTopicLockAcquired.await(100, TimeUnit.MILLISECONDS));

      coordinator.lockTopicAlteration("topic1", true);
      coordinator.unlockTopicAlteration("topic1", true);

      coordinator.unlockTopicAlteration("topic1", false);
      topic1LockHeld = false;
      Assert.assertTrue(sameTopicLockAcquired.await(5, TimeUnit.SECONDS));
      sameTopicAlteration.get(5, TimeUnit.SECONDS);
    } finally {
      if (topic1LockHeld) {
        coordinator.unlockTopicAlteration("topic1", false);
      }
      executor.shutdownNow();
    }
  }

  @Test
  public void testOwnerTransferPreservesConcurrentAlterationDuringLeaseWait() throws Exception {
    final String topicName = "test_topic";
    final String initialColumnFilter = "old-column-filter";
    final String latestColumnFilter = "latest-column-filter";
    final long ownerLeaseDurationMs = 60_000L;
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final Map<String, String> initialAttributes = new HashMap<>();
    initialAttributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    initialAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "1");
    initialAttributes.put(
        TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));
    initialAttributes.put(TopicConstant.COLUMN_FILTER_KEY, initialColumnFilter);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo
            .createTopic(new CreateTopicPlan(new TopicMeta(topicName, 1L, initialAttributes)))
            .getCode());

    final SubscriptionCoordinator coordinator =
        new SubscriptionCoordinator(Mockito.mock(ConfigManager.class), subscriptionInfo) {
          @Override
          void waitForOwnerLeaseExpiration(
              final String waitingTopicName,
              final boolean isTableModel,
              final long waitingLeaseDurationMs) {
            Assert.assertEquals(topicName, waitingTopicName);
            Assert.assertFalse(isTableModel);
            Assert.assertEquals(ownerLeaseDurationMs, waitingLeaseDurationMs);

            final Map<String, String> updatedAttributes = new HashMap<>();
            updatedAttributes.put(TopicConstant.COLUMN_FILTER_KEY, latestColumnFilter);
            final TopicMeta concurrentlyUpdatedTopicMeta =
                subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(
                    topicName, updatedAttributes);
            Assert.assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                subscriptionInfo
                    .alterTopic(new AlterTopicPlan(concurrentlyUpdatedTopicMeta))
                    .getCode());
          }
        };

    final Map<String, String> ownerTransferAttributes = new HashMap<>();
    ownerTransferAttributes.put(TopicConstant.OWNER_ID_KEY, "owner2");
    ownerTransferAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "2");
    ownerTransferAttributes.put(
        TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));
    final TAlterTopicReq ownerTransferRequest =
        new TAlterTopicReq()
            .setTopicName(topicName)
            .setTopicAttributes(ownerTransferAttributes)
            .setSubscribedConsumerGroupIds(Collections.emptySet());

    Assert.assertTrue(coordinator.blockOwnerLeaseRenewalIfOwnerTransfer(ownerTransferRequest));
    try {
      final TopicMeta result =
          coordinator.buildAlteredTopicMetaAfterOwnerLeaseExpired(ownerTransferRequest);

      Assert.assertEquals("owner2", result.getOwnerId());
      Assert.assertEquals(2L, result.getOwnerEpoch());
      Assert.assertEquals(
          latestColumnFilter, result.getConfig().getString(TopicConstant.COLUMN_FILTER_KEY));
    } finally {
      coordinator.unblockOwnerLeaseRenewal(topicName);
    }
  }
}
