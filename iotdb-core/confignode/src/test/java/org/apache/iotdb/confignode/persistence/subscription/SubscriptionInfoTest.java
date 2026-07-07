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

package org.apache.iotdb.confignode.persistence.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.response.subscription.TopicTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.mpp.rpc.thrift.TTopicOwnerLeaseEntry;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SubscriptionInfoTest {

  @Test
  public void testAlterTopicRejectsOwnerEpochRollback() {
    final String topicName = "topic-" + UUID.randomUUID();
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final TopicMeta initialTopicMeta = createTopicMeta(topicName, "owner1", 5L);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo.createTopic(new CreateTopicPlan(initialTopicMeta)).getCode());

    final TopicMeta transferredTopicMeta = initialTopicMeta.deepCopy();
    transferredTopicMeta.transferOwner("owner2", 6L);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo.alterTopic(new AlterTopicPlan(transferredTopicMeta)).getCode());

    final TSStatus rollbackStatus =
        subscriptionInfo.alterTopic(new AlterTopicPlan(createTopicMeta(topicName, "owner1", 5L)));

    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_OWNER_EPOCH_CONFLICT.getStatusCode(), rollbackStatus.getCode());
    Assert.assertEquals("owner2", subscriptionInfo.getTopicMeta(topicName).getOwnerId());
    Assert.assertEquals(6L, subscriptionInfo.getTopicMeta(topicName).getOwnerEpoch());
  }

  @Test
  public void testAlterTopicTransfersOwnerWithUpdatedAttributes() {
    final String topicName = "topic-" + UUID.randomUUID();
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final TopicMeta initialTopicMeta = createTopicMeta(topicName, "owner1", 5L);
    initialTopicMeta.getConfig().getAttribute().put(TopicConstant.PATH_KEY, "root.sg.**");
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo.createTopic(new CreateTopicPlan(initialTopicMeta)).getCode());

    final Map<String, String> updatedAttributes = new HashMap<>();
    updatedAttributes.put(TopicConstant.OWNER_ID_KEY, "owner2");
    updatedAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "6");
    final TSStatus alterStatus =
        subscriptionInfo.alterTopic(
            new AlterTopicPlan(initialTopicMeta.deepCopyWithUpdatedAttributes(updatedAttributes)));

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), alterStatus.getCode());
    Assert.assertEquals("owner2", subscriptionInfo.getTopicMeta(topicName).getOwnerId());
    Assert.assertEquals(6L, subscriptionInfo.getTopicMeta(topicName).getOwnerEpoch());
    Assert.assertEquals(
        "root.sg.**",
        subscriptionInfo.getTopicMeta(topicName).getConfig().getString(TopicConstant.PATH_KEY));
  }

  @Test
  public void testCollectTopicOwnerLeaseEntriesSkipsBlockedTopic() {
    final String topicName = "topic-" + UUID.randomUUID();
    final String blockedTopicName = "topic-" + UUID.randomUUID();
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final long ownerLeaseDurationMs = 60000;
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo
            .createTopic(
                new CreateTopicPlan(createTopicMeta(topicName, "owner1", 5L, ownerLeaseDurationMs)))
            .getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo
            .createTopic(
                new CreateTopicPlan(
                    createTopicMeta(blockedTopicName, "owner1", 5L, ownerLeaseDurationMs)))
            .getCode());

    final List<TTopicOwnerLeaseEntry> entries =
        subscriptionInfo.collectTopicOwnerLeaseEntries(Collections.singleton(blockedTopicName));

    Assert.assertEquals(1, entries.size());
    final TTopicOwnerLeaseEntry entry = entries.get(0);
    Assert.assertEquals(topicName, entry.getTopicName());
    Assert.assertEquals("owner1", entry.getOwnerId());
    Assert.assertEquals(5L, entry.getOwnerEpoch());
    // Relative remaining duration equals the configured lease duration; read-only, no mutation.
    Assert.assertEquals(ownerLeaseDurationMs, entry.getLeaseRemainingMs());
  }

  @Test
  public void testAlterTopicOwnerAndShowTopicOwner() {
    final String topicName = "topic-" + UUID.randomUUID();
    final long ownerLeaseDurationMs = 123456L;
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final TopicMeta initialTopicMeta = createTopicMeta(topicName, "owner1", 5L);
    initialTopicMeta.getConfig().getAttribute().put(TopicConstant.PATH_KEY, "root.sg.**");
    initialTopicMeta.getConfig().getAttribute().put(TopicConstant.START_TIME_KEY, "0");
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo.createTopic(new CreateTopicPlan(initialTopicMeta)).getCode());

    final Map<String, String> updatedAttributes = new HashMap<>();
    updatedAttributes.put(TopicConstant.OWNER_ID_KEY, "owner2");
    updatedAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "6");
    updatedAttributes.put(
        TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));
    final TopicMeta updatedTopicMeta =
        initialTopicMeta.deepCopyWithUpdatedAttributes(updatedAttributes);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo.alterTopic(new AlterTopicPlan(updatedTopicMeta)).getCode());

    final TShowTopicResp showTopicResp =
        ((TopicTableResp) subscriptionInfo.showTopics()).convertToTShowTopicResp();

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), showTopicResp.status.code);
    Assert.assertEquals(1, showTopicResp.getTopicInfoListSize());

    final TShowTopicInfo showTopicInfo = showTopicResp.getTopicInfoList().get(0);
    Assert.assertEquals(topicName, showTopicInfo.getTopicName());
    Assert.assertEquals(1L, showTopicInfo.getCreationTime());
    Assert.assertTrue(showTopicInfo.getTopicAttributes().contains("path=root.sg.**"));
    Assert.assertTrue(showTopicInfo.getTopicAttributes().contains("start-time=0"));
    Assert.assertTrue(showTopicInfo.getTopicAttributes().contains("owner-id=owner2"));
    Assert.assertTrue(showTopicInfo.getTopicAttributes().contains("owner-epoch=6"));
    Assert.assertTrue(
        showTopicInfo
            .getTopicAttributes()
            .contains("owner-lease-duration-ms=" + ownerLeaseDurationMs));
    // The absolute lease expiry is DataNode-local runtime state; it is never a topic attribute and
    // must not appear in SHOW TOPICS output.
    Assert.assertFalse(showTopicInfo.getTopicAttributes().contains("owner-lease-expire-time-ms"));
  }

  private TopicMeta createTopicMeta(
      final String topicName, final String ownerId, final long ownerEpoch) {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, ownerId);
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
    return new TopicMeta(topicName, 1, topicAttributes);
  }

  private TopicMeta createTopicMeta(
      final String topicName,
      final String ownerId,
      final long ownerEpoch,
      final long ownerLeaseDurationMs) {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, ownerId);
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
    topicAttributes.put(
        TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));
    return new TopicMeta(topicName, 1, topicAttributes);
  }
}
