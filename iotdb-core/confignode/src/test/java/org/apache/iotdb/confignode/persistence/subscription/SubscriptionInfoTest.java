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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SubscriptionInfoTest {

  @Test
  public void testAlterTopicRejectsOwnerEpochRollback() {
    final String topicName = "topic-" + UUID.randomUUID();
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();

    final TopicMeta initialTopicMeta = createTopicMeta(topicName, "sn1", 5L);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo.createTopic(new CreateTopicPlan(initialTopicMeta)).getCode());

    final TopicMeta transferredTopicMeta = initialTopicMeta.deepCopy();
    transferredTopicMeta.transferOwner("sn2", 6L);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo.alterTopic(new AlterTopicPlan(transferredTopicMeta)).getCode());

    final TSStatus rollbackStatus =
        subscriptionInfo.alterTopic(new AlterTopicPlan(createTopicMeta(topicName, "sn1", 5L)));

    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_OWNER_EPOCH_CONFLICT.getStatusCode(), rollbackStatus.getCode());
    Assert.assertEquals("sn2", subscriptionInfo.getTopicMeta(topicName).getOwnerId());
    Assert.assertEquals(6L, subscriptionInfo.getTopicMeta(topicName).getOwnerEpoch());
  }

  private TopicMeta createTopicMeta(
      final String topicName, final String ownerId, final long ownerEpoch) {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, ownerId);
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
    return new TopicMeta(topicName, 1, topicAttributes);
  }
}
