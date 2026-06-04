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

package org.apache.iotdb.commons.subscription.topic;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TopicDeSerTest {

  @Test
  public void test() throws IOException {
    Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put("k1", "v1");
    topicAttributes.put("k2", "v2");

    TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);

    topicMeta.addSubscribedConsumerGroup("test_consumer_group");
    Assert.assertTrue(topicMeta.isSubscribedByConsumerGroup("test_consumer_group"));

    ByteBuffer byteBuffer = topicMeta.serialize();
    TopicMeta topicMeta1 = TopicMeta.deserialize(byteBuffer);
    TopicMeta topicMeta2 = topicMeta1.deepCopy();

    Assert.assertEquals(topicMeta, topicMeta1);
    Assert.assertEquals(topicMeta, topicMeta2);
    Assert.assertEquals(topicMeta.getTopicName(), topicMeta2.getTopicName());
    Assert.assertEquals(topicMeta.getCreationTime(), topicMeta2.getCreationTime());
    Assert.assertEquals(topicMeta.getConfig(), topicMeta2.getConfig());
    Assert.assertEquals(
        topicMeta.getSubscribedConsumerGroupIds(), topicMeta2.getSubscribedConsumerGroupIds());
  }

  @Test
  public void testTopicOwnerDeSer() throws IOException {
    Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "sn1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "5");

    TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);

    Assert.assertTrue(topicMeta.isOwnerFencingEnabled());
    Assert.assertEquals("sn1", topicMeta.getOwnerId());
    Assert.assertEquals(5L, topicMeta.getOwnerEpoch());
    Assert.assertTrue(topicMeta.matchesOwner("sn1", 5L));
    Assert.assertFalse(topicMeta.matchesOwner("sn2", 5L));
    Assert.assertFalse(topicMeta.matchesOwner("sn1", 4L));

    TopicMeta topicMeta1 = TopicMeta.deserialize(topicMeta.serialize());
    TopicMeta topicMeta2 = topicMeta1.deepCopy();

    Assert.assertEquals(topicMeta, topicMeta1);
    Assert.assertEquals(topicMeta, topicMeta2);
    Assert.assertEquals(topicMeta.getOwnerId(), topicMeta2.getOwnerId());
    Assert.assertEquals(topicMeta.getOwnerEpoch(), topicMeta2.getOwnerEpoch());
    Assert.assertEquals(
        topicMeta.getOwnerLeaseExpireTimeMs(), topicMeta2.getOwnerLeaseExpireTimeMs());

    topicMeta.transferOwner("sn2", 6L, 100L);
    Assert.assertEquals("sn2", topicMeta.getOwnerId());
    Assert.assertEquals(6L, topicMeta.getOwnerEpoch());
    Assert.assertEquals("sn2", topicMeta.getConfig().getString(TopicConstant.OWNER_ID_KEY));
    Assert.assertEquals(
        6L, topicMeta.getConfig().getLong(TopicConstant.OWNER_EPOCH_KEY).longValue());
    Assert.assertEquals(
        100L,
        topicMeta.getConfig().getLong(TopicConstant.OWNER_LEASE_EXPIRE_TIME_MS_KEY).longValue());

    topicMeta.clearOwner();
    Assert.assertFalse(topicMeta.isOwnerFencingEnabled());
    Assert.assertFalse(topicMeta.getConfig().hasAttribute(TopicConstant.OWNER_ID_KEY));
    Assert.assertFalse(topicMeta.getConfig().hasAttribute(TopicConstant.OWNER_EPOCH_KEY));
    Assert.assertFalse(
        topicMeta.getConfig().hasAttribute(TopicConstant.OWNER_LEASE_EXPIRE_TIME_MS_KEY));
  }

  @Test
  public void testDeepCopyWithUpdatedAttributesTransfersOwnerAndPreservesExistingConfig() {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.PATH_KEY, "root.sg.**");
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "sn1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "5");
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);
    topicMeta.addSubscribedConsumerGroup("group1");

    final Map<String, String> updatedAttributes = new HashMap<>();
    updatedAttributes.put(TopicConstant.OWNER_ID_KEY, "sn2");
    updatedAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "6");
    final TopicMeta updatedTopicMeta = topicMeta.deepCopyWithUpdatedAttributes(updatedAttributes);

    Assert.assertEquals(
        "root.sg.**", updatedTopicMeta.getConfig().getString(TopicConstant.PATH_KEY));
    Assert.assertEquals("sn2", updatedTopicMeta.getOwnerId());
    Assert.assertEquals(6L, updatedTopicMeta.getOwnerEpoch());
    Assert.assertTrue(updatedTopicMeta.isSubscribedByConsumerGroup("group1"));
    Assert.assertEquals("sn1", topicMeta.getOwnerId());
    Assert.assertEquals(5L, topicMeta.getOwnerEpoch());
  }

  @Test
  public void testSequentialTopicMetaDeserializeDoesNotConsumeNextTopic() throws IOException {
    final TopicMeta firstTopicMeta = new TopicMeta("first_topic", 1, new HashMap<>());
    final TopicMeta secondTopicMeta = new TopicMeta("second_topic", 2, new HashMap<>());

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    firstTopicMeta.serialize(outputStream);
    secondTopicMeta.serialize(outputStream);

    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Assert.assertEquals(firstTopicMeta, TopicMeta.deserialize(inputStream));
    Assert.assertEquals(secondTopicMeta, TopicMeta.deserialize(inputStream));
    Assert.assertEquals(0, inputStream.available());
  }

  @Test
  public void testGenerateExtractorAttributesWithEncryptedPassword() {
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, new HashMap<>());

    final Map<String, String> extractorAttributes =
        topicMeta.generateExtractorAttributes("test_user", "encrypted-password");

    Assert.assertEquals("test_user", extractorAttributes.get("username"));
    Assert.assertEquals("encrypted-password", extractorAttributes.get("password"));
  }
}
