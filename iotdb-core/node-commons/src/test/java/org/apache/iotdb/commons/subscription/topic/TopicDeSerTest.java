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
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "5");

    TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);

    Assert.assertTrue(topicMeta.isOwnerFencingEnabled());
    Assert.assertEquals("owner1", topicMeta.getOwnerId());
    Assert.assertEquals(5L, topicMeta.getOwnerEpoch());
    Assert.assertTrue(topicMeta.matchesOwner("owner1", 5L));
    Assert.assertFalse(topicMeta.matchesOwner("owner2", 5L));
    Assert.assertFalse(topicMeta.matchesOwner("owner1", 4L));

    TopicMeta topicMeta1 = TopicMeta.deserialize(topicMeta.serialize());
    TopicMeta topicMeta2 = topicMeta1.deepCopy();

    Assert.assertEquals(topicMeta, topicMeta1);
    Assert.assertEquals(topicMeta, topicMeta2);
    Assert.assertEquals(topicMeta.getOwnerId(), topicMeta2.getOwnerId());
    Assert.assertEquals(topicMeta.getOwnerEpoch(), topicMeta2.getOwnerEpoch());
    Assert.assertEquals(topicMeta.getOwnerLeaseDurationMs(), topicMeta2.getOwnerLeaseDurationMs());
    Assert.assertEquals(
        topicMeta.getOwnerLeaseExpireTimeMs(), topicMeta2.getOwnerLeaseExpireTimeMs());

    topicMeta.transferOwner("owner2", 6L, 100L);
    Assert.assertEquals("owner2", topicMeta.getOwnerId());
    Assert.assertEquals(6L, topicMeta.getOwnerEpoch());
    Assert.assertEquals("owner2", topicMeta.getConfig().getString(TopicConstant.OWNER_ID_KEY));
    Assert.assertEquals(
        6L, topicMeta.getConfig().getLong(TopicConstant.OWNER_EPOCH_KEY).longValue());
    Assert.assertEquals(
        100L, topicMeta.getConfig().getLong(TopicConstant.OWNER_LEASE_DURATION_MS_KEY).longValue());
    Assert.assertEquals(100L, topicMeta.getOwnerLeaseDurationMs().longValue());
    // A freshly transferred owner has no lease expiry until the first owner-lease heartbeat.
    Assert.assertNull(topicMeta.getOwnerLeaseExpireTimeMs());

    topicMeta.clearOwner();
    Assert.assertFalse(topicMeta.isOwnerFencingEnabled());
    Assert.assertFalse(topicMeta.getConfig().hasAttribute(TopicConstant.OWNER_ID_KEY));
    Assert.assertFalse(topicMeta.getConfig().hasAttribute(TopicConstant.OWNER_EPOCH_KEY));
    Assert.assertFalse(
        topicMeta.getConfig().hasAttribute(TopicConstant.OWNER_LEASE_DURATION_MS_KEY));
  }

  @Test
  public void testDeepCopyWithUpdatedAttributesTransfersOwnerAndPreservesExistingConfig() {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.PATH_KEY, "root.sg.**");
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "5");
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);
    topicMeta.addSubscribedConsumerGroup("group1");

    final Map<String, String> updatedAttributes = new HashMap<>();
    updatedAttributes.put(TopicConstant.OWNER_ID_KEY, "owner2");
    updatedAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "6");
    final TopicMeta updatedTopicMeta = topicMeta.deepCopyWithUpdatedAttributes(updatedAttributes);

    Assert.assertEquals(
        "root.sg.**", updatedTopicMeta.getConfig().getString(TopicConstant.PATH_KEY));
    Assert.assertEquals("owner2", updatedTopicMeta.getOwnerId());
    Assert.assertEquals(6L, updatedTopicMeta.getOwnerEpoch());
    Assert.assertTrue(updatedTopicMeta.isSubscribedByConsumerGroup("group1"));
    Assert.assertEquals("owner1", topicMeta.getOwnerId());
    Assert.assertEquals(5L, topicMeta.getOwnerEpoch());
  }

  @Test
  public void testDeepCopyWithUpdatedAttributesTransfersOwnerAndClearsExistingLease() {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "5");
    topicAttributes.put(TopicConstant.OWNER_LEASE_DURATION_MS_KEY, "60000");
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);

    final Map<String, String> updatedAttributes = new HashMap<>();
    updatedAttributes.put(TopicConstant.OWNER_ID_KEY, "owner2");
    updatedAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "6");
    final TopicMeta updatedTopicMeta = topicMeta.deepCopyWithUpdatedAttributes(updatedAttributes);

    Assert.assertEquals("owner2", updatedTopicMeta.getOwnerId());
    Assert.assertEquals(6L, updatedTopicMeta.getOwnerEpoch());
    Assert.assertNull(updatedTopicMeta.getOwnerLeaseDurationMs());
    Assert.assertNull(updatedTopicMeta.getOwnerLeaseExpireTimeMs());
    Assert.assertFalse(
        updatedTopicMeta.getConfig().hasAttribute(TopicConstant.OWNER_LEASE_DURATION_MS_KEY));
  }

  @Test
  public void testOwnerLeaseExpireIsDataNodeLocalRuntimeOnly() throws IOException {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "5");
    topicAttributes.put(TopicConstant.OWNER_LEASE_DURATION_MS_KEY, "60000");
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);

    // Construction (ConfigNode side) never derives an absolute expiry; that is established only by
    // the DataNode-local owner-lease heartbeat.
    Assert.assertNull(topicMeta.getOwnerLeaseExpireTimeMs());
    Assert.assertTrue(topicMeta.matchesOwner("owner1", 5L));

    // Apply a heartbeat with a relative remaining duration; expiry is now set locally and the owner
    // still matches before the lease drains.
    topicMeta.applyOwnerLeaseFromHeartbeat("owner1", 5L, 60000L);
    Assert.assertNotNull(topicMeta.getOwnerLeaseExpireTimeMs());
    Assert.assertTrue(topicMeta.matchesOwner("owner1", 5L));

    // The locally-derived expiry is runtime-only: it is not a topic attribute, does not affect
    // equality, and does not survive serialization.
    final TopicMeta roundTrip = TopicMeta.deserialize(topicMeta.serialize());
    Assert.assertNull(roundTrip.getOwnerLeaseExpireTimeMs());
    Assert.assertEquals(topicMeta, roundTrip);

    // A stale/mismatched heartbeat is ignored.
    topicMeta.applyOwnerLeaseFromHeartbeat("owner2", 5L, 60000L);
    topicMeta.applyOwnerLeaseFromHeartbeat("owner1", 4L, 60000L);
    Assert.assertTrue(topicMeta.matchesOwner("owner1", 5L));

    // An elapsed lease causes the current owner to stop matching (DataNode fencing).
    topicMeta.applyOwnerLeaseFromHeartbeat("owner1", 5L, -1L);
    Assert.assertFalse(topicMeta.matchesOwner("owner1", 5L));
  }

  @Test
  public void testOwnerLeaseDurationGeneratesExpireTime() {
    final long ownerLeaseDurationMs = 60000;
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "5");
    topicAttributes.put(
        TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));

    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);

    // The lease duration is persisted, but no absolute expiry is derived at construction time: the
    // expiry is established only by the DataNode-local owner-lease heartbeat.
    Assert.assertEquals(ownerLeaseDurationMs, topicMeta.getOwnerLeaseDurationMs().longValue());
    Assert.assertEquals(
        ownerLeaseDurationMs,
        topicMeta.getConfig().getLong(TopicConstant.OWNER_LEASE_DURATION_MS_KEY).longValue());
    Assert.assertNull(topicMeta.getOwnerLeaseExpireTimeMs());
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

  @Test
  public void testOwnerEpochNeverReusedAcrossClearAndSerialization() throws IOException {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, "owner1");
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "6");
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);
    Assert.assertEquals(6L, topicMeta.getMaxOwnerEpoch());

    // Clearing the owner retains the global epoch baseline.
    topicMeta.clearOwner();
    Assert.assertFalse(topicMeta.isOwnerFencingEnabled());
    Assert.assertEquals(6L, topicMeta.getMaxOwnerEpoch());

    // The baseline survives serialization even though the owner is cleared.
    final TopicMeta deserialized = TopicMeta.deserialize(topicMeta.serialize());
    Assert.assertFalse(deserialized.isOwnerFencingEnabled());
    Assert.assertEquals(6L, deserialized.getMaxOwnerEpoch());

    // Re-enabling with an epoch <= the historical max (reuse) is rejected.
    final Map<String, String> reuse = new HashMap<>();
    reuse.put(TopicConstant.OWNER_ID_KEY, "owner2");
    reuse.put(TopicConstant.OWNER_EPOCH_KEY, "6");
    final TopicMeta reuseMeta = deserialized.deepCopyWithUpdatedAttributes(reuse);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> TopicMeta.validateOwnerProgression(deserialized, reuseMeta));

    // Re-enabling with an epoch strictly greater than the historical max is allowed.
    final Map<String, String> next = new HashMap<>();
    next.put(TopicConstant.OWNER_ID_KEY, "owner2");
    next.put(TopicConstant.OWNER_EPOCH_KEY, "7");
    final TopicMeta nextMeta = deserialized.deepCopyWithUpdatedAttributes(next);
    TopicMeta.validateOwnerProgression(deserialized, nextMeta);
    Assert.assertEquals(7L, nextMeta.getOwnerEpoch());
    Assert.assertEquals(7L, nextMeta.getMaxOwnerEpoch());
  }
}
