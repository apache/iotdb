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

package org.apache.iotdb.commons.subscription.meta.consumer;

import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ConsumerGroupMeta {

  private String consumerGroupId;
  private long creationTime;
  private Map<String, Set<String>> topicNameToSubscribedConsumerIdSet = new HashMap<>();
  private Map<String, ConsumerMeta> consumerIdToConsumerMeta = new HashMap<>();

  public ConsumerGroupMeta() {
    // Empty constructor
  }

  public ConsumerGroupMeta(
      final String consumerGroupId, final long creationTime, final ConsumerMeta firstConsumerMeta) {
    this.consumerGroupId = consumerGroupId;
    this.creationTime = creationTime;
    this.topicNameToSubscribedConsumerIdSet = new HashMap<>();
    this.consumerIdToConsumerMeta = new HashMap<>();

    consumerIdToConsumerMeta.put(firstConsumerMeta.getConsumerId(), firstConsumerMeta);
  }

  public ConsumerGroupMeta deepCopy() {
    final ConsumerGroupMeta copied = new ConsumerGroupMeta();
    copied.consumerGroupId = consumerGroupId;
    copied.creationTime = creationTime;
    copied.topicNameToSubscribedConsumerIdSet = new HashMap<>(topicNameToSubscribedConsumerIdSet);
    copied.consumerIdToConsumerMeta = new HashMap<>(consumerIdToConsumerMeta);
    return copied;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public static /* @NonNull */ Set<String> getTopicsUnsubByGroup(
      final ConsumerGroupMeta currentMeta, final ConsumerGroupMeta updatedMeta) {
    if (!Objects.equals(currentMeta.consumerGroupId, updatedMeta.consumerGroupId)) {
      return Collections.emptySet();
    }
    if (!Objects.equals(currentMeta.creationTime, updatedMeta.creationTime)) {
      return Collections.emptySet();
    }

    // no need to check consumerIdToConsumerMeta here to avoid potential inconsistent meta

    final Set<String> unsubscribedTopicNames = new HashSet<>();
    currentMeta
        .topicNameToSubscribedConsumerIdSet
        .keySet()
        .forEach(
            (topicName) -> {
              if (!updatedMeta.topicNameToSubscribedConsumerIdSet.containsKey(topicName)) {
                unsubscribedTopicNames.add(topicName);
              }
            });
    return unsubscribedTopicNames;
  }

  /////////////////////////////// consumer ///////////////////////////////

  public void addConsumer(final ConsumerMeta consumerMeta) {
    consumerIdToConsumerMeta.put(consumerMeta.getConsumerId(), consumerMeta);
  }

  public void removeConsumer(final String consumerId) {
    consumerIdToConsumerMeta.remove(consumerId);
    for (final Map.Entry<String, Set<String>> entry :
        topicNameToSubscribedConsumerIdSet.entrySet()) {
      entry.getValue().remove(consumerId);
      if (entry.getValue().isEmpty()) {
        topicNameToSubscribedConsumerIdSet.remove(entry.getKey());
      }
    }
  }

  public boolean containsConsumer(final String consumerId) {
    return consumerIdToConsumerMeta.containsKey(consumerId);
  }

  public boolean isEmpty() {
    // When there are no consumers in a consumer group, it means that the ConsumerGroupMeta is
    // empty, and at this time, the topicNameToSubscribedConsumerIdSet is also empty.
    return consumerIdToConsumerMeta.isEmpty();
  }

  ////////////////////////// subscription //////////////////////////

  /**
   * Get the consumers subscribing the given topic in this group.
   *
   * @param topic The topic name.
   * @return The set of consumer IDs subscribing the given topic in this group. If no consumer is
   *     subscribing the topic, return an empty set.
   */
  public Set<String> getConsumersSubscribingTopic(final String topic) {
    return topicNameToSubscribedConsumerIdSet.getOrDefault(topic, Collections.emptySet());
  }

  public Set<String> getTopicsSubscribedByConsumer(final String consumerId) {
    final Set<String> topics = new HashSet<>();
    for (final Map.Entry<String, Set<String>> topicNameToSubscribedConsumerId :
        topicNameToSubscribedConsumerIdSet.entrySet()) {
      if (topicNameToSubscribedConsumerId.getValue().contains(consumerId)) {
        topics.add(topicNameToSubscribedConsumerId.getKey());
      }
    }
    return topics;
  }

  public Set<String> getTopicsSubscribedByConsumerGroup() {
    return topicNameToSubscribedConsumerIdSet.keySet();
  }

  public void addSubscription(final String consumerId, final Set<String> topics) {
    if (!consumerIdToConsumerMeta.containsKey(consumerId)) {
      throw new SubscriptionException(
          String.format(
              "Failed to add subscription to consumer group meta: consumer %s does not exist in consumer group %s",
              consumerId, consumerGroupId));
    }

    for (final String topic : topics) {
      topicNameToSubscribedConsumerIdSet
          .computeIfAbsent(topic, k -> new HashSet<>())
          .add(consumerId);
    }
  }

  /**
   * @return topics subscribed by no consumers in this group after this removal.
   */
  public Set<String> removeSubscription(final String consumerId, final Set<String> topics) {
    if (!consumerIdToConsumerMeta.containsKey(consumerId)) {
      throw new SubscriptionException(
          String.format(
              "Failed to remove subscription from consumer group meta: consumer %s does not exist in consumer group %s",
              consumerId, consumerGroupId));
    }

    final Set<String> noSubscriptionTopicAfterRemoval = new HashSet<>();
    for (final String topic : topics) {
      if (topicNameToSubscribedConsumerIdSet.containsKey(topic)) {
        topicNameToSubscribedConsumerIdSet.get(topic).remove(consumerId);
        if (topicNameToSubscribedConsumerIdSet.get(topic).isEmpty()) {
          noSubscriptionTopicAfterRemoval.add(topic);
          topicNameToSubscribedConsumerIdSet.remove(topic);
        }
      }
    }
    return noSubscriptionTopicAfterRemoval;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public ByteBuffer serialize() throws IOException {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupId, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(topicNameToSubscribedConsumerIdSet.size(), outputStream);
    for (final Map.Entry<String, Set<String>> entry :
        topicNameToSubscribedConsumerIdSet.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
      for (final String id : entry.getValue()) {
        ReadWriteIOUtils.write(id, outputStream);
      }
    }

    ReadWriteIOUtils.write(consumerIdToConsumerMeta.size(), outputStream);
    for (final Map.Entry<String, ConsumerMeta> entry : consumerIdToConsumerMeta.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public static ConsumerGroupMeta deserialize(final InputStream inputStream) throws IOException {
    final ConsumerGroupMeta consumerGroupMeta = new ConsumerGroupMeta();

    consumerGroupMeta.consumerGroupId = ReadWriteIOUtils.readString(inputStream);
    consumerGroupMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    consumerGroupMeta.topicNameToSubscribedConsumerIdSet = new HashMap<>();
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);

      final Set<String> value = new HashSet<>();
      final int innerSize = ReadWriteIOUtils.readInt(inputStream);
      for (int j = 0; j < innerSize; ++j) {
        value.add(ReadWriteIOUtils.readString(inputStream));
      }

      consumerGroupMeta.topicNameToSubscribedConsumerIdSet.put(key, value);
    }

    consumerGroupMeta.consumerIdToConsumerMeta = new HashMap<>();
    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final ConsumerMeta value = ConsumerMeta.deserialize(inputStream);
      consumerGroupMeta.consumerIdToConsumerMeta.put(key, value);
    }

    return consumerGroupMeta;
  }

  public static ConsumerGroupMeta deserialize(final ByteBuffer byteBuffer) {
    final ConsumerGroupMeta consumerGroupMeta = new ConsumerGroupMeta();

    consumerGroupMeta.consumerGroupId = ReadWriteIOUtils.readString(byteBuffer);
    consumerGroupMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    consumerGroupMeta.topicNameToSubscribedConsumerIdSet = new HashMap<>();
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);

      final Set<String> value = new HashSet<>();
      final int innerSize = ReadWriteIOUtils.readInt(byteBuffer);
      for (int j = 0; j < innerSize; ++j) {
        value.add(ReadWriteIOUtils.readString(byteBuffer));
      }

      consumerGroupMeta.topicNameToSubscribedConsumerIdSet.put(key, value);
    }

    consumerGroupMeta.consumerIdToConsumerMeta = new HashMap<>();
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final ConsumerMeta value = ConsumerMeta.deserialize(byteBuffer);
      consumerGroupMeta.consumerIdToConsumerMeta.put(key, value);
    }

    return consumerGroupMeta;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ConsumerGroupMeta that = (ConsumerGroupMeta) obj;
    return Objects.equals(consumerGroupId, that.consumerGroupId)
        && creationTime == that.creationTime
        && Objects.equals(
            topicNameToSubscribedConsumerIdSet, that.topicNameToSubscribedConsumerIdSet)
        && Objects.equals(consumerIdToConsumerMeta, that.consumerIdToConsumerMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        consumerGroupId,
        creationTime,
        topicNameToSubscribedConsumerIdSet,
        consumerIdToConsumerMeta);
  }

  @Override
  public String toString() {
    return "ConsumerGroupMeta{"
        + "consumerGroupId='"
        + consumerGroupId
        + "', creationTime="
        + creationTime
        + ", topicNameToSubscribedConsumerIdSet="
        + topicNameToSubscribedConsumerIdSet
        + ", consumerIdToConsumerMeta="
        + consumerIdToConsumerMeta
        + "}";
  }
}
