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
      String consumerGroupId, long creationTime, ConsumerMeta firstConsumerMeta) {
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

  /////////////////////////////// consumer ///////////////////////////////

  public void addConsumer(ConsumerMeta consumerMeta) {
    consumerIdToConsumerMeta.put(consumerMeta.getConsumerId(), consumerMeta);
  }

  public void removeConsumer(String consumerId) {
    consumerIdToConsumerMeta.remove(consumerId);
    for (Set<String> subscribedConsumers : topicNameToSubscribedConsumerIdSet.values()) {
      subscribedConsumers.remove(consumerId);
    }
  }

  public boolean containsConsumer(String consumerId) {
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
  public Set<String> getConsumersSubscribingTopic(String topic) {
    return topicNameToSubscribedConsumerIdSet.getOrDefault(topic, Collections.emptySet());
  }

  public Set<String> getTopicsSubscribedByConsumer(String consumerId) {
    Set<String> topics = new HashSet<>();
    for (Map.Entry<String, Set<String>> topicNameToSubscribedConsumerId :
        topicNameToSubscribedConsumerIdSet.entrySet()) {
      if (topicNameToSubscribedConsumerId.getValue().contains(consumerId)) {
        topics.add(topicNameToSubscribedConsumerId.getKey());
      }
    }
    return topics;
  }

  public void addSubscription(String consumerId, Set<String> topics) {
    if (!consumerIdToConsumerMeta.containsKey(consumerId)) {
      throw new SubscriptionException(
          String.format(
              "Failed to add subscription to consumer group meta: consumer %s does not exist in consumer group %s",
              consumerId, consumerGroupId));
    }

    for (String topic : topics) {
      topicNameToSubscribedConsumerIdSet
          .computeIfAbsent(topic, k -> new HashSet<>())
          .add(consumerId);
    }
  }

  /** @return topics subscribed by no consumers in this group after this removal. */
  public Set<String> removeSubscription(String consumerId, Set<String> topics) {
    if (!consumerIdToConsumerMeta.containsKey(consumerId)) {
      throw new SubscriptionException(
          String.format(
              "Failed to remove subscription from consumer group meta: consumer %s does not exist in consumer group %s",
              consumerId, consumerGroupId));
    }

    Set<String> noSubscriptionTopicAfterRemoval = new HashSet<>();
    for (String topic : topics) {
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
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupId, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(topicNameToSubscribedConsumerIdSet.size(), outputStream);
    for (Map.Entry<String, Set<String>> entry : topicNameToSubscribedConsumerIdSet.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
      for (String id : entry.getValue()) {
        ReadWriteIOUtils.write(id, outputStream);
      }
    }

    ReadWriteIOUtils.write(consumerIdToConsumerMeta.size(), outputStream);
    for (Map.Entry<String, ConsumerMeta> entry : consumerIdToConsumerMeta.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public static ConsumerGroupMeta deserialize(InputStream inputStream) throws IOException {
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

  public static ConsumerGroupMeta deserialize(ByteBuffer byteBuffer) {
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ConsumerGroupMeta that = (ConsumerGroupMeta) obj;
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
