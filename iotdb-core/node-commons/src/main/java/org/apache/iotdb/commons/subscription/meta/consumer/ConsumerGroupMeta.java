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

import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ConsumerGroupMeta {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupMeta.class);

  private String consumerGroupId;
  private long creationTime;
  private Map<String, Set<String>> topicNameToSubscribedConsumerIdSet;
  private Map<String, ConsumerMeta> consumerIdToConsumerMeta;
  private Map<String, Long> topicNameToSubscriptionCreationTime; // used when creationTime < 0

  public ConsumerGroupMeta() {
    this.topicNameToSubscribedConsumerIdSet = new ConcurrentHashMap<>();
    this.consumerIdToConsumerMeta = new ConcurrentHashMap<>();
    this.topicNameToSubscriptionCreationTime = new ConcurrentHashMap<>();
  }

  public ConsumerGroupMeta(
      final String consumerGroupId, final long creationTime, final ConsumerMeta firstConsumerMeta) {
    this();

    this.consumerGroupId = consumerGroupId;
    this.creationTime = -creationTime;

    consumerIdToConsumerMeta.put(firstConsumerMeta.getConsumerId(), firstConsumerMeta);
  }

  public ConsumerGroupMeta deepCopy() {
    final ConsumerGroupMeta copied = new ConsumerGroupMeta();
    copied.consumerGroupId = consumerGroupId;
    copied.creationTime = creationTime;
    copied.topicNameToSubscribedConsumerIdSet =
        new ConcurrentHashMap<>(topicNameToSubscribedConsumerIdSet);
    copied.consumerIdToConsumerMeta = new ConcurrentHashMap<>(consumerIdToConsumerMeta);
    copied.topicNameToSubscriptionCreationTime =
        new ConcurrentHashMap<>(topicNameToSubscriptionCreationTime);
    return copied;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public long getCreationTime() {
    return Math.abs(creationTime);
  }

  private boolean shouldRecordSubscriptionCreationTime() {
    return creationTime < 0;
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

  public void checkAuthorityBeforeJoinConsumerGroup(final ConsumerMeta consumerMeta)
      throws SubscriptionException {
    if (isEmpty()) {
      return;
    }
    final ConsumerMeta existedConsumerMeta = consumerIdToConsumerMeta.values().iterator().next();
    final boolean match =
        Objects.equals(existedConsumerMeta.getUsername(), consumerMeta.getUsername())
            && Objects.equals(existedConsumerMeta.getPassword(), consumerMeta.getPassword());
    if (!match) {
      final String exceptionMessage =
          String.format(
              "Failed to create consumer %s because inconsistent username & password under the same consumer group, expected %s:%s, actual %s:%s",
              consumerMeta.getConsumerId(),
              existedConsumerMeta.getUsername(),
              existedConsumerMeta.getPassword(),
              consumerMeta.getUsername(),
              consumerMeta.getPassword());
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
    }
  }

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

  public ConsumerMeta getConsumerMeta(final String consumerId) {
    return consumerIdToConsumerMeta.get(consumerId);
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

  public Optional<Long> getSubscriptionTime(final String topic) {
    return shouldRecordSubscriptionCreationTime()
        ? Optional.ofNullable(topicNameToSubscriptionCreationTime.get(topic))
        : Optional.empty();
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

  public boolean isTopicSubscribedByConsumerGroup(final String topic) {
    final Set<String> subscribedConsumerIdSet = topicNameToSubscribedConsumerIdSet.get(topic);
    if (Objects.isNull(subscribedConsumerIdSet)) {
      return false;
    }
    return !subscribedConsumerIdSet.isEmpty();
  }

  public boolean allowSubscribeTopicForConsumer(final String topic, final String consumerId) {
    if (!consumerIdToConsumerMeta.containsKey(consumerId)) {
      return false;
    }
    final Set<String> subscribedConsumerIdSet = topicNameToSubscribedConsumerIdSet.get(topic);
    if (Objects.isNull(subscribedConsumerIdSet)) {
      return true;
    }
    if (subscribedConsumerIdSet.isEmpty()) {
      return true;
    }
    final String subscribedConsumerId = subscribedConsumerIdSet.iterator().next();
    return Objects.equals(
        Objects.requireNonNull(consumerIdToConsumerMeta.get(subscribedConsumerId)).getUsername(),
        Objects.requireNonNull(consumerIdToConsumerMeta.get(consumerId)).getUsername());
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
          .computeIfAbsent(
              topic,
              k -> {
                if (shouldRecordSubscriptionCreationTime()) {
                  topicNameToSubscriptionCreationTime.put(topic, System.currentTimeMillis());
                }
                return new HashSet<>();
              })
          .add(consumerId);
    }
  }

  /**
   * @return topics subscribed by no consumers in this group after this removal.
   * @param consumerId if null, remove subscriptions of topics for all consumers
   */
  public Set<String> removeSubscription(
      @Nullable final String consumerId, final Set<String> topics) {
    if (Objects.isNull(consumerId)) {
      return consumerIdToConsumerMeta.keySet().stream()
          .map(id -> removeSubscriptionInternal(id, topics))
          .flatMap(Set::stream)
          .collect(Collectors.toSet());
    }
    return removeSubscriptionInternal(consumerId, topics);
  }

  private Set<String> removeSubscriptionInternal(
      final String consumerId, final Set<String> topics) {
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
          // remove subscription for consumer group
          noSubscriptionTopicAfterRemoval.add(topic);
          topicNameToSubscribedConsumerIdSet.remove(topic);
          if (shouldRecordSubscriptionCreationTime()) {
            topicNameToSubscriptionCreationTime.remove(topic);
          }
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

    if (shouldRecordSubscriptionCreationTime()) {
      ReadWriteIOUtils.write(topicNameToSubscriptionCreationTime.size(), outputStream);
      for (final Map.Entry<String, Long> entry : topicNameToSubscriptionCreationTime.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    }
  }

  public static ConsumerGroupMeta deserialize(final InputStream inputStream) throws IOException {
    final ConsumerGroupMeta consumerGroupMeta = new ConsumerGroupMeta();

    consumerGroupMeta.consumerGroupId = ReadWriteIOUtils.readString(inputStream);
    consumerGroupMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    consumerGroupMeta.topicNameToSubscribedConsumerIdSet = new ConcurrentHashMap<>();
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

    consumerGroupMeta.consumerIdToConsumerMeta = new ConcurrentHashMap<>();
    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final ConsumerMeta value = ConsumerMeta.deserialize(inputStream);
      consumerGroupMeta.consumerIdToConsumerMeta.put(key, value);
    }

    consumerGroupMeta.topicNameToSubscriptionCreationTime = new ConcurrentHashMap<>();
    if (consumerGroupMeta.shouldRecordSubscriptionCreationTime()) {
      size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; ++i) {
        final String key = ReadWriteIOUtils.readString(inputStream);
        final long value = ReadWriteIOUtils.readLong(inputStream);
        consumerGroupMeta.topicNameToSubscriptionCreationTime.put(key, value);
      }
    }

    return consumerGroupMeta;
  }

  public static ConsumerGroupMeta deserialize(final ByteBuffer byteBuffer) {
    final ConsumerGroupMeta consumerGroupMeta = new ConsumerGroupMeta();

    consumerGroupMeta.consumerGroupId = ReadWriteIOUtils.readString(byteBuffer);
    consumerGroupMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    consumerGroupMeta.topicNameToSubscribedConsumerIdSet = new ConcurrentHashMap<>();
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

    consumerGroupMeta.consumerIdToConsumerMeta = new ConcurrentHashMap<>();
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final ConsumerMeta value = ConsumerMeta.deserialize(byteBuffer);
      consumerGroupMeta.consumerIdToConsumerMeta.put(key, value);
    }

    consumerGroupMeta.topicNameToSubscriptionCreationTime = new ConcurrentHashMap<>();
    if (consumerGroupMeta.shouldRecordSubscriptionCreationTime()) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        final String key = ReadWriteIOUtils.readString(byteBuffer);
        final long value = ReadWriteIOUtils.readLong(byteBuffer);
        consumerGroupMeta.topicNameToSubscriptionCreationTime.put(key, value);
      }
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
    return Objects.equals(this.consumerGroupId, that.consumerGroupId)
        && this.creationTime == that.creationTime
        && Objects.equals(
            this.topicNameToSubscribedConsumerIdSet, that.topicNameToSubscribedConsumerIdSet)
        && Objects.equals(this.consumerIdToConsumerMeta, that.consumerIdToConsumerMeta)
        && Objects.equals(
            this.topicNameToSubscriptionCreationTime, that.topicNameToSubscriptionCreationTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        consumerGroupId,
        creationTime,
        topicNameToSubscribedConsumerIdSet,
        consumerIdToConsumerMeta,
        topicNameToSubscriptionCreationTime);
  }

  @Override
  public String toString() {
    return "ConsumerGroupMeta{"
        + "consumerGroupId='"
        + consumerGroupId
        + "', creationTime="
        + getCreationTime()
        + ", topicNameToSubscribedConsumerIdSet="
        + topicNameToSubscribedConsumerIdSet
        + ", consumerIdToConsumerMeta="
        + consumerIdToConsumerMeta
        + ", topicNameToSubscriptionCreationTime="
        + topicNameToSubscriptionCreationTime
        + "}";
  }
}
