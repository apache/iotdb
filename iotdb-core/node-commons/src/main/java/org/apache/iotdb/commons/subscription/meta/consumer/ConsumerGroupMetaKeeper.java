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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ConsumerGroupMetaKeeper {

  private final Map<String, ConsumerGroupMeta> consumerGroupIdToConsumerGroupMetaMap;

  private final ReentrantReadWriteLock consumerGroupMetaKeeperLock;

  public ConsumerGroupMetaKeeper() {
    consumerGroupIdToConsumerGroupMetaMap = new ConcurrentHashMap<>();
    consumerGroupMetaKeeperLock = new ReentrantReadWriteLock(true);
  }

  /////////////////////////////////  Lock  /////////////////////////////////

  public void acquireReadLock() {
    consumerGroupMetaKeeperLock.readLock().lock();
  }

  public void releaseReadLock() {
    consumerGroupMetaKeeperLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    consumerGroupMetaKeeperLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    consumerGroupMetaKeeperLock.writeLock().unlock();
  }

  /////////////////////////////////  ConsumerGroupMeta  /////////////////////////////////

  public boolean containsConsumerGroupMeta(String consumerGroupId) {
    return consumerGroupIdToConsumerGroupMetaMap.containsKey(consumerGroupId);
  }

  public ConsumerGroupMeta getConsumerGroupMeta(String consumerGroupId) {
    return consumerGroupIdToConsumerGroupMetaMap.get(consumerGroupId);
  }

  public Iterable<ConsumerGroupMeta> getAllConsumerGroupMeta() {
    return consumerGroupIdToConsumerGroupMetaMap.values();
  }

  /**
   * Get the consumers subscribing the given topic in the given consumer group.
   *
   * @param topic The topic name.
   * @return The set of consumer IDs subscribing the given topic in this group. If the group does
   *     not exist or no consumer is subscribing the topic, return an empty set.
   */
  public Set<String> getConsumersSubscribingTopic(String consumerGroupId, String topic) {
    return consumerGroupIdToConsumerGroupMetaMap.containsKey(consumerGroupId)
        ? consumerGroupIdToConsumerGroupMetaMap
            .get(consumerGroupId)
            .getConsumersSubscribingTopic(topic)
        : Collections.emptySet();
  }

  public Set<String> getTopicsSubscribedByConsumer(String consumerGroupId, String consumerId) {
    return consumerGroupIdToConsumerGroupMetaMap.containsKey(consumerGroupId)
        ? consumerGroupIdToConsumerGroupMetaMap
            .get(consumerGroupId)
            .getTopicsSubscribedByConsumer(consumerId)
        : Collections.emptySet();
  }

  public void addConsumerGroupMeta(String consumerGroupId, ConsumerGroupMeta consumerGroupMeta) {
    consumerGroupIdToConsumerGroupMetaMap.put(consumerGroupId, consumerGroupMeta);
  }

  public void removeConsumerGroupMeta(String consumerGroupId) {
    consumerGroupIdToConsumerGroupMetaMap.remove(consumerGroupId);
  }

  public void clear() {
    this.consumerGroupIdToConsumerGroupMetaMap.clear();
  }

  public boolean isEmpty() {
    return consumerGroupIdToConsumerGroupMetaMap.isEmpty();
  }

  /////////////////////////////////  TopicMeta  /////////////////////////////////

  public Set<String> getSubscribedConsumerGroupIds(final String topicName) {
    return consumerGroupIdToConsumerGroupMetaMap.entrySet().stream()
        .filter(entry -> entry.getValue().getTopicsSubscribedByConsumerGroup().contains(topicName))
        .map(Entry::getKey)
        .collect(Collectors.toSet());
  }

  public boolean isTopicSubscribedByConsumerGroup(
      final String topicName, final String consumerGroupId) {
    return consumerGroupIdToConsumerGroupMetaMap.containsKey(consumerGroupId)
        && consumerGroupIdToConsumerGroupMetaMap
            .get(consumerGroupId)
            .getTopicsSubscribedByConsumerGroup()
            .contains(topicName);
  }

  public boolean isTopicSubscribedByConsumerGroup(final String topicName) {
    return consumerGroupIdToConsumerGroupMetaMap.values().stream()
        .anyMatch(meta -> meta.getTopicsSubscribedByConsumerGroup().contains(topicName));
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  public void processTakeSnapshot(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupIdToConsumerGroupMetaMap.size(), fileOutputStream);
    for (Map.Entry<String, ConsumerGroupMeta> entry :
        consumerGroupIdToConsumerGroupMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String topicName = ReadWriteIOUtils.readString(fileInputStream);
      consumerGroupIdToConsumerGroupMetaMap.put(
          topicName, ConsumerGroupMeta.deserialize(fileInputStream));
    }
  }

  /////////////////////////////////  Object  /////////////////////////////////

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsumerGroupMetaKeeper that = (ConsumerGroupMetaKeeper) o;
    return Objects.equals(
        consumerGroupIdToConsumerGroupMetaMap, that.consumerGroupIdToConsumerGroupMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroupIdToConsumerGroupMetaMap);
  }

  @Override
  public String toString() {
    return "ConsumerGroupMetaKeeper{"
        + "consumerGroupIdToConsumerGroupMetaMap="
        + consumerGroupIdToConsumerGroupMetaMap
        + '}';
  }
}
