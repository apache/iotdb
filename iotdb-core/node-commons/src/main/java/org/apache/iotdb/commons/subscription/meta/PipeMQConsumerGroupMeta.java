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

package org.apache.iotdb.commons.subscription.meta;

import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PipeMQConsumerGroupMeta {
  private String consumerGroupId;
  private long creationTime;
  private Map<String, Set<String>> topicNameToSubscribedConsumers;
  private Map<String, PipeMQConsumerMeta> consumers;

  public PipeMQConsumerGroupMeta() {
    // Empty constructor
  }

  public PipeMQConsumerGroupMeta(
      String consumerGroupId, long creationTime, PipeMQConsumerMeta firstConsumerMeta) {
    this.consumerGroupId = consumerGroupId;
    this.creationTime = creationTime;
    this.topicNameToSubscribedConsumers = new HashMap<>();
    this.consumers = Collections.singletonMap(firstConsumerMeta.getConsumerId(), firstConsumerMeta);
  }

  public PipeMQConsumerGroupMeta copy() {
    PipeMQConsumerGroupMeta copy = new PipeMQConsumerGroupMeta();
    copy.consumerGroupId = consumerGroupId;
    copy.creationTime = creationTime;
    copy.topicNameToSubscribedConsumers = new HashMap<>(topicNameToSubscribedConsumers);
    copy.consumers = new HashMap<>(consumers);
    return copy;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public void addConsumer(PipeMQConsumerMeta consumerMeta) {
    consumers.put(consumerMeta.getConsumerId(), consumerMeta);
  }

  public void removeConsumer(String consumerId) {
    consumers.remove(consumerId);
    for (Set<String> subscribedConsumers : topicNameToSubscribedConsumers.values()) {
      subscribedConsumers.remove(consumerId);
    }
  }

  public boolean containsConsumer(String consumerId) {
    return consumers.containsKey(consumerId);
  }

  /**
   * Get the consumers subscribing the given topic in this group.
   *
   * @param topic The topic name.
   * @return The set of consumer IDs subscribing the given topic in this group. If no consumer is
   *     subscribing the topic, return an empty set.
   */
  public Set<String> getConsumersSubscribingTopic(String topic) {
    return topicNameToSubscribedConsumers.getOrDefault(topic, Collections.emptySet());
  }

  public void addSubscription(String consumerId, Set<String> topics) {
    if (!consumers.containsKey(consumerId)) {
      throw new PipeException(
          String.format(
              "Failed to add subscription to consumer group meta: consumer %s does not exist in consumer group %s",
              consumerId, consumerGroupId));
    }

    for (String topic : topics) {
      topicNameToSubscribedConsumers.computeIfAbsent(topic, k -> new HashSet<>()).add(consumerId);
    }
  }

  /** @return topics subscribed by no consumers in this group after this removal. */
  public Set<String> removeSubscription(String consumerId, Set<String> topics) {
    if (!consumers.containsKey(consumerId)) {
      throw new PipeException(
          String.format(
              "Failed to remove subscription from consumer group meta: consumer %s does not exist in consumer group %s",
              consumerId, consumerGroupId));
    }

    Set<String> ret = new HashSet<>();
    for (String topic : topics) {
      if (topicNameToSubscribedConsumers.containsKey(topic)) {
        topicNameToSubscribedConsumers.get(topic).remove(consumerId);
        if (topicNameToSubscribedConsumers.get(topic).isEmpty()) {
          ret.add(topic);
          topicNameToSubscribedConsumers.remove(topic);
        }
      }
    }
    return ret;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupId, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(topicNameToSubscribedConsumers.size(), outputStream);
    for (Map.Entry<String, Set<String>> entry : topicNameToSubscribedConsumers.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
      for (String entry1 : entry.getValue()) {
        ReadWriteIOUtils.write(entry1, outputStream);
      }
    }

    ReadWriteIOUtils.write(consumers.size(), outputStream);
    for (Map.Entry<String, PipeMQConsumerMeta> entry : consumers.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupId, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(topicNameToSubscribedConsumers.size(), outputStream);
    for (Map.Entry<String, Set<String>> entry : topicNameToSubscribedConsumers.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
      for (String entry1 : entry.getValue()) {
        ReadWriteIOUtils.write(entry1, outputStream);
      }
    }

    ReadWriteIOUtils.write(consumers.size(), outputStream);
    for (Map.Entry<String, PipeMQConsumerMeta> entry : consumers.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }
  }

  public static PipeMQConsumerGroupMeta deserialize(InputStream inputStream) throws IOException {
    final PipeMQConsumerGroupMeta consumerGroupMeta = new PipeMQConsumerGroupMeta();

    consumerGroupMeta.consumerGroupId = ReadWriteIOUtils.readString(inputStream);
    consumerGroupMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    consumerGroupMeta.topicNameToSubscribedConsumers = new HashMap<>();

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);

      final Set<String> value = new HashSet<>();
      final int innerSize = ReadWriteIOUtils.readInt(inputStream);
      for (int j = 0; j < innerSize; ++j) {
        value.add(ReadWriteIOUtils.readString(inputStream));
      }

      consumerGroupMeta.topicNameToSubscribedConsumers.put(key, value);
    }

    consumerGroupMeta.consumers = new HashMap<>();

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final PipeMQConsumerMeta value = PipeMQConsumerMeta.deserialize(inputStream);
      consumerGroupMeta.consumers.put(key, value);
    }

    return consumerGroupMeta;
  }

  public static PipeMQConsumerGroupMeta deserialize(ByteBuffer byteBuffer) throws IOException {
    final PipeMQConsumerGroupMeta consumerGroupMeta = new PipeMQConsumerGroupMeta();

    consumerGroupMeta.consumerGroupId = ReadWriteIOUtils.readString(byteBuffer);
    consumerGroupMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    consumerGroupMeta.topicNameToSubscribedConsumers = new HashMap<>();

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);

      final Set<String> value = new HashSet<>();
      final int innerSize = ReadWriteIOUtils.readInt(byteBuffer);
      for (int j = 0; j < innerSize; ++j) {
        value.add(ReadWriteIOUtils.readString(byteBuffer));
      }

      consumerGroupMeta.topicNameToSubscribedConsumers.put(key, value);
    }

    consumerGroupMeta.consumers = new HashMap<>();

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final PipeMQConsumerMeta value = PipeMQConsumerMeta.deserialize(byteBuffer);
      consumerGroupMeta.consumers.put(key, value);
    }

    return consumerGroupMeta;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeMQConsumerGroupMeta that = (PipeMQConsumerGroupMeta) obj;
    return consumerGroupId.equals(that.consumerGroupId)
        && creationTime == that.creationTime
        && topicNameToSubscribedConsumers.equals(that.topicNameToSubscribedConsumers)
        && consumers.equals(that.consumers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroupId, creationTime, topicNameToSubscribedConsumers, consumers);
  }

  @Override
  public String toString() {
    return "PipeMQConsumerGroupMeta{"
        + "consumerGroupId='"
        + consumerGroupId
        + "', creationTime="
        + creationTime
        + ", topicNameToSubscribedConsumers="
        + topicNameToSubscribedConsumers
        + ", consumers="
        + consumers
        + "}";
  }
}
