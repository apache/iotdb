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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupMetaKeeper {

  private final Map<String, ConsumerGroupMeta> consumerGroupIDToConsumerGroupMetaMap;

  public ConsumerGroupMetaKeeper() {
    consumerGroupIDToConsumerGroupMetaMap = new ConcurrentHashMap<>();
  }

  public boolean containsConsumerGroupMeta(String consumerGroupID) {
    return consumerGroupIDToConsumerGroupMetaMap.containsKey(consumerGroupID);
  }

  public ConsumerGroupMeta getConsumerGroupMeta(String consumerGroupID) {
    return consumerGroupIDToConsumerGroupMetaMap.get(consumerGroupID);
  }

  /**
   * Get the consumers subscribing the given topic in the given consumer group.
   *
   * @param topic The topic name.
   * @return The set of consumer IDs subscribing the given topic in this group. If the group does
   *     not exist or no consumer is subscribing the topic, return an empty set.
   */
  public Set<String> getConsumersSubscribingTopic(String consumerGroupId, String topic) {
    return consumerGroupIDToConsumerGroupMetaMap.containsKey(consumerGroupId)
        ? consumerGroupIDToConsumerGroupMetaMap
            .get(consumerGroupId)
            .getConsumersSubscribingTopic(topic)
        : Collections.emptySet();
  }

  public void addConsumerGroupMeta(String consumerGroupID, ConsumerGroupMeta consumerGroupMeta) {
    consumerGroupIDToConsumerGroupMetaMap.put(consumerGroupID, consumerGroupMeta);
  }

  public void removeConsumerGroupMeta(String consumerGroupID) {
    consumerGroupIDToConsumerGroupMetaMap.remove(consumerGroupID);
  }

  public void clear() {
    this.consumerGroupIDToConsumerGroupMetaMap.clear();
  }

  public boolean isEmpty() {
    return consumerGroupIDToConsumerGroupMetaMap.isEmpty();
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  public void processTakeSnapshot(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupIDToConsumerGroupMetaMap.size(), fileOutputStream);
    for (Map.Entry<String, ConsumerGroupMeta> entry :
        consumerGroupIDToConsumerGroupMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String topicName = ReadWriteIOUtils.readString(fileInputStream);
      consumerGroupIDToConsumerGroupMetaMap.put(
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
        consumerGroupIDToConsumerGroupMetaMap, that.consumerGroupIDToConsumerGroupMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroupIDToConsumerGroupMetaMap);
  }

  @Override
  public String toString() {
    return "ConsumerGroupMetaKeeper{"
        + "consumerGroupIDToConsumerGroupMetaMap="
        + consumerGroupIDToConsumerGroupMetaMap
        + '}';
  }
}
