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

package org.apache.iotdb.commons.pipe.mq.meta;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeMQConsumerGroupMetaKeeper {
  private Map<String, PipeMQConsumerGroupMeta> consumerGroupIDToPipeMQConsumerGroupMetaMap;

  public PipeMQConsumerGroupMetaKeeper() {
    consumerGroupIDToPipeMQConsumerGroupMetaMap = new ConcurrentHashMap<>();
  }

  public boolean containsPipeMQConsumerGroupMeta(String consumerGroupID) {
    return consumerGroupIDToPipeMQConsumerGroupMetaMap.containsKey(consumerGroupID);
  }

  public PipeMQConsumerGroupMeta getPipeMQConsumerGroupMeta(String consumerGroupID) {
    return consumerGroupIDToPipeMQConsumerGroupMetaMap.get(consumerGroupID);
  }

  /**
   * Get the consumers subscribing the given topic in the given consumer group.
   *
   * @param topic The topic name.
   * @return The set of consumer IDs subscribing the given topic in this group. If the group does
   *     not exist or no consumer is subscribing the topic, return an empty set.
   */
  public Set<String> getConsumersSubscribingTopic(String consumerGroupId, String topic) {
    return consumerGroupIDToPipeMQConsumerGroupMetaMap.containsKey(consumerGroupId)
        ? consumerGroupIDToPipeMQConsumerGroupMetaMap
            .get(consumerGroupId)
            .getConsumersSubscribingTopic(topic)
        : Collections.emptySet();
  }

  public void addPipeMQConsumerGroupMeta(
      String consumerGroupID, PipeMQConsumerGroupMeta pipeMQConsumerGroupMeta) {
    consumerGroupIDToPipeMQConsumerGroupMetaMap.put(consumerGroupID, pipeMQConsumerGroupMeta);
  }

  public void removePipeMQConsumerGroupMeta(String consumerGroupID) {
    consumerGroupIDToPipeMQConsumerGroupMetaMap.remove(consumerGroupID);
  }

  public void clear() {
    this.consumerGroupIDToPipeMQConsumerGroupMetaMap.clear();
  }

  public boolean isEmpty() {
    return consumerGroupIDToPipeMQConsumerGroupMetaMap.isEmpty();
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  public void processTakeSnapshot(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupIDToPipeMQConsumerGroupMetaMap.size(), fileOutputStream);
    for (Map.Entry<String, PipeMQConsumerGroupMeta> entry :
        consumerGroupIDToPipeMQConsumerGroupMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String topicName = ReadWriteIOUtils.readString(fileInputStream);
      consumerGroupIDToPipeMQConsumerGroupMetaMap.put(
          topicName, PipeMQConsumerGroupMeta.deserialize(fileInputStream));
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
    PipeMQConsumerGroupMetaKeeper that = (PipeMQConsumerGroupMetaKeeper) o;
    return Objects.equals(
        consumerGroupIDToPipeMQConsumerGroupMetaMap,
        that.consumerGroupIDToPipeMQConsumerGroupMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroupIDToPipeMQConsumerGroupMetaMap);
  }

  @Override
  public String toString() {
    return "PipeMQConsumerGroupMetaKeeper{"
        + "consumerGroupIDToPipeMQConsumerGroupMetaMap="
        + consumerGroupIDToPipeMQConsumerGroupMetaMap
        + '}';
  }
}
