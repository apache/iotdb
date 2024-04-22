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

package org.apache.iotdb.commons.subscription.meta.topic;

import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TopicMeta {

  private String topicName;
  private long creationTime;
  private TopicConfig config;

  private Set<String> subscribedConsumerGroupIds;

  private TopicMeta() {
    this.config = new TopicConfig(new HashMap<>());

    this.subscribedConsumerGroupIds = new HashSet<>();
  }

  public TopicMeta(String topicName, long creationTime, Map<String, String> topicAttributes) {
    this.topicName = topicName;
    this.creationTime = creationTime;
    this.config = new TopicConfig(topicAttributes);

    this.subscribedConsumerGroupIds = new HashSet<>();
  }

  public TopicMeta deepCopy() {
    final TopicMeta copied = new TopicMeta();
    copied.topicName = topicName;
    copied.creationTime = creationTime;
    copied.config = new TopicConfig(new HashMap<>(config.getAttribute()));

    copied.subscribedConsumerGroupIds = new HashSet<>(subscribedConsumerGroupIds);
    return copied;
  }

  public String getTopicName() {
    return topicName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public TopicConfig getConfig() {
    return config;
  }

  /** @return true if the consumer group did not already subscribe this topic */
  public boolean addSubscribedConsumerGroup(String consumerGroupId) {
    return subscribedConsumerGroupIds.add(consumerGroupId);
  }

  public void removeSubscribedConsumerGroup(String consumerGroupId) {
    subscribedConsumerGroupIds.remove(consumerGroupId);
  }

  public Set<String> getSubscribedConsumerGroupIds() {
    return subscribedConsumerGroupIds;
  }

  public boolean isSubscribedByConsumerGroup(String consumerGroupId) {
    return subscribedConsumerGroupIds.contains(consumerGroupId);
  }

  public boolean hasSubscribedConsumerGroup() {
    return !subscribedConsumerGroupIds.isEmpty();
  }

  ////////////////////////////////////// de/ser ////////////////////////////////

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(topicName, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(config.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }

    ReadWriteIOUtils.write(subscribedConsumerGroupIds.size(), outputStream);
    for (String subscribedConsumerGroupID : subscribedConsumerGroupIds) {
      ReadWriteIOUtils.write(subscribedConsumerGroupID, outputStream);
    }
  }

  public static TopicMeta deserialize(InputStream inputStream) throws IOException {
    final TopicMeta topicMeta = new TopicMeta();

    topicMeta.topicName = ReadWriteIOUtils.readString(inputStream);
    topicMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      topicMeta.config.getAttribute().put(key, value);
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      topicMeta.subscribedConsumerGroupIds.add(ReadWriteIOUtils.readString(inputStream));
    }

    return topicMeta;
  }

  public static TopicMeta deserialize(ByteBuffer byteBuffer) {
    final TopicMeta topicMeta = new TopicMeta();

    topicMeta.topicName = ReadWriteIOUtils.readString(byteBuffer);
    topicMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      topicMeta.config.getAttribute().put(key, value);
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      topicMeta.subscribedConsumerGroupIds.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    return topicMeta;
  }

  /////////////////////////////// utilities ///////////////////////////////

  public Map<String, String> generateExtractorAttributes() {
    Map<String, String> extractorAttributes = new HashMap<>();
    // disable meta sync
    extractorAttributes.put("source", "iotdb-source");
    extractorAttributes.put("inclusion", "data.insert");
    extractorAttributes.put("inclusion.exclusion", "data.delete");
    // path
    extractorAttributes.putAll(config.getAttributesWithSourcePathOrPattern());
    // time
    extractorAttributes.putAll(config.getAttributesWithTimeRange(creationTime));
    return extractorAttributes;
  }

  public Map<String, String> generateProcessorAttributes() {
    return config.getAttributesWithProcessorPrefix();
  }

  public Map<String, String> generateConnectorAttributes(String consumerGroupId) {
    Map<String, String> connectorAttributes = new HashMap<>();
    connectorAttributes.put("sink", "subscription-sink");
    connectorAttributes.put(PipeConnectorConstant.SINK_TOPIC_KEY, topicName);
    connectorAttributes.put(PipeConnectorConstant.SINK_CONSUMER_GROUP_KEY, consumerGroupId);
    return connectorAttributes;
  }

  ////////////////////////////////////// Object ////////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TopicMeta that = (TopicMeta) obj;
    return creationTime == that.creationTime
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(config, that.config)
        && Objects.equals(subscribedConsumerGroupIds, that.subscribedConsumerGroupIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, creationTime, subscribedConsumerGroupIds, config);
  }

  @Override
  public String toString() {
    return "TopicMeta{"
        + "topicName='"
        + topicName
        + '\''
        + ", creationTime="
        + creationTime
        + ", config="
        + config
        + ", subscribedConsumerGroupIds="
        + subscribedConsumerGroupIds
        + '}';
  }
}
