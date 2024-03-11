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

import org.apache.iotdb.commons.subscription.config.TopicConfig;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TopicMeta {
  private String topicName;

  private long createTime;

  private Set<String> subscribedConsumerGroupIDs;
  private TopicConfig config;

  private TopicMeta() {
    // Empty constructor
  }

  public TopicMeta(String topicName, long createTime, Map<String, String> topicAttributes) {
    this.topicName = topicName;
    this.createTime = createTime;
    this.config = new TopicConfig(topicAttributes);
  }

  public TopicMeta copy() {
    TopicMeta copy = new TopicMeta();
    copy.topicName = topicName;
    copy.subscribedConsumerGroupIDs = new HashSet<>(subscribedConsumerGroupIDs);
    copy.config = new TopicConfig(new HashMap<>(config.getAttribute()));
    return copy;
  }

  public String getTopicName() {
    return topicName;
  }

  public long getCreationTime() {
    return createTime;
  }

  /** @return true if the consumer group did not already subscribe this topic */
  public boolean addSubscribedConsumerGroup(String consumerGroupId) {
    return subscribedConsumerGroupIDs.add(consumerGroupId);
  }

  public void removeSubscribedConsumerGroup(String consumerGroupId) {
    subscribedConsumerGroupIDs.remove(consumerGroupId);
  }

  public Set<String> getSubscribedConsumerGroupIDs() {
    return subscribedConsumerGroupIDs;
  }

  public boolean isSubscribedByConsumerGroup(String consumerGroupId) {
    return subscribedConsumerGroupIDs.contains(consumerGroupId);
  }

  public TopicConfig getConfig() {
    return config;
  }

  ////////////////////////////////////// ser deser ////////////////////////////////

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(topicName, outputStream);
    ReadWriteIOUtils.write(createTime, outputStream);

    ReadWriteIOUtils.write(subscribedConsumerGroupIDs.size(), outputStream);
    for (String subscribedConsumerGroupID : subscribedConsumerGroupIDs) {
      ReadWriteIOUtils.write(subscribedConsumerGroupID, outputStream);
    }

    ReadWriteIOUtils.write(config.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(topicName, outputStream);
    ReadWriteIOUtils.write(createTime, outputStream);

    ReadWriteIOUtils.write(subscribedConsumerGroupIDs.size(), outputStream);
    for (String subscribedConsumerGroupID : subscribedConsumerGroupIDs) {
      ReadWriteIOUtils.write(subscribedConsumerGroupID, outputStream);
    }

    ReadWriteIOUtils.write(config.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public static TopicMeta deserialize(InputStream inputStream) throws IOException {
    final TopicMeta topicMeta = new TopicMeta();

    topicMeta.topicName = ReadWriteIOUtils.readString(inputStream);
    topicMeta.createTime = ReadWriteIOUtils.readLong(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      topicMeta.subscribedConsumerGroupIDs.add(ReadWriteIOUtils.readString(inputStream));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      topicMeta.config.getAttribute().put(key, value);
    }

    return topicMeta;
  }

  public static TopicMeta deserialize(ByteBuffer byteBuffer) {
    final TopicMeta topicMeta = new TopicMeta();

    topicMeta.topicName = ReadWriteIOUtils.readString(byteBuffer);
    topicMeta.createTime = ReadWriteIOUtils.readLong(byteBuffer);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      topicMeta.subscribedConsumerGroupIDs.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      topicMeta.config.getAttribute().put(key, value);
    }

    return topicMeta;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TopicMeta that = (TopicMeta) obj;
    return createTime == that.createTime
        && topicName.equals(that.topicName)
        && subscribedConsumerGroupIDs.equals(that.subscribedConsumerGroupIDs)
        && config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, createTime, subscribedConsumerGroupIDs, config);
  }

  @Override
  public String toString() {
    return "TopicMeta{"
        + "topicName='"
        + topicName
        + '\''
        + ", createTime="
        + createTime
        + ", subscribedConsumerGroupIDs="
        + subscribedConsumerGroupIDs
        + ", config="
        + config
        + '}';
  }
}
