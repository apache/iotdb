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

import org.apache.iotdb.commons.pipe.mq.config.PipeMQTopicConfig;
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
import java.util.Set;

public class PipeMQTopicMeta {
  private String topicName;
  private long creationTime;

  private Set<String> subscribedConsumerGroupIDs;
  private PipeMQTopicConfig config;

  private PipeMQTopicMeta() {
    // Empty constructor
  }

  public PipeMQTopicMeta(String topicName, PipeMQTopicConfig config) {
    this.topicName = topicName;
    this.config = config;
  }

  public PipeMQTopicMeta(String topicName, long creationTime, Map<String, String> topicAttributes) {
    this.topicName = topicName;
    this.creationTime = creationTime;
    this.config = new PipeMQTopicConfig(topicAttributes);
  }

  public PipeMQTopicMeta copy() {
    PipeMQTopicMeta copy = new PipeMQTopicMeta();
    copy.topicName = topicName;
    copy.creationTime = creationTime;
    copy.subscribedConsumerGroupIDs = new HashSet<>(subscribedConsumerGroupIDs);
    copy.config = new PipeMQTopicConfig(new HashMap<>(config.getAttributes()));
    return copy;
  }

  public String getTopicName() {
    return topicName;
  }

  public long getCreationTime() {
    return creationTime;
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

  public PipeMQTopicConfig getConfig() {
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
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(subscribedConsumerGroupIDs.size(), outputStream);
    for (String subscribedConsumerGroupID : subscribedConsumerGroupIDs) {
      ReadWriteIOUtils.write(subscribedConsumerGroupID, outputStream);
    }

    ReadWriteIOUtils.write(config.getAttributes().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(topicName, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(subscribedConsumerGroupIDs.size(), outputStream);
    for (String subscribedConsumerGroupID : subscribedConsumerGroupIDs) {
      ReadWriteIOUtils.write(subscribedConsumerGroupID, outputStream);
    }

    ReadWriteIOUtils.write(config.getAttributes().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public static PipeMQTopicMeta deserialize(InputStream inputStream) throws IOException {
    final PipeMQTopicMeta pipeMQTopicMeta = new PipeMQTopicMeta();

    pipeMQTopicMeta.topicName = ReadWriteIOUtils.readString(inputStream);
    pipeMQTopicMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      pipeMQTopicMeta.subscribedConsumerGroupIDs.add(ReadWriteIOUtils.readString(inputStream));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      pipeMQTopicMeta.config.getAttributes().put(key, value);
    }

    return pipeMQTopicMeta;
  }

  public static PipeMQTopicMeta deserialize(ByteBuffer byteBuffer) {
    final PipeMQTopicMeta pipeMQTopicMeta = new PipeMQTopicMeta();

    pipeMQTopicMeta.topicName = ReadWriteIOUtils.readString(byteBuffer);
    pipeMQTopicMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      pipeMQTopicMeta.subscribedConsumerGroupIDs.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    // todo: der PipeMQTopicConfig

    return pipeMQTopicMeta;
  }
}
