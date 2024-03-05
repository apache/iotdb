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

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

public class PipeMQTopicMeta {
  private String topicName;
  private long creationTime;

  private Set<String> subscribedConsumerGroupIDs;
  private final PipeMQTopicConfig config;

  private PipeMQTopicMeta() {
    // Empty constructor
  }

  public PipeMQTopicMeta(String topicName, PipeMQTopicConfig config) {
    this.topicName = topicName;
    this.config = config;
  }

  public String getTopicName() {
    return topicName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public Set<String> getSubscribedConsumerGroupIDs() {
    return subscribedConsumerGroupIDs;
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

    config.serialize(outputStream);
  }

  public static PipeMQTopicMeta deserialize(FileInputStream fileInputStream) throws IOException {
    final PipeMQTopicMeta pipeMQTopicMeta = new PipeMQTopicMeta();

    pipeMQTopicMeta.topicName = ReadWriteIOUtils.readString(fileInputStream);
    pipeMQTopicMeta.creationTime = ReadWriteIOUtils.readLong(fileInputStream);

    int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      pipeMQTopicMeta.subscribedConsumerGroupIDs.add(ReadWriteIOUtils.readString(fileInputStream));
    }

    pipeMQTopicMeta.config = PipeMQTopicConfig.deserialize(fileInputStream);

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
