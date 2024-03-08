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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/** PipeMQSubscriptionMeta is created for show subscription and is not stored in meta keeper. */
public class PipeMQSubscriptionMeta {
  private String topicName;
  private String consumerGroupID;
  private Set<String> consumerIDs;

  public PipeMQSubscriptionMeta() {
    // Empty constructor
  }

  public PipeMQSubscriptionMeta(String topicName, String consumerGroupID, Set<String> consumerIDs) {
    this.topicName = topicName;
    this.consumerGroupID = consumerGroupID;
    this.consumerIDs = consumerIDs;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getConsumerGroupID() {
    return consumerGroupID;
  }

  public Set<String> getConsumerIDs() {
    return consumerIDs;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(topicName, outputStream);
    ReadWriteIOUtils.write(consumerGroupID, outputStream);

    ReadWriteIOUtils.write(consumerIDs.size(), outputStream);
    for (String consumerId : consumerIDs) {
      ReadWriteIOUtils.write(consumerId, outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(topicName, outputStream);
    ReadWriteIOUtils.write(consumerGroupID, outputStream);

    ReadWriteIOUtils.write(consumerIDs.size(), outputStream);
    for (String consumerId : consumerIDs) {
      ReadWriteIOUtils.write(consumerId, outputStream);
    }
  }

  public static PipeMQSubscriptionMeta deserialize(InputStream inputStream) throws IOException {
    final PipeMQSubscriptionMeta pipeMQSubscriptionMeta = new PipeMQSubscriptionMeta();

    pipeMQSubscriptionMeta.topicName = ReadWriteIOUtils.readString(inputStream);
    pipeMQSubscriptionMeta.consumerGroupID = ReadWriteIOUtils.readString(inputStream);
    pipeMQSubscriptionMeta.consumerIDs = new HashSet<>();

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      pipeMQSubscriptionMeta.consumerIDs.add(ReadWriteIOUtils.readString(inputStream));
    }

    return pipeMQSubscriptionMeta;
  }

  public static PipeMQSubscriptionMeta deserialize(ByteBuffer byteBuffer) {
    final PipeMQSubscriptionMeta pipeMQSubscriptionMeta = new PipeMQSubscriptionMeta();

    pipeMQSubscriptionMeta.topicName = ReadWriteIOUtils.readString(byteBuffer);
    pipeMQSubscriptionMeta.consumerGroupID = ReadWriteIOUtils.readString(byteBuffer);
    pipeMQSubscriptionMeta.consumerIDs = new HashSet<>();

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      pipeMQSubscriptionMeta.consumerIDs.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    return pipeMQSubscriptionMeta;
  }
}
