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

import org.apache.iotdb.commons.pipe.mq.config.PipeMQConsumerConfig;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeMQConsumerMeta {
  private String consumerId;
  private long creationTime;
  private PipeMQConsumerConfig config;

  public PipeMQConsumerMeta() {
    // Empty constructor
  }

  public PipeMQConsumerMeta(
      String consumerId, long creationTime, Map<String, String> consumerAttributes) {
    this.consumerId = consumerId;
    this.creationTime = creationTime;
    this.config = new PipeMQConsumerConfig(consumerAttributes);
  }

  public String getConsumerId() {
    return consumerId;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consumerId, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(config.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consumerId, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(config.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public static PipeMQConsumerMeta deserialize(InputStream inputStream) throws IOException {
    final PipeMQConsumerMeta consumerMeta = new PipeMQConsumerMeta();

    consumerMeta.consumerId = ReadWriteIOUtils.readString(inputStream);
    consumerMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    consumerMeta.config = new PipeMQConsumerConfig(new HashMap<>());

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      consumerMeta.config.getAttribute().put(key, value);
    }

    return consumerMeta;
  }

  public static PipeMQConsumerMeta deserialize(ByteBuffer byteBuffer) {
    final PipeMQConsumerMeta consumerMeta = new PipeMQConsumerMeta();

    consumerMeta.consumerId = ReadWriteIOUtils.readString(byteBuffer);
    consumerMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    consumerMeta.config = new PipeMQConsumerConfig(new HashMap<>());

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      consumerMeta.config.getAttribute().put(key, value);
    }

    return consumerMeta;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeMQConsumerMeta that = (PipeMQConsumerMeta) obj;
    return consumerId.equals(that.consumerId)
        && creationTime == that.creationTime
        && config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerId, creationTime, config);
  }

  @Override
  public String toString() {
    return "PipeMQConsumerMeta{"
        + "consumerID='"
        + consumerId
        + "', creationTime="
        + creationTime
        + ", config="
        + config
        + "}";
  }
}
