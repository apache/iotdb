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

import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ConsumerMeta {

  private String consumerId;
  private long creationTime;
  private ConsumerConfig config;

  private ConsumerMeta() {
    // Empty constructor
  }

  public ConsumerMeta(
      String consumerId, long creationTime, Map<String, String> consumerAttributes) {
    this.consumerId = consumerId;
    this.creationTime = creationTime;
    this.config = new ConsumerConfig(consumerAttributes);
  }

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return config.getConsumerGroupId();
  }

  public String getUsername() {
    return config.getUsername();
  }

  public String getPassword() {
    return config.getPassword();
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consumerId, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(config.getAttribute().size(), outputStream);
    for (Map.Entry<String, String> entry : config.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public static ConsumerMeta deserialize(InputStream inputStream) throws IOException {
    final ConsumerMeta consumerMeta = new ConsumerMeta();

    consumerMeta.consumerId = ReadWriteIOUtils.readString(inputStream);
    consumerMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    consumerMeta.config = new ConsumerConfig(new HashMap<>());
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      consumerMeta.config.getAttribute().put(key, value);
    }

    return consumerMeta;
  }

  public static ConsumerMeta deserialize(ByteBuffer byteBuffer) {
    final ConsumerMeta consumerMeta = new ConsumerMeta();

    consumerMeta.consumerId = ReadWriteIOUtils.readString(byteBuffer);
    consumerMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    consumerMeta.config = new ConsumerConfig(new HashMap<>());
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
    ConsumerMeta that = (ConsumerMeta) obj;
    return Objects.equals(consumerId, that.consumerId)
        && Objects.equals(creationTime, that.creationTime)
        && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerId, creationTime, config);
  }

  @Override
  public String toString() {
    return "ConsumerMeta{"
        + "consumerId='"
        + consumerId
        + "', creationTime="
        + creationTime
        + ", config="
        + config
        + "}";
  }
}
