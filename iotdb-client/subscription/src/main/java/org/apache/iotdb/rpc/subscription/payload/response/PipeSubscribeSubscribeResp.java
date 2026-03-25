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

package org.apache.iotdb.rpc.subscription.payload.response;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeSubscribeSubscribeResp extends TPipeSubscribeResp {

  private transient Map<String, TopicConfig> topics = new HashMap<>(); // subscribed topics

  public Map<String, TopicConfig> getTopics() {
    return topics;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeSubscribeResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeSubscribeResp toTPipeSubscribeResp(final TSStatus status) {
    final PipeSubscribeSubscribeResp resp = new PipeSubscribeSubscribeResp();
    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();
    return resp;
  }

  /**
   * Serialize the incoming parameters into `PipeSubscribeSubscribeResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeSubscribeResp toTPipeSubscribeResp(
      final TSStatus status, final Map<String, TopicConfig> topics) throws IOException {
    final PipeSubscribeSubscribeResp resp = toTPipeSubscribeResp(status);

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(topics.size(), outputStream);
      for (final Map.Entry<String, TopicConfig> entry : topics.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        entry.getValue().serialize(outputStream);
      }
      resp.body =
          Collections.singletonList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribeSubscribeResp fromTPipeSubscribeResp(
      final TPipeSubscribeResp subscribeResp) {
    final PipeSubscribeSubscribeResp resp = new PipeSubscribeSubscribeResp();

    if (Objects.nonNull(subscribeResp.body)) {
      for (final ByteBuffer byteBuffer : subscribeResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          final int size = ReadWriteIOUtils.readInt(byteBuffer);
          final Map<String, TopicConfig> topics = new HashMap<>();
          for (int i = 0; i < size; i++) {
            final String topicName = ReadWriteIOUtils.readString(byteBuffer);
            final TopicConfig topicConfig = TopicConfig.deserialize(byteBuffer);
            topics.put(topicName, topicConfig);
          }
          resp.topics = topics;
          break;
        }
      }
    }

    resp.status = subscribeResp.status;
    resp.version = subscribeResp.version;
    resp.type = subscribeResp.type;
    resp.body = subscribeResp.body;
    return resp;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeSubscribeSubscribeResp that = (PipeSubscribeSubscribeResp) obj;
    return Objects.equals(this.topics, that.topics)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topics, status, version, type, body);
  }
}
