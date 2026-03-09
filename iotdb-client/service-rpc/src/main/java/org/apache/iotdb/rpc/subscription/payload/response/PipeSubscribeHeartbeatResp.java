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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeSubscribeHeartbeatResp extends TPipeSubscribeResp {

  private transient Map<String, TopicConfig> topics = new HashMap<>(); // subscribed topics

  private transient Map<Integer, TEndPoint> endPoints = new HashMap<>(); // available endpoints

  private transient List<String> topicNamesToUnsubscribe =
      new ArrayList<>(); // topics should be unsubscribed

  public Map<String, TopicConfig> getTopics() {
    return topics;
  }

  public Map<Integer, TEndPoint> getEndPoints() {
    return endPoints;
  }

  public List<String> getTopicNamesToUnsubscribe() {
    return topicNamesToUnsubscribe;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeHeartbeatResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeHeartbeatResp toTPipeSubscribeResp(final TSStatus status) {
    final PipeSubscribeHeartbeatResp resp = new PipeSubscribeHeartbeatResp();

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();

    return resp;
  }

  /**
   * Serialize the incoming parameters into `PipeSubscribeHeartbeatResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeHeartbeatResp toTPipeSubscribeResp(
      final TSStatus status,
      final Map<String, TopicConfig> topics,
      final Map<Integer, TEndPoint> endPoints,
      final List<String> topicNamesToUnsubscribe)
      throws IOException {
    final PipeSubscribeHeartbeatResp resp = toTPipeSubscribeResp(status);

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(topics.size(), outputStream);
      for (final Map.Entry<String, TopicConfig> entry : topics.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        entry.getValue().serialize(outputStream);
      }
      ReadWriteIOUtils.write(endPoints.size(), outputStream);
      for (final Map.Entry<Integer, TEndPoint> entry : endPoints.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue().getIp(), outputStream);
        ReadWriteIOUtils.write(entry.getValue().getPort(), outputStream);
      }
      ReadWriteIOUtils.writeStringList(topicNamesToUnsubscribe, outputStream);
      resp.body =
          Collections.singletonList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribeHeartbeatResp fromTPipeSubscribeResp(
      final TPipeSubscribeResp heartbeatResp) {
    final PipeSubscribeHeartbeatResp resp = new PipeSubscribeHeartbeatResp();

    if (Objects.nonNull(heartbeatResp.body)) {
      for (final ByteBuffer byteBuffer : heartbeatResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          {
            final int size = ReadWriteIOUtils.readInt(byteBuffer);
            final Map<String, TopicConfig> topics = new HashMap<>();
            for (int i = 0; i < size; i++) {
              final String topicName = ReadWriteIOUtils.readString(byteBuffer);
              final TopicConfig topicConfig = TopicConfig.deserialize(byteBuffer);
              topics.put(topicName, topicConfig);
            }
            resp.topics = topics;
          }
          {
            final int size = ReadWriteIOUtils.readInt(byteBuffer);
            final Map<Integer, TEndPoint> endPoints = new HashMap<>();
            for (int i = 0; i < size; i++) {
              final int nodeId = ReadWriteIOUtils.readInt(byteBuffer);
              final String ip = ReadWriteIOUtils.readString(byteBuffer);
              final int port = ReadWriteIOUtils.readInt(byteBuffer);
              endPoints.put(nodeId, new TEndPoint(ip, port));
            }
            resp.endPoints = endPoints;
          }
          resp.topicNamesToUnsubscribe = ReadWriteIOUtils.readStringList(byteBuffer);
          break;
        }
      }
    }

    resp.status = heartbeatResp.status;
    resp.version = heartbeatResp.version;
    resp.type = heartbeatResp.type;
    resp.body = heartbeatResp.body;

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
    final PipeSubscribeHeartbeatResp that = (PipeSubscribeHeartbeatResp) obj;
    return Objects.equals(this.topics, that.topics)
        && Objects.equals(this.endPoints, that.endPoints)
        && Objects.equals(this.topicNamesToUnsubscribe, that.topicNamesToUnsubscribe)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topics, endPoints, topicNamesToUnsubscribe, status, version, type, body);
  }
}
