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
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeSubscribeCommitResp extends TPipeSubscribeResp {

  private transient List<SubscriptionCommitContext> acceptedCommitContexts = new ArrayList<>();

  private transient Map<String, TopicProgress> committedProgressByTopic = new LinkedHashMap<>();

  public List<SubscriptionCommitContext> getAcceptedCommitContexts() {
    return acceptedCommitContexts;
  }

  public Map<String, TopicProgress> getCommittedProgressByTopic() {
    return committedProgressByTopic;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeCommitResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeCommitResp toTPipeSubscribeResp(final TSStatus status) {
    final PipeSubscribeCommitResp resp = new PipeSubscribeCommitResp();

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();

    return resp;
  }

  /**
   * Serialize the incoming parameters into `PipeSubscribeCommitResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeCommitResp toTPipeSubscribeResp(
      final TSStatus status,
      final List<SubscriptionCommitContext> acceptedCommitContexts,
      final Map<String, TopicProgress> committedProgressByTopic)
      throws IOException {
    final PipeSubscribeCommitResp resp = toTPipeSubscribeResp(status);
    resp.acceptedCommitContexts = acceptedCommitContexts;
    resp.committedProgressByTopic = committedProgressByTopic;

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(acceptedCommitContexts.size(), outputStream);
      for (final SubscriptionCommitContext commitContext : acceptedCommitContexts) {
        commitContext.serialize(outputStream);
      }
      ReadWriteIOUtils.write(committedProgressByTopic.size(), outputStream);
      for (final Map.Entry<String, TopicProgress> entry : committedProgressByTopic.entrySet()) {
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
  public static PipeSubscribeCommitResp fromTPipeSubscribeResp(
      final TPipeSubscribeResp commitResp) {
    final PipeSubscribeCommitResp resp = new PipeSubscribeCommitResp();

    if (Objects.nonNull(commitResp.body)) {
      for (final ByteBuffer byteBuffer : commitResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          final int acceptedSize = ReadWriteIOUtils.readInt(byteBuffer);
          final List<SubscriptionCommitContext> acceptedCommitContexts = new ArrayList<>();
          for (int i = 0; i < acceptedSize; i++) {
            acceptedCommitContexts.add(SubscriptionCommitContext.deserialize(byteBuffer));
          }
          final int committedProgressSize = ReadWriteIOUtils.readInt(byteBuffer);
          final Map<String, TopicProgress> committedProgressByTopic = new LinkedHashMap<>();
          for (int i = 0; i < committedProgressSize; i++) {
            committedProgressByTopic.put(
                ReadWriteIOUtils.readString(byteBuffer), TopicProgress.deserialize(byteBuffer));
          }
          resp.acceptedCommitContexts = acceptedCommitContexts;
          resp.committedProgressByTopic = committedProgressByTopic;
          break;
        }
      }
    }

    resp.status = commitResp.status;
    resp.version = commitResp.version;
    resp.type = commitResp.type;
    resp.body = commitResp.body;

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
    final PipeSubscribeCommitResp that = (PipeSubscribeCommitResp) obj;
    return Objects.equals(this.acceptedCommitContexts, that.acceptedCommitContexts)
        && Objects.equals(this.committedProgressByTopic, that.committedProgressByTopic)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        acceptedCommitContexts, committedProgressByTopic, status, version, type, body);
  }
}
