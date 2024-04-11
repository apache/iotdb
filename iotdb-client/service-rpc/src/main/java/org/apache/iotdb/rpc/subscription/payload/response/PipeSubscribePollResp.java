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
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.common.EnrichedTablets;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionRawMessage;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PipeSubscribePollResp extends TPipeSubscribeResp {

  private transient List<SubscriptionRawMessage> messages = new ArrayList<>();

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribePollResp`, called by the subscription
   * server.
   */
  public static PipeSubscribePollResp toTPipeSubscribeResp(
      TSStatus status, List<SubscriptionRawMessage> messages) {
    final PipeSubscribePollResp resp = new PipeSubscribePollResp();

    resp.messages = messages;

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();
    try {
      resp.body = new ArrayList<>();
      for (final SubscriptionRawMessage message: messages) {
        resp.body.add(SubscriptionRawMessage.serialize(message));
      }
    } catch (IOException e) {
      resp.status = RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_POLL_ERROR, e.getMessage());
    }

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribePollResp fromTPipeSubscribeResp(TPipeSubscribeResp pollResp) {
    final PipeSubscribePollResp resp = new PipeSubscribePollResp();

    if (Objects.nonNull(pollResp.body)) {
      for (final ByteBuffer byteBuffer : pollResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          resp.messages.add(SubscriptionRawMessage.deserialize(byteBuffer));
        }
      }
    }

    resp.status = pollResp.status;
    resp.version = pollResp.version;
    resp.type = pollResp.type;
    resp.body = pollResp.body;

    return resp;
  }

  /////////////////////////////// Utility ///////////////////////////////

  public static List<ByteBuffer> serializeEnrichedTabletsWithByteBufferList(
      List<Pair<ByteBuffer, EnrichedTablets>> enrichedTabletsWithByteBufferList)
      throws IOException {
    List<ByteBuffer> byteBufferList = new ArrayList<>();
    for (Pair<ByteBuffer, EnrichedTablets> enrichedTabletsWithByteBuffer :
        enrichedTabletsWithByteBufferList) {
      if (Objects.nonNull(enrichedTabletsWithByteBuffer.getLeft())) {
        byteBufferList.add(enrichedTabletsWithByteBuffer.getLeft());
      } else {
        byteBufferList.add(serializeEnrichedTablets(enrichedTabletsWithByteBuffer.getRight()));
      }
    }
    return byteBufferList;
  }

  public static ByteBuffer serializeEnrichedTablets(EnrichedTablets enrichedTablets)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      enrichedTablets.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeSubscribePollResp that = (PipeSubscribePollResp) obj;
    return Objects.equals(this.enrichedTabletsList, that.enrichedTabletsList)
        && Objects.equals(this.topicNameToTsFileNameMap, that.topicNameToTsFileNameMap)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(enrichedTabletsList, topicNameToTsFileNameMap, status, version, type, body);
  }
}
