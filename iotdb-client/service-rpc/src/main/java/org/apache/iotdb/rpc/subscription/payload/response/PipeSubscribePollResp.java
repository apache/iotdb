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
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeSubscribePollResp extends TPipeSubscribeResp {

  private transient List<SubscriptionPolledMessage> messages = new ArrayList<>();

  public List<SubscriptionPolledMessage> getMessages() {
    return messages;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribePollResp`, called by the subscription
   * server.
   */
  public static PipeSubscribePollResp toTPipeSubscribeResp(
      TSStatus status, List<SubscriptionPolledMessage> messages) {
    final PipeSubscribePollResp resp = new PipeSubscribePollResp();

    resp.messages = messages;

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();
    try {
      resp.body = new ArrayList<>();
      for (final SubscriptionPolledMessage message : messages) {
        final ByteBuffer byteBuffer =
            Objects.nonNull(message.getByteBuffer())
                ? message.getByteBuffer()
                : SubscriptionPolledMessage.serialize(message);
        resp.body.add(byteBuffer);
        message.resetByteBuffer(); // maybe friendly for gc
      }
    } catch (IOException e) {
      resp.status =
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_SERIALIZATION_ERROR, e.getMessage());
    }

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribePollResp fromTPipeSubscribeResp(TPipeSubscribeResp pollResp) {
    final PipeSubscribePollResp resp = new PipeSubscribePollResp();

    if (Objects.nonNull(pollResp.body)) {
      for (final ByteBuffer byteBuffer : pollResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          resp.messages.add(SubscriptionPolledMessage.deserialize(byteBuffer));
        }
      }
    }

    resp.status = pollResp.status;
    resp.version = pollResp.version;
    resp.type = pollResp.type;
    resp.body = pollResp.body;

    return resp;
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
    return Objects.equals(this.messages, that.messages)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(messages, status, version, type, body);
  }
}
