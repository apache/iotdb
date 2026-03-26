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
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

public class PipeSubscribeHandshakeResp extends TPipeSubscribeResp {

  private transient int dataNodeId;

  private transient String consumerId;

  private transient String consumerGroupId;

  public int getDataNodeId() {
    return dataNodeId;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeHandshakeResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeHandshakeResp toTPipeSubscribeResp(
      final TSStatus status,
      final int dataNodeId,
      final String consumerId,
      final String consumerGroupId) {
    final PipeSubscribeHandshakeResp resp = new PipeSubscribeHandshakeResp();

    resp.dataNodeId = dataNodeId;
    resp.consumerId = consumerId;
    resp.consumerGroupId = consumerGroupId;

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(dataNodeId, outputStream);
      ReadWriteIOUtils.write(consumerId, outputStream);
      ReadWriteIOUtils.write(consumerGroupId, outputStream);
      resp.body =
          Collections.singletonList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    } catch (final IOException e) {
      resp.status = RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_HANDSHAKE_ERROR, e.getMessage());
      return resp;
    }

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribeHandshakeResp fromTPipeSubscribeResp(
      final TPipeSubscribeResp handshakeResp) {
    final PipeSubscribeHandshakeResp resp = new PipeSubscribeHandshakeResp();

    if (Objects.nonNull(handshakeResp.body)) {
      for (final ByteBuffer byteBuffer : handshakeResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          resp.dataNodeId = ReadWriteIOUtils.readInt(byteBuffer);
          resp.consumerId = ReadWriteIOUtils.readString(byteBuffer);
          resp.consumerGroupId = ReadWriteIOUtils.readString(byteBuffer);
          break;
        }
      }
    }

    resp.status = handshakeResp.status;
    resp.version = handshakeResp.version;
    resp.type = handshakeResp.type;
    resp.body = handshakeResp.body;

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
    final PipeSubscribeHandshakeResp that = (PipeSubscribeHandshakeResp) obj;
    return Objects.equals(this.dataNodeId, that.dataNodeId)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataNodeId, status, version, type, body);
  }
}
