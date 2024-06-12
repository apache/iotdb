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
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class PipeSubscribeUnsubscribeResp extends TPipeSubscribeResp {

  private transient Set<String> topicNames = new HashSet<>(); // subscribed topic names

  public Set<String> getTopicNames() {
    return topicNames;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeUnsubscribeResp`, called by the
   * subscription server.
   */
  public static PipeSubscribeUnsubscribeResp toTPipeSubscribeResp(final TSStatus status) {
    final PipeSubscribeUnsubscribeResp resp = new PipeSubscribeUnsubscribeResp();
    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();
    return resp;
  }

  /**
   * Serialize the incoming parameters into `PipeSubscribeUnsubscribeResp`, called by the
   * subscription server.
   */
  public static PipeSubscribeUnsubscribeResp toTPipeSubscribeResp(
      final TSStatus status, final Set<String> topicNames) throws IOException {
    final PipeSubscribeUnsubscribeResp resp = new PipeSubscribeUnsubscribeResp();

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.writeObjectSet(topicNames, outputStream);
      resp.body =
          Collections.singletonList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }

    return resp;
  }

  /**
   * Serialize the incoming parameters into `PipeSubscribeUnsubscribeResp`, called by the
   * subscription server.
   */
  public static PipeSubscribeUnsubscribeResp toTPipeSubscribeResp(
      final TSStatus status, final Set<String> topicNames) throws IOException {
    final PipeSubscribeUnsubscribeResp resp = new PipeSubscribeUnsubscribeResp();

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.writeObjectSet(topicNames, outputStream);
      resp.body =
          Collections.singletonList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    }

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribeUnsubscribeResp fromTPipeSubscribeResp(
      final TPipeSubscribeResp unsubscribeResp) {
    final PipeSubscribeUnsubscribeResp resp = new PipeSubscribeUnsubscribeResp();

    if (Objects.nonNull(unsubscribeResp.body)) {
      for (final ByteBuffer byteBuffer : unsubscribeResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          resp.topicNames = ReadWriteIOUtils.readObjectSet(byteBuffer);
          break;
        }
      }
    }

    resp.status = unsubscribeResp.status;
    resp.version = unsubscribeResp.version;
    resp.type = unsubscribeResp.type;
    resp.body = unsubscribeResp.body;
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
    final PipeSubscribeUnsubscribeResp that = (PipeSubscribeUnsubscribeResp) obj;
    return Objects.equals(this.topicNames, that.topicNames)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicNames, status, version, type, body);
  }
}
