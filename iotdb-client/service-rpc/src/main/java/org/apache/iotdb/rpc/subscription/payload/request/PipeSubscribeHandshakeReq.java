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

package org.apache.iotdb.rpc.subscription.payload.request;

import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;

import java.io.IOException;
import java.util.Objects;

public class PipeSubscribeHandshakeReq extends TPipeSubscribeReq {

  private transient ConsumerConfig consumerConfig = new ConsumerConfig();

  public ConsumerConfig getConsumerConfig() {
    return consumerConfig;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeHandshakeReq`, called by the subscription
   * client.
   */
  public static PipeSubscribeHandshakeReq toTPipeSubscribeReq(final ConsumerConfig consumerConfig)
      throws IOException {
    final PipeSubscribeHandshakeReq req = new PipeSubscribeHandshakeReq();

    req.consumerConfig = consumerConfig;

    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.HANDSHAKE.getType();
    req.body = ConsumerConfig.serialize(consumerConfig);

    return req;
  }

  /** Deserialize `TPipeSubscribeReq` to obtain parameters, called by the subscription server. */
  public static PipeSubscribeHandshakeReq fromTPipeSubscribeReq(
      final TPipeSubscribeReq handshakeReq) {
    final PipeSubscribeHandshakeReq req = new PipeSubscribeHandshakeReq();

    if (Objects.nonNull(handshakeReq.body) && handshakeReq.body.hasRemaining()) {
      req.consumerConfig = ConsumerConfig.deserialize(handshakeReq.body);
    }

    req.version = handshakeReq.version;
    req.type = handshakeReq.type;
    req.body = handshakeReq.body;

    return req;
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
    final PipeSubscribeHandshakeReq that = (PipeSubscribeHandshakeReq) obj;
    return Objects.equals(this.consumerConfig, that.consumerConfig)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerConfig, version, type, body);
  }
}
