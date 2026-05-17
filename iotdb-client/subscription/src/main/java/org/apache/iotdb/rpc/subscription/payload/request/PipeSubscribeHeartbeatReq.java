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

import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeSubscribeHeartbeatReq extends TPipeSubscribeReq {

  private transient List<SubscriptionCommitContext> processorBufferedCommitContexts =
      new ArrayList<>();

  public List<SubscriptionCommitContext> getProcessorBufferedCommitContexts() {
    return processorBufferedCommitContexts;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeHeartbeatReq`, called by the subscription
   * client.
   */
  public static PipeSubscribeHeartbeatReq toTPipeSubscribeReq() {
    final PipeSubscribeHeartbeatReq req = new PipeSubscribeHeartbeatReq();

    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.HEARTBEAT.getType();

    return req;
  }

  /**
   * Serialize the incoming parameters into `PipeSubscribeHeartbeatReq`, called by the subscription
   * client.
   */
  public static PipeSubscribeHeartbeatReq toTPipeSubscribeReq(
      final List<SubscriptionCommitContext> processorBufferedCommitContexts) throws IOException {
    final PipeSubscribeHeartbeatReq req = toTPipeSubscribeReq();
    req.processorBufferedCommitContexts =
        Objects.nonNull(processorBufferedCommitContexts)
            ? processorBufferedCommitContexts
            : new ArrayList<>();

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(req.processorBufferedCommitContexts.size(), outputStream);
      for (final SubscriptionCommitContext commitContext : req.processorBufferedCommitContexts) {
        commitContext.serialize(outputStream);
      }
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  /** Deserialize `TPipeSubscribeReq` to obtain parameters, called by the subscription server. */
  public static PipeSubscribeHeartbeatReq fromTPipeSubscribeReq(
      final TPipeSubscribeReq heartbeatReq) {
    final PipeSubscribeHeartbeatReq req = new PipeSubscribeHeartbeatReq();

    if (Objects.nonNull(heartbeatReq.body) && heartbeatReq.body.hasRemaining()) {
      final int size = ReadWriteIOUtils.readInt(heartbeatReq.body);
      for (int i = 0; i < size; ++i) {
        req.processorBufferedCommitContexts.add(
            SubscriptionCommitContext.deserialize(heartbeatReq.body));
      }
    }

    req.version = heartbeatReq.version;
    req.type = heartbeatReq.type;
    req.body = heartbeatReq.body;

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
    final PipeSubscribeHeartbeatReq that = (PipeSubscribeHeartbeatReq) obj;
    return Objects.equals(
            this.processorBufferedCommitContexts, that.processorBufferedCommitContexts)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(processorBufferedCommitContexts, version, type, body);
  }
}
