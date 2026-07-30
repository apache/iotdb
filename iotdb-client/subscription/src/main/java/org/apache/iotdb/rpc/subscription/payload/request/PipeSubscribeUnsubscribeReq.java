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

import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class PipeSubscribeUnsubscribeReq extends TPipeSubscribeReq {

  private transient Set<String> topicNames = new HashSet<>();

  public Set<String> getTopicNames() {
    return topicNames;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeUnsubscribeReq`, called by the
   * subscription client.
   */
  public static PipeSubscribeUnsubscribeReq toTPipeSubscribeReq(final Set<String> topicNames)
      throws IOException {
    final PipeSubscribeUnsubscribeReq req = new PipeSubscribeUnsubscribeReq();

    req.topicNames = topicNames;

    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.UNSUBSCRIBE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.writeObjectSet(topicNames, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  /** Deserialize `TPipeSubscribeReq` to obtain parameters, called by the subscription server. */
  public static PipeSubscribeUnsubscribeReq fromTPipeSubscribeReq(
      final TPipeSubscribeReq unsubscribeReq) {
    final PipeSubscribeUnsubscribeReq req = new PipeSubscribeUnsubscribeReq();

    if (Objects.nonNull(unsubscribeReq.body) && unsubscribeReq.body.hasRemaining()) {
      req.topicNames = ReadWriteIOUtils.readObjectSet(unsubscribeReq.body);
    }

    req.version = unsubscribeReq.version;
    req.type = unsubscribeReq.type;
    req.body = unsubscribeReq.body;

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
    final PipeSubscribeUnsubscribeReq that = (PipeSubscribeUnsubscribeReq) obj;
    return Objects.equals(this.topicNames, that.topicNames)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicNames, version, type, body);
  }
}
