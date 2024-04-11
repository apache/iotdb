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

import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeSubscribeCommitReq extends TPipeSubscribeReq {

  private transient List<SubscriptionCommitContext> commitContexts = new ArrayList<>();

  public List<SubscriptionCommitContext> getCommitContexts() {
    return commitContexts;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeCommitReq`, called by the subscription
   * client.
   */
  public static PipeSubscribeCommitReq toTPipeSubscribeReq(
      List<SubscriptionCommitContext> commitContexts) throws IOException {
    final PipeSubscribeCommitReq req = new PipeSubscribeCommitReq();

    req.commitContexts = commitContexts;

    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.COMMIT.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(commitContexts.size(), outputStream);
      for (final SubscriptionCommitContext commitContext : commitContexts) {
        commitContext.serialize(outputStream);
      }
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  /** Deserialize `TPipeSubscribeReq` to obtain parameters, called by the subscription server. */
  public static PipeSubscribeCommitReq fromTPipeSubscribeReq(TPipeSubscribeReq commitReq) {
    final PipeSubscribeCommitReq req = new PipeSubscribeCommitReq();

    if (Objects.nonNull(commitReq.body) && commitReq.body.hasRemaining()) {
      int size = ReadWriteIOUtils.readInt(commitReq.body);
      for (int i = 0; i < size; ++i) {
        req.commitContexts.add(SubscriptionCommitContext.deserialize(commitReq.body));
      }
    }

    req.version = commitReq.version;
    req.type = commitReq.type;
    req.body = commitReq.body;

    return req;
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
    PipeSubscribeCommitReq that = (PipeSubscribeCommitReq) obj;
    return Objects.equals(this.commitContexts, that.commitContexts)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitContexts, version, type, body);
  }
}
