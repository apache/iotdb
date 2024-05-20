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
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PipeSubscribePollResp extends TPipeSubscribeResp {

  private transient List<EnrichedTablets> enrichedTabletsList = new ArrayList<>();

  public List<EnrichedTablets> getEnrichedTabletsList() {
    return enrichedTabletsList;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribePollResp`, called by the subscription
   * server.
   */
  public static PipeSubscribePollResp toTPipeSubscribeResp(
      TSStatus status, List<Pair<ByteBuffer, EnrichedTablets>> enrichedTabletsWithByteBufferList) {
    final PipeSubscribePollResp resp = new PipeSubscribePollResp();

    resp.enrichedTabletsList =
        enrichedTabletsWithByteBufferList.stream().map(Pair::getRight).collect(Collectors.toList());

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.POLL_TABLETS.getType();
    try {
      resp.body = serializeEnrichedTabletsWithByteBufferList(enrichedTabletsWithByteBufferList);
    } catch (IOException e) {
      resp.status = RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_POLL_ERROR, e.getMessage());
    }

    return resp;
  }

  public static PipeSubscribePollResp directToTPipeSubscribeResp(
      TSStatus status, List<ByteBuffer> byteBuffers) {
    final PipeSubscribePollResp resp = new PipeSubscribePollResp();

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.POLL_TABLETS.getType();
    resp.body = byteBuffers;

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribePollResp fromTPipeSubscribeResp(TPipeSubscribeResp pollResp) {
    final PipeSubscribePollResp resp = new PipeSubscribePollResp();

    if (Objects.nonNull(pollResp.body)) {
      for (ByteBuffer byteBuffer : pollResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          resp.enrichedTabletsList.add(EnrichedTablets.deserialize(byteBuffer));
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

  public static List<ByteBuffer> serializeEnrichedTabletsList(
      List<EnrichedTablets> enrichedTabletsList) throws IOException {
    List<ByteBuffer> byteBufferList = new ArrayList<>();
    for (EnrichedTablets enrichedTablets : enrichedTabletsList) {
      byteBufferList.add(serializeEnrichedTablets(enrichedTablets));
    }
    return byteBufferList;
  }

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
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(enrichedTabletsList, status, version, type, body);
  }
}
