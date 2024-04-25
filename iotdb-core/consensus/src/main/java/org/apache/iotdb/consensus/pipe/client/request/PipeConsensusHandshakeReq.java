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

package org.apache.iotdb.consensus.pipe.client.request;

import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeConsensusHandshakeReq extends TPipeConsensusTransferReq {
  // Use map to store params to ensure scalability
  private transient Map<String, String> params;

  public Map<String, String> getParams() {
    return params;
  }

  /////////////////////////////// Thrift ///////////////////////////////
  public final PipeConsensusHandshakeReq convertToTPipeConsensusTransferReq(
      Map<String, String> params) throws IOException {
    this.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    this.type = PipeConsensusRequestType.PIPE_CONSENSUS_HANDSHAKE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(params.size(), outputStream);
      for (final Map.Entry<String, String> entry : params.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    this.params = params;
    return this;
  }

  public final PipeConsensusHandshakeReq translateFromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    Map<String, String> params = new HashMap<>();
    final int size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(transferReq.body);
      final String value = ReadWriteIOUtils.readString(transferReq.body);
      params.put(key, value);
    }
    this.params = params;

    version = transferReq.version;
    type = transferReq.type;
    body = transferReq.body;

    return this;
  }

  public static PipeConsensusHandshakeReq toTPipeConsensusTransferReq(Map<String, String> params)
      throws IOException {
    return new PipeConsensusHandshakeReq().convertToTPipeConsensusTransferReq(params);
  }

  public static PipeConsensusHandshakeReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    return new PipeConsensusHandshakeReq().translateFromTPipeConsensusTransferReq(transferReq);
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
    PipeConsensusHandshakeReq that = (PipeConsensusHandshakeReq) obj;
    return Objects.equals(params, that.params)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(params, version, type, body);
  }
}
