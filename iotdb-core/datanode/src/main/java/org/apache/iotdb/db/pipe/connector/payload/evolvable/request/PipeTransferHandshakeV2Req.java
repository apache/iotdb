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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeTransferHandshakeV2Req extends TPipeTransferReq {

  private transient Map<String, String> params;

  private PipeTransferHandshakeV2Req() {
    // Empty constructor
  }

  public Map<String, String> getParams() {
    return params;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferHandshakeV2Req toTPipeTransferReq(Map<String, String> params)
      throws TException, IOException {
    final PipeTransferHandshakeV2Req handshakeReq = new PipeTransferHandshakeV2Req();

    handshakeReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    handshakeReq.type = PipeRequestType.HANDSHAKE_V2.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(params.size(), outputStream);
      for (final Map.Entry<String, String> entry : params.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      handshakeReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    handshakeReq.params = params;

    return handshakeReq;
  }

  public static PipeTransferHandshakeV2Req fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferHandshakeV2Req handshakeReq = new PipeTransferHandshakeV2Req();

    Map<String, String> params = new HashMap<>();
    final int size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(transferReq.body);
      final String value = ReadWriteIOUtils.readString(transferReq.body);
      params.put(key, value);
    }
    handshakeReq.params = params;

    handshakeReq.version = transferReq.version;
    handshakeReq.type = transferReq.type;
    handshakeReq.body = transferReq.body;

    return handshakeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTransferHandshakeBytes(HashMap<String, String> params) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.HANDSHAKE_V2.getType(), outputStream);
      ReadWriteIOUtils.write(params.size(), outputStream);
      for (final Map.Entry<String, String> entry : params.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      return byteArrayOutputStream.toByteArray();
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
    PipeTransferHandshakeV2Req that = (PipeTransferHandshakeV2Req) obj;
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
