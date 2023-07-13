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

package org.apache.iotdb.db.pipe.connector.v1.request;

import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorRequestVersion;
import org.apache.iotdb.db.pipe.connector.v1.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferHandshakeReq extends TPipeTransferReq {

  private String timestampPrecision;

  private PipeTransferHandshakeReq() {
    // Empty constructor
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public static PipeTransferHandshakeReq toTPipeTransferReq(String timestampPrecision)
      throws IOException {
    final PipeTransferHandshakeReq handshakeReq = new PipeTransferHandshakeReq();

    handshakeReq.timestampPrecision = timestampPrecision;

    handshakeReq.version = IoTDBThriftConnectorRequestVersion.VERSION_1.getVersion();
    handshakeReq.type = PipeRequestType.HANDSHAKE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      handshakeReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return handshakeReq;
  }

  public static PipeTransferHandshakeReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferHandshakeReq handshakeReq = new PipeTransferHandshakeReq();

    handshakeReq.timestampPrecision = ReadWriteIOUtils.readString(transferReq.body);

    handshakeReq.version = transferReq.version;
    handshakeReq.type = transferReq.type;
    handshakeReq.body = transferReq.body;

    return handshakeReq;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeTransferHandshakeReq that = (PipeTransferHandshakeReq) obj;
    return timestampPrecision.equals(that.timestampPrecision)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestampPrecision, version, type, body);
  }
}
