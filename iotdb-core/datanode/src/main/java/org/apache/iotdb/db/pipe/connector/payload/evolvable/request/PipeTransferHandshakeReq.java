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

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferHandshakeReq extends TPipeTransferReq {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferHandshakeReq.class);

  private transient String timestampPrecision;
  private transient String clusterId;

  private PipeTransferHandshakeReq() {
    // Empty constructor
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public String getClusterId() {
    return clusterId;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferHandshakeReq toTPipeTransferReq(String timestampPrecision)
      throws IOException, ClientManagerException, TException {
    final PipeTransferHandshakeReq handshakeReq = new PipeTransferHandshakeReq();

    handshakeReq.timestampPrecision = timestampPrecision;

    handshakeReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    handshakeReq.type = PipeRequestType.HANDSHAKE.getType();

    try (final ConfigNodeClient configNodeClient =
            ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID);
        final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      handshakeReq.clusterId = configNodeClient.getClusterId().clusterId;
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      ReadWriteIOUtils.write(handshakeReq.clusterId, outputStream);
      handshakeReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return handshakeReq;
  }

  public static PipeTransferHandshakeReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferHandshakeReq handshakeReq = new PipeTransferHandshakeReq();

    handshakeReq.timestampPrecision = ReadWriteIOUtils.readString(transferReq.body);

    // It is possible to catch BufferUnderflowException if the older version send handshake request
    // to newer version.
    try {
      handshakeReq.clusterId = ReadWriteIOUtils.readString(transferReq.body);
    } catch (BufferUnderflowException e) {
      handshakeReq.clusterId = null;
      LOGGER.warn("Unable to get clusterId from handshake request.");
    }

    handshakeReq.version = transferReq.version;
    handshakeReq.type = transferReq.type;
    handshakeReq.body = transferReq.body;

    return handshakeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTransferHandshakeBytes(String timestampPrecision) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.HANDSHAKE.getType(), outputStream);
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
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
