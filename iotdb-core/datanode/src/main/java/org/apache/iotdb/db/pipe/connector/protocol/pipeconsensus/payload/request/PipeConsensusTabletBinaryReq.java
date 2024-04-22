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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request;

import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeConsensusTabletBinaryReq extends TPipeConsensusTransferReq {
  private transient ByteBuffer byteBuffer;

  private PipeConsensusTabletBinaryReq() {
    // Do nothing
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeConsensusTabletBinaryReq toTPipeConsensusTransferReq(ByteBuffer byteBuffer) {
    final PipeConsensusTabletBinaryReq req = new PipeConsensusTabletBinaryReq();
    req.byteBuffer = byteBuffer;

    req.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    req.type = PipeConsensusRequestType.TRANSFER_TABLET_BINARY.getType();
    req.body = byteBuffer;

    return req;
  }

  public static PipeConsensusTabletBinaryReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    final PipeConsensusTabletBinaryReq binaryReq = new PipeConsensusTabletBinaryReq();
    binaryReq.byteBuffer = transferReq.body;

    binaryReq.version = transferReq.version;
    binaryReq.type = transferReq.type;
    binaryReq.body = transferReq.body;

    return binaryReq;
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
    PipeConsensusTabletBinaryReq that = (PipeConsensusTabletBinaryReq) obj;
    return byteBuffer.equals(that.byteBuffer)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(byteBuffer, version, type, body);
  }
}
