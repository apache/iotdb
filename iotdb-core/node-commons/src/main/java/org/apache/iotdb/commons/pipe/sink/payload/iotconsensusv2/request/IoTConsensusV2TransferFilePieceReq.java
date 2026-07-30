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

package org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public abstract class IoTConsensusV2TransferFilePieceReq extends TIoTConsensusV2TransferReq {

  private transient String fileName;
  private transient long startWritingOffset;
  private transient byte[] filePiece;

  public final String getFileName() {
    return fileName;
  }

  public final long getStartWritingOffset() {
    return startWritingOffset;
  }

  public final byte[] getFilePiece() {
    return filePiece;
  }

  protected abstract IoTConsensusV2RequestType getPlanType();

  /////////////////////////////// Thrift ///////////////////////////////

  protected final IoTConsensusV2TransferFilePieceReq convertToTIoTConsensusV2TransferReq(
      String snapshotName,
      long startWritingOffset,
      byte[] snapshotPiece,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      int thisDataNodeId)
      throws IOException {

    this.fileName = snapshotName;
    this.startWritingOffset = startWritingOffset;
    this.filePiece = snapshotPiece;

    this.commitId = commitId;
    this.consensusGroupId = consensusGroupId;
    this.dataNodeId = thisDataNodeId;
    this.version = IoTConsensusV2RequestVersion.VERSION_1.getVersion();
    this.type = getPlanType().getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(snapshotName, outputStream);
      ReadWriteIOUtils.write(startWritingOffset, outputStream);
      ReadWriteIOUtils.write(new Binary(snapshotPiece), outputStream);
      body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return this;
  }

  protected final IoTConsensusV2TransferFilePieceReq translateFromTIoTConsensusV2TransferReq(
      TIoTConsensusV2TransferReq transferReq) {

    fileName = ReadWriteIOUtils.readString(transferReq.body);
    startWritingOffset = ReadWriteIOUtils.readLong(transferReq.body);
    filePiece = ReadWriteIOUtils.readBinary(transferReq.body).getValues();

    version = transferReq.version;
    type = transferReq.type;
    body = transferReq.body;
    commitId = transferReq.commitId;
    dataNodeId = transferReq.dataNodeId;
    consensusGroupId = transferReq.consensusGroupId;

    return this;
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
    IoTConsensusV2TransferFilePieceReq that = (IoTConsensusV2TransferFilePieceReq) obj;
    return fileName.equals(that.fileName)
        && startWritingOffset == that.startWritingOffset
        && Arrays.equals(filePiece, that.filePiece)
        && version == that.version
        && type == that.type
        && body.equals(that.body)
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(dataNodeId, that.dataNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fileName,
        startWritingOffset,
        Arrays.hashCode(filePiece),
        version,
        type,
        body,
        commitId,
        consensusGroupId,
        dataNodeId);
  }
}
