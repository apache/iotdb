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

package org.apache.iotdb.commons.pipe.connector.payload.request;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class PipeTransferSnapshotPieceReq extends TPipeTransferReq {

  private transient String snapshotName;
  private transient long startWritingOffset;
  private transient byte[] snapshotPiece;

  private PipeTransferSnapshotPieceReq() {
    // Empty constructor
  }

  public String getsnapshotName() {
    return snapshotName;
  }

  public long getStartWritingOffset() {
    return startWritingOffset;
  }

  public byte[] getsnapshotPiece() {
    return snapshotPiece;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferSnapshotPieceReq toTPipeTransferReq(
      String snapshotName, long startWritingOffset, byte[] snapshotPiece) throws IOException {
    final PipeTransferSnapshotPieceReq snapshotPieceReq = new PipeTransferSnapshotPieceReq();

    snapshotPieceReq.snapshotName = snapshotName;
    snapshotPieceReq.startWritingOffset = startWritingOffset;
    snapshotPieceReq.snapshotPiece = snapshotPiece;

    snapshotPieceReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    snapshotPieceReq.type = PipeRequestType.TRANSFER_SNAPSHOT_PIECE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(snapshotName, outputStream);
      ReadWriteIOUtils.write(startWritingOffset, outputStream);
      ReadWriteIOUtils.write(new Binary(snapshotPiece), outputStream);
      snapshotPieceReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return snapshotPieceReq;
  }

  public static PipeTransferSnapshotPieceReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferSnapshotPieceReq snapshotPieceReq = new PipeTransferSnapshotPieceReq();

    snapshotPieceReq.snapshotName = ReadWriteIOUtils.readString(transferReq.body);
    snapshotPieceReq.startWritingOffset = ReadWriteIOUtils.readLong(transferReq.body);
    snapshotPieceReq.snapshotPiece = ReadWriteIOUtils.readBinary(transferReq.body).getValues();

    snapshotPieceReq.version = transferReq.version;
    snapshotPieceReq.type = transferReq.type;
    snapshotPieceReq.body = transferReq.body;

    return snapshotPieceReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      String snapshotName, long startWritingOffset, byte[] snapshotPiece) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_SNAPSHOT_PIECE.getType(), outputStream);
      ReadWriteIOUtils.write(snapshotName, outputStream);
      ReadWriteIOUtils.write(startWritingOffset, outputStream);
      ReadWriteIOUtils.write(new Binary(snapshotPiece), outputStream);
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
    PipeTransferSnapshotPieceReq that = (PipeTransferSnapshotPieceReq) obj;
    return snapshotName.equals(that.snapshotName)
        && startWritingOffset == that.startWritingOffset
        && Arrays.equals(snapshotPiece, that.snapshotPiece)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        snapshotName, startWritingOffset, Arrays.hashCode(snapshotPiece), version, type, body);
  }
}
