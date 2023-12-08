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
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferSnapshotSealReq extends TPipeTransferReq {

  private transient String snapshotName;
  private transient long snapshotLength;

  private PipeTransferSnapshotSealReq() {
    // Empty constructor
  }

  public String getsnapshotName() {
    return snapshotName;
  }

  public long getsnapshotLength() {
    return snapshotLength;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferSnapshotSealReq toTPipeTransferReq(
      String snapshotName, long snapshotLength) throws IOException {
    final PipeTransferSnapshotSealReq snapshotSealReq = new PipeTransferSnapshotSealReq();

    snapshotSealReq.snapshotName = snapshotName;
    snapshotSealReq.snapshotLength = snapshotLength;

    snapshotSealReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    snapshotSealReq.type = PipeRequestType.TRANSFER_SNAPSHOT_SEAL.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(snapshotName, outputStream);
      ReadWriteIOUtils.write(snapshotLength, outputStream);
      snapshotSealReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return snapshotSealReq;
  }

  public static PipeTransferSnapshotSealReq fromTPipeTransferReq(TPipeTransferReq req) {
    final PipeTransferSnapshotSealReq snapshotSealReq = new PipeTransferSnapshotSealReq();

    snapshotSealReq.snapshotName = ReadWriteIOUtils.readString(req.body);
    snapshotSealReq.snapshotLength = ReadWriteIOUtils.readLong(req.body);

    snapshotSealReq.version = req.version;
    snapshotSealReq.type = req.type;
    snapshotSealReq.body = req.body;

    return snapshotSealReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferSnapshotSealBytes(String snapshotName, long snapshotLength)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_SNAPSHOT_SEAL.getType(), outputStream);
      ReadWriteIOUtils.write(snapshotName, outputStream);
      ReadWriteIOUtils.write(snapshotLength, outputStream);
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
    PipeTransferSnapshotSealReq that = (PipeTransferSnapshotSealReq) obj;
    return snapshotName.equals(that.snapshotName)
        && snapshotLength == that.snapshotLength
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotName, snapshotLength, version, type, body);
  }
}
