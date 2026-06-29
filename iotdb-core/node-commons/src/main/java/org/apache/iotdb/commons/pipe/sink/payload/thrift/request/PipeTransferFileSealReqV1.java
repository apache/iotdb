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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.request;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class PipeTransferFileSealReqV1 extends TPipeTransferReq {

  private transient String fileName;
  private transient long fileLength;

  public final String getFileName() {
    return fileName;
  }

  public final long getFileLength() {
    return fileLength;
  }

  protected abstract PipeRequestType getPlanType();

  /////////////////////////////// Thrift ///////////////////////////////

  protected PipeTransferFileSealReqV1 convertToTPipeTransferReq(String fileName, long fileLength)
      throws IOException {

    this.fileName = fileName;
    this.fileLength = fileLength;

    this.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    this.type = getPlanType().getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return this;
  }

  public PipeTransferFileSealReqV1 translateFromTPipeTransferReq(TPipeTransferReq req) {

    fileName = ReadWriteIOUtils.readString(req.body);
    fileLength = ReadWriteIOUtils.readLong(req.body);

    version = req.version;
    type = req.type;

    return this;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public byte[] convertToTPipeTransferSnapshotSealBytes(String fileName, long fileLength)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(getPlanType().getType(), outputStream);
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
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
    PipeTransferFileSealReqV1 that = (PipeTransferFileSealReqV1) obj;
    return fileName.equals(that.fileName)
        && fileLength == that.fileLength
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, fileLength, version, type, body);
  }
}
