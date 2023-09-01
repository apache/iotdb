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

import org.apache.iotdb.db.pipe.connector.payload.evolvable.PipeRequestType;
import org.apache.iotdb.db.pipe.connector.protocol.IoTDBConnectorRequestVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferFileSealWithParseReq extends TPipeTransferReq {

  private transient String fileName;
  private transient long fileLength;

  private transient long startTime;
  private transient long endTime;

  private transient String pattern;

  private PipeTransferFileSealWithParseReq() {
    // Empty constructor
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileLength() {
    return fileLength;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public String getPattern() {
    return pattern;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferFileSealWithParseReq toTPipeTransferReq(
      String fileName, long fileLength, long startTime, long endTime, String pattern)
      throws IOException {
    final PipeTransferFileSealWithParseReq fileSealReq = new PipeTransferFileSealWithParseReq();

    fileSealReq.fileName = fileName;
    fileSealReq.fileLength = fileLength;
    fileSealReq.startTime = startTime;
    fileSealReq.endTime = endTime;
    fileSealReq.pattern = pattern;

    fileSealReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    fileSealReq.type = PipeRequestType.TRANSFER_FILE_SEAL_WITH_PATTERN.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
      ReadWriteIOUtils.write(startTime, outputStream);
      ReadWriteIOUtils.write(endTime, outputStream);
      ReadWriteIOUtils.write(pattern, outputStream);
      fileSealReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return fileSealReq;
  }

  public static PipeTransferFileSealWithParseReq fromTPipeTransferReq(TPipeTransferReq req) {
    final PipeTransferFileSealWithParseReq fileSealReq = new PipeTransferFileSealWithParseReq();

    fileSealReq.fileName = ReadWriteIOUtils.readString(req.body);
    fileSealReq.fileLength = ReadWriteIOUtils.readLong(req.body);
    fileSealReq.startTime = ReadWriteIOUtils.readLong(req.body);
    fileSealReq.endTime = ReadWriteIOUtils.readLong(req.body);
    fileSealReq.pattern = ReadWriteIOUtils.readString(req.body);

    fileSealReq.version = req.version;
    fileSealReq.type = req.type;
    fileSealReq.body = req.body;

    return fileSealReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferFileSealWithParseBytes(
      String fileName, long fileLength, long startTime, long endTime, String pattern)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(
          PipeRequestType.TRANSFER_FILE_SEAL_WITH_PATTERN.getType(), outputStream);
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
      ReadWriteIOUtils.write(startTime, outputStream);
      ReadWriteIOUtils.write(endTime, outputStream);
      ReadWriteIOUtils.write(pattern, outputStream);
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
    PipeTransferFileSealWithParseReq that = (PipeTransferFileSealWithParseReq) obj;
    return fileName.equals(that.fileName)
        && fileLength == that.fileLength
        && startTime == that.startTime
        && endTime == that.endTime
        && pattern.equals(that.pattern)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, fileLength, startTime, endTime, pattern, version, type, body);
  }
}
