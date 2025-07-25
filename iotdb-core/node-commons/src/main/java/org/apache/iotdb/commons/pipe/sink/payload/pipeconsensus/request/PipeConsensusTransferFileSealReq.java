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

package org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class PipeConsensusTransferFileSealReq
    extends org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq {

  private transient String fileName;
  private transient long fileLength;
  private transient long pointCount;

  public final String getFileName() {
    return fileName;
  }

  public final long getFileLength() {
    return fileLength;
  }

  public final long getPointCount() {
    return pointCount;
  }

  protected abstract PipeConsensusRequestType getPlanType();

  /////////////////////////////// Thrift ///////////////////////////////

  protected PipeConsensusTransferFileSealReq convertToTPipeConsensusTransferReq(
      String fileName,
      long fileLength,
      long pointCount,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId)
      throws IOException {

    this.fileName = fileName;
    this.fileLength = fileLength;
    this.pointCount = pointCount;

    this.commitId = commitId;
    this.consensusGroupId = consensusGroupId;
    this.dataNodeId = thisDataNodeId;
    this.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    this.type = getPlanType().getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
      ReadWriteIOUtils.write(pointCount, outputStream);
      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      progressIndex.serialize(outputStream);
      this.progressIndex =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return this;
  }

  public PipeConsensusTransferFileSealReq translateFromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq req) {

    fileName = ReadWriteIOUtils.readString(req.body);
    fileLength = ReadWriteIOUtils.readLong(req.body);
    pointCount = ReadWriteIOUtils.readLong(req.body);

    version = req.version;
    type = req.type;
    body = req.body;
    commitId = req.commitId;
    dataNodeId = req.dataNodeId;
    consensusGroupId = req.consensusGroupId;
    progressIndex = req.progressIndex;

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
    PipeConsensusTransferFileSealReq that = (PipeConsensusTransferFileSealReq) obj;
    return fileName.equals(that.fileName)
        && fileLength == that.fileLength
        && pointCount == that.pointCount
        && version == that.version
        && type == that.type
        && body.equals(that.body)
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(dataNodeId, that.dataNodeId)
        && Objects.equals(progressIndex, that.progressIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fileName,
        fileLength,
        pointCount,
        version,
        type,
        body,
        commitId,
        consensusGroupId,
        dataNodeId,
        progressIndex);
  }
}
