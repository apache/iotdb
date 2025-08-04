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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class PipeConsensusTransferFileSealWithModReq extends TPipeConsensusTransferReq {

  private transient List<String> fileNames;
  private transient List<Long> fileLengths;
  private transient List<Long> pointCounts;
  private transient Map<String, String> parameters;

  public final List<String> getFileNames() {
    return fileNames;
  }

  public final List<Long> getFileLengths() {
    return fileLengths;
  }

  public final List<Long> getPointCounts() {
    return pointCounts;
  }

  public final Map<String, String> getParameters() {
    return parameters;
  }

  protected abstract PipeConsensusRequestType getPlanType();

  /////////////////////////////// Thrift ///////////////////////////////

  protected PipeConsensusTransferFileSealWithModReq convertToTPipeConsensusTransferReq(
      List<String> fileNames,
      List<Long> fileLengths,
      List<Long> pointCounts,
      Map<String, String> parameters,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId)
      throws IOException {

    this.fileNames = fileNames;
    this.fileLengths = fileLengths;
    this.pointCounts = pointCounts;
    this.parameters = parameters;

    this.commitId = commitId;
    this.consensusGroupId = consensusGroupId;
    this.dataNodeId = thisDataNodeId;
    this.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    this.type = getPlanType().getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileNames.size(), outputStream);
      for (String fileName : fileNames) {
        ReadWriteIOUtils.write(fileName, outputStream);
      }
      ReadWriteIOUtils.write(fileLengths.size(), outputStream);
      for (Long fileLength : fileLengths) {
        ReadWriteIOUtils.write(fileLength, outputStream);
      }
      ReadWriteIOUtils.write(pointCounts.size(), outputStream);
      for (Long pointCount : pointCounts) {
        ReadWriteIOUtils.write(pointCount, outputStream);
      }
      ReadWriteIOUtils.write(parameters.size(), outputStream);
      for (final Map.Entry<String, String> entry : parameters.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
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

  public PipeConsensusTransferFileSealWithModReq translateFromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq req) {
    fileNames = new ArrayList<>();
    int size = ReadWriteIOUtils.readInt(req.body);
    for (int i = 0; i < size; ++i) {
      fileNames.add(ReadWriteIOUtils.readString(req.body));
    }

    fileLengths = new ArrayList<>();
    size = ReadWriteIOUtils.readInt(req.body);
    for (int i = 0; i < size; ++i) {
      fileLengths.add(ReadWriteIOUtils.readLong(req.body));
    }

    pointCounts = new ArrayList<>();
    size = ReadWriteIOUtils.readInt(req.body);
    for (int i = 0; i < size; ++i) {
      pointCounts.add(ReadWriteIOUtils.readLong(req.body));
    }

    parameters = new HashMap<>();
    size = ReadWriteIOUtils.readInt(req.body);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(req.body);
      final String value = ReadWriteIOUtils.readString(req.body);
      parameters.put(key, value);
    }

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
    PipeConsensusTransferFileSealWithModReq that = (PipeConsensusTransferFileSealWithModReq) obj;
    return Objects.equals(fileNames, that.fileNames)
        && Objects.equals(fileLengths, that.fileLengths)
        && Objects.equals(pointCounts, that.pointCounts)
        && Objects.equals(parameters, that.parameters)
        && Objects.equals(version, that.version)
        && Objects.equals(type, that.type)
        && Objects.equals(body, that.body)
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(progressIndex, that.progressIndex)
        && Objects.equals(dataNodeId, that.dataNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fileNames,
        fileLengths,
        pointCounts,
        parameters,
        version,
        type,
        body,
        commitId,
        consensusGroupId,
        dataNodeId,
        progressIndex);
  }
}
