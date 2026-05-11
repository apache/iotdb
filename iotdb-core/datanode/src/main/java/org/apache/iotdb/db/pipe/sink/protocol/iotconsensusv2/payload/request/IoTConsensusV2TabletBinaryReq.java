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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestType;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestVersion;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;

import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class IoTConsensusV2TabletBinaryReq extends TIoTConsensusV2TransferReq {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTConsensusV2TabletBinaryReq.class);
  private transient ByteBuffer byteBuffer;

  private IoTConsensusV2TabletBinaryReq() {
    // Do nothing
  }

  public InsertNode convertToInsertNode() {
    final PlanNode node = WALEntry.deserializeForConsensus(byteBuffer);
    return node instanceof InsertNode ? (InsertNode) node : null;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static IoTConsensusV2TabletBinaryReq toTIoTConsensusV2TransferReq(
      ByteBuffer byteBuffer,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId) {
    final IoTConsensusV2TabletBinaryReq req = new IoTConsensusV2TabletBinaryReq();
    req.byteBuffer = byteBuffer;

    req.commitId = commitId;
    req.consensusGroupId = consensusGroupId;
    req.dataNodeId = thisDataNodeId;
    req.version = IoTConsensusV2RequestVersion.VERSION_1.getVersion();
    req.type = IoTConsensusV2RequestType.TRANSFER_TABLET_BINARY.getType();
    req.body = byteBuffer;

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      progressIndex.serialize(outputStream);
      req.progressIndex =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize progress index {}", progressIndex, e);
    }

    return req;
  }

  public static IoTConsensusV2TabletBinaryReq fromTIoTConsensusV2TransferReq(
      TIoTConsensusV2TransferReq transferReq) {
    final IoTConsensusV2TabletBinaryReq binaryReq = new IoTConsensusV2TabletBinaryReq();
    binaryReq.byteBuffer = transferReq.body;

    binaryReq.version = transferReq.version;
    binaryReq.type = transferReq.type;
    binaryReq.body = transferReq.body;
    binaryReq.commitId = transferReq.commitId;
    binaryReq.dataNodeId = transferReq.dataNodeId;
    binaryReq.consensusGroupId = transferReq.consensusGroupId;
    binaryReq.progressIndex = transferReq.progressIndex;

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
    IoTConsensusV2TabletBinaryReq that = (IoTConsensusV2TabletBinaryReq) obj;
    return byteBuffer.equals(that.byteBuffer)
        && version == that.version
        && type == that.type
        && body.equals(that.body)
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(progressIndex, that.progressIndex)
        && Objects.equals(dataNodeId, that.dataNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        byteBuffer, version, type, body, commitId, consensusGroupId, dataNodeId, progressIndex);
  }
}
