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

package org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;

import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeConsensusTabletBinaryReq extends TPipeConsensusTransferReq {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusTabletBinaryReq.class);
  private transient ByteBuffer byteBuffer;

  private PipeConsensusTabletBinaryReq() {
    // Do nothing
  }

  public InsertNode convertToInsertNode() {
    final PlanNode node = WALEntry.deserializeForConsensus(byteBuffer);
    return node instanceof InsertNode ? (InsertNode) node : null;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeConsensusTabletBinaryReq toTPipeConsensusTransferReq(
      ByteBuffer byteBuffer,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId) {
    final PipeConsensusTabletBinaryReq req = new PipeConsensusTabletBinaryReq();
    req.byteBuffer = byteBuffer;

    req.commitId = commitId;
    req.consensusGroupId = consensusGroupId;
    req.dataNodeId = thisDataNodeId;
    req.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    req.type = PipeConsensusRequestType.TRANSFER_TABLET_BINARY.getType();
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

  public static PipeConsensusTabletBinaryReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    final PipeConsensusTabletBinaryReq binaryReq = new PipeConsensusTabletBinaryReq();
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
    PipeConsensusTabletBinaryReq that = (PipeConsensusTabletBinaryReq) obj;
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
