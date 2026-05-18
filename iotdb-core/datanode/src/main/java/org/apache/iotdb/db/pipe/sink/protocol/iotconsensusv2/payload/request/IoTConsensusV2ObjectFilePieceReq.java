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
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestType;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestVersion;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;

import java.nio.ByteBuffer;
import java.util.Objects;

public class IoTConsensusV2ObjectFilePieceReq extends TIoTConsensusV2TransferReq {

  private transient ObjectNode objectNode;

  private IoTConsensusV2ObjectFilePieceReq() {
    // Do nothing
  }

  public ObjectNode getObjectNode() {
    return objectNode;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static IoTConsensusV2ObjectFilePieceReq toTIoTConsensusV2TransferReq(
      final ObjectNode objectNode,
      final TCommitId commitId,
      final TConsensusGroupId consensusGroupId,
      final int thisDataNodeId) {
    final IoTConsensusV2ObjectFilePieceReq req = new IoTConsensusV2ObjectFilePieceReq();

    req.objectNode = objectNode;
    req.commitId = commitId;
    req.consensusGroupId = consensusGroupId;
    req.dataNodeId = thisDataNodeId;
    req.version = IoTConsensusV2RequestVersion.VERSION_1.getVersion();
    req.type = IoTConsensusV2RequestType.TRANSFER_OBJECT_FILE_PIECE.getType();
    req.body = objectNode.serialize();

    return req;
  }

  public static IoTConsensusV2ObjectFilePieceReq fromTIoTConsensusV2TransferReq(
      final TIoTConsensusV2TransferReq transferReq) {
    final IoTConsensusV2ObjectFilePieceReq req = new IoTConsensusV2ObjectFilePieceReq();

    final ByteBuffer body = transferReq.body.duplicate();
    final PlanNode planNode = WALEntry.deserializeForConsensus(body);
    req.objectNode = (ObjectNode) planNode;

    req.version = transferReq.version;
    req.type = transferReq.type;
    req.body = transferReq.body;
    req.commitId = transferReq.commitId;
    req.dataNodeId = transferReq.dataNodeId;
    req.consensusGroupId = transferReq.consensusGroupId;

    return req;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final IoTConsensusV2ObjectFilePieceReq that = (IoTConsensusV2ObjectFilePieceReq) obj;
    return version == that.version
        && type == that.type
        && Objects.equals(body, that.body)
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(dataNodeId, that.dataNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, type, body, commitId, consensusGroupId, dataNodeId);
  }
}
