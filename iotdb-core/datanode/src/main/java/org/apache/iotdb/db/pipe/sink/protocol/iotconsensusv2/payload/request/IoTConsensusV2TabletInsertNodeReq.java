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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;

import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class IoTConsensusV2TabletInsertNodeReq extends TIoTConsensusV2TransferReq {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTConsensusV2TabletInsertNodeReq.class);
  private transient InsertNode insertNode;

  private IoTConsensusV2TabletInsertNodeReq() {
    // Do nothing
  }

  public InsertNode getInsertNode() {
    return insertNode;
  }

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static IoTConsensusV2TabletInsertNodeReq toTIoTConsensusV2TransferRawReq(
      InsertNode insertNode,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId) {
    final IoTConsensusV2TabletInsertNodeReq req = new IoTConsensusV2TabletInsertNodeReq();

    req.insertNode = insertNode;
    req.commitId = commitId;
    req.consensusGroupId = consensusGroupId;
    req.dataNodeId = thisDataNodeId;

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

  /////////////////////////////// Thrift ///////////////////////////////

  public static IoTConsensusV2TabletInsertNodeReq toTIoTConsensusV2TransferReq(
      InsertNode insertNode,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId) {
    final IoTConsensusV2TabletInsertNodeReq req = new IoTConsensusV2TabletInsertNodeReq();

    req.insertNode = insertNode;

    req.commitId = commitId;
    req.consensusGroupId = consensusGroupId;
    req.dataNodeId = thisDataNodeId;
    req.version = IoTConsensusV2RequestVersion.VERSION_1.getVersion();
    req.type = IoTConsensusV2RequestType.TRANSFER_TABLET_INSERT_NODE.getType();
    req.body = insertNode.serializeToByteBuffer();

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

  public static IoTConsensusV2TabletInsertNodeReq fromTIoTConsensusV2TransferReq(
      TIoTConsensusV2TransferReq transferReq) {
    final IoTConsensusV2TabletInsertNodeReq insertNodeReq = new IoTConsensusV2TabletInsertNodeReq();

    insertNodeReq.insertNode = (InsertNode) PlanNodeType.deserialize(transferReq.body);

    insertNodeReq.version = transferReq.version;
    insertNodeReq.type = transferReq.type;
    insertNodeReq.body = transferReq.body;
    insertNodeReq.commitId = transferReq.commitId;
    insertNodeReq.dataNodeId = transferReq.dataNodeId;
    insertNodeReq.consensusGroupId = transferReq.consensusGroupId;
    insertNodeReq.progressIndex = transferReq.progressIndex;

    return insertNodeReq;
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
    IoTConsensusV2TabletInsertNodeReq that = (IoTConsensusV2TabletInsertNodeReq) obj;
    return Objects.equals(insertNode, that.insertNode)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body)
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(progressIndex, that.progressIndex)
        && Objects.equals(dataNodeId, that.dataNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        insertNode, version, type, body, commitId, consensusGroupId, dataNodeId, progressIndex);
  }
}
