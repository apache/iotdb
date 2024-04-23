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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request;

import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeConsensusTabletBatchReq extends TPipeConsensusTransferReq {
  private final transient List<PipeConsensusTabletBinaryReq> binaryReqs = new ArrayList<>();
  private final transient List<PipeConsensusTabletInsertNodeReq> insertNodeReqs = new ArrayList<>();
  private final transient List<PipeConsensusTabletRawReq> tabletReqs = new ArrayList<>();
  private final transient List<TPipeConsensusTransferReq> transferReqs = new ArrayList<>();

  private PipeConsensusTabletBatchReq() {
    // do nothing
  }

  // TODO: do we need to construct statements for receiver? since receiver should apply request
  // through DataRegionStateMachine(planNode)

  /////////////////////////////// Thrift ///////////////////////////////
  public static PipeConsensusTabletBatchReq toTPipeConsensusTransferReq(
      List<ByteBuffer> binaryBuffers,
      List<ByteBuffer> insertNodeBuffers,
      List<ByteBuffer> tabletBuffers,
      List<TCommitId> commitIds)
      throws IOException {
    final PipeConsensusTabletBatchReq batchReq = new PipeConsensusTabletBatchReq();

    // batchReq.binaryReqs, batchReq.insertNodeReqs, batchReq.tabletReqs are empty
    // when this method is called from
    // PipeConsensusTabletBatchReqBuilder.toTPipeConsensusTransferReq()

    batchReq.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    batchReq.type = PipeConsensusRequestType.TRANSFER_TABLET_BATCH.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(binaryBuffers.size(), outputStream);
      for (final ByteBuffer binaryBuffer : binaryBuffers) {
        ReadWriteIOUtils.write(binaryBuffer.limit(), outputStream);
        outputStream.write(binaryBuffer.array(), 0, binaryBuffer.limit());
      }

      ReadWriteIOUtils.write(insertNodeBuffers.size(), outputStream);
      for (final ByteBuffer insertNodeBuffer : insertNodeBuffers) {
        outputStream.write(insertNodeBuffer.array(), 0, insertNodeBuffer.limit());
      }

      ReadWriteIOUtils.write(tabletBuffers.size(), outputStream);
      for (final ByteBuffer tabletBuffer : tabletBuffers) {
        outputStream.write(tabletBuffer.array(), 0, tabletBuffer.limit());
      }

      batchReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return batchReq;
  }

  public static PipeConsensusTabletBatchReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    final PipeConsensusTabletBatchReq batchReq = new PipeConsensusTabletBatchReq();

    int size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      final int length = ReadWriteIOUtils.readInt(transferReq.body);
      final byte[] body = new byte[length];
      transferReq.body.get(body);
      batchReq.binaryReqs.add(
          PipeConsensusTabletBinaryReq.toTPipeConsensusTransferReq(ByteBuffer.wrap(body)));
    }

    size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.insertNodeReqs.add(
          PipeConsensusTabletInsertNodeReq.toTPipeConsensusTransferRawReq(
              (InsertNode) PlanFragment.deserializeHelper(transferReq.body, null)));
    }

    size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.tabletReqs.add(
          PipeConsensusTabletRawReq.toTPipeConsensusTransferRawReq(
              Tablet.deserialize(transferReq.body), ReadWriteIOUtils.readBool(transferReq.body)));
    }

    batchReq.version = transferReq.version;
    batchReq.type = transferReq.type;
    batchReq.body = transferReq.body;

    return batchReq;
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
    PipeConsensusTabletBatchReq that = (PipeConsensusTabletBatchReq) obj;
    return binaryReqs.equals(that.binaryReqs)
        && insertNodeReqs.equals(that.insertNodeReqs)
        && tabletReqs.equals(that.tabletReqs)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(binaryReqs, insertNodeReqs, tabletReqs, version, type, body);
  }
}
