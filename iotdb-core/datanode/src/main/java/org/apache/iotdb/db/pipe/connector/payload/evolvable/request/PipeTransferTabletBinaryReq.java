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

import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiver;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferTabletBinaryReq extends TPipeTransferReq {

  protected transient ByteBuffer byteBuffer;

  protected PipeTransferTabletBinaryReq() {
    // Do nothing
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public InsertBaseStatement constructStatement() {
    final InsertNode insertNode = parseByteBuffer();

    if (!(insertNode instanceof InsertRowNode
        || insertNode instanceof InsertTabletNode
        || insertNode instanceof InsertRowsNode)) {
      throw new UnsupportedOperationException(
          String.format(
              "Unknown InsertNode type %s when constructing statement from insert node.",
              insertNode));
    }

    return (InsertBaseStatement)
        IoTDBDataNodeReceiver.PLAN_TO_STATEMENT_VISITOR.process(insertNode, null);
  }

  protected InsertNode parseByteBuffer() {
    final PlanNode node = WALEntry.deserializeForConsensus(byteBuffer);
    return node instanceof InsertNode ? (InsertNode) node : null;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletBinaryReq toTPipeTransferReq(final ByteBuffer byteBuffer) {
    final PipeTransferTabletBinaryReq req = new PipeTransferTabletBinaryReq();
    req.byteBuffer = byteBuffer;

    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_BINARY.getType();
    req.body = byteBuffer;

    return req;
  }

  public static PipeTransferTabletBinaryReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferTabletBinaryReq binaryReq = new PipeTransferTabletBinaryReq();
    binaryReq.byteBuffer = transferReq.body;

    binaryReq.version = transferReq.version;
    binaryReq.type = transferReq.type;
    binaryReq.body = transferReq.body;

    return binaryReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(final ByteBuffer byteBuffer) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_BINARY.getType(), outputStream);
      return BytesUtils.concatByteArray(byteArrayOutputStream.toByteArray(), byteBuffer.array());
    }
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
    final PipeTransferTabletBinaryReq that = (PipeTransferTabletBinaryReq) obj;
    return byteBuffer.equals(that.byteBuffer)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(byteBuffer, version, type, body);
  }
}
