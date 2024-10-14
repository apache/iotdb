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

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferTabletBinaryReqV2 extends PipeTransferTabletBinaryReq {
  private transient String dataBaseName;

  protected PipeTransferTabletBinaryReqV2() {
    // Do nothing
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public String getDataBaseName() {
    return dataBaseName;
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

    final InsertBaseStatement statement =
        (InsertBaseStatement)
            IoTDBDataNodeReceiver.PLAN_TO_STATEMENT_VISITOR.process(insertNode, null);
    if (Objects.isNull(dataBaseName) || dataBaseName.isEmpty()) {
      return statement;
    }
    statement.setWriteToTable(true);
    statement.setDatabaseName(dataBaseName);
    return statement;
  }

  private InsertNode parseByteBuffer() {
    final PlanNode node = WALEntry.deserializeForConsensus(byteBuffer);
    return node instanceof InsertNode ? (InsertNode) node : null;
  }

  /////////////////////////////// Batch ///////////////////////////////

  public static PipeTransferTabletBinaryReqV2 toTPipeTransferBinaryReq(
      final ByteBuffer byteBuffer, final String dataBaseName) {
    final PipeTransferTabletBinaryReqV2 req = new PipeTransferTabletBinaryReqV2();
    req.byteBuffer = byteBuffer;
    req.dataBaseName = dataBaseName;
    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_BINARY_V2.getType();

    return req;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletBinaryReqV2 toTPipeTransferReq(
      final ByteBuffer byteBuffer, final String dataBaseName) throws IOException {
    final PipeTransferTabletBinaryReqV2 req = new PipeTransferTabletBinaryReqV2();
    req.byteBuffer = byteBuffer;
    req.dataBaseName = dataBaseName;
    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_BINARY_V2.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(byteBuffer.limit(), outputStream);
      outputStream.write(byteBuffer.array(), 0, byteBuffer.limit());
      ReadWriteIOUtils.write(dataBaseName, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  public static PipeTransferTabletBinaryReqV2 fromTPipeTransferReq(
      final org.apache.iotdb.service.rpc.thrift.TPipeTransferReq transferReq) {
    final PipeTransferTabletBinaryReqV2 binaryReq = new PipeTransferTabletBinaryReqV2();

    final int length = ReadWriteIOUtils.readInt(transferReq.body);
    final byte[] body = new byte[length];
    transferReq.body.get(body);
    binaryReq.byteBuffer = ByteBuffer.wrap(body);
    binaryReq.dataBaseName = ReadWriteIOUtils.readString(transferReq.body);

    transferReq.body.position(0);
    binaryReq.version = transferReq.version;
    binaryReq.type = transferReq.type;
    binaryReq.body = transferReq.body;

    return binaryReq;
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
    final PipeTransferTabletBinaryReqV2 that = (PipeTransferTabletBinaryReqV2) obj;
    return byteBuffer.equals(that.byteBuffer)
        && Objects.equals(dataBaseName, that.dataBaseName)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(byteBuffer, dataBaseName, version, type, body);
  }
}
