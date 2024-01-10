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

import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeTransferTabletBatchReq extends TPipeTransferReq {

  private final transient List<PipeTransferTabletBinaryReq> binaryReqs = new ArrayList<>();
  private final transient List<PipeTransferTabletInsertNodeReq> insertNodeReqs = new ArrayList<>();
  private final transient List<PipeTransferTabletRawReq> tabletReqs = new ArrayList<>();

  private PipeTransferTabletBatchReq() {
    // Empty constructor
  }

  public Pair<InsertRowsStatement, InsertMultiTabletsStatement> constructStatements() {
    final InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    final InsertMultiTabletsStatement insertMultiTabletsStatement =
        new InsertMultiTabletsStatement();

    final List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    final List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();

    for (final PipeTransferTabletBinaryReq binaryReq : binaryReqs) {
      final InsertBaseStatement statement = binaryReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      if (statement instanceof InsertRowStatement) {
        insertRowStatementList.add((InsertRowStatement) statement);
      } else if (statement instanceof InsertTabletStatement) {
        insertTabletStatementList.add((InsertTabletStatement) statement);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "unknown InsertBaseStatement %s constructed from PipeTransferTabletBinaryReq.",
                binaryReq));
      }
    }

    for (final PipeTransferTabletInsertNodeReq insertNodeReq : insertNodeReqs) {
      final InsertBaseStatement statement = insertNodeReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      if (statement instanceof InsertRowStatement) {
        insertRowStatementList.add((InsertRowStatement) statement);
      } else if (statement instanceof InsertTabletStatement) {
        insertTabletStatementList.add((InsertTabletStatement) statement);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "unknown InsertBaseStatement %s constructed from PipeTransferTabletInsertNodeReq.",
                insertNodeReq));
      }
    }

    for (final PipeTransferTabletRawReq tabletReq : tabletReqs) {
      final InsertTabletStatement statement = tabletReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      insertTabletStatementList.add(statement);
    }

    insertRowsStatement.setInsertRowStatementList(insertRowStatementList);
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    return new Pair<>(insertRowsStatement, insertMultiTabletsStatement);
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletBatchReq toTPipeTransferReq(
      List<ByteBuffer> binaryBuffers,
      List<ByteBuffer> insertNodeBuffers,
      List<ByteBuffer> tabletBuffers)
      throws IOException {
    final PipeTransferTabletBatchReq batchReq = new PipeTransferTabletBatchReq();

    // batchReq.binaryReqs, batchReq.insertNodeReqs, batchReq.tabletReqs are empty
    // when this method is called from PipeTransferTabletBatchReqBuilder.toTPipeTransferReq()

    batchReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    batchReq.type = PipeRequestType.TRANSFER_TABLET_BATCH.getType();
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

  public static PipeTransferTabletBatchReq fromTPipeTransferReq(TPipeTransferReq transferReq)
      throws IOException {
    final PipeTransferTabletBatchReq batchReq = new PipeTransferTabletBatchReq();

    int size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      final int length = ReadWriteIOUtils.readInt(transferReq.body);
      final byte[] body = new byte[length];
      transferReq.body.get(body);
      batchReq.binaryReqs.add(
          PipeTransferTabletBinaryReq.toTPipeTransferReq(ByteBuffer.wrap(body)));
    }

    size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.insertNodeReqs.add(
          PipeTransferTabletInsertNodeReq.toTPipeTransferReq(
              (InsertNode) PlanFragment.deserializeHelper(transferReq.body, null)));
    }

    size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.tabletReqs.add(
          PipeTransferTabletRawReq.toTPipeTransferReq(
              Tablet.deserialize(transferReq.body), ReadWriteIOUtils.readBool(transferReq.body)));
    }

    batchReq.version = transferReq.version;
    batchReq.type = transferReq.type;
    batchReq.body = transferReq.body;

    return batchReq;
  }

  /////////////////////////////// TestOnly ///////////////////////////////

  @TestOnly
  public List<PipeTransferTabletBinaryReq> getBinaryReqs() {
    return binaryReqs;
  }

  @TestOnly
  public List<PipeTransferTabletInsertNodeReq> getInsertNodeReqs() {
    return insertNodeReqs;
  }

  @TestOnly
  public List<PipeTransferTabletRawReq> getTabletReqs() {
    return tabletReqs;
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
    PipeTransferTabletBatchReq that = (PipeTransferTabletBatchReq) obj;
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
