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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeTransferTabletBatchReqV2 extends TPipeTransferReq {

  private final transient List<PipeTransferTabletBinaryReqV2> binaryReqs = new ArrayList<>();
  private final transient List<PipeTransferTabletInsertNodeReqV2> insertNodeReqs =
      new ArrayList<>();
  private final transient List<PipeTransferTabletRawReqV2> tabletReqs = new ArrayList<>();

  private PipeTransferTabletBatchReqV2() {
    // Empty constructor
  }

  public List<InsertBaseStatement> constructStatements() {
    final List<InsertBaseStatement> statements = new ArrayList<>();

    final InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    final InsertMultiTabletsStatement insertMultiTabletsStatement =
        new InsertMultiTabletsStatement();

    final List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    final List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();

    for (final PipeTransferTabletBinaryReqV2 binaryReq : binaryReqs) {
      final InsertBaseStatement statement = binaryReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      if (statement.isWriteToTable()) {
        statements.add(statement);
        continue;
      }
      if (statement instanceof InsertRowStatement) {
        insertRowStatementList.add((InsertRowStatement) statement);
      } else if (statement instanceof InsertTabletStatement) {
        insertTabletStatementList.add((InsertTabletStatement) statement);
      } else if (statement instanceof InsertRowsStatement) {
        insertRowStatementList.addAll(
            ((InsertRowsStatement) statement).getInsertRowStatementList());
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "unknown InsertBaseStatement %s constructed from PipeTransferTabletBinaryReqV2.",
                binaryReq));
      }
    }

    for (final PipeTransferTabletInsertNodeReqV2 insertNodeReq : insertNodeReqs) {
      final InsertBaseStatement statement = insertNodeReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      if (statement.isWriteToTable()) {
        statements.add(statement);
        continue;
      }
      if (statement instanceof InsertRowStatement) {
        insertRowStatementList.add((InsertRowStatement) statement);
      } else if (statement instanceof InsertTabletStatement) {
        insertTabletStatementList.add((InsertTabletStatement) statement);
      } else if (statement instanceof InsertRowsStatement) {
        insertRowStatementList.addAll(
            ((InsertRowsStatement) statement).getInsertRowStatementList());
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unknown InsertBaseStatement %s constructed from PipeTransferTabletInsertNodeReqV2.",
                statement));
      }
    }

    for (final PipeTransferTabletRawReqV2 tabletReq : tabletReqs) {
      final InsertTabletStatement statement = tabletReq.constructStatement();
      if (statement.isEmpty()) {
        continue;
      }
      if (statement.isWriteToTable()) {
        statements.add(statement);
        continue;
      }
      insertTabletStatementList.add(statement);
    }

    insertRowsStatement.setInsertRowStatementList(insertRowStatementList);
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    if (!insertRowsStatement.isEmpty()) {
      statements.add(insertRowsStatement);
    }
    if (!insertMultiTabletsStatement.isEmpty()) {
      statements.add(insertMultiTabletsStatement);
    }
    return statements;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletBatchReqV2 toTPipeTransferReq(
      final List<ByteBuffer> binaryBuffers,
      final List<ByteBuffer> insertNodeBuffers,
      final List<ByteBuffer> tabletBuffers,
      final List<String> binaryDataBases,
      final List<String> insertNodeDataBases,
      final List<String> tabletDataBases)
      throws IOException {
    final PipeTransferTabletBatchReqV2 batchReq = new PipeTransferTabletBatchReqV2();

    batchReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    batchReq.type = PipeRequestType.TRANSFER_TABLET_BATCH_V2.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(binaryBuffers.size(), outputStream);
      for (int i = 0; i < binaryBuffers.size(); i++) {
        final ByteBuffer binaryBuffer = binaryBuffers.get(i);
        ReadWriteIOUtils.write(binaryBuffer.limit(), outputStream);
        outputStream.write(binaryBuffer.array(), 0, binaryBuffer.limit());
        ReadWriteIOUtils.write(binaryDataBases.get(i), outputStream);
      }

      ReadWriteIOUtils.write(insertNodeBuffers.size(), outputStream);
      for (int i = 0; i < insertNodeBuffers.size(); i++) {
        final ByteBuffer insertNodeBuffer = insertNodeBuffers.get(i);
        outputStream.write(insertNodeBuffer.array(), 0, insertNodeBuffer.limit());
        ReadWriteIOUtils.write(insertNodeDataBases.get(i), outputStream);
      }

      ReadWriteIOUtils.write(tabletBuffers.size(), outputStream);
      for (int i = 0; i < tabletBuffers.size(); i++) {
        final ByteBuffer tabletBuffer = tabletBuffers.get(i);
        outputStream.write(tabletBuffer.array(), 0, tabletBuffer.limit());
        ReadWriteIOUtils.write(tabletDataBases.get(i), outputStream);
      }

      batchReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return batchReq;
  }

  public static PipeTransferTabletBatchReqV2 fromTPipeTransferReq(
      final org.apache.iotdb.service.rpc.thrift.TPipeTransferReq transferReq) {
    final PipeTransferTabletBatchReqV2 batchReq = new PipeTransferTabletBatchReqV2();

    int size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      final int length = ReadWriteIOUtils.readInt(transferReq.body);
      final byte[] body = new byte[length];
      transferReq.body.get(body);
      batchReq.binaryReqs.add(
          PipeTransferTabletBinaryReqV2.toTPipeTransferBinaryReq(
              ByteBuffer.wrap(body), ReadWriteIOUtils.readString(transferReq.body)));
    }

    size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.insertNodeReqs.add(
          PipeTransferTabletInsertNodeReqV2.toTabletInsertNodeReq(
              (InsertNode) PlanFragment.deserializeHelper(transferReq.body, null),
              ReadWriteIOUtils.readString(transferReq.body)));
    }

    size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.tabletReqs.add(
          PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
              Tablet.deserialize(transferReq.body),
              ReadWriteIOUtils.readBool(transferReq.body),
              ReadWriteIOUtils.readString(transferReq.body)));
    }

    batchReq.version = transferReq.version;
    batchReq.type = transferReq.type;
    batchReq.body = transferReq.body;

    return batchReq;
  }

  /////////////////////////////// TestOnly ///////////////////////////////

  @TestOnly
  public List<PipeTransferTabletBinaryReqV2> getBinaryReqs() {
    return binaryReqs;
  }

  @TestOnly
  public List<PipeTransferTabletInsertNodeReqV2> getInsertNodeReqs() {
    return insertNodeReqs;
  }

  @TestOnly
  public List<PipeTransferTabletRawReqV2> getTabletReqs() {
    return tabletReqs;
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
    final PipeTransferTabletBatchReqV2 that = (PipeTransferTabletBatchReqV2) obj;
    return Objects.equals(binaryReqs, that.binaryReqs)
        && Objects.equals(insertNodeReqs, that.insertNodeReqs)
        && Objects.equals(tabletReqs, that.tabletReqs)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(binaryReqs, insertNodeReqs, tabletReqs, version, type, body);
  }
}
