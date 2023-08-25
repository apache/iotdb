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

import org.apache.iotdb.db.pipe.connector.payload.evolvable.PipeRequestType;
import org.apache.iotdb.db.pipe.connector.protocol.IoTDBConnectorRequestVersion;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
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
      final Statement statement = binaryReq.constructStatement();
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
      final Statement insertStatement = insertNodeReq.constructStatement();
      if (insertStatement instanceof InsertRowStatement) {
        insertRowStatementList.add((InsertRowStatement) insertStatement);
      } else if (insertStatement instanceof InsertTabletStatement) {
        insertTabletStatementList.add((InsertTabletStatement) insertStatement);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "unknown InsertBaseStatement %s constructed from PipeTransferTabletInsertNodeReq.",
                insertNodeReq));
      }
    }

    for (final PipeTransferTabletRawReq tabletReq : tabletReqs) {
      insertTabletStatementList.add(tabletReq.constructStatement());
    }

    insertRowsStatement.setInsertRowStatementList(insertRowStatementList);
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    return new Pair<>(insertRowsStatement, insertMultiTabletsStatement);
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletBatchReq toTPipeTransferReq(List<TPipeTransferReq> reqs)
      throws IOException {
    final PipeTransferTabletBatchReq batchReq = new PipeTransferTabletBatchReq();

    for (final TPipeTransferReq req : reqs) {
      if (req instanceof PipeTransferTabletBinaryReq) {
        batchReq.binaryReqs.add((PipeTransferTabletBinaryReq) req);
      } else if (req instanceof PipeTransferTabletInsertNodeReq) {
        batchReq.insertNodeReqs.add((PipeTransferTabletInsertNodeReq) req);
      } else if (req instanceof PipeTransferTabletRawReq) {
        batchReq.tabletReqs.add((PipeTransferTabletRawReq) req);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "unknown TPipeTransferReq type %s when constructing PipeTransferTabletBatchReq",
                req.getType()));
      }
    }

    batchReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    batchReq.type = PipeRequestType.TRANSFER_TABLET_BATCH.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(batchReq.binaryReqs.size(), outputStream);
      for (final PipeTransferTabletBinaryReq binaryReq : batchReq.binaryReqs) {
        ReadWriteIOUtils.write(binaryReq.getBody().length, outputStream);
        outputStream.write(binaryReq.getBody());
      }

      ReadWriteIOUtils.write(batchReq.insertNodeReqs.size(), outputStream);
      for (final PipeTransferTabletInsertNodeReq insertNodeReq : batchReq.insertNodeReqs) {
        insertNodeReq.getInsertNode().serialize(outputStream);
      }

      ReadWriteIOUtils.write(batchReq.tabletReqs.size(), outputStream);
      for (final PipeTransferTabletRawReq tabletReq : batchReq.tabletReqs) {
        tabletReq.getTablet().serialize(outputStream);
        ReadWriteIOUtils.write(tabletReq.getIsAligned(), outputStream);
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
              (InsertNode) PlanFragment.deserializeHelper(transferReq.body)));
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
