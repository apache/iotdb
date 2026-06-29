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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils.TabletStringInternPool;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

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
      } else if (statement instanceof InsertRowsStatement) {
        insertRowStatementList.addAll(
            ((InsertRowsStatement) statement).getInsertRowStatementList());
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unknown InsertBaseStatement %s constructed from PipeTransferTabletBinaryReq.",
                statement));
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
      } else if (statement instanceof InsertRowsStatement) {
        insertRowStatementList.addAll(
            ((InsertRowsStatement) statement).getInsertRowStatementList());
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unknown InsertBaseStatement %s constructed from PipeTransferTabletInsertNodeReq.",
                statement));
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
      final List<ByteBuffer> insertNodeBuffers, final List<ByteBuffer> tabletBuffers)
      throws IOException {
    final PipeTransferTabletBatchReq batchReq = new PipeTransferTabletBatchReq();

    // batchReq.insertNodeReqs, batchReq.tabletReqs are empty
    // when this method is called from PipeTransferTabletBatchReqBuilder.toTPipeTransferReq()

    batchReq.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    batchReq.type = PipeRequestType.TRANSFER_TABLET_BATCH.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      // Binary buffer, for rolling upgrade
      ReadWriteIOUtils.write(0, outputStream);

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

  public static PipeTransferTabletBatchReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferTabletBatchReq batchReq = new PipeTransferTabletBatchReq();
    final TabletStringInternPool tabletStringInternPool = new TabletStringInternPool();

    // Legacy 1.3.x batch bodies may carry WAL binary requests before insert nodes and tablets.
    int size = readNonNegativeSize(transferReq.body, "binary request count");
    for (int i = 0; i < size; ++i) {
      final int length = readNonNegativeSize(transferReq.body, "binary request body length");
      if (length > transferReq.body.remaining()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid binary request body length %s, remaining body length %s.",
                length, transferReq.body.remaining()));
      }
      final byte[] body = new byte[length];
      transferReq.body.get(body);
      batchReq.binaryReqs.add(
          PipeTransferTabletBinaryReq.toTPipeTransferReq(ByteBuffer.wrap(body)));
    }

    size = readNonNegativeSize(transferReq.body, "insert node count");
    for (int i = 0; i < size; ++i) {
      final int startPosition = transferReq.body.position();
      try {
        batchReq.insertNodeReqs.add(
            PipeTransferTabletInsertNodeReq.toTPipeTransferRawReq(
                (InsertNode) PlanFragment.deserializeHelper(transferReq.body, null)));
      } catch (final RuntimeException e) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to deserialize insert node %s/%s in tablet batch at body position %s with remaining body length %s.",
                i + 1, size, startPosition, transferReq.body.remaining()),
            e);
      }
    }

    size = readNonNegativeSize(transferReq.body, "raw tablet count");
    for (int i = 0; i < size; ++i) {
      final int startPosition = transferReq.body.position();
      try {
        batchReq.tabletReqs.add(
            PipeTransferTabletRawReq.toTPipeTransferRawReq(
                transferReq.body, tabletStringInternPool));
      } catch (final RuntimeException e) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to deserialize raw tablet %s/%s in tablet batch at body position %s with remaining body length %s.",
                i + 1, size, startPosition, transferReq.body.remaining()),
            e);
      }
    }

    batchReq.version = transferReq.version;
    batchReq.type = transferReq.type;

    return batchReq;
  }

  private static int readNonNegativeSize(final ByteBuffer buffer, final String fieldName) {
    if (buffer.remaining() < Integer.BYTES) {
      throw new IllegalArgumentException(
          String.format(
              "Insufficient bytes to read %s in tablet batch, remaining body length %s.",
              fieldName, buffer.remaining()));
    }

    final int size = ReadWriteIOUtils.readInt(buffer);
    if (size < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid negative %s %s in tablet batch.", fieldName, size));
    }
    return size;
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
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTransferTabletBatchReq that = (PipeTransferTabletBatchReq) obj;
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
