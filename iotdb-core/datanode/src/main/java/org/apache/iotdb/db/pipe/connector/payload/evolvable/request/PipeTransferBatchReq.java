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
import org.apache.iotdb.db.pipe.connector.protocol.thrift.IoTDBThriftConnectorRequestVersion;
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

public class PipeTransferBatchReq extends TPipeTransferReq {

  private final transient List<PipeTransferInsertNodeReq> insertNodeReqs = new ArrayList<>();
  private final transient List<PipeTransferTabletReq> tabletReqs = new ArrayList<>();

  private PipeTransferBatchReq() {
    // Empty constructor
  }

  public static PipeTransferBatchReq toTPipeTransferReq(List<TPipeTransferReq> reqs)
      throws IOException {
    final PipeTransferBatchReq batchReq = new PipeTransferBatchReq();

    for (TPipeTransferReq tReq : reqs) {
      if (tReq instanceof PipeTransferInsertNodeReq) {
        batchReq.insertNodeReqs.add((PipeTransferInsertNodeReq) tReq);
      } else if (tReq instanceof PipeTransferTabletReq) {
        batchReq.tabletReqs.add((PipeTransferTabletReq) tReq);
      }
    }

    batchReq.version = IoTDBThriftConnectorRequestVersion.VERSION_1.getVersion();
    batchReq.type = PipeRequestType.TRANSFER_BATCH.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(batchReq.insertNodeReqs.size(), outputStream);
      for (PipeTransferInsertNodeReq insertNodeReq : batchReq.insertNodeReqs) {
        insertNodeReq.getInsertNode().serialize(outputStream);
      }

      ReadWriteIOUtils.write(batchReq.tabletReqs.size(), outputStream);
      for (PipeTransferTabletReq tabletReq : batchReq.tabletReqs) {
        tabletReq.getTablet().serialize(outputStream);
        ReadWriteIOUtils.write(tabletReq.getIsAligned(), outputStream);
      }

      batchReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return batchReq;
  }

  public static PipeTransferBatchReq fromTPipeTransferReq(TPipeTransferReq transferReq)
      throws IOException {
    final PipeTransferBatchReq batchReq = new PipeTransferBatchReq();

    int size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.insertNodeReqs.add(
          PipeTransferInsertNodeReq.toTPipeTransferReq(
              (InsertNode) PlanFragment.deserializeHelper(transferReq.body)));
    }

    size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      batchReq.tabletReqs.add(
          PipeTransferTabletReq.toTPipeTransferReq(
              Tablet.deserialize(transferReq.body), ReadWriteIOUtils.readBool(transferReq.body)));
    }

    batchReq.version = transferReq.version;
    batchReq.type = transferReq.type;
    batchReq.body = transferReq.body;

    return batchReq;
  }

  public Pair<InsertRowsStatement, InsertMultiTabletsStatement> constructStatementPair() {
    InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    InsertMultiTabletsStatement insertMultiTabletsStatement = new InsertMultiTabletsStatement();

    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    List<InsertTabletStatement> insertTabletList = new ArrayList<>();

    for (PipeTransferInsertNodeReq insertNodeReq : insertNodeReqs) {
      Statement insertStatement = insertNodeReq.constructStatement();
      if (insertStatement instanceof InsertRowStatement) {
        insertRowStatementList.add((InsertRowStatement) insertStatement);
      } else if (insertStatement instanceof InsertTabletStatement) {
        insertTabletList.add((InsertTabletStatement) insertStatement);
      }
    }

    for (PipeTransferTabletReq tabletReq : tabletReqs) {
      insertTabletList.add(tabletReq.constructStatement());
    }

    insertRowsStatement.setInsertRowStatementList(insertRowStatementList);
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletList);
    return new Pair<>(insertRowsStatement, insertMultiTabletsStatement);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeTransferBatchReq that = (PipeTransferBatchReq) obj;
    return insertNodeReqs.equals(that.insertNodeReqs)
        && tabletReqs.equals(that.tabletReqs)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(insertNodeReqs, tabletReqs, version, type, body);
  }
}
