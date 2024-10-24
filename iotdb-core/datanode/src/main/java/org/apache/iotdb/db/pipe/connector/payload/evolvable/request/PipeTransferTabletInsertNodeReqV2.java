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
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class PipeTransferTabletInsertNodeReqV2 extends PipeTransferTabletInsertNodeReq {

  protected transient String dataBaseName;

  private PipeTransferTabletInsertNodeReqV2() {
    // Do nothing
  }

  public String getDataBaseName() {
    return dataBaseName;
  }

  @Override
  public InsertBaseStatement constructStatement() {
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

    // Tree model
    if (Objects.isNull(dataBaseName)) {
      return statement;
    }

    // Table model
    statement.setWriteToTable(true);
    if (statement instanceof InsertRowsStatement) {
      List<InsertRowStatement> rowStatements =
          ((InsertRowsStatement) statement).getInsertRowStatementList();
      if (rowStatements != null && !rowStatements.isEmpty()) {
        for (InsertRowStatement insertRowStatement : rowStatements) {
          insertRowStatement.setWriteToTable(true);
          insertRowStatement.setDatabaseName(dataBaseName);
        }
      }
    }
    statement.setDatabaseName(dataBaseName);
    return statement;
  }

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static PipeTransferTabletInsertNodeReqV2 toTabletInsertNodeReq(
      final InsertNode insertNode, final String dataBaseName) {
    final PipeTransferTabletInsertNodeReqV2 req = new PipeTransferTabletInsertNodeReqV2();

    req.insertNode = insertNode;
    req.dataBaseName = dataBaseName;
    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2.getType();

    return req;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletInsertNodeReqV2 toTPipeTransferReq(
      final InsertNode insertNode, final String dataBaseName) {
    final PipeTransferTabletInsertNodeReqV2 req = new PipeTransferTabletInsertNodeReqV2();

    req.insertNode = insertNode;
    req.dataBaseName = dataBaseName;

    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      insertNode.serialize(outputStream);
      ReadWriteIOUtils.write(req.dataBaseName, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return req;
  }

  public static PipeTransferTabletInsertNodeReqV2 fromTPipeTransferReq(
      final org.apache.iotdb.service.rpc.thrift.TPipeTransferReq transferReq) {
    final PipeTransferTabletInsertNodeReqV2 insertNodeReq = new PipeTransferTabletInsertNodeReqV2();

    insertNodeReq.insertNode = (InsertNode) PlanFragment.deserializeHelper(transferReq.body, null);
    insertNodeReq.dataBaseName = ReadWriteIOUtils.readString(transferReq.body);

    insertNodeReq.version = transferReq.version;
    insertNodeReq.type = transferReq.type;
    insertNodeReq.body = transferReq.body;

    return insertNodeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(final InsertNode insertNode, final String dataBaseName)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(
          PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2.getType(), outputStream);
      insertNode.serialize(outputStream);
      ReadWriteIOUtils.write(dataBaseName, outputStream);
      return byteArrayOutputStream.toByteArray();
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
    if (!super.equals(obj)) {
      return false;
    }
    final PipeTransferTabletInsertNodeReqV2 that = (PipeTransferTabletInsertNodeReqV2) obj;
    return Objects.equals(dataBaseName, that.dataBaseName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), dataBaseName);
  }
}
