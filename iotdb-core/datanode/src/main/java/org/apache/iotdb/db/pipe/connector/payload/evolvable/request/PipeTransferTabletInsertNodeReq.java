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
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class PipeTransferTabletInsertNodeReq extends TPipeTransferReq {

  protected transient InsertNode insertNode;

  protected PipeTransferTabletInsertNodeReq() {
    // Do nothing
  }

  public InsertNode getInsertNode() {
    return insertNode;
  }

  public InsertBaseStatement constructStatement() {
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

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static PipeTransferTabletInsertNodeReq toTPipeTransferRawReq(final InsertNode insertNode) {
    final PipeTransferTabletInsertNodeReq req = new PipeTransferTabletInsertNodeReq();

    req.insertNode = insertNode;
    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_INSERT_NODE.getType();
    return req;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTabletInsertNodeReq toTPipeTransferReq(final InsertNode insertNode) {
    final PipeTransferTabletInsertNodeReq req = new PipeTransferTabletInsertNodeReq();

    req.insertNode = insertNode;

    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TABLET_INSERT_NODE.getType();
    req.body = insertNode.serializeToByteBuffer();

    return req;
  }

  public static PipeTransferTabletInsertNodeReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferTabletInsertNodeReq insertNodeReq = new PipeTransferTabletInsertNodeReq();

    insertNodeReq.insertNode = (InsertNode) PlanFragment.deserializeHelper(transferReq.body, null);

    insertNodeReq.version = transferReq.version;
    insertNodeReq.type = transferReq.type;
    insertNodeReq.body = transferReq.body;

    return insertNodeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(final InsertNode insertNode) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_INSERT_NODE.getType(), outputStream);
      return BytesUtils.concatByteArray(
          byteArrayOutputStream.toByteArray(), insertNode.serializeToByteBuffer().array());
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
    final PipeTransferTabletInsertNodeReq that = (PipeTransferTabletInsertNodeReq) obj;
    return Objects.equals(insertNode, that.insertNode)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(insertNode, version, type, body);
  }
}
