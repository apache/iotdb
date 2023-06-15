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

package org.apache.iotdb.db.pipe.connector.v1.request;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorRequestVersion;
import org.apache.iotdb.db.pipe.connector.v1.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

public class PipeTransferInsertNodeReq extends TPipeTransferReq {

  private InsertNode insertNode;

  private PipeTransferInsertNodeReq() {}

  public InsertNode getInsertNode() {
    return insertNode;
  }

  public Statement constructStatement() {
    if (insertNode instanceof InsertRowNode) {
      final InsertRowNode node = (InsertRowNode) insertNode;

      final InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(node.getDevicePath());
      statement.setTime(node.getTime());
      statement.setMeasurements(node.getMeasurements());
      statement.setDataTypes(node.getDataTypes());
      statement.setValues(node.getValues());
      statement.setNeedInferType(node.isNeedInferType());
      statement.setAligned(node.isAligned());
      statement.setMeasurementSchemas(node.getMeasurementSchemas());
      return statement;
    }

    if (insertNode instanceof InsertTabletNode) {
      final InsertTabletNode node = (InsertTabletNode) insertNode;

      final InsertTabletStatement statement = new InsertTabletStatement();
      statement.setDevicePath(node.getDevicePath());
      statement.setMeasurements(node.getMeasurements());
      statement.setTimes(node.getTimes());
      statement.setColumns(node.getColumns());
      statement.setBitMaps(node.getBitMaps());
      statement.setRowCount(node.getRowCount());
      statement.setDataTypes(node.getDataTypes());
      statement.setAligned(node.isAligned());
      statement.setMeasurementSchemas(node.getMeasurementSchemas());
      return statement;
    }

    throw new UnsupportedOperationException(
        String.format(
            "unknown InsertNode type %s when constructing statement from insert node.",
            insertNode));
  }

  public static PipeTransferInsertNodeReq toTPipeTransferReq(InsertNode insertNode) {
    final PipeTransferInsertNodeReq req = new PipeTransferInsertNodeReq();

    req.insertNode = insertNode;

    req.version = IoTDBThriftConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_INSERT_NODE.getType();
    req.body = insertNode.serializeToByteBuffer();

    return req;
  }

  public static PipeTransferInsertNodeReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferInsertNodeReq insertNodeReq = new PipeTransferInsertNodeReq();

    insertNodeReq.insertNode = (InsertNode) PlanNodeType.deserialize(transferReq.body);

    insertNodeReq.version = transferReq.version;
    insertNodeReq.type = transferReq.type;
    insertNodeReq.body = transferReq.body;

    return insertNodeReq;
  }
}
