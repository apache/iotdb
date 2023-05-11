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

package org.apache.iotdb.db.pipe.receive.request;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeTransferInsertNodeReq extends TPipeTransferReq {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferInsertNodeReq.class);
  private final InsertNode insertNode;

  public PipeTransferInsertNodeReq(String pipeVersion, InsertNode insertNode) {
    this.pipeVersion = pipeVersion;
    this.insertNode = insertNode;
  }

  public Statement constructStatement() {
    if (insertNode instanceof InsertRowNode) {
      InsertRowNode node = (InsertRowNode) insertNode;
      InsertRowStatement statement = new InsertRowStatement();

      statement.setDevicePath(node.getDevicePath());
      statement.setTime(node.getTime());
      statement.setMeasurements(node.getMeasurements());
      statement.setDataTypes(node.getDataTypes());
      statement.setValues(node.getValues());
      statement.setNeedInferType(true);
      statement.setAligned(node.isAligned());
      return statement;
    } else if (insertNode instanceof InsertTabletNode) {
      InsertTabletNode node = (InsertTabletNode) insertNode;
      InsertTabletStatement statement = new InsertTabletStatement();

      statement.setDevicePath(node.getDevicePath());
      statement.setMeasurements(node.getMeasurements());
      statement.setTimes(node.getTimes());
      statement.setColumns(node.getColumns());
      statement.setBitMaps(node.getBitMaps());
      statement.setRowCount(node.getRowCount());
      statement.setDataTypes(node.getDataTypes());
      statement.setAligned(node.isAligned());
      return statement;
    } else {
      LOGGER.warn(
          String.format(
              "Unknown Insert Node type %s when constructing statement from insert node.",
              insertNode));
      return null;
    }
  }

  @Override
  public short getType() {
    return PipeTransferReqType.INSERT_NODE.getNodeType();
  }

  public TPipeTransferReq toTPipeTransferReq() {
    this.type = getType();
    this.body = insertNode.serializeToByteBuffer();
    return this;
  }

  public static PipeTransferInsertNodeReq fromTPipeTransferReq(TPipeTransferReq req) {
    return new PipeTransferInsertNodeReq(
        req.pipeVersion, (InsertNode) PlanNodeType.deserialize(req.body));
  }
}
