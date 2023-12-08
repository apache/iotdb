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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class PipeTransferSchemaPlanReq extends TPipeTransferReq {

  private transient PlanNode planNode;

  private PipeTransferSchemaPlanReq() {
    // Do nothing
  }

  public PlanNode getPlanNode() {
    return planNode;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferSchemaPlanReq toTPipeTransferReq(PlanNode planNode) {
    final PipeTransferSchemaPlanReq req = new PipeTransferSchemaPlanReq();

    req.planNode = planNode;

    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_SCHEMA_PLAN.getType();
    req.body = planNode.serializeToByteBuffer();

    return req;
  }

  public static PipeTransferSchemaPlanReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferSchemaPlanReq planNodeReq = new PipeTransferSchemaPlanReq();

    planNodeReq.planNode = PlanNodeType.deserialize(transferReq.body);

    planNodeReq.version = transferReq.version;
    planNodeReq.type = transferReq.type;
    planNodeReq.body = transferReq.body;

    return planNodeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////
  public static byte[] toTransferInsertNodeBytes(PlanNode planNode) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_SCHEMA_PLAN.getType(), outputStream);
      return BytesUtils.concatByteArray(
          byteArrayOutputStream.toByteArray(), planNode.serializeToByteBuffer().array());
    }
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
    PipeTransferSchemaPlanReq that = (PipeTransferSchemaPlanReq) obj;
    return planNode.equals(that.planNode)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(planNode, version, type, body);
  }
}
