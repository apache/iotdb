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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class PipeTransferPlanNodeReq extends TPipeTransferReq {

  protected transient PlanNode planNode;

  protected PipeTransferPlanNodeReq() {
    // Do nothing
  }

  public PlanNode getPlanNode() {
    return planNode;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferPlanNodeReq toTPipeTransferReq(final PlanNode planNode) {
    final PipeTransferPlanNodeReq req = new PipeTransferPlanNodeReq();

    req.planNode = planNode;

    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_PLAN_NODE.getType();
    req.body = planNode.serializeToByteBuffer();

    return req;
  }

  public static PipeTransferPlanNodeReq fromTPipeTransferReq(final TPipeTransferReq transferReq) {
    final PipeTransferPlanNodeReq planNodeReq = new PipeTransferPlanNodeReq();

    planNodeReq.planNode = PlanNodeType.deserialize(transferReq.body);

    planNodeReq.version = transferReq.version;
    planNodeReq.type = transferReq.type;

    return planNodeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(final PlanNode planNode) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_PLAN_NODE.getType(), outputStream);
      return BytesUtils.concatByteArray(
          byteArrayOutputStream.toByteArray(), planNode.serializeToByteBuffer().array());
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
    final PipeTransferPlanNodeReq that = (PipeTransferPlanNodeReq) obj;
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
