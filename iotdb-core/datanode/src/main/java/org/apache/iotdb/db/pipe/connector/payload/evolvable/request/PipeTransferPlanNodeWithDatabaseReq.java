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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeTransferPlanNodeWithDatabaseReq extends PipeTransferPlanNodeReq {
  private transient String databaseName;

  protected PipeTransferPlanNodeWithDatabaseReq() {
    super();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferPlanNodeWithDatabaseReq toTPipeTransferReq(
      final PlanNode planNode, final String databaseName) {
    final PipeTransferPlanNodeWithDatabaseReq req = new PipeTransferPlanNodeWithDatabaseReq();

    req.planNode = planNode;
    req.databaseName = databaseName;

    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_PLAN_NODE_WITH_DATABASE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      planNode.serialize(outputStream);
      ReadWriteIOUtils.write(req.databaseName, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    return req;
  }

  public static PipeTransferPlanNodeWithDatabaseReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferPlanNodeWithDatabaseReq planNodeReq =
        new PipeTransferPlanNodeWithDatabaseReq();

    planNodeReq.planNode = PlanNodeType.deserialize(transferReq.body);
    planNodeReq.databaseName = ReadWriteIOUtils.readString(transferReq.body);

    planNodeReq.version = transferReq.version;
    planNodeReq.type = transferReq.type;
    planNodeReq.body = transferReq.body;

    return planNodeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(final PlanNode planNode, final String database)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(
          PipeRequestType.TRANSFER_PLAN_NODE_WITH_DATABASE.getType(), outputStream);
      planNode.serialize(outputStream);
      ReadWriteIOUtils.write(database, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    return super.equals(obj)
        && Objects.equals(databaseName, ((PipeTransferPlanNodeWithDatabaseReq) obj).databaseName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databaseName);
  }
}
