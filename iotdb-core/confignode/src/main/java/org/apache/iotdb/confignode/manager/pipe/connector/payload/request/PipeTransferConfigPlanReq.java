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

package org.apache.iotdb.confignode.manager.pipe.connector.payload.request;

import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.request.TransferConfigPlanReq;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class PipeTransferConfigPlanReq extends TransferConfigPlanReq {

  private transient ConfigPhysicalPlan plan;

  private PipeTransferConfigPlanReq() {
    // Empty constructor
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferConfigPlanReq toTPipeTransferReq(ConfigPhysicalPlan plan) {
    final PipeTransferConfigPlanReq req = new PipeTransferConfigPlanReq();

    req.plan = plan;

    req.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_CONFIG_PLAN.getType();
    req.body = plan.serializeToByteBuffer();

    return req;
  }

  public static PipeTransferConfigPlanReq fromTPipeTransferReq(TPipeTransferReq transferReq)
      throws IOException {
    final PipeTransferConfigPlanReq configPlanReq = new PipeTransferConfigPlanReq();

    configPlanReq.plan = ConfigPhysicalPlan.Factory.create(transferReq.body);

    configPlanReq.version = transferReq.version;
    configPlanReq.type = transferReq.type;
    configPlanReq.body = transferReq.body;

    return configPlanReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTransferInsertNodeBytes(IConsensusRequest consensusRequest)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_CONFIG_PLAN.getType(), outputStream);
      return BytesUtils.concatByteArray(
          byteArrayOutputStream.toByteArray(), consensusRequest.serializeToByteBuffer().array());
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
    PipeTransferConfigPlanReq that = (PipeTransferConfigPlanReq) obj;
    return plan.equals(that.plan)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(plan, version, type, body);
  }
}
