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

package org.apache.iotdb.confignode.manager.pipe.sink.payload;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class PipeTransferConfigPlanReq extends TPipeTransferReq {

  private PipeTransferConfigPlanReq() {
    // Empty constructor
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferConfigPlanReq toTPipeTransferReq(IConsensusRequest consensusRequest) {
    final PipeTransferConfigPlanReq req = new PipeTransferConfigPlanReq();

    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_CONFIG_PLAN.getType();
    req.body = consensusRequest.serializeToByteBuffer();

    return req;
  }

  public static PipeTransferConfigPlanReq fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferConfigPlanReq configPlanReq = new PipeTransferConfigPlanReq();

    configPlanReq.version = transferReq.version;
    configPlanReq.type = transferReq.type;
    configPlanReq.body = transferReq.body;

    return configPlanReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(IConsensusRequest consensusRequest) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_CONFIG_PLAN.getType(), outputStream);
      return BytesUtils.concatByteArray(
          byteArrayOutputStream.toByteArray(), consensusRequest.serializeToByteBuffer().array());
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  // do not overwrite equals() and hashCode() methods for there is no extra member variable
}
