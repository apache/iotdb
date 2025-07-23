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

package org.apache.iotdb.db.pipe.connector.protocol.airgap;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBAirGapConnector;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.IOException;
import java.util.HashMap;

public abstract class IoTDBDataNodeAirGapConnector extends IoTDBAirGapConnector {

  @Override
  protected boolean mayNeedHandshakeWhenFail() {
    return false;
  }

  @Override
  protected byte[] generateHandShakeV1Payload() throws IOException {
    return PipeTransferDataNodeHandshakeV1Req.toTPipeTransferBytes(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  @Override
  protected byte[] generateHandShakeV2Payload() throws IOException {
    final HashMap<String, String> params = new HashMap<>();
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID,
        IoTDBDescriptor.getInstance().getConfig().getClusterId());
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_CONVERT_ON_TYPE_MISMATCH,
        Boolean.toString(shouldReceiverConvertOnTypeMismatch));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, loadTsFileStrategy);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, username);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE,
        Boolean.toString(loadTsFileValidation));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_MARK_AS_PIPE_REQUEST,
        Boolean.toString(shouldMarkAsPipeRequest));

    return PipeTransferDataNodeHandshakeV2Req.toTPipeTransferBytes(params);
  }

  protected void doTransferWrapper(
      final AirGapSocket socket,
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionWritePlanEvent.increaseReferenceCount(
        IoTDBDataNodeAirGapConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeSchemaRegionWritePlanEvent);
    } finally {
      pipeSchemaRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBDataNodeAirGapConnector.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket,
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    if (!send(
        pipeSchemaRegionWritePlanEvent.getPipeName(),
        pipeSchemaRegionWritePlanEvent.getCreationTime(),
        socket,
        PipeTransferPlanNodeReq.toTPipeTransferBytes(
            pipeSchemaRegionWritePlanEvent.getPlanNode()))) {
      final String errorMessage =
          String.format(
              "Transfer data node write plan %s error. Socket: %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          pipeSchemaRegionWritePlanEvent.toString());
    }
  }
}
