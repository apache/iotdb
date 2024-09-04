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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBAirGapConnector;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class IoTDBDataNodeAirGapConnector extends IoTDBAirGapConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataNodeAirGapConnector.class);

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeConfig pipeConfig = PipeConfig.getInstance();
    final Set<TEndPoint> givenNodeUrls = parseNodeUrls(validator.getParameters());

    validator.validate(
        empty -> {
          try {
            // Ensure the sink doesn't point to the air gap receiver on DataNode itself
            return !(pipeConfig.getPipeAirGapReceiverEnabled()
                && NodeUrlUtils.containsLocalAddress(
                    givenNodeUrls.stream()
                        .filter(
                            tEndPoint ->
                                tEndPoint.getPort() == pipeConfig.getPipeAirGapReceiverPort())
                        .map(TEndPoint::getIp)
                        .collect(Collectors.toList())));
          } catch (final UnknownHostException e) {
            LOGGER.warn("Unknown host when checking pipe sink IP.", e);
            return false;
          }
        },
        String.format(
            "One of the endpoints %s of the receivers is pointing back to the air gap receiver %s on sender itself, or unknown host when checking pipe sink IP.",
            givenNodeUrls,
            new TEndPoint(
                IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
                pipeConfig.getPipeAirGapReceiverPort())));
  }

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
