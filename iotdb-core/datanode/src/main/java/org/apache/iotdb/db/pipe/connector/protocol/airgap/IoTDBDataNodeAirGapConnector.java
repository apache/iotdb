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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBAirGapConnector;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class IoTDBDataNodeAirGapConnector extends IoTDBAirGapConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataNodeAirGapConnector.class);

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeConfig pipeConfig = PipeConfig.getInstance();
    Set<TEndPoint> givenNodeUrls = parseNodeUrls(validator.getParameters());

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
          } catch (UnknownHostException e) {
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

    return PipeTransferDataNodeHandshakeV2Req.toTPipeTransferBytes(params);
  }

  protected void doTransfer(
      Socket socket, PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    if (!send(
        socket,
        PipeTransferPlanNodeReq.toTPipeTransferBytes(
            pipeSchemaRegionWritePlanEvent.getPlanNode()))) {
      throw new PipeException(
          String.format(
              "Transfer data node write plan %s error. Socket: %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), socket));
    }
  }

  protected void transferFilePieces(File file, Socket socket, boolean isMultiFile)
      throws PipeException, IOException {
    final int readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
      while (true) {
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] payload =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);
        if (!send(
            socket,
            isMultiFile
                ? PipeTransferTsFilePieceWithModReq.toTPipeTransferBytes(
                    file.getName(), position, payload)
                : PipeTransferTsFilePieceReq.toTPipeTransferBytes(
                    file.getName(), position, payload))) {
          throw new PipeException(
              String.format("Transfer file %s error. Socket %s.", file, socket));
        } else {
          position += readLength;
        }
      }
    }
  }
}
