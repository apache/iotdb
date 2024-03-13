/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.connector.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBSslSyncConnector;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.client.IoTDBDataNodeSyncClientManager;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class IoTDBDataNodeSyncConnector extends IoTDBSslSyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataNodeSyncConnector.class);

  protected IoTDBDataNodeSyncClientManager clientManager;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final IoTDBConfig iotdbConfig = IoTDBDescriptor.getInstance().getConfig();
    final Set<TEndPoint> givenNodeUrls = parseNodeUrls(validator.getParameters());

    validator.validate(
        empty -> {
          try {
            // Ensure the sink doesn't point to the thrift receiver on DataNode itself
            return !NodeUrlUtils.containsLocalAddress(
                givenNodeUrls.stream()
                    .filter(tEndPoint -> tEndPoint.getPort() == iotdbConfig.getRpcPort())
                    .map(TEndPoint::getIp)
                    .collect(Collectors.toList()));
          } catch (UnknownHostException e) {
            LOGGER.warn("Unknown host when checking pipe sink IP.", e);
            return false;
          }
        },
        String.format(
            "One of the endpoints %s of the receivers is pointing back to the thrift receiver %s on sender itself, "
                + "or unknown host when checking pipe sink IP.",
            givenNodeUrls, new TEndPoint(iotdbConfig.getRpcAddress(), iotdbConfig.getRpcPort())));
  }

  @Override
  protected IoTDBSyncClientManager constructClient(
      List<TEndPoint> nodeUrls,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      boolean useLeaderCache) {
    clientManager =
        new IoTDBDataNodeSyncClientManager(
            nodeUrls, useSSL, trustStorePath, trustStorePwd, useLeaderCache);
    return clientManager;
  }

  protected void doTransfer(PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    final TPipeTransferResp resp;
    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferPlanNodeReq.toTPipeTransferReq(
                      pipeSchemaRegionWritePlanEvent.getPlanNode()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer schema region write plan %s, because %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    receiverStatusHandler.handle(
        status,
        String.format(
            "Transfer data node write plan %s error, result status %s.",
            pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), status),
        pipeSchemaRegionWritePlanEvent.getPlanNode().toString());
  }

  protected void transferFilePieces(
      File file, Pair<IoTDBSyncClient, Boolean> clientAndStatus, boolean isMultiFile)
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

        final byte[] payLoad =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);
        final PipeTransferFilePieceResp resp;
        try {
          resp =
              PipeTransferFilePieceResp.fromTPipeTransferResp(
                  clientAndStatus
                      .getLeft()
                      .pipeTransfer(
                          isMultiFile
                              ? PipeTransferTsFilePieceWithModReq.toTPipeTransferReq(
                                  file.getName(), position, payLoad)
                              : PipeTransferTsFilePieceReq.toTPipeTransferReq(
                                  file.getName(), position, payLoad)));
        } catch (Exception e) {
          clientAndStatus.setRight(false);
          throw new PipeConnectionException(
              String.format(
                  "Network error when transfer file %s, because %s.", file, e.getMessage()),
              e);
        }

        position += readLength;

        // This case only happens when the connection is broken, and the connector is reconnected
        // to the receiver, then the receiver will redirect the file position to the last position
        if (resp.getStatus().getCode()
            == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
          position = resp.getEndWritingOffset();
          reader.seek(position);
          LOGGER.info("Redirect file position to {}.", position);
          continue;
        }

        receiverStatusHandler.handle(
            resp.getStatus(),
            String.format("Transfer file %s error, result status %s.", file, resp.getStatus()),
            file.getName());
      }
    }
  }
}
