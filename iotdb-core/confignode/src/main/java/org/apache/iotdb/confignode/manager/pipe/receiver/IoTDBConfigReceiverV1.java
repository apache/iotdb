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

package org.apache.iotdb.confignode.manager.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeTransferFileSealReq;
import org.apache.iotdb.commons.pipe.receiver.IoTDBFileReceiverV1;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.request.PipeTransferConfigHandshakeReq;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.request.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.request.PipeTransferConfigSnapShotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.request.PipeTransferConfigSnapShotSealReq;
import org.apache.iotdb.db.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTDBConfigReceiverV1 extends IoTDBFileReceiverV1 {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigReceiverV1.class);

  private static final AtomicInteger queryIndex = new AtomicInteger(0);

  @Override
  public IoTDBConnectorRequestVersion getVersion() {
    return IoTDBConnectorRequestVersion.VERSION_1;
  }

  @Override
  public TPipeTransferResp receive(TPipeTransferReq req) {
    try {
      final short rawRequestType = req.getType();
      if (PipeRequestType.isValidatedRequestType(rawRequestType)) {
        switch (PipeRequestType.valueOf(rawRequestType)) {
          case CONFIGNODE_HANDSHAKE:
            return handleTransferHandshake(
                PipeTransferConfigHandshakeReq.fromTPipeTransferReq(req));
          case TRANSFER_CONFIG_PLAN:
            return handleTransferConfigPlan(PipeTransferConfigPlanReq.fromTPipeTransferReq(req));
          case TRANSFER_CONFIG_SNAPSHOT_PIECE:
            return handleTransferFilePiece(
                PipeTransferConfigSnapShotPieceReq.fromTPipeTransferReq(req),
                req instanceof AirGapPseudoTPipeTransferRequest);
          case TRANSFER_CONFIG_SNAPSHOT_SEAL:
            return handleTransferFileSeal(
                PipeTransferConfigSnapShotSealReq.fromTPipeTransferReq(req));
          default:
            break;
        }
      }

      // Unknown request type, which means the request can not be handled by this receiver,
      // maybe the version of the receiver is not compatible with the sender
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TYPE_ERROR,
              String.format("Unsupported PipeRequestType on ConfigNode %s.", rawRequestType));
      LOGGER.warn("Unsupported PipeRequestType on ConfigNode, response status = {}.", status);
      return new TPipeTransferResp(status);
    } catch (IOException e) {
      String error = String.format("Serialization error during pipe receiving, %s", e);
      LOGGER.warn(error);
      return new TPipeTransferResp(RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, error));
    }
  }

  private TPipeTransferResp handleTransferConfigPlan(PipeTransferConfigPlanReq req) {
    // TODO
    return new TPipeTransferResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  // Used to construct pipe related procedures
  private String getPseudoQueryId() {
    return "pipe" + System.currentTimeMillis() + queryIndex.getAndIncrement();
  }

  @Override
  protected String getReceiverFileBaseDir() {
    return ConfigNodeDescriptor.getInstance().getConf().getPipeReceiverFileDir();
  }

  @Override
  protected TSStatus loadFile(PipeTransferFileSealReq req, String fileAbsolutePath) {
    // TODO
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
