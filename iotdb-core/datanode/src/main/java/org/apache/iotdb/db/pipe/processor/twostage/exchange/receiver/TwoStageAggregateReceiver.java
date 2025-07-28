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

package org.apache.iotdb.db.pipe.processor.twostage.exchange.receiver;

import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiver;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.db.pipe.processor.twostage.combiner.PipeCombineHandlerManager;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.CombineRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.RequestType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoStageAggregateReceiver implements IoTDBReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoStageAggregateReceiver.class);

  @Override
  public IoTDBSinkRequestVersion getVersion() {
    return IoTDBSinkRequestVersion.VERSION_2;
  }

  @Override
  public TPipeTransferResp receive(TPipeTransferReq req) {
    try {
      final short rawRequestType = req.getType();
      if (RequestType.isValidatedRequestType(rawRequestType)) {
        switch (RequestType.valueOf(rawRequestType)) {
          case COMBINE:
            return PipeCombineHandlerManager.getInstance()
                .handle(CombineRequest.fromTPipeTransferReq(req));
          case FETCH_COMBINE_RESULT:
            return PipeCombineHandlerManager.getInstance()
                .handle(FetchCombineResultRequest.fromTPipeTransferReq(req));
          default:
            break;
        }
      }

      LOGGER.warn("Unknown request type {}: {}.", rawRequestType, req);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_TYPE_ERROR,
              String.format("Unknown request type %s.", rawRequestType)));
    } catch (Exception e) {
      LOGGER.warn("Error occurs when receiving request: {}.", req, e);
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_ERROR,
              String.format("Error occurs when receiving request: %s.", e.getMessage())));
    }
  }

  @Override
  public void handleExit() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Two stage aggregate receiver is exiting.");
    }
  }
}
