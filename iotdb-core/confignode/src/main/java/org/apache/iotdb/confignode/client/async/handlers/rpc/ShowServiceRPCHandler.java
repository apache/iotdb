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

package org.apache.iotdb.confignode.client.async.handlers.rpc;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TShowServiceInstanceResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ShowServiceRPCHandler
    extends DataNodeAsyncRequestRPCHandler<TShowServiceInstanceResp> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowServiceRPCHandler.class);

  protected ShowServiceRPCHandler(
      CnToDnAsyncRequestType requestType,
      int requestId,
      TDataNodeLocation targetNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TShowServiceInstanceResp> integerTShowServiceInstanceRespMap,
      CountDownLatch countDownLatch) {
    super(
        requestType,
        requestId,
        targetNode,
        dataNodeLocationMap,
        integerTShowServiceInstanceRespMap,
        countDownLatch);
  }

  @Override
  public void onComplete(TShowServiceInstanceResp response) {
    // Put response
    responseMap.put(requestId, response);
    if (response.getStatus().getCode() == 0) {
      LOGGER.info("Successfully {} on Data Node: {}", requestType, formattedTargetLocation);
    } else {
      LOGGER.error(
          "Failed to {} on ConfigNode: {}, response: {}",
          requestType,
          formattedTargetLocation,
          response);
    }
    nodeLocationMap.remove(requestId);
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    String errorMsg =
        "Failed to "
            + requestType
            + " on DataNode: "
            + formattedTargetLocation
            + ", exception: "
            + e.getMessage();
    LOGGER.error(errorMsg);
    responseMap.put(
        requestId,
        new TShowServiceInstanceResp(
            new TSStatus(
                RpcUtils.getStatus(
                    TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), errorMsg))));
    countDownLatch.countDown();
  }
}
