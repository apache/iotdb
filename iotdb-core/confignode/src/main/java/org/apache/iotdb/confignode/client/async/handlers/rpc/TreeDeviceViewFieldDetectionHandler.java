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
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class TreeDeviceViewFieldDetectionHandler
    extends DataNodeAsyncRequestRPCHandler<TDeviceViewResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TreeDeviceViewFieldDetectionHandler.class);

  protected TreeDeviceViewFieldDetectionHandler(
      final CnToDnAsyncRequestType requestType,
      final int requestId,
      final TDataNodeLocation targetNode,
      final Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      final Map<Integer, TDeviceViewResp> integerTDeviceViewRespMap,
      final CountDownLatch countDownLatch) {
    super(
        requestType,
        requestId,
        targetNode,
        dataNodeLocationMap,
        integerTDeviceViewRespMap,
        countDownLatch);
  }

  @Override
  public void onComplete(final TDeviceViewResp response) {
    // Put response
    responseMap.put(requestId, response);

    if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info("Successfully {} on DataNode: {}", requestType, formattedTargetLocation);
    } else {
      LOGGER.error(
          "Failed to {} on DataNode: {}, response: {}",
          requestType,
          formattedTargetLocation,
          response);
    }

    // Always remove to avoid retrying
    nodeLocationMap.remove(requestId);

    // Always CountDown
    countDownLatch.countDown();
  }

  @Override
  public void onError(final Exception e) {
    final String errorMsg =
        "Failed to "
            + requestType
            + " on DataNode: "
            + formattedTargetLocation
            + ", exception: "
            + e.getMessage();
    LOGGER.warn(errorMsg, e);

    // Ensure that the map is always non-null
    responseMap.put(
        requestId,
        new TDeviceViewResp()
            .setStatus(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, errorMsg))
            .setDeviewViewFieldTypeMap(new HashMap<>()));

    // Always CountDown
    countDownLatch.countDown();
  }
}
