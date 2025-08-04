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

package org.apache.iotdb.db.protocol.client.dn;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** General RPC handler for TSStatus response type. */
public class AsyncTSStatusRPCHandler extends DataNodeAsyncRequestRPCHandler<TSStatus> {

  private final boolean keepSilent;
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTSStatusRPCHandler.class);

  public AsyncTSStatusRPCHandler(
      DnToDnRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TSStatus> responseMap,
      CountDownLatch countDownLatch,
      boolean keepSilent) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
    this.keepSilent = keepSilent;
  }

  @Override
  public void onComplete(TSStatus response) {
    // Put response
    responseMap.put(requestId, response);

    if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Remove only if success
      nodeLocationMap.remove(requestId);
      if (!keepSilent) {
        LOGGER.info("Successfully {} on DataNode: {}", requestType, formattedTargetLocation);
      }
    } else {
      if (!keepSilent) {
        LOGGER.error(
            "Failed to {} on DataNode: {}, response: {}",
            requestType,
            formattedTargetLocation,
            response);
      }
    }

    // Always CountDown
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
    if (!keepSilent) {
      LOGGER.error(errorMsg);
    }

    responseMap.put(
        requestId,
        new TSStatus(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), errorMsg)));

    // Always CountDown
    countDownLatch.countDown();
  }
}
