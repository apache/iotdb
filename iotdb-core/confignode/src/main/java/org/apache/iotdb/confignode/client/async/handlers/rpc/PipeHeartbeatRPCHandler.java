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
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class PipeHeartbeatRPCHandler extends DataNodeAsyncRequestRPCHandler<TPipeHeartbeatResp> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatRPCHandler.class);

  public PipeHeartbeatRPCHandler(
      CnToDnAsyncRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TPipeHeartbeatResp> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TPipeHeartbeatResp response) {
    // Put response
    responseMap.put(requestId, response);
    nodeLocationMap.remove(requestId);
    LOGGER.info("Successfully {} on DataNode: {}", requestType, formattedTargetLocation);

    // Always CountDown
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    LOGGER.error(
        "Failed to {} on DataNode: {}, exception: {}",
        requestType,
        formattedTargetLocation,
        e.getMessage());

    // Always CountDown
    countDownLatch.countDown();
  }
}
