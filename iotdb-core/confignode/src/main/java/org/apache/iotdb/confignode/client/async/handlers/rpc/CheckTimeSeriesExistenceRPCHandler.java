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
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class CheckTimeSeriesExistenceRPCHandler
    extends DataNodeAsyncRequestRPCHandler<TCheckTimeSeriesExistenceResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CheckTimeSeriesExistenceRPCHandler.class);

  public CheckTimeSeriesExistenceRPCHandler(
      CnToDnAsyncRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TCheckTimeSeriesExistenceResp> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TCheckTimeSeriesExistenceResp response) {
    TSStatus tsStatus = response.getStatus();
    responseMap.put(requestId, response);
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      nodeLocationMap.remove(requestId);
      LOGGER.info("Successfully check timeseries existence on DataNode: {}", targetNode);
    } else if (tsStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      nodeLocationMap.remove(requestId);
      LOGGER.error("Failed to check timeseries existence on DataNode {}, {}", targetNode, tsStatus);
    } else {
      LOGGER.error("Failed to check timeseries existence on DataNode {}, {}", targetNode, tsStatus);
    }
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    String errorMsg =
        "Check timeseries existence error on DataNode: {id="
            + targetNode.getDataNodeId()
            + ", internalEndPoint="
            + targetNode.getInternalEndPoint()
            + "}"
            + e.getMessage();
    LOGGER.error(errorMsg);

    countDownLatch.countDown();
    TCheckTimeSeriesExistenceResp resp = new TCheckTimeSeriesExistenceResp();
    resp.setStatus(
        new TSStatus(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), errorMsg)));
    responseMap.put(requestId, resp);
  }
}
