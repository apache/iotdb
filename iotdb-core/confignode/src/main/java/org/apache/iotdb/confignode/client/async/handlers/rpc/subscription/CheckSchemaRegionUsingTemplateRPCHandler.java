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

package org.apache.iotdb.confignode.client.async.handlers.rpc.subscription;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.rpc.AbstractAsyncRPCHandler;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class CheckSchemaRegionUsingTemplateRPCHandler
    extends AbstractAsyncRPCHandler<TCheckSchemaRegionUsingTemplateResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CheckSchemaRegionUsingTemplateRPCHandler.class);

  public CheckSchemaRegionUsingTemplateRPCHandler(
      DataNodeRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TCheckSchemaRegionUsingTemplateResp> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TCheckSchemaRegionUsingTemplateResp response) {
    TSStatus tsStatus = response.getStatus();
    responseMap.put(requestId, response);
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataNodeLocationMap.remove(requestId);
      LOGGER.info(
          "Successfully check schema region using template on DataNode: {}", targetDataNode);
    } else if (tsStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      dataNodeLocationMap.remove(requestId);
      LOGGER.error(
          "Failed to check schema region using template on DataNode {}, {}",
          targetDataNode,
          tsStatus);
    } else {
      LOGGER.error(
          "Failed to check schema region using template on DataNode {}, {}",
          targetDataNode,
          tsStatus);
    }
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    String errorMsg =
        "Count paths using template error on DataNode: {id="
            + targetDataNode.getDataNodeId()
            + ", internalEndPoint="
            + targetDataNode.getInternalEndPoint()
            + "}"
            + e.getMessage();
    LOGGER.error(errorMsg);

    countDownLatch.countDown();
    TCheckSchemaRegionUsingTemplateResp resp = new TCheckSchemaRegionUsingTemplateResp();
    resp.setStatus(
        new TSStatus(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), errorMsg)));
    responseMap.put(requestId, resp);
  }
}
