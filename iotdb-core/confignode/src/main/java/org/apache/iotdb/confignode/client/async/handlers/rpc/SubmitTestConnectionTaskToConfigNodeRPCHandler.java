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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SubmitTestConnectionTaskToConfigNodeRPCHandler
    extends AbstractAsyncRPCHandler2<TTestConnectionResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubmitTestConnectionTaskToConfigNodeRPCHandler.class);

  public SubmitTestConnectionTaskToConfigNodeRPCHandler(
      ConfigNodeRequestType requestType,
      int requestId,
      TConfigNodeLocation targetConfigNode,
      Map<Integer, TConfigNodeLocation> configNodeLocationMap,
      Map<Integer, TTestConnectionResp> responseMap,
      CountDownLatch countDownLatch) {
    super(
        requestType,
        requestId,
        targetConfigNode,
        configNodeLocationMap,
        responseMap,
        countDownLatch);
  }

  @Override
  public void onComplete(TTestConnectionResp resp) {
    responseMap.put(requestId, resp);
    configNodeLocationMap.remove(requestId);
    countDownLatch.countDown();
  }

  // TODO: 什么情况下会error？
  @Override
  public void onError(Exception e) {
    LOGGER.error("gg", e);
    TTestConnectionResp resp = new TTestConnectionResp();
    resp.setStatus(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    responseMap.put(requestId, new TTestConnectionResp());
    configNodeLocationMap.remove(requestId);
    countDownLatch.countDown();
  }
}
