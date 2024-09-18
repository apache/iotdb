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

package org.apache.iotdb.db.protocol.client.cn;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class ConfigNodeAsyncRequestRPCHandler<Response>
    extends AsyncRequestRPCHandler<Response, DnToCnRequestType, TConfigNodeLocation> {

  protected ConfigNodeAsyncRequestRPCHandler(
      DnToCnRequestType configNodeRequestType,
      int requestId,
      TConfigNodeLocation targetNode,
      Map<Integer, TConfigNodeLocation> integerTConfigNodeLocationMap,
      Map<Integer, Response> integerResponseMap,
      CountDownLatch countDownLatch) {
    super(
        configNodeRequestType,
        requestId,
        targetNode,
        integerTConfigNodeLocationMap,
        integerResponseMap,
        countDownLatch);
  }

  @Override
  protected String generateFormattedTargetLocation(TConfigNodeLocation configNodeLocation) {
    return "{id="
        + targetNode.getConfigNodeId()
        + ", internalEndPoint="
        + targetNode.getInternalEndPoint()
        + "}";
  }

  public static ConfigNodeAsyncRequestRPCHandler<?> buildHandler(
      AsyncRequestContext<?, ?, DnToCnRequestType, TConfigNodeLocation> context,
      int requestId,
      TConfigNodeLocation targetConfigNode) {
    DnToCnRequestType requestType = context.getRequestType();
    Map<Integer, TConfigNodeLocation> nodeLocationMap = context.getNodeLocationMap();
    Map<Integer, ?> responseMap = context.getResponseMap();
    CountDownLatch countDownLatch = context.getCountDownLatch();
    switch (requestType) {
      case SUBMIT_TEST_CONNECTION_TASK:
      case TEST_CONNECTION:
      default:
        return new AsyncConfigNodeTSStatusRPCHandler(
            requestType,
            requestId,
            targetConfigNode,
            nodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,
            countDownLatch);
    }
  }
}
