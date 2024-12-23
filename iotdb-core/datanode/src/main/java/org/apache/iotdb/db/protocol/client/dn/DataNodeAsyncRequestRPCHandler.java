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
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class DataNodeAsyncRequestRPCHandler<Response>
    extends AsyncRequestRPCHandler<Response, DnToDnRequestType, TDataNodeLocation> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataNodeAsyncRequestRPCHandler.class);

  protected DataNodeAsyncRequestRPCHandler(
      DnToDnRequestType dataNodeToDataNodeRequestType,
      int requestId,
      TDataNodeLocation targetNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, Response> integerResponseMap,
      CountDownLatch countDownLatch) {
    super(
        dataNodeToDataNodeRequestType,
        requestId,
        targetNode,
        dataNodeLocationMap,
        integerResponseMap,
        countDownLatch);
  }

  @Override
  protected String generateFormattedTargetLocation(TDataNodeLocation dataNodeLocation) {
    return "{id="
        + targetNode.getDataNodeId()
        + ", internalEndPoint="
        + targetNode.getInternalEndPoint()
        + "}";
  }

  public static DataNodeAsyncRequestRPCHandler<?> createAsyncRPCHandler(
      final AsyncRequestContext<?, ?, DnToDnRequestType, TDataNodeLocation> context,
      final int requestId,
      final TDataNodeLocation targetDataNode) {
    final DnToDnRequestType requestType = context.getRequestType();
    final Map<Integer, TDataNodeLocation> nodeLocationMap = context.getNodeLocationMap();
    final Map<Integer, ?> responseMap = context.getResponseMap();
    final CountDownLatch countDownLatch = context.getCountDownLatch();
    switch (requestType) {
      case TEST_CONNECTION:
      case UPDATE_ATTRIBUTE:
        return new AsyncTSStatusRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            nodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,
            countDownLatch);
      default:
        throw new UnsupportedOperationException("request type is not supported: " + requestType);
    }
  }
}
