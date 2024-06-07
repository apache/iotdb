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
import org.apache.iotdb.commons.client.gg.AsyncRequestContext;

import java.util.Map;

/**
 * Asynchronous Client handler.
 *
 * @param <Request> ClassName of RPC request
 * @param <Response> ClassName of RPC response
 */
public class AsyncDataNodeRequestContext<Request, Response> extends AsyncRequestContext<Request, Response, DataNodeRequestType, AbstractAsyncRPCHandler, TDataNodeLocation> {

  public AsyncDataNodeRequestContext(DataNodeRequestType dataNodeRequestType) {
    super(dataNodeRequestType);
  }

  public AsyncDataNodeRequestContext(DataNodeRequestType dataNodeRequestType, Map<Integer, TDataNodeLocation> configNodeLocationMap) {
    super(dataNodeRequestType, configNodeLocationMap);
  }

  public AsyncDataNodeRequestContext(DataNodeRequestType dataNodeRequestType, Request request, Map<Integer, TDataNodeLocation> configNodeLocationMap) {
    super(dataNodeRequestType, request, configNodeLocationMap);
  }

  public AbstractAsyncRPCHandler<?> createAsyncRPCHandler(
      int requestId, TDataNodeLocation targetDataNode) {
    switch (requestType) {
      default:
        return new AsyncTSStatusRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            nodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,  // TODO: 研究一下能不能不强转
            countDownLatch);
    }
  }
}
