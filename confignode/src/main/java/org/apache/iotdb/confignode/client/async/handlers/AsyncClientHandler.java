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
package org.apache.iotdb.confignode.client.async.handlers;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class AsyncClientHandler<Q, R> {

  // Type of RPC request
  protected final DataNodeRequestType requestType;

  private final Map<Integer, Q> requestMap;

  /**
   * Map key: The indices of asynchronous RPC requests
   *
   * <p>Map value: The target DataNodes of corresponding indices
   *
   * <p>All kinds of AsyncHandler will remove its targetDataNode from the dataNodeLocationMap only
   * if its corresponding RPC request success
   */
  private final Map<Integer, TDataNodeLocation> dataNodeLocationMap;

  /**
   * Map key: The indices(targetDataNode's ID) of asynchronous RPC requests
   *
   * <p>Map value: The response of corresponding indices
   *
   * <p>All kinds of AsyncHandler will add response to the responseMap after its corresponding RPC
   * request finished
   */
  private final Map<Integer, R> responseMap;

  private CountDownLatch countDownLatch;

  public AsyncClientHandler(DataNodeRequestType requestType) {
    this.requestType = requestType;
  }

  /** Always reset CountDownLatch before retry */
  public void resetCountDownLatch() {
    countDownLatch = new CountDownLatch(dataNodeLocationMap.size());
  }

  public DataNodeRequestType getRequestType() {
    return requestType;
  }

  public List<Integer> getRequestIndices() {
    return new ArrayList<>(dataNodeLocationMap.keySet());
  }

  public Q getRequest(int requestId) {
    return requestMap.get(requestId);
  }

  public TDataNodeLocation getDataNodeLocation(int requestId) {
    return dataNodeLocationMap.get(requestId);
  }

  public CountDownLatch getCountDownLatch() {
    return countDownLatch;
  }

  public AbstractAsyncRPCHandler<?> createAsyncRPCHandler(
      int requestId, TDataNodeLocation targetDataNode) {

    switch (requestType) {



      case SET_TTL:
      default:
        return new AsyncTSStatusRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            dataNodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,
            countDownLatch);
    }
  }
}
