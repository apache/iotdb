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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Asynchronous Client handler.
 *
 * @param <Q> ClassName of RPC request
 * @param <R> ClassName of RPC response
 */
public class AsyncConfigNodeClientHandler<Q, R> {

  // Type of RPC request
  protected final ConfigNodeRequestType requestType;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The corresponding RPC request
   */
  private final Map<Integer, Q> requestMap;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The target ConfigNodes of corresponding indices
   *
   * <p>All kinds of AsyncHandler will remove its targetConfigNode from the configNodeLocationMap
   * only if its corresponding RPC request success
   */
  private final Map<Integer, TConfigNodeLocation> configNodeLocationMap;

  /**
   * Map key: The indices(targetConfigNode's ID) of asynchronous RPC requests.
   *
   * <p>Map value: The response of corresponding indices
   *
   * <p>All kinds of AsyncHandler will add response to the responseMap after its corresponding RPC
   * request finished
   */
  private final Map<Integer, R> responseMap;

  private CountDownLatch countDownLatch;

  /** Custom constructor. */
  public AsyncConfigNodeClientHandler(ConfigNodeRequestType requestType) {
    this.requestType = requestType;
    this.requestMap = new ConcurrentHashMap<>();
    this.configNodeLocationMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  public void putRequest(int requestId, Q request) {
    requestMap.put(requestId, request);
  }

  public void putConfigNodeLocation(int requestId, TConfigNodeLocation configNodeLocation) {
    configNodeLocationMap.put(requestId, configNodeLocation);
  }

  /** Constructor for null requests. */
  public AsyncConfigNodeClientHandler(
      ConfigNodeRequestType requestType, Map<Integer, TConfigNodeLocation> configNodeLocationMap) {
    this.requestType = requestType;
    this.configNodeLocationMap = configNodeLocationMap;

    this.requestMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  /** Constructor for unique request. */
  public AsyncConfigNodeClientHandler(
      ConfigNodeRequestType requestType,
      Q request,
      Map<Integer, TConfigNodeLocation> configNodeLocationMap) {
    this.requestType = requestType;
    this.configNodeLocationMap = configNodeLocationMap;

    this.requestMap = new ConcurrentHashMap<>();
    this.configNodeLocationMap
        .keySet()
        .forEach(configNodeId -> this.requestMap.put(configNodeId, request));

    this.responseMap = new ConcurrentHashMap<>();
  }

  public ConfigNodeRequestType getRequestType() {
    return requestType;
  }

  public List<Integer> getRequestIndices() {
    return new ArrayList<>(configNodeLocationMap.keySet());
  }

  public Q getRequest(int requestId) {
    return requestMap.get(requestId);
  }

  public TConfigNodeLocation getConfigNodeLocation(int requestId) {
    return configNodeLocationMap.get(requestId);
  }

  public List<R> getResponseList() {
    return new ArrayList<>(responseMap.values());
  }

  public Map<Integer, R> getResponseMap() {
    return responseMap;
  }

  /** Always reset CountDownLatch before retry. */
  public void resetCountDownLatch() {
    countDownLatch = new CountDownLatch(configNodeLocationMap.size());
  }

  public CountDownLatch getCountDownLatch() {
    return countDownLatch;
  }

  public AbstractAsyncRPCHandler2<?> createAsyncRPCHandler(
      int requestId, TConfigNodeLocation targetConfigNode) {
    switch (requestType) {
      case SUBMIT_TEST_CONNECTION_TASK:
      case TEST_CONNECTION:
      default:
        return new AsyncTSStatusRPCHandler2(
            requestType,
            requestId,
            targetConfigNode,
            configNodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,
            countDownLatch);
    }
  }
}
