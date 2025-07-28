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

package org.apache.iotdb.commons.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Asynchronous Client handler.
 *
 * @param <Request> ClassName of RPC request
 * @param <Response> ClassName of RPC response
 */
public class AsyncRequestContext<Request, Response, RequestType, NodeLocation> {

  // Type of RPC request
  protected final RequestType requestType;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The corresponding RPC request
   */
  private final Map<Integer, Request> requestMap;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The target Nodes of corresponding indices
   *
   * <p>All kinds of AsyncHandler will remove its targetNode from the nodeLocationMap only if its
   * corresponding RPC request success
   */
  private final ConcurrentHashMap<Integer, NodeLocation> nodeLocationMap;

  /**
   * Map key: The indices(targetNode's ID) of asynchronous RPC requests.
   *
   * <p>Map value: The response of corresponding indices
   *
   * <p>All kinds of AsyncHandler will add response to the responseMap after its corresponding RPC
   * request finished
   */
  private final Map<Integer, Response> responseMap;

  private CountDownLatch countDownLatch;

  /** Custom constructor. */
  public AsyncRequestContext(final RequestType requestType) {
    this.requestType = requestType;
    this.requestMap = new ConcurrentHashMap<>();
    this.nodeLocationMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  public void putRequest(final int requestId, final Request request) {
    requestMap.put(requestId, request);
  }

  public Request putRequestIfAbsent(final int requestId, final Request request) {
    return requestMap.putIfAbsent(requestId, request);
  }

  public void putNodeLocation(final int requestId, final NodeLocation nodeLocation) {
    nodeLocationMap.put(requestId, nodeLocation);
  }

  /** Constructor for null requests. */
  public AsyncRequestContext(RequestType requestType, Map<Integer, NodeLocation> nodeLocationMap) {
    this.requestType = requestType;
    this.nodeLocationMap = new ConcurrentHashMap<>(nodeLocationMap);
    this.requestMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  /** Constructor for unique request. */
  public AsyncRequestContext(
      RequestType requestType, Request request, Map<Integer, NodeLocation> nodeLocationMap) {
    this.requestType = requestType;
    this.nodeLocationMap = new ConcurrentHashMap<>(nodeLocationMap);
    this.requestMap = new ConcurrentHashMap<>();
    this.nodeLocationMap.keySet().forEach(nodeId -> this.requestMap.put(nodeId, request));
    this.responseMap = new ConcurrentHashMap<>();
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public List<Integer> getRequestIndices() {
    return new ArrayList<>(nodeLocationMap.keySet());
  }

  public Request getRequest(int requestId) {
    return requestMap.get(requestId);
  }

  public NodeLocation getNodeLocation(int requestId) {
    return nodeLocationMap.get(requestId);
  }

  public Map<Integer, NodeLocation> getNodeLocationMap() {
    return nodeLocationMap;
  }

  public List<Response> getResponseList() {
    return new ArrayList<>(responseMap.values());
  }

  public Map<Integer, Response> getResponseMap() {
    return responseMap;
  }

  /** Always reset CountDownLatch before retry. */
  public void resetCountDownLatch() {
    countDownLatch = new CountDownLatch(nodeLocationMap.size());
  }

  public CountDownLatch getCountDownLatch() {
    return countDownLatch;
  }
}
