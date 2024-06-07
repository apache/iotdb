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
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.rpc.AbstractAsyncRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.AsyncTSStatusRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.CheckTimeSeriesExistenceRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.CountPathsUsingTemplateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.FetchSchemaBlackListRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.PipeHeartbeatRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.PipePushMetaRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.SchemaUpdateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.SubmitTestConnectionTaskRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.TransferLeaderRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.subscription.CheckSchemaRegionUsingTemplateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.subscription.ConsumerGroupPushMetaRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.subscription.TopicPushMetaRPCHandler;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceResp;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListResp;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;

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
public class AsyncRequestContext<Q, R> {

  // Type of RPC request
  protected final DataNodeRequestType requestType;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The corresponding RPC request
   */
  private final Map<Integer, Q> requestMap;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The target DataNodes of corresponding indices
   *
   * <p>All kinds of AsyncHandler will remove its targetDataNode from the dataNodeLocationMap only
   * if its corresponding RPC request success
   */
  private final Map<Integer, TDataNodeLocation> dataNodeLocationMap;

  /**
   * Map key: The indices(targetDataNode's ID) of asynchronous RPC requests.
   *
   * <p>Map value: The response of corresponding indices
   *
   * <p>All kinds of AsyncHandler will add response to the responseMap after its corresponding RPC
   * request finished
   */
  private final Map<Integer, R> responseMap;

  private CountDownLatch countDownLatch;

  /** Custom constructor. */
  public AsyncRequestContext(DataNodeRequestType requestType) {
    this.requestType = requestType;
    this.requestMap = new ConcurrentHashMap<>();
    this.dataNodeLocationMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  public void putRequest(int requestId, Q request) {
    requestMap.put(requestId, request);
  }

  public void putDataNodeLocation(int requestId, TDataNodeLocation dataNodeLocation) {
    dataNodeLocationMap.put(requestId, dataNodeLocation);
  }

  /** Constructor for null requests. */
  public AsyncRequestContext(
      DataNodeRequestType requestType, Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    this.requestType = requestType;
    this.dataNodeLocationMap = dataNodeLocationMap;

    this.requestMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  /** Constructor for unique request. */
  public AsyncRequestContext(
      DataNodeRequestType requestType,
      Q request,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    this.requestType = requestType;
    this.dataNodeLocationMap = dataNodeLocationMap;
    this.requestMap = new ConcurrentHashMap<>();
    this.dataNodeLocationMap
        .keySet()
        .forEach(dataNodeId -> this.requestMap.put(dataNodeId, request));
    this.responseMap = new ConcurrentHashMap<>();
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

  public Map<Integer, TDataNodeLocation> getDataNodeLocationMap() {
    return dataNodeLocationMap;
  }

  public List<R> getResponseList() {
    return new ArrayList<>(responseMap.values());
  }

  public Map<Integer, R> getResponseMap() {
    return responseMap;
  }

  /** Always reset CountDownLatch before retry. */
  public void resetCountDownLatch() {
    countDownLatch = new CountDownLatch(dataNodeLocationMap.size());
  }

  public CountDownLatch getCountDownLatch() {
    return countDownLatch;
  }
}
