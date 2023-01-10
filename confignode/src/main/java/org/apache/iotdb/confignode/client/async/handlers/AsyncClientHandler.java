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
import org.apache.iotdb.confignode.client.async.handlers.rpc.AbstractAsyncRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.AsyncTSStatusRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.CountPathsUsingTemplateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.DeleteSchemaRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.FetchSchemaBlackListRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.OperatePipeRPCHandler;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListResp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Asynchronous Client handler
 *
 * @param <Q> ClassName of RPC request
 * @param <R> ClassName of RPC response
 */
public class AsyncClientHandler<Q, R> {

  // Type of RPC request
  protected final DataNodeRequestType requestType;

  /**
   * Map key: The indices of asynchronous RPC requests
   *
   * <p>Map value: The corresponding RPC request
   */
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

  /** Custom constructor */
  public AsyncClientHandler(DataNodeRequestType requestType) {
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

  /** Constructor for null requests */
  public AsyncClientHandler(
      DataNodeRequestType requestType, Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    this.requestType = requestType;
    this.dataNodeLocationMap = dataNodeLocationMap;

    this.requestMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  /** Constructor for unique request */
  public AsyncClientHandler(
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

  public List<R> getResponseList() {
    return new ArrayList<>(responseMap.values());
  }

  public Map<Integer, R> getResponseMap() {
    return responseMap;
  }

  /** Always reset CountDownLatch before retry */
  public void resetCountDownLatch() {
    countDownLatch = new CountDownLatch(dataNodeLocationMap.size());
  }

  public CountDownLatch getCountDownLatch() {
    return countDownLatch;
  }

  public AbstractAsyncRPCHandler<?> createAsyncRPCHandler(
      int requestId, TDataNodeLocation targetDataNode) {
    switch (requestType) {
      case CONSTRUCT_SCHEMA_BLACK_LIST:
      case ROLLBACK_SCHEMA_BLACK_LIST:
      case DELETE_DATA_FOR_DELETE_SCHEMA:
      case DELETE_TIMESERIES:
      case CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
      case ROLLBACK_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
      case DEACTIVATE_TEMPLATE:
        return new DeleteSchemaRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            dataNodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,
            countDownLatch);
      case FETCH_SCHEMA_BLACK_LIST:
        return new FetchSchemaBlackListRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            dataNodeLocationMap,
            (Map<Integer, TFetchSchemaBlackListResp>) responseMap,
            countDownLatch);
      case PRE_CREATE_PIPE:
      case OPERATE_PIPE:
      case ROLLBACK_OPERATE_PIPE:
        return new OperatePipeRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            dataNodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,
            countDownLatch);
      case COUNT_PATHS_USING_TEMPLATE:
        return new CountPathsUsingTemplateRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            dataNodeLocationMap,
            (Map<Integer, TCountPathsUsingTemplateResp>) responseMap,
            countDownLatch);
      case SET_TTL:
      case CREATE_DATA_REGION:
      case CREATE_SCHEMA_REGION:
      case CREATE_FUNCTION:
      case DROP_FUNCTION:
      case CREATE_TRIGGER_INSTANCE:
      case DROP_TRIGGER_INSTANCE:
      case ACTIVE_TRIGGER_INSTANCE:
      case INACTIVE_TRIGGER_INSTANCE:
      case UPDATE_TRIGGER_LOCATION:
      case MERGE:
      case FULL_MERGE:
      case FLUSH:
      case CLEAR_CACHE:
      case LOAD_CONFIGURATION:
      case SET_SYSTEM_STATUS:
      case UPDATE_REGION_ROUTE_MAP:
      case BROADCAST_LATEST_CONFIG_NODE_GROUP:
      case INVALIDATE_MATCHED_SCHEMA_CACHE:
      case UPDATE_TEMPLATE:
      case CHANGE_REGION_LEADER:
      case KILL_QUERY_INSTANCE:
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
