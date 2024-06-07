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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.commons.client.gg.AsyncRequestContext;
import org.apache.iotdb.confignode.client.DataNodeRequestType;

import org.apache.iotdb.confignode.client.async.handlers.AsyncDataNodeRequestContext;
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
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractAsyncRPCHandler<Response> implements AsyncMethodCallback<Response> {

  // Type of RPC request
  protected final DataNodeRequestType requestType;
  // Index of request
  protected final int requestId;
  // Target DataNode
  protected final TDataNodeLocation targetDataNode;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The target DataNodes of corresponding indices
   *
   * <p>All kinds of AsyncHandler will remove its targetDataNode from the dataNodeLocationMap only
   * if its corresponding RPC request success
   */
  protected final Map<Integer, TDataNodeLocation> dataNodeLocationMap;

  /**
   * Map key: The indices(targetDataNode's ID) of asynchronous RPC requests.
   *
   * <p>Map value: The response of corresponding indices
   *
   * <p>All kinds of AsyncHandler will add response to the responseMap after its corresponding RPC
   * request finished
   */
  protected final Map<Integer, Response> responseMap;

  // All kinds of AsyncHandler will invoke countDown after its corresponding RPC request finished
  protected final CountDownLatch countDownLatch;

  protected final String formattedTargetLocation;

  protected AbstractAsyncRPCHandler(
      DataNodeRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, Response> responseMap,
      CountDownLatch countDownLatch) {
    this.requestType = requestType;
    this.requestId = requestId;
    this.targetDataNode = targetDataNode;
    this.formattedTargetLocation =
        "{id="
            + targetDataNode.getDataNodeId()
            + ", internalEndPoint="
            + targetDataNode.getInternalEndPoint()
            + "}";

    this.dataNodeLocationMap = dataNodeLocationMap;
    this.responseMap = responseMap;
    this.countDownLatch = countDownLatch;
  }

  public static AbstractAsyncRPCHandler<?> buildHandler(AsyncRequestContext<?, ?, DataNodeRequestType, TDataNodeLocation> context,
                                                        int requestId, TDataNodeLocation targetDataNode) {
    DataNodeRequestType requestType = context.getRequestType();
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = context.getNodeLocationMap();
    Map<Integer, ?> responseMap = context.getResponseMap();
    CountDownLatch countDownLatch = context.getCountDownLatch();
    switch (requestType) {
      case CONSTRUCT_SCHEMA_BLACK_LIST:
      case ROLLBACK_SCHEMA_BLACK_LIST:
      case DELETE_DATA_FOR_DELETE_SCHEMA:
      case DELETE_TIMESERIES:
      case CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
      case ROLLBACK_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
      case DEACTIVATE_TEMPLATE:
      case CONSTRUCT_VIEW_SCHEMA_BLACK_LIST:
      case ROLLBACK_VIEW_SCHEMA_BLACK_LIST:
      case DELETE_VIEW:
      case ALTER_VIEW:
        return new SchemaUpdateRPCHandler(
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
      case COUNT_PATHS_USING_TEMPLATE:
        return new CountPathsUsingTemplateRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TCountPathsUsingTemplateResp>) responseMap,
                countDownLatch);
      case CHECK_SCHEMA_REGION_USING_TEMPLATE:
        return new CheckSchemaRegionUsingTemplateRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TCheckSchemaRegionUsingTemplateResp>) responseMap,
                countDownLatch);
      case CHECK_TIMESERIES_EXISTENCE:
        return new CheckTimeSeriesExistenceRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TCheckTimeSeriesExistenceResp>) responseMap,
                countDownLatch);
      case PIPE_HEARTBEAT:
        return new PipeHeartbeatRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TPipeHeartbeatResp>) responseMap,
                countDownLatch);
      case PIPE_PUSH_ALL_META:
      case PIPE_PUSH_SINGLE_META:
      case PIPE_PUSH_MULTI_META:
        return new PipePushMetaRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TPushPipeMetaResp>) responseMap,
                countDownLatch);
      case TOPIC_PUSH_ALL_META:
      case TOPIC_PUSH_SINGLE_META:
      case TOPIC_PUSH_MULTI_META:
        return new TopicPushMetaRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TPushTopicMetaResp>) responseMap,
                countDownLatch);
      case CONSUMER_GROUP_PUSH_ALL_META:
      case CONSUMER_GROUP_PUSH_SINGLE_META:
        return new ConsumerGroupPushMetaRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TPushConsumerGroupMetaResp>) responseMap,
                countDownLatch);
      case CHANGE_REGION_LEADER:
        return new TransferLeaderRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TRegionLeaderChangeResp>) responseMap,
                countDownLatch);
      case SUBMIT_TEST_CONNECTION_TASK:
        return new SubmitTestConnectionTaskRPCHandler(
                requestType,
                requestId,
                targetDataNode,
                dataNodeLocationMap,
                (Map<Integer, TTestConnectionResp>) responseMap,
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
      case START_REPAIR_DATA:
      case STOP_REPAIR_DATA:
      case LOAD_CONFIGURATION:
      case SET_SYSTEM_STATUS:
      case UPDATE_REGION_ROUTE_MAP:
      case INVALIDATE_MATCHED_SCHEMA_CACHE:
      case UPDATE_TEMPLATE:
      case KILL_QUERY_INSTANCE:
      case RESET_PEER_LIST:
      case TEST_CONNECTION:
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
