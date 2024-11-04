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
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
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

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class DataNodeAsyncRequestRPCHandler<Response>
    extends AsyncRequestRPCHandler<Response, CnToDnAsyncRequestType, TDataNodeLocation> {

  protected DataNodeAsyncRequestRPCHandler(
      CnToDnAsyncRequestType requestType,
      int requestId,
      TDataNodeLocation targetNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, Response> integerResponseMap,
      CountDownLatch countDownLatch) {
    super(
        requestType,
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

  public static DataNodeAsyncRequestRPCHandler<?> buildHandler(
      AsyncRequestContext<?, ?, CnToDnAsyncRequestType, TDataNodeLocation> context,
      int requestId,
      TDataNodeLocation targetDataNode) {
    CnToDnAsyncRequestType requestType = context.getRequestType();
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
      case INVALIDATE_LAST_CACHE:
      case CLEAN_DATA_NODE_CACHE:
      case STOP_AND_CLEAR_DATA_NODE:
      case START_REPAIR_DATA:
      case STOP_REPAIR_DATA:
      case LOAD_CONFIGURATION:
      case SET_SYSTEM_STATUS:
      case UPDATE_REGION_ROUTE_MAP:
      case INVALIDATE_SCHEMA_CACHE:
      case INVALIDATE_MATCHED_SCHEMA_CACHE:
      case UPDATE_TEMPLATE:
      case UPDATE_TABLE:
      case KILL_QUERY_INSTANCE:
      case RESET_PEER_LIST:
      case TEST_CONNECTION:
      default:
        return new DataNodeTSStatusRPCHandler(
            requestType,
            requestId,
            targetDataNode,
            dataNodeLocationMap,
            (Map<Integer, TSStatus>) responseMap,
            countDownLatch);
    }
  }
}
