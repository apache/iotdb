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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.gg.AsyncRequestContext;
import org.apache.iotdb.commons.client.gg.AsyncRequestManager;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.rpc.CheckTimeSeriesExistenceRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.CountPathsUsingTemplateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.DataNodeAsyncRequestRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.DataNodeTSStatusRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.FetchSchemaBlackListRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.PipeHeartbeatRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.PipePushMetaRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.SchemaUpdateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.SubmitTestConnectionTaskRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.TransferLeaderRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.subscription.CheckSchemaRegionUsingTemplateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.subscription.ConsumerGroupPushMetaRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.subscription.TopicPushMetaRPCHandler;
import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TAlterViewReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructViewSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeactivateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteViewSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropPipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSinglePipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackViewSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTriggerLocationReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Asynchronously send RPC requests to DataNodes. See queryengine.thrift for more details. */
public class AsyncDataNodeInternalServiceRequestManager
    extends AsyncRequestManager<
        DataNodeRequestType, TDataNodeLocation, AsyncDataNodeInternalServiceClient> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncDataNodeInternalServiceRequestManager.class);

  private AsyncDataNodeInternalServiceRequestManager() {
    super();
  }

  @Override
  protected void initClientManager() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
  }

  @Override
  protected TEndPoint nodeLocationToEndPoint(TDataNodeLocation dataNodeLocation) {
    return dataNodeLocation.getInternalEndPoint();
  }

  @Override
  protected void sendAsyncRequestToNode(
      AsyncRequestContext<?, ?, DataNodeRequestType, TDataNodeLocation> requestContext,
      int requestId,
      TDataNodeLocation targetNode,
      int retryCount) {

    try {
      AsyncDataNodeInternalServiceClient client;
      client = clientManager.borrowClient(nodeLocationToEndPoint(targetNode));
      Object req = requestContext.getRequest(requestId);
      DataNodeAsyncRequestRPCHandler<?> handler =
          DataNodeAsyncRequestRPCHandler.buildHandler(requestContext, requestId, targetNode);

      DataNodeTSStatusRPCHandler defaultHandler = null;
      if (handler instanceof DataNodeTSStatusRPCHandler) {
        defaultHandler = (DataNodeTSStatusRPCHandler) handler;
      }

      switch (requestContext.getRequestType()) {
        case SET_TTL:
          client.setTTL((TSetTTLReq) req, defaultHandler);
          break;
        case CREATE_DATA_REGION:
          client.createDataRegion((TCreateDataRegionReq) req, defaultHandler);
          break;
        case DELETE_REGION:
          client.deleteRegion((TConsensusGroupId) req, defaultHandler);
          break;
        case CREATE_SCHEMA_REGION:
          client.createSchemaRegion((TCreateSchemaRegionReq) req, defaultHandler);
          break;
        case CREATE_FUNCTION:
          client.createFunction((TCreateFunctionInstanceReq) req, defaultHandler);
          break;
        case DROP_FUNCTION:
          client.dropFunction((TDropFunctionInstanceReq) req, defaultHandler);
          break;
        case CREATE_TRIGGER_INSTANCE:
          client.createTriggerInstance((TCreateTriggerInstanceReq) req, defaultHandler);
          break;
        case DROP_TRIGGER_INSTANCE:
          client.dropTriggerInstance((TDropTriggerInstanceReq) req, defaultHandler);
          break;
        case ACTIVE_TRIGGER_INSTANCE:
          client.activeTriggerInstance((TActiveTriggerInstanceReq) req, defaultHandler);
          break;
        case INACTIVE_TRIGGER_INSTANCE:
          client.inactiveTriggerInstance((TInactiveTriggerInstanceReq) req, defaultHandler);
          break;
        case UPDATE_TRIGGER_LOCATION:
          client.updateTriggerLocation((TUpdateTriggerLocationReq) req, defaultHandler);
          break;
        case CREATE_PIPE_PLUGIN:
          client.createPipePlugin((TCreatePipePluginInstanceReq) req, defaultHandler);
          break;
        case DROP_PIPE_PLUGIN:
          client.dropPipePlugin((TDropPipePluginInstanceReq) req, defaultHandler);
          break;
        case PIPE_PUSH_ALL_META:
          client.pushPipeMeta((TPushPipeMetaReq) req, (PipePushMetaRPCHandler) handler);
          break;
        case PIPE_PUSH_SINGLE_META:
          client.pushSinglePipeMeta((TPushSinglePipeMetaReq) req, (PipePushMetaRPCHandler) handler);
          break;
        case PIPE_PUSH_MULTI_META:
          client.pushMultiPipeMeta((TPushMultiPipeMetaReq) req, (PipePushMetaRPCHandler) handler);
          break;
        case TOPIC_PUSH_ALL_META:
          client.pushTopicMeta((TPushTopicMetaReq) req, (TopicPushMetaRPCHandler) handler);
          break;
        case TOPIC_PUSH_SINGLE_META:
          client.pushSingleTopicMeta(
              (TPushSingleTopicMetaReq) req, (TopicPushMetaRPCHandler) handler);
          break;
        case TOPIC_PUSH_MULTI_META:
          client.pushMultiTopicMeta(
              (TPushMultiTopicMetaReq) req, (TopicPushMetaRPCHandler) handler);
          break;
        case CONSUMER_GROUP_PUSH_ALL_META:
          client.pushConsumerGroupMeta(
              (TPushConsumerGroupMetaReq) req, (ConsumerGroupPushMetaRPCHandler) handler);
          break;
        case CONSUMER_GROUP_PUSH_SINGLE_META:
          client.pushSingleConsumerGroupMeta(
              (TPushSingleConsumerGroupMetaReq) req, (ConsumerGroupPushMetaRPCHandler) handler);
          break;
        case PIPE_HEARTBEAT:
          client.pipeHeartbeat((TPipeHeartbeatReq) req, (PipeHeartbeatRPCHandler) handler);
          break;
        case MERGE:
        case FULL_MERGE:
          client.merge(defaultHandler);
          break;
        case FLUSH:
          client.flush((TFlushReq) req, defaultHandler);
          break;
        case CLEAR_CACHE:
          client.clearCache(defaultHandler);
          break;
        case START_REPAIR_DATA:
          client.startRepairData(defaultHandler);
          break;
        case STOP_REPAIR_DATA:
          client.stopRepairData(defaultHandler);
          break;
        case LOAD_CONFIGURATION:
          client.loadConfiguration(defaultHandler);
          break;
        case SET_SYSTEM_STATUS:
          client.setSystemStatus((String) req, defaultHandler);
          break;
        case UPDATE_REGION_ROUTE_MAP:
          client.updateRegionCache((TRegionRouteReq) req, defaultHandler);
          break;
        case CHANGE_REGION_LEADER:
          client.changeRegionLeader(
              (TRegionLeaderChangeReq) req, (TransferLeaderRPCHandler) handler);
          break;
        case CONSTRUCT_SCHEMA_BLACK_LIST:
          client.constructSchemaBlackList(
              (TConstructSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case ROLLBACK_SCHEMA_BLACK_LIST:
          client.rollbackSchemaBlackList(
              (TRollbackSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case FETCH_SCHEMA_BLACK_LIST:
          client.fetchSchemaBlackList(
              (TFetchSchemaBlackListReq) req, (FetchSchemaBlackListRPCHandler) handler);
          break;
        case INVALIDATE_MATCHED_SCHEMA_CACHE:
          client.invalidateMatchedSchemaCache(
              (TInvalidateMatchedSchemaCacheReq) req, defaultHandler);
          break;
        case DELETE_DATA_FOR_DELETE_SCHEMA:
          client.deleteDataForDeleteSchema(
              (TDeleteDataForDeleteSchemaReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case DELETE_TIMESERIES:
          client.deleteTimeSeries((TDeleteTimeSeriesReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
          client.constructSchemaBlackListWithTemplate(
              (TConstructSchemaBlackListWithTemplateReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case ROLLBACK_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
          client.rollbackSchemaBlackListWithTemplate(
              (TRollbackSchemaBlackListWithTemplateReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case DEACTIVATE_TEMPLATE:
          client.deactivateTemplate((TDeactivateTemplateReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case UPDATE_TEMPLATE:
          client.updateTemplate((TUpdateTemplateReq) req, defaultHandler);
          break;
        case COUNT_PATHS_USING_TEMPLATE:
          client.countPathsUsingTemplate(
              (TCountPathsUsingTemplateReq) req, (CountPathsUsingTemplateRPCHandler) handler);
          break;
        case CHECK_SCHEMA_REGION_USING_TEMPLATE:
          client.checkSchemaRegionUsingTemplate(
              (TCheckSchemaRegionUsingTemplateReq) req,
              (CheckSchemaRegionUsingTemplateRPCHandler) handler);
          break;
        case CHECK_TIMESERIES_EXISTENCE:
          client.checkTimeSeriesExistence(
              (TCheckTimeSeriesExistenceReq) req, (CheckTimeSeriesExistenceRPCHandler) handler);
          break;
        case CONSTRUCT_VIEW_SCHEMA_BLACK_LIST:
          client.constructViewSchemaBlackList(
              (TConstructViewSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case ROLLBACK_VIEW_SCHEMA_BLACK_LIST:
          client.rollbackViewSchemaBlackList(
              (TRollbackViewSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case DELETE_VIEW:
          client.deleteViewSchema((TDeleteViewSchemaReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case ALTER_VIEW:
          client.alterView((TAlterViewReq) req, (SchemaUpdateRPCHandler) handler);
          break;
        case KILL_QUERY_INSTANCE:
          client.killQueryInstance((String) req, defaultHandler);
          break;
        case SET_SPACE_QUOTA:
          client.setSpaceQuota((TSetSpaceQuotaReq) req, defaultHandler);
          break;
        case SET_THROTTLE_QUOTA:
          client.setThrottleQuota((TSetThrottleQuotaReq) req, defaultHandler);
          break;
        case RESET_PEER_LIST:
          client.resetPeerList((TResetPeerListReq) req, defaultHandler);
          break;
        case SUBMIT_TEST_CONNECTION_TASK:
          client.submitTestConnectionTask(
              (TNodeLocations) req, (SubmitTestConnectionTaskRPCHandler) handler);
          break;
        case TEST_CONNECTION:
          client.testConnection(defaultHandler);
          break;
        default:
          LOGGER.error(
              "Unexpected DataNode Request Type: {} when sendAsyncRequestToDataNode",
              requestContext.getRequestType());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on DataNode {}, because {}, retrying {}...",
          requestContext.getRequestType(),
          nodeLocationToEndPoint(targetNode),
          e.getMessage(),
          retryCount);
    }
  }

    private static class ClientPoolHolder {

    private static final AsyncDataNodeInternalServiceRequestManager INSTANCE =
        new AsyncDataNodeInternalServiceRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncDataNodeInternalServiceRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
