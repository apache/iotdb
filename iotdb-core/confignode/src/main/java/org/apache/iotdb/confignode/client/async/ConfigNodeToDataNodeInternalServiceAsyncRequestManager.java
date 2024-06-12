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
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.commons.client.request.DataNodeInternalServiceRequestManager;
import org.apache.iotdb.confignode.client.ConfigNodeToDataNodeRequestType;
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
public class ConfigNodeToDataNodeInternalServiceAsyncRequestManager
    extends DataNodeInternalServiceRequestManager<ConfigNodeToDataNodeRequestType> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConfigNodeToDataNodeInternalServiceAsyncRequestManager.class);

  @Override
  protected void initActionMapBuilder() {
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.SET_TTL,
        (req, client, handler) ->
            client.setTTL((TSetTTLReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CREATE_DATA_REGION,
        (req, client, handler) ->
            client.createDataRegion(
                (TCreateDataRegionReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DELETE_REGION,
        (req, client, handler) ->
            client.deleteRegion((TConsensusGroupId) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CREATE_SCHEMA_REGION,
        (req, client, handler) ->
            client.createSchemaRegion(
                (TCreateSchemaRegionReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CREATE_FUNCTION,
        (req, client, handler) ->
            client.createFunction(
                (TCreateFunctionInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DROP_FUNCTION,
        (req, client, handler) ->
            client.dropFunction(
                (TDropFunctionInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CREATE_TRIGGER_INSTANCE,
        (req, client, handler) ->
            client.createTriggerInstance(
                (TCreateTriggerInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DROP_TRIGGER_INSTANCE,
        (req, client, handler) ->
            client.dropTriggerInstance(
                (TDropTriggerInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.ACTIVE_TRIGGER_INSTANCE,
        (req, client, handler) ->
            client.activeTriggerInstance(
                (TActiveTriggerInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.INACTIVE_TRIGGER_INSTANCE,
        (req, client, handler) ->
            client.inactiveTriggerInstance(
                (TInactiveTriggerInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.UPDATE_TRIGGER_LOCATION,
        (req, client, handler) ->
            client.updateTriggerLocation(
                (TUpdateTriggerLocationReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CREATE_PIPE_PLUGIN,
        (req, client, handler) ->
            client.createPipePlugin(
                (TCreatePipePluginInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DROP_PIPE_PLUGIN,
        (req, client, handler) ->
            client.dropPipePlugin(
                (TDropPipePluginInstanceReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.PIPE_PUSH_ALL_META,
        (req, client, handler) ->
            client.pushPipeMeta((TPushPipeMetaReq) req, (PipePushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.PIPE_PUSH_SINGLE_META,
        (req, client, handler) ->
            client.pushSinglePipeMeta(
                (TPushSinglePipeMetaReq) req, (PipePushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.PIPE_PUSH_MULTI_META,
        (req, client, handler) ->
            client.pushMultiPipeMeta(
                (TPushMultiPipeMetaReq) req, (PipePushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.TOPIC_PUSH_ALL_META,
        (req, client, handler) ->
            client.pushTopicMeta((TPushTopicMetaReq) req, (TopicPushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.TOPIC_PUSH_SINGLE_META,
        (req, client, handler) ->
            client.pushSingleTopicMeta(
                (TPushSingleTopicMetaReq) req, (TopicPushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.TOPIC_PUSH_MULTI_META,
        (req, client, handler) ->
            client.pushMultiTopicMeta(
                (TPushMultiTopicMetaReq) req, (TopicPushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CONSUMER_GROUP_PUSH_ALL_META,
        (req, client, handler) ->
            client.pushConsumerGroupMeta(
                (TPushConsumerGroupMetaReq) req, (ConsumerGroupPushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CONSUMER_GROUP_PUSH_SINGLE_META,
        (req, client, handler) ->
            client.pushSingleConsumerGroupMeta(
                (TPushSingleConsumerGroupMetaReq) req, (ConsumerGroupPushMetaRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.PIPE_HEARTBEAT,
        (req, client, handler) ->
            client.pipeHeartbeat((TPipeHeartbeatReq) req, (PipeHeartbeatRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.MERGE,
        (req, client, handler) -> client.merge((DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.FULL_MERGE,
        (req, client, handler) -> client.merge((DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.FLUSH,
        (req, client, handler) ->
            client.flush((TFlushReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CLEAR_CACHE,
        (req, client, handler) -> client.clearCache((DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.START_REPAIR_DATA,
        (req, client, handler) -> client.startRepairData((DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.STOP_REPAIR_DATA,
        (req, client, handler) -> client.stopRepairData((DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.LOAD_CONFIGURATION,
        (req, client, handler) -> client.loadConfiguration((DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.SET_SYSTEM_STATUS,
        (req, client, handler) ->
            client.setSystemStatus((String) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.UPDATE_REGION_ROUTE_MAP,
        (req, client, handler) ->
            client.updateRegionCache((TRegionRouteReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CHANGE_REGION_LEADER,
        (req, client, handler) ->
            client.changeRegionLeader(
                (TRegionLeaderChangeReq) req, (TransferLeaderRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CONSTRUCT_SCHEMA_BLACK_LIST,
        (req, client, handler) ->
            client.constructSchemaBlackList(
                (TConstructSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.ROLLBACK_SCHEMA_BLACK_LIST,
        (req, client, handler) ->
            client.rollbackSchemaBlackList(
                (TRollbackSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.FETCH_SCHEMA_BLACK_LIST,
        (req, client, handler) ->
            client.fetchSchemaBlackList(
                (TFetchSchemaBlackListReq) req, (FetchSchemaBlackListRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
        (req, client, handler) ->
            client.invalidateMatchedSchemaCache(
                (TInvalidateMatchedSchemaCacheReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
        (req, client, handler) ->
            client.deleteDataForDeleteSchema(
                (TDeleteDataForDeleteSchemaReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DELETE_TIMESERIES,
        (req, client, handler) ->
            client.deleteTimeSeries((TDeleteTimeSeriesReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE,
        (req, client, handler) ->
            client.constructSchemaBlackListWithTemplate(
                (TConstructSchemaBlackListWithTemplateReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.ROLLBACK_SCHEMA_BLACK_LIST_WITH_TEMPLATE,
        (req, client, handler) ->
            client.rollbackSchemaBlackListWithTemplate(
                (TRollbackSchemaBlackListWithTemplateReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DEACTIVATE_TEMPLATE,
        (req, client, handler) ->
            client.deactivateTemplate(
                (TDeactivateTemplateReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.UPDATE_TEMPLATE,
        (req, client, handler) ->
            client.updateTemplate((TUpdateTemplateReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.COUNT_PATHS_USING_TEMPLATE,
        (req, client, handler) ->
            client.countPathsUsingTemplate(
                (TCountPathsUsingTemplateReq) req, (CountPathsUsingTemplateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CHECK_SCHEMA_REGION_USING_TEMPLATE,
        (req, client, handler) ->
            client.checkSchemaRegionUsingTemplate(
                (TCheckSchemaRegionUsingTemplateReq) req,
                (CheckSchemaRegionUsingTemplateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CHECK_TIMESERIES_EXISTENCE,
        (req, client, handler) ->
            client.checkTimeSeriesExistence(
                (TCheckTimeSeriesExistenceReq) req, (CheckTimeSeriesExistenceRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.CONSTRUCT_VIEW_SCHEMA_BLACK_LIST,
        (req, client, handler) ->
            client.constructViewSchemaBlackList(
                (TConstructViewSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.ROLLBACK_VIEW_SCHEMA_BLACK_LIST,
        (req, client, handler) ->
            client.rollbackViewSchemaBlackList(
                (TRollbackViewSchemaBlackListReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.DELETE_VIEW,
        (req, client, handler) ->
            client.deleteViewSchema((TDeleteViewSchemaReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.ALTER_VIEW,
        (req, client, handler) ->
            client.alterView((TAlterViewReq) req, (SchemaUpdateRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.KILL_QUERY_INSTANCE,
        (req, client, handler) ->
            client.killQueryInstance((String) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.SET_SPACE_QUOTA,
        (req, client, handler) ->
            client.setSpaceQuota((TSetSpaceQuotaReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.SET_THROTTLE_QUOTA,
        (req, client, handler) ->
            client.setThrottleQuota(
                (TSetThrottleQuotaReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.RESET_PEER_LIST,
        (req, client, handler) ->
            client.resetPeerList((TResetPeerListReq) req, (DataNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.SUBMIT_TEST_CONNECTION_TASK,
        (req, client, handler) ->
            client.submitTestConnectionTask(
                (TNodeLocations) req, (SubmitTestConnectionTaskRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToDataNodeRequestType.TEST_CONNECTION,
        (req, client, handler) -> client.testConnection((DataNodeTSStatusRPCHandler) handler));
  }

  @Override
  protected AsyncRequestRPCHandler<?, ConfigNodeToDataNodeRequestType, TDataNodeLocation>
      buildHandler(
          AsyncRequestContext<?, ?, ConfigNodeToDataNodeRequestType, TDataNodeLocation>
              requestContext,
          int requestId,
          TDataNodeLocation targetNode) {
    return DataNodeAsyncRequestRPCHandler.buildHandler(requestContext, requestId, targetNode);
  }

  private static class ClientPoolHolder {

    private static final ConfigNodeToDataNodeInternalServiceAsyncRequestManager INSTANCE =
        new ConfigNodeToDataNodeInternalServiceAsyncRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static ConfigNodeToDataNodeInternalServiceAsyncRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
