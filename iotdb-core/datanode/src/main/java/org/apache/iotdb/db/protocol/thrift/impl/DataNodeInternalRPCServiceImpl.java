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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TLoadSample;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSender;
import org.apache.iotdb.common.rpc.thrift.TServiceType;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSettleReq;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.cache.CacheClearOptions;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCType;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.client.cn.DnToCnInternalServiceAsyncRequestManager;
import org.apache.iotdb.db.protocol.client.cn.DnToCnRequestType;
import org.apache.iotdb.db.protocol.client.dn.DataNodeExternalServiceAsyncRequestManager;
import org.apache.iotdb.db.protocol.client.dn.DataNodeMPPServiceAsyncRequestManager;
import org.apache.iotdb.db.protocol.client.dn.DnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.db.protocol.client.dn.DnToDnRequestType;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.executor.RegionExecutionResult;
import org.apache.iotdb.db.queryengine.execution.executor.RegionReadExecutor;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceFailureInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.SchemaSourceFactory;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.PreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.RollbackPreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.RollbackSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.ConstructLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.DeleteLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.RollbackLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedNonWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.ConstructTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDevicesInBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.RollbackTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableAttributeColumnDropNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableSchemaQueryWriteVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.service.DataNode;
import org.apache.iotdb.db.service.RegionMigrateService;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairTaskStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.settle.SettleRequestHandler;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.rescon.quotas.DataNodeSpaceQuotaManager;
import org.apache.iotdb.db.storageengine.rescon.quotas.DataNodeThrottleQuotaManager;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.trigger.executor.TriggerFireResult;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TAlterViewReq;
import org.apache.iotdb.mpp.rpc.thrift.TAttributeUpdateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelPlanFragmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelResp;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceResp;
import org.apache.iotdb.mpp.rpc.thrift.TCleanDataNodeCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructViewSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TDeactivateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteColumnDataReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataOrDevicesForDropTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteViewSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropPipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TExecuteCQ;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceInfoReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListResp;
import org.apache.iotdb.mpp.rpc.thrift.TFireTriggerReq;
import org.apache.iotdb.mpp.rpc.thrift.TFireTriggerResp;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceInfoResp;
import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateColumnCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateTableCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaRespExceptionMessage;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSinglePipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaRespExceptionMessage;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackViewSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchRequest;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchResponse;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TTableDeviceDeletionWithPatternAndFilterReq;
import org.apache.iotdb.mpp.rpc.thrift.TTableDeviceDeletionWithPatternOrModReq;
import org.apache.iotdb.mpp.rpc.thrift.TTableDeviceInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTriggerLocationReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.client.request.TestConnectionUtils.testConnectionsImpl;
import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.service.RegionMigrateService.REGION_MIGRATE_PROCESS;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class DataNodeInternalRPCServiceImpl implements IDataNodeRPCService.Iface {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataNodeInternalRPCServiceImpl.class);

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;

  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final StorageEngine storageEngine = StorageEngine.getInstance();

  private final TableDeviceSchemaCache tableDeviceSchemaCache =
      TableDeviceSchemaCache.getInstance();

  private final DataNodeRegionManager regionManager = DataNodeRegionManager.getInstance();

  private final DataNodeSpaceQuotaManager spaceQuotaManager =
      DataNodeSpaceQuotaManager.getInstance();

  private final DataNodeThrottleQuotaManager throttleQuotaManager =
      DataNodeThrottleQuotaManager.getInstance();

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private static final String SYSTEM = "system";

  public DataNodeInternalRPCServiceImpl() {
    super();
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
  }

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(final TSendFragmentInstanceReq req) {
    LOGGER.debug("receive FragmentInstance to group[{}]", req.getConsensusGroupId());

    // Deserialize ConsensusGroupId
    ConsensusGroupId groupId = null;
    if (req.consensusGroupId != null) {
      try {
        groupId = ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
      } catch (final Exception e) {
        LOGGER.warn("Deserialize ConsensusGroupId failed. ", e);
        final TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp(false);
        resp.setMessage("Deserialize ConsensusGroupId failed: " + e.getMessage());
        return resp;
      }
    }

    // We deserialize here instead of the underlying state machine because parallelism is possible
    // here but not at the underlying state machine
    final FragmentInstance fragmentInstance;
    try {
      fragmentInstance = FragmentInstance.deserializeFrom(req.fragmentInstance.body);
    } catch (final Exception e) {
      LOGGER.warn("Deserialize FragmentInstance failed.", e);
      TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp(false);
      resp.setMessage("Deserialize FragmentInstance failed: " + e.getMessage());
      return resp;
    }

    RegionExecutionResult executionResult = null;
    if (Objects.nonNull(groupId)) {
      // For schema query, there may be a "write" before "read"
      // the result is not null iff the "write" has failed
      executionResult =
          new TableSchemaQueryWriteVisitor().processFragment(fragmentInstance, groupId);
    }
    if (Objects.isNull(executionResult)) {
      final RegionReadExecutor executor = new RegionReadExecutor();
      executionResult =
          groupId == null
              ? executor.execute(fragmentInstance)
              : executor.execute(groupId, fragmentInstance);
    }

    final TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp();
    resp.setAccepted(executionResult.isAccepted());
    resp.setMessage(executionResult.getMessage());
    resp.setNeedRetry(executionResult.isReadNeedRetry());
    resp.setStatus(executionResult.getStatus());
    return resp;
  }

  @Override
  public TSendBatchPlanNodeResp sendBatchPlanNode(TSendBatchPlanNodeReq req) {
    List<TSendSinglePlanNodeResp> responses =
        req.getRequests().stream()
            .map(
                request -> {
                  ConsensusGroupId groupId =
                      ConsensusGroupId.Factory.createFromTConsensusGroupId(
                          request.getConsensusGroupId());
                  PlanNode planNode = PlanNodeType.deserialize(request.planNode.body);
                  RegionWriteExecutor executor = new RegionWriteExecutor();
                  TSendSinglePlanNodeResp resp = new TSendSinglePlanNodeResp();
                  RegionExecutionResult executionResult = executor.execute(groupId, planNode);
                  resp.setAccepted(executionResult.isAccepted());
                  resp.setMessage(executionResult.getMessage());
                  resp.setStatus(executionResult.getStatus());
                  return resp;
                })
            .collect(Collectors.toList());
    return new TSendBatchPlanNodeResp(responses);
  }

  @Override
  public TFragmentInstanceInfoResp fetchFragmentInstanceInfo(TFetchFragmentInstanceInfoReq req) {
    FragmentInstanceId instanceId = FragmentInstanceId.fromThrift(req.fragmentInstanceId);
    FragmentInstanceInfo info = FragmentInstanceManager.getInstance().getInstanceInfo(instanceId);
    if (info != null) {
      TFragmentInstanceInfoResp resp = new TFragmentInstanceInfoResp(info.getState().toString());
      resp.setEndTime(info.getEndTime());
      resp.setFailedMessages(ImmutableList.of(info.getMessage()));
      try {
        List<ByteBuffer> failureInfoList = new ArrayList<>();
        for (FragmentInstanceFailureInfo failureInfo : info.getFailureInfoList()) {
          failureInfoList.add(failureInfo.serialize());
        }
        resp.setFailureInfoList(failureInfoList);
        info.getErrorCode().ifPresent(resp::setErrorCode);
        return resp;
      } catch (IOException e) {
        return resp;
      }
    } else {
      return new TFragmentInstanceInfoResp(FragmentInstanceState.NO_SUCH_INSTANCE.toString());
    }
  }

  @Override
  public TCancelResp cancelQuery(TCancelQueryReq req) {
    try (SetThreadName threadName = new SetThreadName(req.getQueryId())) {
      List<FragmentInstanceId> taskIds =
          req.getFragmentInstanceIds().stream()
              .map(FragmentInstanceId::fromThrift)
              .collect(Collectors.toList());
      for (FragmentInstanceId taskId : taskIds) {
        FragmentInstanceManager.getInstance().cancelTask(taskId, req.hasThrowable);
      }
      return new TCancelResp(true);
    }
  }

  @Override
  public TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req) {
    throw new NotImplementedException();
  }

  @Override
  public TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req) {
    throw new NotImplementedException();
  }

  @Override
  public TSchemaFetchResponse fetchSchema(final TSchemaFetchRequest req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TLoadResp sendTsFilePieceNode(final TTsFilePieceReq req) {
    LOGGER.info("Receive load node from uuid {}.", req.uuid);

    final ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    final LoadTsFilePieceNode pieceNode = (LoadTsFilePieceNode) PlanNodeType.deserialize(req.body);
    if (pieceNode == null) {
      return createTLoadResp(
          new TSStatus(TSStatusCode.DESERIALIZE_PIECE_OF_TSFILE_ERROR.getStatusCode()));
    }
    final TSStatus resultStatus =
        StorageEngine.getInstance()
            .writeLoadTsFileNode((DataRegionId) groupId, pieceNode, req.uuid);

    return createTLoadResp(resultStatus);
  }

  @Override
  public TLoadResp sendLoadCommand(final TLoadCommandReq req) {
    final ProgressIndex progressIndex;
    if (req.isSetProgressIndex()) {
      progressIndex = ProgressIndexType.deserializeFrom(ByteBuffer.wrap(req.getProgressIndex()));
    } else {
      // fallback to use local generated progress index for compatibility
      progressIndex = PipeDataNodeAgent.runtime().getNextProgressIndexForTsFileLoad();
      LOGGER.info(
          "Use local generated load progress index {} for uuid {}.", progressIndex, req.uuid);
    }

    return createTLoadResp(
        StorageEngine.getInstance()
            .executeLoadCommand(
                LoadTsFileScheduler.LoadCommand.values()[req.commandType],
                req.uuid,
                req.isSetIsGeneratedByPipe() && req.isGeneratedByPipe,
                progressIndex));
  }

  @Override
  public TSStatus updateAttribute(final TAttributeUpdateReq req) {
    TableDeviceSchemaFetcher.getInstance().getAttributeGuard().handleAttributeUpdate(req);
    return RpcUtils.SUCCESS_STATUS;
  }

  private TLoadResp createTLoadResp(final TSStatus resultStatus) {
    final boolean isAccepted = RpcUtils.SUCCESS_STATUS.equals(resultStatus);
    final TLoadResp loadResp = new TLoadResp(isAccepted);
    if (!isAccepted) {
      loadResp.setMessage(resultStatus.getMessage());
      loadResp.setStatus(resultStatus);
    }
    return loadResp;
  }

  @Override
  public TSStatus createSchemaRegion(final TCreateSchemaRegionReq req) {
    return regionManager.createSchemaRegion(req.getRegionReplicaSet(), req.getStorageGroup());
  }

  @Override
  public TSStatus createDataRegion(TCreateDataRegionReq req) {
    return regionManager.createDataRegion(req.getRegionReplicaSet(), req.getStorageGroup());
  }

  @Override
  public TSStatus invalidatePartitionCache(final TInvalidateCacheReq req) {
    ClusterPartitionFetcher.getInstance().invalidAllCache();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus invalidateLastCache(final String database) {
    TreeDeviceSchemaCacheManager.getInstance().invalidateDatabaseLastCache(database);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus invalidateSchemaCache(final TInvalidateCacheReq req) {
    DataNodeSchemaLockManager.getInstance().takeWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    TreeDeviceSchemaCacheManager.getInstance().takeWriteLock();
    try {
      // req.getFullPath() is a database path
      ClusterTemplateManager.getInstance().invalid(req.getFullPath());
      // clear table related cache
      final String database = req.getFullPath().substring(5);
      DataNodeTableCache.getInstance().invalid(database);
      tableDeviceSchemaCache.invalidate(database);
      LOGGER.info("Schema cache of {} has been invalidated", req.getFullPath());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      TreeDeviceSchemaCacheManager.getInstance().releaseWriteLock();
      DataNodeSchemaLockManager.getInstance().releaseWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    }
  }

  @Override
  public TSStatus constructSchemaBlackList(final TConstructSchemaBlackListReq req)
      throws TException {
    final PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    final AtomicLong preDeletedNum = new AtomicLong(0);
    final TSStatus executionResult =
        executeSchemaBlackListTask(
            req.getSchemaRegionIdList(),
            consensusGroupId -> {
              final String storageGroup =
                  schemaEngine
                      .getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()))
                      .getDatabaseFullPath();
              final PathPatternTree filteredPatternTree =
                  filterPathPatternTree(patternTree, storageGroup);
              if (filteredPatternTree.isEmpty()) {
                return new TSStatus(TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode());
              }
              final RegionWriteExecutor executor = new RegionWriteExecutor();
              final TSStatus status =
                  executor
                      .execute(
                          new SchemaRegionId(consensusGroupId.getId()),
                          new ConstructSchemaBlackListNode(new PlanNodeId(""), filteredPatternTree))
                      .getStatus();
              if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                  || status.code == TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode()) {
                preDeletedNum.getAndAdd(Long.parseLong(status.getMessage()));
              }
              return status;
            });
    executionResult.setMessage(String.valueOf(preDeletedNum.get()));
    return executionResult;
  }

  @Override
  public TSStatus rollbackSchemaBlackList(TRollbackSchemaBlackListReq req) {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return executeInternalSchemaTask(
        req.getSchemaRegionIdList(),
        consensusGroupId -> {
          String storageGroup =
              schemaEngine
                  .getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()))
                  .getDatabaseFullPath();
          PathPatternTree filteredPatternTree = filterPathPatternTree(patternTree, storageGroup);
          if (filteredPatternTree.isEmpty()) {
            return RpcUtils.SUCCESS_STATUS;
          }
          RegionWriteExecutor executor = new RegionWriteExecutor();
          return executor
              .execute(
                  new SchemaRegionId(consensusGroupId.getId()),
                  new RollbackSchemaBlackListNode(new PlanNodeId(""), filteredPatternTree))
              .getStatus();
        });
  }

  @Override
  public TSStatus invalidateMatchedSchemaCache(TInvalidateMatchedSchemaCacheReq req) {
    TreeDeviceSchemaCacheManager cache = TreeDeviceSchemaCacheManager.getInstance();
    DataNodeSchemaLockManager.getInstance().takeWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    cache.takeWriteLock();
    try {
      cache.invalidate(PathPatternTree.deserialize(req.pathPatternTree).getAllPathPatterns(true));
    } finally {
      cache.releaseWriteLock();
      DataNodeSchemaLockManager.getInstance().releaseWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public TFetchSchemaBlackListResp fetchSchemaBlackList(TFetchSchemaBlackListReq req) {
    PathPatternTree patternTree = PathPatternTree.deserialize(req.pathPatternTree);
    TFetchSchemaBlackListResp resp = new TFetchSchemaBlackListResp();
    PathPatternTree result = new PathPatternTree();
    for (TConsensusGroupId consensusGroupId : req.getSchemaRegionIdList()) {
      // todo implement as consensus layer read request
      try {
        ISchemaRegion schemaRegion =
            schemaEngine.getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()));
        PathPatternTree filteredPatternTree =
            filterPathPatternTree(patternTree, schemaRegion.getDatabaseFullPath());
        if (filteredPatternTree.isEmpty()) {
          continue;
        }
        for (PartialPath path : schemaRegion.fetchSchemaBlackList(filteredPatternTree)) {
          result.appendFullPath(path);
        }
      } catch (MetadataException e) {
        LOGGER.warn(e.getMessage(), e);
        resp.setStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        return resp;
      }
    }
    resp.setStatus(RpcUtils.SUCCESS_STATUS);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    result.constructTree();
    try {
      result.serialize(dataOutputStream);
    } catch (IOException ignored) {
      // Won't reach here
    }
    resp.setPathPatternTree(outputStream.toByteArray());
    return resp;
  }

  @Override
  public TSStatus deleteDataForDeleteSchema(final TDeleteDataForDeleteSchemaReq req) {
    final List<MeasurementPath> pathList =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()))
            .getAllPathPatterns(true);
    return executeInternalSchemaTask(
        req.getDataRegionIdList(),
        consensusGroupId ->
            new RegionWriteExecutor()
                .execute(
                    new DataRegionId(consensusGroupId.getId()),
                    req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe()
                        ? new PipeEnrichedDeleteDataNode(
                            new DeleteDataNode(
                                new PlanNodeId(""), pathList, Long.MIN_VALUE, Long.MAX_VALUE))
                        : new DeleteDataNode(
                            new PlanNodeId(""), pathList, Long.MIN_VALUE, Long.MAX_VALUE))
                .getStatus());
  }

  @Override
  public TSStatus deleteTimeSeries(final TDeleteTimeSeriesReq req) throws TException {
    final PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return executeInternalSchemaTask(
        req.getSchemaRegionIdList(),
        consensusGroupId -> {
          String storageGroup =
              schemaEngine
                  .getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()))
                  .getDatabaseFullPath();
          PathPatternTree filteredPatternTree = filterPathPatternTree(patternTree, storageGroup);
          if (filteredPatternTree.isEmpty()) {
            return RpcUtils.SUCCESS_STATUS;
          }
          RegionWriteExecutor executor = new RegionWriteExecutor();
          return executor
              .execute(
                  new SchemaRegionId(consensusGroupId.getId()),
                  req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe()
                      ? new PipeEnrichedNonWritePlanNode(
                          new DeleteTimeSeriesNode(new PlanNodeId(""), filteredPatternTree))
                      : new DeleteTimeSeriesNode(new PlanNodeId(""), filteredPatternTree))
              .getStatus();
        });
  }

  @Override
  public TSStatus constructSchemaBlackListWithTemplate(TConstructSchemaBlackListWithTemplateReq req)
      throws TException {
    AtomicInteger preDeactivateTemplateNum = new AtomicInteger(0);
    Map<PartialPath, List<Integer>> templateSetInfo =
        transformTemplateSetInfo(req.getTemplateSetInfo());
    TSStatus executionResult =
        executeInternalSchemaTask(
            req.getSchemaRegionIdList(),
            consensusGroupId -> {
              Map<PartialPath, List<Integer>> filteredTemplateSetInfo =
                  filterTemplateSetInfo(templateSetInfo, consensusGroupId);
              if (filteredTemplateSetInfo.isEmpty()) {
                return RpcUtils.SUCCESS_STATUS;
              }

              RegionWriteExecutor executor = new RegionWriteExecutor();
              TSStatus status =
                  executor
                      .execute(
                          new SchemaRegionId(consensusGroupId.getId()),
                          new PreDeactivateTemplateNode(
                              new PlanNodeId(""), filteredTemplateSetInfo))
                      .getStatus();
              if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                preDeactivateTemplateNum.getAndAdd(Integer.parseInt(status.getMessage()));
              }
              return status;
            });
    executionResult.setMessage(String.valueOf(preDeactivateTemplateNum.get()));
    return executionResult;
  }

  private Map<PartialPath, List<Integer>> transformTemplateSetInfo(
      Map<String, List<Integer>> rawTemplateSetInfo) {
    Map<PartialPath, List<Integer>> result = new HashMap<>();
    rawTemplateSetInfo.forEach(
        (k, v) -> {
          try {
            result.put(new PartialPath(k), v);
          } catch (IllegalPathException ignored) {
            // Won't reach here
          }
        });
    return result;
  }

  private Map<PartialPath, List<Integer>> filterTemplateSetInfo(
      Map<PartialPath, List<Integer>> templateSetInfo, TConsensusGroupId consensusGroupId) {

    Map<PartialPath, List<Integer>> result = new HashMap<>();
    PartialPath storageGroupPath = getStorageGroupPath(consensusGroupId);
    if (null != storageGroupPath) {
      PartialPath storageGroupPattern = storageGroupPath.concatNode(MULTI_LEVEL_PATH_WILDCARD);
      templateSetInfo.forEach(
          (k, v) -> {
            if (storageGroupPattern.overlapWith(k) || storageGroupPath.overlapWith(k)) {
              result.put(k, v);
            }
          });
    }
    return result;
  }

  private PartialPath getStorageGroupPath(TConsensusGroupId consensusGroupId) {
    PartialPath storageGroupPath = null;
    try {
      storageGroupPath =
          new PartialPath(
              schemaEngine
                  .getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()))
                  .getDatabaseFullPath());
    } catch (IllegalPathException ignored) {
      // Won't reach here
    }
    return storageGroupPath;
  }

  @Override
  public TSStatus rollbackSchemaBlackListWithTemplate(TRollbackSchemaBlackListWithTemplateReq req) {
    Map<PartialPath, List<Integer>> templateSetInfo =
        transformTemplateSetInfo(req.getTemplateSetInfo());
    return executeInternalSchemaTask(
        req.getSchemaRegionIdList(),
        consensusGroupId -> {
          Map<PartialPath, List<Integer>> filteredTemplateSetInfo =
              filterTemplateSetInfo(templateSetInfo, consensusGroupId);
          if (filteredTemplateSetInfo.isEmpty()) {
            return RpcUtils.SUCCESS_STATUS;
          }

          RegionWriteExecutor executor = new RegionWriteExecutor();
          return executor
              .execute(
                  new SchemaRegionId(consensusGroupId.getId()),
                  new RollbackPreDeactivateTemplateNode(
                      new PlanNodeId(""), filteredTemplateSetInfo))
              .getStatus();
        });
  }

  @Override
  public TSStatus deactivateTemplate(TDeactivateTemplateReq req) throws TException {
    Map<PartialPath, List<Integer>> templateSetInfo =
        transformTemplateSetInfo(req.getTemplateSetInfo());
    return executeInternalSchemaTask(
        req.getSchemaRegionIdList(),
        consensusGroupId -> {
          Map<PartialPath, List<Integer>> filteredTemplateSetInfo =
              filterTemplateSetInfo(templateSetInfo, consensusGroupId);
          if (filteredTemplateSetInfo.isEmpty()) {
            return RpcUtils.SUCCESS_STATUS;
          }

          RegionWriteExecutor executor = new RegionWriteExecutor();
          return executor
              .execute(
                  new SchemaRegionId(consensusGroupId.getId()),
                  req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe()
                      ? new PipeEnrichedNonWritePlanNode(
                          new DeactivateTemplateNode(new PlanNodeId(""), filteredTemplateSetInfo))
                      : new DeactivateTemplateNode(new PlanNodeId(""), filteredTemplateSetInfo))
              .getStatus();
        });
  }

  @Override
  public TCountPathsUsingTemplateResp countPathsUsingTemplate(TCountPathsUsingTemplateReq req) {
    PathPatternTree patternTree = PathPatternTree.deserialize(req.patternTree);
    TCountPathsUsingTemplateResp resp = new TCountPathsUsingTemplateResp();
    AtomicLong result = new AtomicLong(0);
    resp.setStatus(
        executeInternalSchemaTask(
            req.getSchemaRegionIdList(),
            consensusGroupId -> {
              ReadWriteLock readWriteLock =
                  regionManager.getRegionLock(new SchemaRegionId(consensusGroupId.getId()));
              // Count paths using template for unset template shall block all template activation
              readWriteLock.writeLock().lock();
              try {
                ISchemaRegion schemaRegion =
                    schemaEngine.getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()));
                PathPatternTree filteredPatternTree =
                    filterPathPatternTree(patternTree, schemaRegion.getDatabaseFullPath());
                if (filteredPatternTree.isEmpty()) {
                  return RpcUtils.SUCCESS_STATUS;
                }
                result.getAndAdd(
                    schemaRegion.countPathsUsingTemplate(req.getTemplateId(), filteredPatternTree));
                return RpcUtils.SUCCESS_STATUS;
              } catch (MetadataException e) {
                LOGGER.warn(e.getMessage(), e);
                return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
              } finally {
                readWriteLock.writeLock().unlock();
              }
            }));
    resp.setCount(result.get());
    return resp;
  }

  @Override
  public TCheckSchemaRegionUsingTemplateResp checkSchemaRegionUsingTemplate(
      TCheckSchemaRegionUsingTemplateReq req) {
    TCheckSchemaRegionUsingTemplateResp resp = new TCheckSchemaRegionUsingTemplateResp();
    AtomicBoolean result = new AtomicBoolean(false);
    resp.setStatus(
        executeInternalSchemaTask(
            req.getSchemaRegionIdList(),
            consensusGroupId -> {
              ReadWriteLock readWriteLock =
                  regionManager.getRegionLock(new SchemaRegionId(consensusGroupId.getId()));
              readWriteLock.writeLock().lock();
              try {
                ISchemaRegion schemaRegion =
                    schemaEngine.getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()));
                if (schemaRegion.getSchemaRegionStatistics().getTemplateActivatedNumber() > 0) {
                  result.set(true);
                }
                return RpcUtils.SUCCESS_STATUS;
              } finally {
                readWriteLock.writeLock().unlock();
              }
            }));
    resp.setResult(result.get());
    return resp;
  }

  @Override
  public TCheckTimeSeriesExistenceResp checkTimeSeriesExistence(
      final TCheckTimeSeriesExistenceReq req) {
    final PathPatternTree patternTree = PathPatternTree.deserialize(req.patternTree);
    final TCheckTimeSeriesExistenceResp resp = new TCheckTimeSeriesExistenceResp();
    final TSStatus status =
        executeInternalSchemaTask(
            req.getSchemaRegionIdList(),
            consensusGroupId -> {
              ReadWriteLock readWriteLock =
                  regionManager.getRegionLock(new SchemaRegionId(consensusGroupId.getId()));
              // Check timeseries existence for set template shall block all timeseries creation
              readWriteLock.writeLock().lock();
              try {
                final ISchemaRegion schemaRegion =
                    schemaEngine.getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()));
                final PathPatternTree filteredPatternTree =
                    filterPathPatternTree(patternTree, schemaRegion.getDatabaseFullPath());
                if (filteredPatternTree.isEmpty()) {
                  return RpcUtils.SUCCESS_STATUS;
                }
                for (final PartialPath pattern : filteredPatternTree.getAllPathPatterns()) {
                  ISchemaSource<ITimeSeriesSchemaInfo> schemaSource =
                      SchemaSourceFactory.getTimeSeriesSchemaCountSource(
                          pattern, false, null, null, SchemaConstant.ALL_MATCH_SCOPE);
                  try (final ISchemaReader<ITimeSeriesSchemaInfo> schemaReader =
                      schemaSource.getSchemaReader(schemaRegion)) {
                    if (schemaReader.hasNext()) {
                      return RpcUtils.getStatus(TSStatusCode.TIMESERIES_ALREADY_EXIST);
                    }
                  } catch (final Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
                  }
                }
                return RpcUtils.SUCCESS_STATUS;
              } finally {
                readWriteLock.writeLock().unlock();
              }
            });
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setStatus(RpcUtils.SUCCESS_STATUS);
      resp.setExists(false);
    } else if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      boolean hasFailure = false;
      for (final TSStatus subStatus : status.getSubStatus()) {
        if (subStatus.getCode() == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
          resp.setExists(true);
        } else if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          hasFailure = true;
          break;
        }
      }
      if (hasFailure) {
        resp.setStatus(status);
      } else {
        resp.setStatus(RpcUtils.SUCCESS_STATUS);
      }
    } else {
      resp.setStatus(status);
    }
    return resp;
  }

  @Override
  public TSStatus constructViewSchemaBlackList(final TConstructViewSchemaBlackListReq req) {
    final PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    final AtomicInteger preDeletedNum = new AtomicInteger(0);
    final TSStatus executionResult =
        executeInternalSchemaTask(
            req.getSchemaRegionIdList(),
            consensusGroupId -> {
              final String storageGroup =
                  schemaEngine
                      .getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()))
                      .getDatabaseFullPath();
              final PathPatternTree filteredPatternTree =
                  filterPathPatternTree(patternTree, storageGroup);
              if (filteredPatternTree.isEmpty()) {
                return RpcUtils.SUCCESS_STATUS;
              }
              final RegionWriteExecutor executor = new RegionWriteExecutor();
              final TSStatus status =
                  executor
                      .execute(
                          new SchemaRegionId(consensusGroupId.getId()),
                          new ConstructLogicalViewBlackListNode(
                              new PlanNodeId(""), filteredPatternTree))
                      .getStatus();
              if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                preDeletedNum.getAndAdd(Integer.parseInt(status.getMessage()));
              }
              return status;
            });
    executionResult.setMessage(String.valueOf(preDeletedNum.get()));
    return executionResult;
  }

  @Override
  public TSStatus rollbackViewSchemaBlackList(final TRollbackViewSchemaBlackListReq req) {
    final PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return executeInternalSchemaTask(
        req.getSchemaRegionIdList(),
        consensusGroupId -> {
          final String storageGroup =
              schemaEngine
                  .getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()))
                  .getDatabaseFullPath();
          final PathPatternTree filteredPatternTree =
              filterPathPatternTree(patternTree, storageGroup);
          if (filteredPatternTree.isEmpty()) {
            return RpcUtils.SUCCESS_STATUS;
          }
          final RegionWriteExecutor executor = new RegionWriteExecutor();
          return executor
              .execute(
                  new SchemaRegionId(consensusGroupId.getId()),
                  new RollbackLogicalViewBlackListNode(new PlanNodeId(""), filteredPatternTree))
              .getStatus();
        });
  }

  @Override
  public TSStatus deleteViewSchema(TDeleteViewSchemaReq req) {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return executeInternalSchemaTask(
        req.getSchemaRegionIdList(),
        consensusGroupId -> {
          String storageGroup =
              schemaEngine
                  .getSchemaRegion(new SchemaRegionId(consensusGroupId.getId()))
                  .getDatabaseFullPath();
          PathPatternTree filteredPatternTree = filterPathPatternTree(patternTree, storageGroup);
          if (filteredPatternTree.isEmpty()) {
            return RpcUtils.SUCCESS_STATUS;
          }
          RegionWriteExecutor executor = new RegionWriteExecutor();
          return executor
              .execute(
                  new SchemaRegionId(consensusGroupId.getId()),
                  req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe()
                      ? new PipeEnrichedNonWritePlanNode(
                          new DeleteLogicalViewNode(new PlanNodeId(""), filteredPatternTree))
                      : new DeleteLogicalViewNode(new PlanNodeId(""), filteredPatternTree))
              .getStatus();
        });
  }

  @Override
  public TSStatus alterView(TAlterViewReq req) {
    List<TConsensusGroupId> consensusGroupIdList = req.getSchemaRegionIdList();
    List<ByteBuffer> viewBinaryList = req.getViewBinaryList();
    Map<TConsensusGroupId, Map<PartialPath, ViewExpression>> schemaRegionRequestMap =
        new HashMap<>();
    for (int i = 0; i < consensusGroupIdList.size(); i++) {
      ByteBuffer byteBuffer = viewBinaryList.get(i);
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      Map<PartialPath, ViewExpression> viewMap = new HashMap<>();
      for (int j = 0; j < size; j++) {
        viewMap.put(
            (PartialPath) PathDeserializeUtil.deserialize(byteBuffer),
            ViewExpression.deserialize(byteBuffer));
      }
      schemaRegionRequestMap.put(consensusGroupIdList.get(i), viewMap);
    }
    return executeInternalSchemaTask(
        consensusGroupIdList,
        consensusGroupId -> {
          RegionWriteExecutor executor = new RegionWriteExecutor();
          return executor
              .execute(
                  new SchemaRegionId(consensusGroupId.getId()),
                  req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe()
                      ? new PipeEnrichedNonWritePlanNode(
                          new AlterLogicalViewNode(
                              new PlanNodeId(""), schemaRegionRequestMap.get(consensusGroupId)))
                      : new AlterLogicalViewNode(
                          new PlanNodeId(""), schemaRegionRequestMap.get(consensusGroupId)))
              .getStatus();
        });
  }

  @Override
  public TPushPipeMetaResp pushPipeMeta(final TPushPipeMetaReq req) {
    try {
      final List<TPushPipeMetaRespExceptionMessage> exceptionMessages =
          PipeDataNodeAgent.task()
              .handlePipeMetaChanges(
                  req.getPipeMetas().stream()
                      .map(PipeMeta::deserialize4TaskAgent)
                      .collect(Collectors.toList()));

      return exceptionMessages.isEmpty()
          ? new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          : new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(exceptionMessages);
    } catch (final Exception e) {
      LOGGER.error("Error occurred when pushing pipe meta", e);
      return new TPushPipeMetaResp()
          .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()));
    }
  }

  @Override
  public TPushPipeMetaResp pushSinglePipeMeta(final TPushSinglePipeMetaReq req) {
    try {
      final TPushPipeMetaRespExceptionMessage exceptionMessage;
      if (req.isSetPipeNameToDrop()) {
        exceptionMessage = PipeDataNodeAgent.task().handleDropPipe(req.getPipeNameToDrop());
      } else if (req.isSetPipeMeta()) {
        final PipeMeta pipeMeta =
            PipeMeta.deserialize4TaskAgent(ByteBuffer.wrap(req.getPipeMeta()));
        exceptionMessage = PipeDataNodeAgent.task().handleSinglePipeMetaChanges(pipeMeta);
      } else {
        throw new Exception("Invalid TPushSinglePipeMetaReq");
      }
      return exceptionMessage == null
          ? new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          : new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(Collections.singletonList(exceptionMessage));
    } catch (final Exception e) {
      LOGGER.error("Error occurred when pushing single pipe meta", e);
      return new TPushPipeMetaResp()
          .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()));
    }
  }

  @Override
  public TPushPipeMetaResp pushMultiPipeMeta(TPushMultiPipeMetaReq req) {
    boolean hasException = false;
    // If there is any exception, we use the size of exceptionMessages to record the fail index
    List<TPushPipeMetaRespExceptionMessage> exceptionMessages = new ArrayList<>();
    try {
      if (req.isSetPipeNamesToDrop()) {
        for (String pipeNameToDrop : req.getPipeNamesToDrop()) {
          TPushPipeMetaRespExceptionMessage message =
              PipeDataNodeAgent.task().handleDropPipe(pipeNameToDrop);
          exceptionMessages.add(message);
          if (message != null) {
            // If there is any exception, skip the remaining pipes
            hasException = true;
            break;
          }
        }
      } else if (req.isSetPipeMetas()) {
        for (ByteBuffer byteBuffer : req.getPipeMetas()) {
          final PipeMeta pipeMeta = PipeMeta.deserialize4TaskAgent(byteBuffer);
          TPushPipeMetaRespExceptionMessage message =
              PipeDataNodeAgent.task().handleSinglePipeMetaChanges(pipeMeta);
          exceptionMessages.add(message);
          if (message != null) {
            // If there is any exception, skip the remaining pipes
            hasException = true;
            break;
          }
        }
      } else {
        throw new Exception("Invalid TPushMultiPipeMetaReq");
      }

      return hasException
          ? new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(exceptionMessages)
          : new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (Exception e) {
      LOGGER.warn("Error occurred when pushing multi pipe meta", e);
      return new TPushPipeMetaResp()
          .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
          .setExceptionMessages(exceptionMessages);
    }
  }

  @Override
  public TPushTopicMetaResp pushTopicMeta(TPushTopicMetaReq req) {
    final List<TopicMeta> topicMetas = new ArrayList<>();
    for (ByteBuffer byteBuffer : req.getTopicMetas()) {
      topicMetas.add(TopicMeta.deserialize(byteBuffer));
    }
    try {
      TPushTopicMetaRespExceptionMessage exceptionMessage =
          SubscriptionAgent.topic().handleTopicMetaChanges(topicMetas);

      return exceptionMessage == null
          ? new TPushTopicMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          : new TPushTopicMetaResp()
              .setStatus(new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(Collections.singletonList(exceptionMessage));
    } catch (Exception e) {
      LOGGER.warn("Error occurred when pushing topic meta", e);
      return new TPushTopicMetaResp()
          .setStatus(new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode()));
    }
  }

  @Override
  public TPushTopicMetaResp pushSingleTopicMeta(TPushSingleTopicMetaReq req) {
    try {
      final TPushTopicMetaRespExceptionMessage exceptionMessage;
      if (req.isSetTopicNameToDrop()) {
        exceptionMessage = SubscriptionAgent.topic().handleDropTopic(req.getTopicNameToDrop());
      } else if (req.isSetTopicMeta()) {
        exceptionMessage =
            SubscriptionAgent.topic()
                .handleSingleTopicMetaChanges(
                    TopicMeta.deserialize(ByteBuffer.wrap(req.getTopicMeta())));
      } else {
        throw new SubscriptionException("Invalid request " + req + " from config node.");
      }

      return exceptionMessage == null
          ? new TPushTopicMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          : new TPushTopicMetaResp()
              .setStatus(new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(Collections.singletonList(exceptionMessage));
    } catch (Exception e) {
      LOGGER.warn("Error occurred when pushing single topic meta", e);
      return new TPushTopicMetaResp()
          .setStatus(new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode()));
    }
  }

  @Override
  public TPushTopicMetaResp pushMultiTopicMeta(TPushMultiTopicMetaReq req) {
    boolean hasException = false;
    // If there is any exception, we use the size of exceptionMessages to record the fail index
    List<TPushTopicMetaRespExceptionMessage> exceptionMessages = new ArrayList<>();
    try {
      if (req.isSetTopicNamesToDrop()) {
        for (String topicNameToDrop : req.getTopicNamesToDrop()) {
          TPushTopicMetaRespExceptionMessage message =
              SubscriptionAgent.topic().handleDropTopic(topicNameToDrop);
          exceptionMessages.add(message);
          if (message != null) {
            // If there is any exception, skip the remaining topics
            hasException = true;
            break;
          }
        }
      } else if (req.isSetTopicMetas()) {
        for (ByteBuffer byteBuffer : req.getTopicMetas()) {
          final TopicMeta topicMeta = TopicMeta.deserialize(byteBuffer);
          TPushTopicMetaRespExceptionMessage message =
              SubscriptionAgent.topic().handleSingleTopicMetaChanges(topicMeta);
          exceptionMessages.add(message);
          if (message != null) {
            // If there is any exception, skip the remaining pipes
            hasException = true;
            break;
          }
        }
      } else {
        throw new Exception("Invalid TPushMultiTopicMetaReq");
      }

      return hasException
          ? new TPushTopicMetaResp()
              .setStatus(new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(exceptionMessages)
          : new TPushTopicMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (Exception e) {
      LOGGER.warn("Error occurred when pushing multi topic meta", e);
      return new TPushTopicMetaResp()
          .setStatus(new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode()))
          .setExceptionMessages(exceptionMessages);
    }
  }

  @Override
  public TPushConsumerGroupMetaResp pushConsumerGroupMeta(TPushConsumerGroupMetaReq req) {
    final List<ConsumerGroupMeta> consumerGroupMetas = new ArrayList<>();
    for (ByteBuffer byteBuffer : req.getConsumerGroupMetas()) {
      consumerGroupMetas.add(ConsumerGroupMeta.deserialize(byteBuffer));
    }
    try {
      TPushConsumerGroupMetaRespExceptionMessage exceptionMessage =
          SubscriptionAgent.consumer().handleConsumerGroupMetaChanges(consumerGroupMetas);

      return exceptionMessage == null
          ? new TPushConsumerGroupMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          : new TPushConsumerGroupMetaResp()
              .setStatus(new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(Collections.singletonList(exceptionMessage));
    } catch (Exception e) {
      LOGGER.warn("Error occurred when pushing consumer group meta", e);
      return new TPushConsumerGroupMetaResp()
          .setStatus(new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode()));
    }
  }

  @Override
  public TPushConsumerGroupMetaResp pushSingleConsumerGroupMeta(
      TPushSingleConsumerGroupMetaReq req) {
    try {
      final TPushConsumerGroupMetaRespExceptionMessage exceptionMessage;
      if (req.isSetConsumerGroupNameToDrop()) {
        exceptionMessage =
            SubscriptionAgent.consumer().handleDropConsumerGroup(req.getConsumerGroupNameToDrop());
      } else if (req.isSetConsumerGroupMeta()) {
        exceptionMessage =
            SubscriptionAgent.consumer()
                .handleSingleConsumerGroupMetaChanges(
                    ConsumerGroupMeta.deserialize(ByteBuffer.wrap(req.getConsumerGroupMeta())));
      } else {
        throw new SubscriptionException("Invalid request " + req + " from config node.");
      }

      return exceptionMessage == null
          ? new TPushConsumerGroupMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          : new TPushConsumerGroupMetaResp()
              .setStatus(new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(Collections.singletonList(exceptionMessage));
    } catch (Exception e) {
      LOGGER.warn("Error occurred when pushing single consumer group meta", e);
      return new TPushConsumerGroupMetaResp()
          .setStatus(new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode()));
    }
  }

  @Override
  public TPipeHeartbeatResp pipeHeartbeat(TPipeHeartbeatReq req) throws TException {
    final TPipeHeartbeatResp resp = new TPipeHeartbeatResp();
    PipeDataNodeAgent.task().collectPipeMetaList(req, resp);
    return resp;
  }

  private TSStatus executeSchemaBlackListTask(
      final List<TConsensusGroupId> consensusGroupIdList,
      final Function<TConsensusGroupId, TSStatus> executeOnOneRegion) {
    // Not guarantee sequence
    final List<TSStatus> statusList = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean hasFailure = new AtomicBoolean(false);

    consensusGroupIdList.parallelStream()
        .forEach(
            consensusGroupId -> {
              final TSStatus status = executeOnOneRegion.apply(consensusGroupId);
              if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                  && status.getCode() != TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode()) {
                hasFailure.set(true);
              }
              statusList.add(status);
            });

    if (hasFailure.get()) {
      return RpcUtils.getStatus(statusList);
    } else {
      return statusList.stream()
          .filter(tsStatus -> tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .findFirst()
          .orElse(new TSStatus(TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode()));
    }
  }

  private TSStatus executeInternalSchemaTask(
      final List<TConsensusGroupId> consensusGroupIdList,
      final Function<TConsensusGroupId, TSStatus> executeOnOneRegion) {
    // Not guarantee sequence
    final List<TSStatus> statusList = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean hasFailure = new AtomicBoolean(false);

    consensusGroupIdList.parallelStream()
        .forEach(
            consensusGroupId -> {
              final TSStatus status = executeOnOneRegion.apply(consensusGroupId);
              if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                hasFailure.set(true);
              }
              statusList.add(status);
            });

    if (hasFailure.get()) {
      return RpcUtils.getStatus(statusList);
    } else {
      return RpcUtils.SUCCESS_STATUS;
    }
  }

  @Override
  public TSStatus executeCQ(TExecuteCQ req) {

    IClientSession session = new InternalClientSession(req.cqId);

    SESSION_MANAGER.registerSession(session);

    SESSION_MANAGER.supplySession(
        session, req.getUsername(), ZoneId.of(req.getZoneId()), ClientVersion.V_1_0);

    String executedSQL = req.queryBody;

    try {
      QueryStatement s =
          (QueryStatement) StatementGenerator.createStatement(req.queryBody, session.getZoneId());
      if (s == null) {
        return RpcUtils.getStatus(
            TSStatusCode.SQL_PARSE_ERROR, "This operation type is not supported");
      }

      // 1. Add time filter in where
      Expression timeFilter =
          new LogicAndExpression(
              new GreaterEqualExpression(
                  new TimestampOperand(),
                  new ConstantOperand(TSDataType.INT64, String.valueOf(req.startTime))),
              new LessThanExpression(
                  new TimestampOperand(),
                  new ConstantOperand(TSDataType.INT64, String.valueOf(req.endTime))));
      if (s.getWhereCondition() != null) {
        s.getWhereCondition()
            .setPredicate(new LogicAndExpression(timeFilter, s.getWhereCondition().getPredicate()));
      } else {
        s.setWhereCondition(new WhereCondition(timeFilter));
      }

      // 2. Add time range in group by time
      if (s.getGroupByTimeComponent() != null) {
        s.getGroupByTimeComponent().setStartTime(req.startTime);
        s.getGroupByTimeComponent().setEndTime(req.endTime);
        s.getGroupByTimeComponent().setLeftCRightO(true);
      }
      executedSQL = String.join(" ", s.constructFormattedSQL().split("\n")).replaceAll(" +", " ");

      long queryId =
          SESSION_MANAGER.requestQueryId(session, SESSION_MANAGER.requestStatementId(session));
      // Create and cache dataset
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              executedSQL,
              partitionFetcher,
              schemaFetcher,
              req.getTimeout());

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return result.status;
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        if (queryExecution != null) {
          // Consume up all the result
          while (true) {
            Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
            if (!optionalTsBlock.isPresent()) {
              break;
            }
          }
        }
        return result.status;
      }
    } catch (Exception e) {
      // TODO call the coordinator to release query resource
      return onQueryException(e, "\"" + executedSQL + "\". " + OperationType.EXECUTE_STATEMENT);
    } finally {
      SESSION_MANAGER.closeSession(session, COORDINATOR::cleanupQueryExecution);
      SESSION_MANAGER.removeCurrSession();
    }
  }

  @Override
  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) throws TException {
    return spaceQuotaManager.setSpaceQuota(req);
  }

  @Override
  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) throws TException {
    return throttleQuotaManager.setThrottleQuota(req);
  }

  @Override
  public TFetchFragmentInstanceStatisticsResp fetchFragmentInstanceStatistics(
      TFetchFragmentInstanceStatisticsReq req) throws TException {
    FragmentInstanceManager fragmentInstanceManager = FragmentInstanceManager.getInstance();
    TFetchFragmentInstanceStatisticsResp resp;
    try {
      resp =
          fragmentInstanceManager.getFragmentInstanceStatistics(
              FragmentInstanceId.fromThrift(req.getFragmentInstanceId()));
      resp.setStatus(RpcUtils.SUCCESS_STATUS);
    } catch (Exception e) {
      resp = new TFetchFragmentInstanceStatisticsResp();
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.EXPLAIN_ANALYZE_FETCH_ERROR, e.getMessage()));
      LOGGER.error(e.getMessage());
    }
    return resp;
  }

  @Override
  public TSStatus updateTable(final TUpdateTableReq req) {
    switch (TsTableInternalRPCType.getType(req.type)) {
      case PRE_CREATE_OR_ADD_COLUMN:
        DataNodeSchemaLockManager.getInstance().takeWriteLock(SchemaLockType.TIMESERIES_VS_TABLE);
        try {
          Pair<String, TsTable> pair =
              TsTableInternalRPCUtil.deserializeSingleTsTableWithDatabase(req.getTableInfo());
          DataNodeTableCache.getInstance().preUpdateTable(pair.left, pair.right);
        } finally {
          DataNodeSchemaLockManager.getInstance()
              .releaseWriteLock(SchemaLockType.TIMESERIES_VS_TABLE);
        }
        break;
      case ROLLBACK_CREATE_OR_ADD_COLUMN:
        DataNodeTableCache.getInstance()
            .rollbackUpdateTable(
                ReadWriteIOUtils.readString(req.tableInfo),
                ReadWriteIOUtils.readString(req.tableInfo));
        break;
      case COMMIT_CREATE_OR_ADD_COLUMN:
        DataNodeTableCache.getInstance()
            .commitUpdateTable(
                ReadWriteIOUtils.readString(req.tableInfo),
                ReadWriteIOUtils.readString(req.tableInfo));
        break;
      default:
        LOGGER.warn("Unsupported type {} when updating table", req.type);
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PARAMETER);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus invalidateTableCache(final TInvalidateTableCacheReq req) {
    DataNodeSchemaLockManager.getInstance().takeWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    try {
      TableDeviceSchemaCache.getInstance()
          .invalidate(PathUtils.unQualifyDatabaseName(req.getDatabase()), req.getTableName());
      return StatusUtils.OK;
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    }
  }

  @Override
  public TSStatus deleteDataForDropTable(final TDeleteDataOrDevicesForDropTableReq req) {
    return executeInternalSchemaTask(
        req.getRegionIdList(),
        consensusGroupId ->
            new RegionWriteExecutor()
                .execute(
                    new DataRegionId(consensusGroupId.getId()),
                    new RelationalDeleteDataNode(
                        new PlanNodeId(""),
                        new TableDeletionEntry(
                            new DeletionPredicate(req.getTableName()),
                            new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE))))
                .getStatus());
  }

  @Override
  public TSStatus deleteDevicesForDropTable(final TDeleteDataOrDevicesForDropTableReq req) {
    return executeInternalSchemaTask(
        req.getRegionIdList(),
        consensusGroupId ->
            new RegionWriteExecutor()
                .execute(
                    new SchemaRegionId(consensusGroupId.getId()),
                    new DeleteTableDeviceNode(new PlanNodeId(""), req.getTableName()))
                .getStatus());
  }

  @Override
  public TSStatus constructTableDeviceBlackList(
      final TTableDeviceDeletionWithPatternAndFilterReq req) {
    final AtomicLong preDeletedNum = new AtomicLong(0);
    final TSStatus executionResult =
        executeSchemaBlackListTask(
            req.getSchemaRegionIdList(),
            consensusGroupId -> {
              final TSStatus status =
                  new RegionWriteExecutor()
                      .execute(
                          new SchemaRegionId(consensusGroupId.getId()),
                          new ConstructTableDevicesBlackListNode(
                              new PlanNodeId(""),
                              req.getTableName(),
                              req.getPatternInfo(),
                              req.getFilterInfo()))
                      .getStatus();
              if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                preDeletedNum.getAndAdd(Long.parseLong(status.getMessage()));
              }
              return status;
            });
    executionResult.setMessage(String.valueOf(preDeletedNum.get()));
    return executionResult;
  }

  @Override
  public TSStatus rollbackTableDeviceBlackList(final TTableDeviceDeletionWithPatternOrModReq req) {
    return executeInternalSchemaTask(
        req.getRegionIdList(),
        consensusGroupId ->
            new RegionWriteExecutor()
                .execute(
                    new SchemaRegionId(consensusGroupId.getId()),
                    new RollbackTableDevicesBlackListNode(
                        new PlanNodeId(""), req.getTableName(), req.getPatternOrModInfo()))
                .getStatus());
  }

  @Override
  public TSStatus invalidateMatchedTableDeviceCache(final TTableDeviceInvalidateCacheReq req) {
    DataNodeSchemaLockManager.getInstance().takeWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    try {
      TableDeviceSchemaCache.getInstance()
          .invalidate(
              req.getDatabase(),
              req.getTableName(),
              DeleteDevice.constructPaths(
                  req.getDatabase(), req.getTableName(), req.getPatternInfo()));
      return StatusUtils.OK;
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    }
  }

  @Override
  public TSStatus deleteDataForTableDevice(final TTableDeviceDeletionWithPatternOrModReq req) {
    if (req.getPatternOrModInfo().length == 0) {
      // TODO: Not supported by DeletionEntry, fetch blacklist from schema region
      return StatusUtils.OK;
    }
    return executeInternalSchemaTask(
        req.getRegionIdList(),
        consensusGroupId ->
            new RegionWriteExecutor()
                .execute(
                    new DataRegionId(consensusGroupId.getId()),
                    new RelationalDeleteDataNode(
                        new PlanNodeId(""),
                        DeleteDevice.constructModEntries(req.getPatternOrModInfo())))
                .getStatus());
  }

  @Override
  public TSStatus deleteTableDeviceInBlackList(final TTableDeviceDeletionWithPatternOrModReq req) {
    return executeInternalSchemaTask(
        req.getRegionIdList(),
        consensusGroupId ->
            new RegionWriteExecutor()
                .execute(
                    new SchemaRegionId(consensusGroupId.getId()),
                    new DeleteTableDevicesInBlackListNode(
                        new PlanNodeId(""), req.getTableName(), req.getPatternOrModInfo()))
                .getStatus());
  }

  @Override
  public TSStatus invalidateColumnCache(final TInvalidateColumnCacheReq req) {
    DataNodeSchemaLockManager.getInstance().takeWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    try {
      TableDeviceSchemaCache.getInstance()
          .invalidate(
              req.getDatabase(),
              req.getTableName(),
              req.getColumnName(),
              req.isIsAttributeColumn());
      return StatusUtils.OK;
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseWriteLock(SchemaLockType.VALIDATE_VS_DELETION);
    }
  }

  @Override
  public TSStatus deleteColumnData(final TDeleteColumnDataReq req) {
    return executeInternalSchemaTask(
        req.getRegionIdList(),
        consensusGroupId ->
            req.isIsAttributeColumn()
                ? new RegionWriteExecutor()
                    .execute(
                        new SchemaRegionId(consensusGroupId.getId()),
                        new TableAttributeColumnDropNode(
                            new PlanNodeId(""), req.getTableName(), req.getColumnName()))
                    .getStatus()
                : new RegionWriteExecutor()
                    .execute(
                        new DataRegionId(consensusGroupId.getId()),
                        new RelationalDeleteDataNode(
                            new PlanNodeId(""),
                            new TableDeletionEntry(
                                new DeletionPredicate(
                                    req.getTableName(),
                                    new IDPredicate.NOP(),
                                    Collections.singletonList(req.getColumnName())),
                                new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE))))
                    .getStatus());
  }

  public TTestConnectionResp submitTestConnectionTask(final TNodeLocations nodeLocations) {
    return new TTestConnectionResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        Stream.of(
                testAllConfigNodeConnection(nodeLocations.getConfigNodeLocations()),
                testAllDataNodeInternalServiceConnection(nodeLocations.getDataNodeLocations()),
                testAllDataNodeMPPServiceConnection(nodeLocations.getDataNodeLocations()),
                testAllDataNodeExternalServiceConnection(nodeLocations.getDataNodeLocations()))
            .flatMap(Collection::stream)
            .collect(Collectors.toList()));
  }

  private static <Location, RequestType> List<TTestConnectionResult> testConnections(
      final List<Location> nodeLocations,
      final Function<Location, Integer> getId,
      final Function<Location, TEndPoint> getEndPoint,
      final TServiceType serviceType,
      final RequestType requestType,
      final Consumer<AsyncRequestContext<Object, TSStatus, RequestType, Location>> sendRequest) {
    final TSender sender =
        new TSender()
            .setDataNodeLocation(
                IoTDBDescriptor.getInstance().getConfig().generateLocalDataNodeLocation());
    return testConnectionsImpl(
        nodeLocations, sender, getId, getEndPoint, serviceType, requestType, sendRequest);
  }

  private List<TTestConnectionResult> testAllConfigNodeConnection(
      List<TConfigNodeLocation> configNodeLocations) {
    return testConnections(
        configNodeLocations,
        TConfigNodeLocation::getConfigNodeId,
        TConfigNodeLocation::getInternalEndPoint,
        TServiceType.ConfigNodeInternalService,
        DnToCnRequestType.TEST_CONNECTION,
        (AsyncRequestContext<Object, TSStatus, DnToCnRequestType, TConfigNodeLocation> handler) ->
            DnToCnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequest(handler));
  }

  private List<TTestConnectionResult> testAllDataNodeInternalServiceConnection(
      List<TDataNodeLocation> dataNodeLocations) {
    return testConnections(
        dataNodeLocations,
        TDataNodeLocation::getDataNodeId,
        TDataNodeLocation::getInternalEndPoint,
        TServiceType.DataNodeInternalService,
        DnToDnRequestType.TEST_CONNECTION,
        (AsyncRequestContext<Object, TSStatus, DnToDnRequestType, TDataNodeLocation> handler) ->
            DnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequest(handler));
  }

  private List<TTestConnectionResult> testAllDataNodeMPPServiceConnection(
      List<TDataNodeLocation> dataNodeLocations) {
    return testConnections(
        dataNodeLocations,
        TDataNodeLocation::getDataNodeId,
        TDataNodeLocation::getMPPDataExchangeEndPoint,
        TServiceType.DataNodeMPPService,
        DnToDnRequestType.TEST_CONNECTION,
        (AsyncRequestContext<Object, TSStatus, DnToDnRequestType, TDataNodeLocation> handler) ->
            DataNodeMPPServiceAsyncRequestManager.getInstance().sendAsyncRequest(handler));
  }

  private List<TTestConnectionResult> testAllDataNodeExternalServiceConnection(
      List<TDataNodeLocation> dataNodeLocations) {
    return testConnections(
        dataNodeLocations,
        TDataNodeLocation::getDataNodeId,
        TDataNodeLocation::getClientRpcEndPoint,
        TServiceType.DataNodeExternalService,
        DnToDnRequestType.TEST_CONNECTION,
        (AsyncRequestContext<Object, TSStatus, DnToDnRequestType, TDataNodeLocation> handler) ->
            DataNodeExternalServiceAsyncRequestManager.getInstance().sendAsyncRequest(handler));
  }

  @Override
  public TSStatus testConnectionEmptyRPC() throws TException {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private PathPatternTree filterPathPatternTree(PathPatternTree patternTree, String storageGroup) {
    PathPatternTree filteredPatternTree = new PathPatternTree();
    try {
      PartialPath storageGroupPattern =
          new PartialPath(storageGroup).concatNode(MULTI_LEVEL_PATH_WILDCARD);
      for (PartialPath pathPattern : patternTree.getOverlappedPathPatterns(storageGroupPattern)) {
        filteredPatternTree.appendPathPattern(pathPattern);
      }
      filteredPatternTree.constructTree();
    } catch (IllegalPathException e) {
      // Won't reach here
    }
    return filteredPatternTree;
  }

  @Override
  public TDataNodeHeartbeatResp getDataNodeHeartBeat(TDataNodeHeartbeatReq req) throws TException {
    TDataNodeHeartbeatResp resp = new TDataNodeHeartbeatResp();

    // Judging leader if necessary
    if (req.isNeedJudgeLeader()) {
      // Always get logical clock before judging leader
      // to ensure that the leader is up-to-date
      resp.setConsensusLogicalTimeMap(getLogicalClockMap());
      resp.setJudgedLeaders(getJudgedLeaders());
    }

    // Sampling load if necessary
    if (req.isNeedSamplingLoad()) {
      TLoadSample loadSample = new TLoadSample();

      // Sample cpu load
      double cpuLoad =
          MetricService.getInstance()
              .getAutoGauge(
                  SystemMetric.SYS_CPU_LOAD.toString(),
                  MetricLevel.CORE,
                  Tag.NAME.toString(),
                  SYSTEM)
              .getValue();
      if (cpuLoad != 0) {
        loadSample.setCpuUsageRate(cpuLoad);
      }

      // Sample memory load
      double usedMemory = getMemory("jvm.memory.used.bytes");
      double maxMemory = getMemory("jvm.memory.max.bytes");
      if (usedMemory != 0 && maxMemory != 0) {
        loadSample.setMemoryUsageRate(usedMemory * 100 / maxMemory);
      }

      // Sample disk load
      sampleDiskLoad(loadSample);

      resp.setLoadSample(loadSample);
    }
    AuthorityChecker.getAuthorityFetcher().refreshToken();
    resp.setHeartbeatTimestamp(req.getHeartbeatTimestamp());
    resp.setStatus(commonConfig.getNodeStatus().getStatus());
    if (commonConfig.getStatusReason() != null) {
      resp.setStatusReason(commonConfig.getStatusReason());
    }
    if (req.getSchemaRegionIds() != null) {
      spaceQuotaManager.updateSpaceQuotaUsage(req.getSpaceQuotaUsage());
      resp.setRegionDeviceUsageMap(
          schemaEngine.countDeviceNumBySchemaRegion(req.getSchemaRegionIds()));
      resp.setRegionSeriesUsageMap(
          schemaEngine.countTimeSeriesNumBySchemaRegion(req.getSchemaRegionIds()));
    }
    if (req.getDataRegionIds() != null) {
      spaceQuotaManager.setDataRegionIds(req.getDataRegionIds());
      resp.setRegionDisk(spaceQuotaManager.getRegionDisk());
    }
    // Update schema quota if necessary
    SchemaEngine.getInstance().updateAndFillSchemaCountMap(req, resp);

    // Update pipe meta if necessary
    if (req.isNeedPipeMetaList()) {
      PipeDataNodeAgent.task().collectPipeMetaList(resp);
    }

    if (req.isSetConfigNodeEndPoints()) {
      if (ConfigNodeInfo.getInstance()
          .updateConfigNodeList(new ArrayList<>(req.getConfigNodeEndPoints()))) {
        resp.setConfirmedConfigNodeEndPoints(req.getConfigNodeEndPoints());
      }
    }

    return resp;
  }

  @Override
  public TSStatus updateRegionCache(TRegionRouteReq req) {
    boolean result = ClusterPartitionFetcher.getInstance().updateRegionCache(req);
    if (result) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } else {
      return RpcUtils.getStatus(TSStatusCode.PARTITION_CACHE_UPDATE_ERROR);
    }
  }

  private Map<TConsensusGroupId, Boolean> getJudgedLeaders() {
    Map<TConsensusGroupId, Boolean> result = new HashMap<>();
    DataRegionConsensusImpl.getInstance()
        .getAllConsensusGroupIds()
        .forEach(
            groupId ->
                result.put(
                    groupId.convertToTConsensusGroupId(),
                    DataRegionConsensusImpl.getInstance().isLeader(groupId)));

    SchemaRegionConsensusImpl.getInstance()
        .getAllConsensusGroupIds()
        .forEach(
            groupId ->
                result.put(
                    groupId.convertToTConsensusGroupId(),
                    SchemaRegionConsensusImpl.getInstance().isLeader(groupId)));

    return result;
  }

  private Map<TConsensusGroupId, Long> getLogicalClockMap() {
    Map<TConsensusGroupId, Long> result = new HashMap<>();
    DataRegionConsensusImpl.getInstance()
        .getAllConsensusGroupIds()
        .forEach(
            groupId ->
                result.put(
                    groupId.convertToTConsensusGroupId(),
                    DataRegionConsensusImpl.getInstance().getLogicalClock(groupId)));

    SchemaRegionConsensusImpl.getInstance()
        .getAllConsensusGroupIds()
        .forEach(
            groupId ->
                result.put(
                    groupId.convertToTConsensusGroupId(),
                    SchemaRegionConsensusImpl.getInstance().getLogicalClock(groupId)));

    return result;
  }

  private long getLogicalClock(TConsensusGroupId groupId) {
    switch (groupId.getType()) {
      case DataRegion:
        return DataRegionConsensusImpl.getInstance()
            .getLogicalClock(ConsensusGroupId.Factory.createFromTConsensusGroupId(groupId));
      case SchemaRegion:
        return SchemaRegionConsensusImpl.getInstance()
            .getLogicalClock(ConsensusGroupId.Factory.createFromTConsensusGroupId(groupId));
      default:
        throw new IllegalArgumentException("Unknown consensus group type: " + groupId.getType());
    }
  }

  private double getMemory(String gaugeName) {
    double result = 0d;
    try {
      //
      List<String> heapIds = Arrays.asList("PS Eden Space", "PS Old Eden", "Ps Survivor Space");
      List<String> noHeapIds = Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace");

      for (String id : heapIds) {
        AutoGauge gauge =
            MetricService.getInstance()
                .getAutoGauge(gaugeName, MetricLevel.IMPORTANT, "id", id, "area", "heap");
        result += gauge.getValue();
      }
      for (String id : noHeapIds) {
        AutoGauge gauge =
            MetricService.getInstance()
                .getAutoGauge(gaugeName, MetricLevel.IMPORTANT, "id", id, "area", "noheap");
        result += gauge.getValue();
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to get memory from metric because: ", e);
      return 0d;
    }
    return result;
  }

  private void sampleDiskLoad(TLoadSample loadSample) {
    double availableDisk =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_AVAILABLE_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                SYSTEM)
            .getValue();
    double totalDisk =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_TOTAL_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                SYSTEM)
            .getValue();

    if (availableDisk != 0 && totalDisk != 0) {
      double freeDiskRatio = availableDisk / totalDisk;
      loadSample.setFreeDiskSpace(availableDisk);
      loadSample.setDiskUsageRate(1d - freeDiskRatio);
      // Reset NodeStatus if necessary
      if (freeDiskRatio < commonConfig.getDiskSpaceWarningThreshold()) {
        LOGGER.warn(
            "The available disk space is : {}, "
                + "the total disk space is : {}, "
                + "and the remaining disk usage ratio: {} is "
                + "less than disk_space_warning_threshold: {}, set system to readonly!",
            RamUsageEstimator.humanReadableUnits((long) availableDisk),
            RamUsageEstimator.humanReadableUnits((long) totalDisk),
            freeDiskRatio,
            commonConfig.getDiskSpaceWarningThreshold());
        commonConfig.setNodeStatus(NodeStatus.ReadOnly);
        commonConfig.setStatusReason(NodeStatus.DISK_FULL);
      } else if (NodeStatus.ReadOnly.equals(commonConfig.getNodeStatus())
          && NodeStatus.DISK_FULL.equals(commonConfig.getStatusReason())) {
        commonConfig.setNodeStatus(NodeStatus.Running);
        commonConfig.setStatusReason(null);
      }
    }
  }

  @Override
  public TSStatus invalidatePermissionCache(TInvalidatePermissionCacheReq req) {
    if (AuthorityChecker.invalidateCache(req.getUsername(), req.getRoleName())) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
    return RpcUtils.getStatus(TSStatusCode.CLEAR_PERMISSION_CACHE_ERROR);
  }

  @Override
  public TSStatus merge() throws TException {
    try {
      storageEngine.mergeAll();
    } catch (StorageEngineException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus startRepairData() throws TException {
    if (!storageEngine.isReadyForNonReadWriteFunctions()) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "not all sg is ready");
    }
    IoTDBConfig iotdbConfig = IoTDBDescriptor.getInstance().getConfig();
    if (!iotdbConfig.isEnableSeqSpaceCompaction() || !iotdbConfig.isEnableUnseqSpaceCompaction()) {
      return RpcUtils.getStatus(
          TSStatusCode.EXECUTE_STATEMENT_ERROR,
          "cannot start repair task because inner space compaction is not enabled");
    }
    try {
      if (storageEngine.repairData()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        if (CompactionScheduleTaskManager.getRepairTaskManagerInstance().getRepairTaskStatus()
            == RepairTaskStatus.STOPPING) {
          return RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "previous repair task is still stopping");
        } else {
          return RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "already have a running repair task");
        }
      }
    } catch (StorageEngineException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
  }

  @Override
  public TSStatus stopRepairData() throws TException {
    try {
      storageEngine.stopRepairData();
    } catch (StorageEngineException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    try {
      storageEngine.operateFlush(req);
    } catch (Exception e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus clearCache(final Set<Integer> clearCacheOptions) throws TException {
    return clearCacheImpl(
        clearCacheOptions.stream()
            .map(i -> CacheClearOptions.values()[i])
            .collect(Collectors.toSet()));
  }

  public TSStatus clearCacheImpl(final Set<CacheClearOptions> options) {
    try {
      if (options.contains(CacheClearOptions.DEFAULT)
          || options.contains(CacheClearOptions.QUERY)) {
        storageEngine.clearCache();
      }
      if (options.contains(CacheClearOptions.QUERY)
          && options.contains(CacheClearOptions.TABLE_ATTRIBUTE)
          && options.contains(CacheClearOptions.TREE_SCHEMA)) {
        tableDeviceSchemaCache.invalidateAll();
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }
      if (options.contains(CacheClearOptions.QUERY)) {
        tableDeviceSchemaCache.invalidateLastCache();
      }
      if (options.contains(CacheClearOptions.TABLE_ATTRIBUTE)) {
        tableDeviceSchemaCache.invalidateAttributeCache();
      }
      if (options.contains(CacheClearOptions.TREE_SCHEMA)) {
        tableDeviceSchemaCache.invalidateTreeSchema();
      }
    } catch (final Exception e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus setConfiguration(TSetConfigurationReq req) {
    return StorageEngine.getInstance().setConfiguration(req);
  }

  @Override
  public TSStatus settle(TSettleReq req) throws TException {
    return SettleRequestHandler.getInstance().handleSettleRequest(req);
  }

  @Override
  public TSStatus loadConfiguration() throws TException {
    try {
      IoTDBDescriptor.getInstance().loadHotModifiedProps();
    } catch (Exception e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TShowConfigurationResp showConfiguration() {
    TShowConfigurationResp resp =
        new TShowConfigurationResp(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS), "");
    try {
      URL propsUrl = IoTDBDescriptor.getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
      resp.setContent(ConfigurationFileUtils.readConfigFileContent(propsUrl));
    } catch (Exception e) {
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()));
    }
    return resp;
  }

  @Override
  public TSStatus setSystemStatus(String status) throws TException {
    try {
      commonConfig.setNodeStatus(NodeStatus.parse(status));
      if (commonConfig.getNodeStatus().equals(NodeStatus.Removing)) {
        PipeDataNodeAgent.runtime().stop();
      }
    } catch (Exception e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus killQueryInstance(String queryId) {
    Coordinator coordinator = Coordinator.getInstance();
    if (queryId == null) {
      coordinator.getAllQueryExecutions().forEach(IQueryExecution::cancel);
    } else {
      Optional<IQueryExecution> queryExecution =
          coordinator.getAllQueryExecutions().stream()
              .filter(iQueryExecution -> iQueryExecution.getQueryId().equals(queryId))
              .findAny();
      if (queryExecution.isPresent()) {
        queryExecution.get().cancel();
      } else {
        return new TSStatus(TSStatusCode.NO_SUCH_QUERY.getStatusCode()).setMessage("No such query");
      }
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    try {
      return storageEngine.setTTL(req);
    } catch (IllegalPathException e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
  }

  @Override
  public TSStatus updateTemplate(TUpdateTemplateReq req) {
    switch (TemplateInternalRPCUpdateType.getType(req.type)) {
      case ADD_TEMPLATE_SET_INFO:
        DataNodeSchemaLockManager.getInstance()
            .takeWriteLock(SchemaLockType.TIMESERIES_VS_TEMPLATE);
        try {
          ClusterTemplateManager.getInstance().addTemplateSetInfo(req.getTemplateInfo());
        } finally {
          DataNodeSchemaLockManager.getInstance()
              .releaseWriteLock(SchemaLockType.TIMESERIES_VS_TEMPLATE);
        }
        break;
      case INVALIDATE_TEMPLATE_SET_INFO:
        ClusterTemplateManager.getInstance().invalidateTemplateSetInfo(req.getTemplateInfo());
        break;
      case ADD_TEMPLATE_PRE_SET_INFO:
        ClusterTemplateManager.getInstance().addTemplatePreSetInfo(req.getTemplateInfo());
        break;
      case COMMIT_TEMPLATE_SET_INFO:
        ClusterTemplateManager.getInstance().commitTemplatePreSetInfo(req.getTemplateInfo());
        break;
      case UPDATE_TEMPLATE_INFO:
        ClusterTemplateManager.getInstance().updateTemplateInfo(req.getTemplateInfo());
        break;
      default:
        LOGGER.warn("Unsupported type {} when updating template", req.type);
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PARAMETER);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus deleteRegion(TConsensusGroupId tconsensusGroupId) {
    ConsensusGroupId consensusGroupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(tconsensusGroupId);
    if (consensusGroupId instanceof DataRegionId) {
      try {
        DataRegionConsensusImpl.getInstance().deleteLocalPeer(consensusGroupId);
        Optional.ofNullable(
                DeletionResourceManager.getInstance(String.valueOf(tconsensusGroupId.getId())))
            .ifPresent(DeletionResourceManager::close);
      } catch (ConsensusException e) {
        if (!(e instanceof ConsensusGroupNotExistException)) {
          return RpcUtils.getStatus(TSStatusCode.DELETE_REGION_ERROR, e.getMessage());
        }
      }
      return regionManager.deleteDataRegion((DataRegionId) consensusGroupId);
    } else {
      try {
        SchemaRegionConsensusImpl.getInstance().deleteLocalPeer(consensusGroupId);
      } catch (ConsensusException e) {
        if (!(e instanceof ConsensusGroupNotExistException)) {
          return RpcUtils.getStatus(TSStatusCode.DELETE_REGION_ERROR, e.getMessage());
        }
      }
      return regionManager.deleteSchemaRegion((SchemaRegionId) consensusGroupId);
    }
  }

  @Override
  public TRegionLeaderChangeResp changeRegionLeader(TRegionLeaderChangeReq req) {
    LOGGER.info("[ChangeRegionLeader] {}", req);
    TRegionLeaderChangeResp resp = new TRegionLeaderChangeResp();

    TSStatus successStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    TConsensusGroupId tgId = req.getRegionId();
    ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(tgId);
    TEndPoint newNode = getConsensusEndPoint(req.getNewLeaderNode(), regionId);
    Peer newLeaderPeer = new Peer(regionId, req.getNewLeaderNode().getDataNodeId(), newNode);

    if (isLeader(regionId)) {
      String msg =
          "[ChangeRegionLeader] The current DataNode: "
              + req.getNewLeaderNode().getDataNodeId()
              + " is already the leader of RegionGroup: "
              + regionId
              + ", skip leader transfer.";
      LOGGER.info(msg);
      resp.setStatus(successStatus.setMessage(msg));
      resp.setConsensusLogicalTimestamp(getLogicalClock(req.getRegionId()));
      return resp;
    }

    LOGGER.info(
        "[ChangeRegionLeader] Start change the leader of RegionGroup: {} to DataNode: {}",
        regionId,
        req.getNewLeaderNode().getDataNodeId());
    resp.setStatus(transferLeader(regionId, newLeaderPeer));
    resp.setConsensusLogicalTimestamp(getLogicalClock(req.getRegionId()));
    return resp;
  }

  private TSStatus transferLeader(ConsensusGroupId regionId, Peer newLeaderPeer) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    try {
      if (regionId instanceof DataRegionId) {
        DataRegionConsensusImpl.getInstance().transferLeader(regionId, newLeaderPeer);
      } else if (regionId instanceof SchemaRegionId) {
        SchemaRegionConsensusImpl.getInstance().transferLeader(regionId, newLeaderPeer);
      } else {
        status.setCode(TSStatusCode.REGION_LEADER_CHANGE_ERROR.getStatusCode());
        status.setMessage("[ChangeRegionLeader] Error Region type: " + regionId);
        return status;
      }
    } catch (ConsensusException e) {
      LOGGER.warn(
          "[ChangeRegionLeader] Failed to change the leader of RegionGroup: {}", regionId, e);
      status.setCode(TSStatusCode.REGION_LEADER_CHANGE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
      return status;
    }

    status.setMessage(
        "[ChangeRegionLeader] Successfully change the leader of RegionGroup: "
            + regionId
            + " to "
            + newLeaderPeer.getNodeId());
    return status;
  }

  private boolean isLeader(ConsensusGroupId regionId) {
    if (regionId instanceof DataRegionId) {
      return DataRegionConsensusImpl.getInstance().isLeader(regionId);
    }
    if (regionId instanceof SchemaRegionId) {
      return SchemaRegionConsensusImpl.getInstance().isLeader(regionId);
    }
    LOGGER.warn("region {} type is illegal", regionId);
    return false;
  }

  @Override
  public TSStatus createNewRegionPeer(TCreatePeerReq req) {
    ConsensusGroupId regionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getRegionId());
    List<Peer> peers =
        req.getRegionLocations().stream()
            .map(
                location ->
                    new Peer(
                        regionId,
                        location.getDataNodeId(),
                        getConsensusEndPoint(location, regionId)))
            .collect(Collectors.toList());
    TSStatus status = createNewRegion(regionId, req.getStorageGroup());
    if (!isSucceed(status)) {
      return status;
    }
    return createNewRegionPeer(regionId, peers);
  }

  @Override
  public TSStatus addRegionPeer(TMaintainPeerReq req) {
    TConsensusGroupId regionId = req.getRegionId();
    String selectedDataNodeIP = req.getDestNode().getInternalEndPoint().getIp();
    boolean submitSucceed = RegionMigrateService.getInstance().submitAddRegionPeerTask(req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (submitSucceed) {
      LOGGER.info(
          "Successfully submit addRegionPeer task for region: {}, target DataNode: {}",
          regionId,
          selectedDataNodeIP);
      return status;
    }
    status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
    status.setMessage("Submit addRegionPeer task failed, region: " + regionId);
    return status;
  }

  @Override
  public TSStatus removeRegionPeer(TMaintainPeerReq req) {
    TConsensusGroupId regionId = req.getRegionId();
    String selectedDataNodeIP = req.getDestNode().getInternalEndPoint().getIp();
    boolean submitSucceed = RegionMigrateService.getInstance().submitRemoveRegionPeerTask(req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (submitSucceed) {
      LOGGER.info(
          "Successfully submit removeRegionPeer task for region: {}, DataNode to be removed: {}",
          regionId,
          selectedDataNodeIP);
      return status;
    }
    status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
    status.setMessage("Submit removeRegionPeer task failed, region: " + regionId);
    return status;
  }

  @Override
  public TSStatus deleteOldRegionPeer(TMaintainPeerReq req) {
    TConsensusGroupId regionId = req.getRegionId();
    String selectedDataNodeIP = req.getDestNode().getInternalEndPoint().getIp();
    boolean submitSucceed = RegionMigrateService.getInstance().submitDeleteOldRegionPeerTask(req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (submitSucceed) {
      LOGGER.info(
          "Successfully submit deleteOldRegionPeer task for region: {}, DataNode to be removed: {}",
          regionId,
          selectedDataNodeIP);
      return status;
    }
    status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
    status.setMessage("Submit deleteOldRegionPeer task failed, region: " + regionId);
    return status;
  }

  // TODO: return which DataNode fail
  @Override
  public TSStatus resetPeerList(TResetPeerListReq req) throws TException {
    return RegionMigrateService.getInstance().resetPeerList(req);
  }

  @Override
  public TRegionMigrateResult getRegionMaintainResult(long taskId) throws TException {
    return RegionMigrateService.getInstance().getRegionMaintainResult(taskId);
  }

  private TSStatus createNewRegion(ConsensusGroupId regionId, String storageGroup) {
    return regionManager.createNewRegion(regionId, storageGroup);
  }

  @Override
  public TSStatus createFunction(TCreateFunctionInstanceReq req) {
    try {
      UDFInformation udfInformation = UDFInformation.deserialize(req.udfInformation);
      UDFManagementService.getInstance().register(udfInformation, req.jarFile);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CREATE_UDF_ON_DATANODE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  @Override
  public TSStatus dropFunction(TDropFunctionInstanceReq req) {
    try {
      UDFManagementService.getInstance()
          .deregister(req.model, req.getFunctionName(), req.isNeedToDeleteJar());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.DROP_UDF_ON_DATANODE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  @Override
  public TSStatus createTriggerInstance(TCreateTriggerInstanceReq req) {
    TriggerInformation triggerInformation = TriggerInformation.deserialize(req.triggerInformation);
    try {
      // Register trigger information with TriggerRegistrationService
      // Config nodes take responsibility for synchronization control
      TriggerManagementService.getInstance().register(triggerInformation, req.jarFile);
    } catch (Exception e) {
      LOGGER.warn(
          "Error occurred when creating trigger instance for trigger: {}. The cause is {}.",
          triggerInformation.getTriggerName(),
          e);
      return new TSStatus(TSStatusCode.CREATE_TRIGGER_INSTANCE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus activeTriggerInstance(TActiveTriggerInstanceReq req) {
    try {
      TriggerManagementService.getInstance().activeTrigger(req.triggerName);
    } catch (Exception e) {
      LOGGER.warn(
          "Error occurred during active trigger instance for trigger: {}. The cause is {}.",
          req.triggerName,
          e);
      return new TSStatus(TSStatusCode.ACTIVE_TRIGGER_INSTANCE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus inactiveTriggerInstance(TInactiveTriggerInstanceReq req) {
    try {
      TriggerManagementService.getInstance().inactiveTrigger(req.triggerName);
    } catch (Exception e) {
      LOGGER.warn(
          "Error occurred when try to inactive trigger instance for trigger: {}. The cause is {}. ",
          req.triggerName,
          e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus dropTriggerInstance(TDropTriggerInstanceReq req) {
    try {
      TriggerManagementService.getInstance().dropTrigger(req.triggerName, req.needToDeleteJarFile);
    } catch (Exception e) {
      LOGGER.warn(
          "Error occurred when dropping trigger instance for trigger: {}. The cause is {}.",
          req.triggerName,
          e);
      return new TSStatus(TSStatusCode.DROP_TRIGGER_INSTANCE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus updateTriggerLocation(TUpdateTriggerLocationReq req) {
    try {
      TriggerManagementService.getInstance()
          .updateLocationOfStatefulTrigger(req.triggerName, req.newLocation);
    } catch (Exception e) {
      LOGGER.warn(
          "Error occurred when updating Location for trigger: {}. The cause is {}.",
          req.triggerName,
          e);
      return new TSStatus(TSStatusCode.UPDATE_TRIGGER_LOCATION_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TFireTriggerResp fireTrigger(TFireTriggerReq req) {
    String triggerName = req.getTriggerName();
    TriggerExecutor executor = TriggerManagementService.getInstance().getExecutor(triggerName);
    // No executor for given trigger name on this data node
    if (executor == null) {
      return new TFireTriggerResp(false, TriggerFireResult.FAILED_NO_TERMINATION.getId());
    }
    TriggerFireResult result = TriggerFireResult.SUCCESS;
    try {
      boolean fireResult =
          executor.fire(
              Tablet.deserialize(req.tablet), TriggerEvent.construct(req.getTriggerEvent()));
      if (!fireResult) {
        result =
            executor.getFailureStrategy().equals(FailureStrategy.PESSIMISTIC)
                ? TriggerFireResult.TERMINATION
                : TriggerFireResult.FAILED_NO_TERMINATION;
      }
    } catch (Exception e) {
      result =
          executor.getFailureStrategy().equals(FailureStrategy.PESSIMISTIC)
              ? TriggerFireResult.TERMINATION
              : TriggerFireResult.FAILED_NO_TERMINATION;
    }
    return new TFireTriggerResp(true, result.getId());
  }

  private TEndPoint getConsensusEndPoint(
      TDataNodeLocation nodeLocation, ConsensusGroupId regionId) {
    if (regionId instanceof DataRegionId) {
      return nodeLocation.getDataRegionConsensusEndPoint();
    }
    return nodeLocation.getSchemaRegionConsensusEndPoint();
  }

  @Override
  public TSStatus createPipePlugin(TCreatePipePluginInstanceReq req) {
    try {
      PipePluginMeta pipePluginMeta = PipePluginMeta.deserialize(req.pipePluginMeta);
      PipeDataNodeAgent.plugin().register(pipePluginMeta, req.jarFile);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ON_DATANODE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  @Override
  public TSStatus dropPipePlugin(TDropPipePluginInstanceReq req) {
    try {
      PipeDataNodeAgent.plugin().deregister(req.getPipePluginName(), req.isNeedToDeleteJar());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.DROP_PIPE_PLUGIN_ON_DATANODE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  private boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private TSStatus createNewRegionPeer(ConsensusGroupId regionId, List<Peer> peers) {
    LOGGER.info(
        "{}, Start to createNewRegionPeer {} to region {}",
        REGION_MIGRATE_PROCESS,
        peers,
        regionId);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    try {
      if (regionId instanceof DataRegionId) {
        // RatisConsensus requires an empty peer list during region transition
        final List<Peer> createPeers =
            ConsensusFactory.RATIS_CONSENSUS.equals(
                    IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass())
                ? Collections.emptyList()
                : peers;
        DataRegionConsensusImpl.getInstance().createLocalPeer(regionId, createPeers);
      } else {
        // RatisConsensus requires an empty peer list during region transition
        final List<Peer> createPeers =
            ConsensusFactory.RATIS_CONSENSUS.equals(
                    IoTDBDescriptor.getInstance()
                        .getConfig()
                        .getSchemaRegionConsensusProtocolClass())
                ? Collections.emptyList()
                : peers;
        SchemaRegionConsensusImpl.getInstance().createLocalPeer(regionId, createPeers);
      }
    } catch (ConsensusException e) {
      if (!(e instanceof ConsensusGroupAlreadyExistException)) {
        LOGGER.warn(
            "{}, CreateNewRegionPeer error, peers: {}, regionId: {}, errorMessage",
            REGION_MIGRATE_PROCESS,
            peers,
            regionId,
            e);
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage(e.getMessage());
        return status;
      }
    }
    LOGGER.info(
        "{}, Succeed to createNewRegionPeer {} for region {}",
        REGION_MIGRATE_PROCESS,
        peers,
        regionId);
    status.setMessage("createNewRegionPeer succeed, regionId: " + regionId);
    return status;
  }

  @Override
  public TSStatus cleanDataNodeCache(TCleanDataNodeCacheReq req) {
    LOGGER.info("start disable data node in the request: {}", req);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    status.setMessage("disable datanode succeed");
    // TODO what need to clean?
    ClusterPartitionFetcher.getInstance().invalidAllCache();
    TreeDeviceSchemaCacheManager.getInstance().takeWriteLock();
    try {
      TreeDeviceSchemaCacheManager.getInstance().cleanUp();
    } finally {
      TreeDeviceSchemaCacheManager.getInstance().releaseWriteLock();
    }
    DataNodeDevicePathCache.getInstance().cleanUp();
    return status;
  }

  @SuppressWarnings("squid:S2142") // ignore Either re-interrupt this method or rethrow
  @Override
  public TSStatus stopAndClearDataNode() {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    LOGGER.info("Execute stopDataNode RPC method");

    // kill the datanode process 20 seconds later
    // because datanode process cannot exit normally for the reason of InterruptedException
    new Thread(
            () -> {
              try {
                TimeUnit.SECONDS.sleep(20);
              } catch (InterruptedException e) {
                LOGGER.warn("Meets InterruptedException in stopDataNode RPC method");
              } finally {
                LOGGER.info("Executing system.exit(0) in stopDataNode RPC method after 20 seconds");
                System.exit(0);
              }
            })
        .start();

    try {
      DataNode.getInstance().stop();
      status.setMessage("stop datanode succeed");
      DataNode.getInstance().deleteDataNodeSystemProperties();
    } catch (Exception e) {
      LOGGER.warn("Stop Data Node error", e);
      status.setCode(TSStatusCode.DATANODE_STOP_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }

  public void handleClientExit() {
    // Do nothing
  }
}
