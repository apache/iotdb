1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.manager;
1
1import org.apache.iotdb.common.rpc.thrift.Model;
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
1import org.apache.iotdb.common.rpc.thrift.TEndPoint;
1import org.apache.iotdb.common.rpc.thrift.TSStatus;
1import org.apache.iotdb.commons.cluster.NodeStatus;
1import org.apache.iotdb.commons.conf.CommonConfig;
1import org.apache.iotdb.commons.conf.CommonDescriptor;
1import org.apache.iotdb.commons.conf.IoTDBConstant;
1import org.apache.iotdb.commons.exception.IoTDBException;
1import org.apache.iotdb.commons.path.PartialPath;
1import org.apache.iotdb.commons.path.PathDeserializeUtil;
1import org.apache.iotdb.commons.path.PathPatternTree;
1import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
1import org.apache.iotdb.commons.schema.table.TsTable;
1import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
1import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
1import org.apache.iotdb.commons.service.metric.MetricService;
1import org.apache.iotdb.commons.trigger.TriggerInformation;
1import org.apache.iotdb.commons.utils.StatusUtils;
1import org.apache.iotdb.commons.utils.TestOnly;
1import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
1import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
1import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
1import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
1import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
1import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
1import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
1import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
1import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
1import org.apache.iotdb.confignode.manager.partition.PartitionManager;
1import org.apache.iotdb.confignode.persistence.ProcedureInfo;
1import org.apache.iotdb.confignode.procedure.PartitionTableAutoCleaner;
1import org.apache.iotdb.confignode.procedure.Procedure;
1import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
1import org.apache.iotdb.confignode.procedure.ProcedureMetrics;
1import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
1import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
1import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
1import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
1import org.apache.iotdb.confignode.procedure.impl.model.CreateModelProcedure;
1import org.apache.iotdb.confignode.procedure.impl.model.DropModelProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.RemoveAINodeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.CreatePipePluginProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.DropPipePluginProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleLeaderChangeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeMetaSyncProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.AlterPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.StopPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.region.AddRegionPeerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.CreateRegionGroupsProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.ReconstructRegionProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
1import org.apache.iotdb.confignode.procedure.impl.region.RegionOperationProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionPeerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.SetTTLProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.AbstractAlterOrDropTableProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.DeleteDevicesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.AddViewColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.SetViewPropertiesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.CreateConsumerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.DropConsumerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime.ConsumerGroupMetaSyncProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.CreateSubscriptionProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.DropSubscriptionProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.topic.CreateTopicProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.topic.DropTopicProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.topic.runtime.TopicMetaSyncProcedure;
1import org.apache.iotdb.confignode.procedure.impl.sync.AuthOperationProcedure;
1import org.apache.iotdb.confignode.procedure.impl.testonly.AddNeverFinishSubProcedureProcedure;
1import org.apache.iotdb.confignode.procedure.impl.testonly.CreateManyDatabasesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
1import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
1import org.apache.iotdb.confignode.procedure.scheduler.SimpleProcedureScheduler;
1import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
1import org.apache.iotdb.confignode.procedure.store.IProcedureStore;
1import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
1import org.apache.iotdb.confignode.procedure.store.ProcedureType;
1import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
1import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
1import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
1import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
1import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
1import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
1import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
1import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
1import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
1import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
1import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
1import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceReq;
1import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceResp;
1import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
1import org.apache.iotdb.confignode.rpc.thrift.TExtendRegionReq;
1import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
1import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
1import org.apache.iotdb.confignode.rpc.thrift.TRemoveRegionReq;
1import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
1import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
1import org.apache.iotdb.consensus.ConsensusFactory;
1import org.apache.iotdb.db.exception.BatchProcessException;
1import org.apache.iotdb.db.schemaengine.template.Template;
1import org.apache.iotdb.db.utils.constant.SqlConstant;
1import org.apache.iotdb.rpc.RpcUtils;
1import org.apache.iotdb.rpc.TSStatusCode;
1
1import org.apache.ratis.util.AutoCloseableLock;
1import org.apache.tsfile.utils.Binary;
1import org.apache.tsfile.utils.Pair;
1import org.apache.tsfile.utils.ReadWriteIOUtils;
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import javax.annotation.Nonnull;
1import javax.annotation.Nullable;
1
1import java.io.IOException;
1import java.nio.ByteBuffer;
1import java.util.ArrayList;
1import java.util.Arrays;
1import java.util.HashMap;
1import java.util.List;
1import java.util.Map;
1import java.util.Objects;
1import java.util.Optional;
1import java.util.Set;
1import java.util.concurrent.ScheduledExecutorService;
1import java.util.concurrent.locks.ReentrantLock;
1import java.util.function.BiFunction;
1import java.util.stream.Collectors;
1import java.util.stream.Stream;
1
1public class ProcedureManager {
1  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureManager.class);
1
1  private static final ConfigNodeConfig CONFIG_NODE_CONFIG =
1      ConfigNodeDescriptor.getInstance().getConf();
1  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
1
1  public static final long PROCEDURE_WAIT_TIME_OUT = COMMON_CONFIG.getDnConnectionTimeoutInMS();
1  public static final int PROCEDURE_WAIT_RETRY_TIMEOUT = 10;
1  private static final String PROCEDURE_TIMEOUT_MESSAGE =
1      "Timed out to wait for procedure return. The procedure is still running.";
1
1  private final ConfigManager configManager;
1  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
1  private ProcedureScheduler scheduler;
1  private IProcedureStore store;
1  private ConfigNodeProcedureEnv env;
1
1  private final long planSizeLimit;
1  private ProcedureMetrics procedureMetrics;
1
1  private final PartitionTableAutoCleaner partitionTableCleaner;
1
1  private final ReentrantLock tableLock = new ReentrantLock();
1
1  public ProcedureManager(ConfigManager configManager, ProcedureInfo procedureInfo) {
1    this.configManager = configManager;
1    this.scheduler = new SimpleProcedureScheduler();
1    this.store = new ConfigProcedureStore(configManager, procedureInfo);
1    this.env = new ConfigNodeProcedureEnv(configManager, scheduler);
1    this.executor = new ProcedureExecutor<>(env, store, scheduler);
1    this.planSizeLimit =
1        ConfigNodeDescriptor.getInstance()
1                .getConf()
1                .getConfigNodeRatisConsensusLogAppenderBufferSize()
1            - IoTDBConstant.RAFT_LOG_BASIC_SIZE;
1    this.procedureMetrics = new ProcedureMetrics(this);
1    this.partitionTableCleaner = new PartitionTableAutoCleaner<>(configManager);
1  }
1
1  public void startExecutor() {
1    if (!executor.isRunning()) {
1      executor.init(CONFIG_NODE_CONFIG.getProcedureCoreWorkerThreadsCount());
1      executor.startWorkers();
1      executor.startCompletedCleaner(
1          CONFIG_NODE_CONFIG.getProcedureCompletedCleanInterval(),
1          CONFIG_NODE_CONFIG.getProcedureCompletedEvictTTL());
1      executor.addInternalProcedure(partitionTableCleaner);
1      store.start();
1      LOGGER.info("ProcedureManager is started successfully.");
1    }
1  }
1
1  public void stopExecutor() {
1    if (executor.isRunning()) {
1      executor.stop();
1      if (!executor.isRunning()) {
1        executor.join();
1        store.stop();
1        LOGGER.info("ProcedureManager is stopped successfully.");
1      }
1      executor.removeInternalProcedure(partitionTableCleaner);
1    }
1  }
1
1  @TestOnly
1  public TSStatus createManyDatabases() {
1    this.executor.submitProcedure(new CreateManyDatabasesProcedure());
1    return StatusUtils.OK;
1  }
1
1  @TestOnly
1  public TSStatus testSubProcedure() {
1    this.executor.submitProcedure(new AddNeverFinishSubProcedureProcedure());
1    return StatusUtils.OK;
1  }
1
1  public TSStatus deleteDatabases(
1      final List<TDatabaseSchema> deleteSgSchemaList, final boolean isGeneratedByPipe) {
1    final List<DeleteDatabaseProcedure> procedures = new ArrayList<>();
1    final long startCheckTimeForProcedures = System.currentTimeMillis();
1    for (final TDatabaseSchema databaseSchema : deleteSgSchemaList) {
1      final String database = databaseSchema.getName();
1      boolean hasOverlappedTask = false;
1      synchronized (this) {
1        while (executor.isRunning()
1            && System.currentTimeMillis() - startCheckTimeForProcedures < PROCEDURE_WAIT_TIME_OUT) {
1          final Pair<Long, Boolean> procedureIdDuplicatePair =
1              checkDuplicateTableTask(
1                  database, null, null, null, null, ProcedureType.DELETE_DATABASE_PROCEDURE);
1          hasOverlappedTask = procedureIdDuplicatePair.getRight();
1
1          if (Boolean.FALSE.equals(procedureIdDuplicatePair.getRight())) {
1            DeleteDatabaseProcedure procedure =
1                new DeleteDatabaseProcedure(databaseSchema, isGeneratedByPipe);
1            this.executor.submitProcedure(procedure);
1            procedures.add(procedure);
1            break;
1          }
1          try {
1            wait(PROCEDURE_WAIT_RETRY_TIMEOUT);
1          } catch (final InterruptedException e) {
1            Thread.currentThread().interrupt();
1          }
1        }
1        if (hasOverlappedTask) {
1          return RpcUtils.getStatus(
1              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
1              String.format(
1                  "Some other task is operating table under the database %s, please retry after the procedure finishes.",
1                  database));
1        }
1      }
1    }
1    List<TSStatus> results = new ArrayList<>(procedures.size());
1    procedures.forEach(procedure -> results.add(waitingProcedureFinished(procedure)));
1    // Clear the previously deleted regions
1    final PartitionManager partitionManager = getConfigManager().getPartitionManager();
1    partitionManager.getRegionMaintainer().submit(partitionManager::maintainRegionReplicas);
1    if (results.stream()
1        .allMatch(result -> result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
1      return StatusUtils.OK;
1    } else {
1      return RpcUtils.getStatus(results);
1    }
1  }
1
1  public TSStatus deleteTimeSeries(
1      String queryId, PathPatternTree patternTree, boolean isGeneratedByPipe) {
1    DeleteTimeSeriesProcedure procedure = null;
1    synchronized (this) {
1      boolean hasOverlappedTask = false;
1      ProcedureType type;
1      DeleteTimeSeriesProcedure deleteTimeSeriesProcedure;
1      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
1        type = ProcedureFactory.getProcedureType(runningProcedure);
1        if (type == null || !type.equals(ProcedureType.DELETE_TIMESERIES_PROCEDURE)) {
1          continue;
1        }
1        deleteTimeSeriesProcedure = ((DeleteTimeSeriesProcedure) runningProcedure);
1        if (queryId.equals(deleteTimeSeriesProcedure.getQueryId())) {
1          procedure = deleteTimeSeriesProcedure;
1          break;
1        }
1        if (patternTree.isOverlapWith(deleteTimeSeriesProcedure.getPatternTree())) {
1          hasOverlappedTask = true;
1          break;
1        }
1      }
1
1      if (procedure == null) {
1        if (hasOverlappedTask) {
1          return RpcUtils.getStatus(
1              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
1              "Some other task is deleting some target timeseries.");
1        }
1        procedure = new DeleteTimeSeriesProcedure(queryId, patternTree, isGeneratedByPipe);
1        this.executor.submitProcedure(procedure);
1      }
1    }
1    return waitingProcedureFinished(procedure);
1  }
1
1  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) {
1    String queryId = req.getQueryId();
1    PathPatternTree patternTree =
1        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
1    DeleteLogicalViewProcedure procedure = null;
1    synchronized (this) {
1      boolean hasOverlappedTask = false;
1      ProcedureType type;
1      DeleteLogicalViewProcedure deleteLogicalViewProcedure;
1      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
1        type = ProcedureFactory.getProcedureType(runningProcedure);
1        if (type == null || !type.equals(ProcedureType.DELETE_LOGICAL_VIEW_PROCEDURE)) {
1          continue;
1        }
1        deleteLogicalViewProcedure = ((DeleteLogicalViewProcedure) runningProcedure);
1        if (queryId.equals(deleteLogicalViewProcedure.getQueryId())) {
1          procedure = deleteLogicalViewProcedure;
1          break;
1        }
1        if (patternTree.isOverlapWith(deleteLogicalViewProcedure.getPatternTree())) {
1          hasOverlappedTask = true;
1          break;
1        }
1      }
1
1      if (procedure == null) {
1        if (hasOverlappedTask) {
1          return RpcUtils.getStatus(
1              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
1              "Some other task is deleting some target views.");
1        }
1        procedure =
1            new DeleteLogicalViewProcedure(
1                queryId, patternTree, req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe());
1        this.executor.submitProcedure(procedure);
1      }
1    }
1    return waitingProcedureFinished(procedure);
1  }
1
1  public TSStatus alterLogicalView(final TAlterLogicalViewReq req) {
1    final String queryId = req.getQueryId();
1    final ByteBuffer byteBuffer = ByteBuffer.wrap(req.getViewBinary());
1    final Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
1    final int size = byteBuffer.getInt();
1    PartialPath path;
1    ViewExpression viewExpression;
1    for (int i = 0; i < size; i++) {
1      path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
1      viewExpression = ViewExpression.deserialize(byteBuffer);
1      viewPathToSourceMap.put(path, viewExpression);
1    }
1
1    AlterLogicalViewProcedure procedure = null;
1    synchronized (this) {
1      ProcedureType type;
1      AlterLogicalViewProcedure alterLogicalViewProcedure;
1      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
1        type = ProcedureFactory.getProcedureType(runningProcedure);
1        if (type == null || !type.equals(ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE)) {
1          continue;
1        }
1        alterLogicalViewProcedure = ((AlterLogicalViewProcedure) runningProcedure);
1        if (queryId.equals(alterLogicalViewProcedure.getQueryId())) {
1          procedure = alterLogicalViewProcedure;
1          break;
1        }
1      }
1
1      if (procedure == null) {
1        procedure =
1            new AlterLogicalViewProcedure(
1                queryId,
1                viewPathToSourceMap,
1                req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe());
1        this.executor.submitProcedure(procedure);
1      }
1    }
1    return waitingProcedureFinished(procedure);
1  }
1
1  public TSStatus setSchemaTemplate(
1      String queryId, String templateName, String templateSetPath, boolean isGeneratedByPipe) {
1    SetTemplateProcedure procedure = null;
1    synchronized (this) {
1      boolean hasOverlappedTask = false;
1      ProcedureType type;
1      SetTemplateProcedure setTemplateProcedure;
1      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
1        type = ProcedureFactory.getProcedureType(runningProcedure);
1        if (type == null || !type.equals(ProcedureType.SET_TEMPLATE_PROCEDURE)) {
1          continue;
1        }
1        setTemplateProcedure = (SetTemplateProcedure) runningProcedure;
1        if (queryId.equals(setTemplateProcedure.getQueryId())) {
1          procedure = setTemplateProcedure;
1          break;
1        }
1        if (templateSetPath.equals(setTemplateProcedure.getTemplateSetPath())) {
1          hasOverlappedTask = true;
1          break;
1        }
1      }
1
1      if (procedure == null) {
1        if (hasOverlappedTask) {
1          return RpcUtils.getStatus(
1              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
1              "Some other task is setting template on target path.");
1        }
1        procedure =
1            new SetTemplateProcedure(queryId, templateName, templateSetPath, isGeneratedByPipe);
1        this.executor.submitProcedure(procedure);
1      }
1    }
1    return waitingProcedureFinished(procedure);
1  }
1
1  public TSStatus deactivateTemplate(
1      String queryId, Map<PartialPath, List<Template>> templateSetInfo, boolean isGeneratedByPipe) {
1    DeactivateTemplateProcedure procedure = null;
1    synchronized (this) {
1      boolean hasOverlappedTask = false;
1      ProcedureType type;
1      DeactivateTemplateProcedure deactivateTemplateProcedure;
1      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
1        type = ProcedureFactory.getProcedureType(runningProcedure);
1        if (type == null || !type.equals(ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE)) {
1          continue;
1        }
1        deactivateTemplateProcedure = (DeactivateTemplateProcedure) runningProcedure;
1        if (queryId.equals(deactivateTemplateProcedure.getQueryId())) {
1          procedure = deactivateTemplateProcedure;
1          break;
1        }
1        for (PartialPath pattern : templateSetInfo.keySet()) {
1          for (PartialPath existingPattern :
1              deactivateTemplateProcedure.getTemplateSetInfo().keySet()) {
1            if (pattern.overlapWith(existingPattern)) {
1              hasOverlappedTask = true;
1              break;
1            }
1          }
1          if (hasOverlappedTask) {
1            break;
1          }
1        }
1        if (hasOverlappedTask) {
1          break;
1        }
1      }
1
1      if (procedure == null) {
1        if (hasOverlappedTask) {
1          return RpcUtils.getStatus(
1              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
1              "Some other task is deactivating some target template from target path.");
1        }
1        procedure = new DeactivateTemplateProcedure(queryId, templateSetInfo, isGeneratedByPipe);
1        this.executor.submitProcedure(procedure);
1      }
1    }
1    return waitingProcedureFinished(procedure);
1  }
1
1  public TSStatus unsetSchemaTemplate(
1      String queryId, Template template, PartialPath path, boolean isGeneratedByPipe) {
1    UnsetTemplateProcedure procedure = null;
1    synchronized (this) {
1      boolean hasOverlappedTask = false;
1      ProcedureType type;
1      UnsetTemplateProcedure unsetTemplateProcedure;
1      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
1        type = ProcedureFactory.getProcedureType(runningProcedure);
1        if (type == null || !type.equals(ProcedureType.UNSET_TEMPLATE_PROCEDURE)) {
1          continue;
1        }
1        unsetTemplateProcedure = (UnsetTemplateProcedure) runningProcedure;
1        if (queryId.equals(unsetTemplateProcedure.getQueryId())) {
1          procedure = unsetTemplateProcedure;
1          break;
1        }
1        if (template.getId() == unsetTemplateProcedure.getTemplateId()
1            && path.equals(unsetTemplateProcedure.getPath())) {
1          hasOverlappedTask = true;
1          break;
1        }
1      }
1
1      if (procedure == null) {
1        if (hasOverlappedTask) {
1          return RpcUtils.getStatus(
1              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
1              "Some other task is unsetting target template from target path "
1                  + path.getFullPath());
1        }
1        procedure = new UnsetTemplateProcedure(queryId, template, path, isGeneratedByPipe);
1        this.executor.submitProcedure(procedure);
1      }
1    }
1    return waitingProcedureFinished(procedure);
1  }
1
1  /**
1   * Generate an {@link AddConfigNodeProcedure}, and serially execute all the {@link
1   * AddConfigNodeProcedure}s.
1   */
1  public void addConfigNode(TConfigNodeRegisterReq req) {
1    final AddConfigNodeProcedure addConfigNodeProcedure =
1        new AddConfigNodeProcedure(req.getConfigNodeLocation(), req.getVersionInfo());
1    this.executor.submitProcedure(addConfigNodeProcedure);
1  }
1
1  /**
1   * Generate a {@link RemoveConfigNodeProcedure}, and serially execute all the {@link
1   * RemoveConfigNodeProcedure}s.
1   */
1  public void removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
1    final RemoveConfigNodeProcedure removeConfigNodeProcedure =
1        new RemoveConfigNodeProcedure(removeConfigNodePlan.getConfigNodeLocation());
1    this.executor.submitProcedure(removeConfigNodeProcedure);
1    LOGGER.info("Submit RemoveConfigNodeProcedure successfully: {}", removeConfigNodePlan);
1  }
1
1  /**
1   * Generate {@link RemoveDataNodesProcedure}s, and serially execute all the {@link
1   * RemoveDataNodesProcedure}s.
1   */
1  public boolean removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
1    Map<Integer, NodeStatus> nodeStatusMap = new HashMap<>();
1    removeDataNodePlan
1        .getDataNodeLocations()
1        .forEach(
1            datanode ->
1                nodeStatusMap.put(
1                    datanode.getDataNodeId(),
1                    configManager.getLoadManager().getNodeStatus(datanode.getDataNodeId())));
1    this.executor.submitProcedure(
1        new RemoveDataNodesProcedure(removeDataNodePlan.getDataNodeLocations(), nodeStatusMap));
1    LOGGER.info(
1        "Submit RemoveDataNodesProcedure successfully, {}",
1        removeDataNodePlan.getDataNodeLocations());
1    return true;
1  }
1
1  public boolean removeAINode(RemoveAINodePlan removeAINodePlan) {
1    this.executor.submitProcedure(new RemoveAINodeProcedure(removeAINodePlan.getAINodeLocation()));
1    LOGGER.info(
1        "Submit RemoveAINodeProcedure successfully, {}", removeAINodePlan.getAINodeLocation());
1    return true;
1  }
1
1  public TSStatus checkRemoveDataNodes(List<TDataNodeLocation> dataNodeLocations) {
1    // 1. Only one RemoveDataNodesProcedure is allowed in the cluster
1    Optional<Procedure<ConfigNodeProcedureEnv>> anotherRemoveProcedure =
1        getExecutor().getProcedures().values().stream()
1            .filter(
1                procedure -> {
1                  if (procedure instanceof RemoveDataNodesProcedure) {
1                    return !procedure.isFinished();
1                  }
1                  return false;
1                })
1            .findAny();
1
1    String failMessage = null;
1    if (anotherRemoveProcedure.isPresent()) {
1      List<TDataNodeLocation> anotherRemoveDataNodes =
1          ((RemoveDataNodesProcedure) anotherRemoveProcedure.get()).getRemovedDataNodes();
1      failMessage =
1          String.format(
1              "Submit RemoveDataNodesProcedure failed, "
1                  + "because another RemoveDataNodesProcedure %s is already in processing. "
1                  + "IoTDB is able to have at most 1 RemoveDataNodesProcedure at the same time. "
1                  + "For further information, please search [pid%d] in log. ",
1              anotherRemoveDataNodes, anotherRemoveProcedure.get().getProcId());
1    }
1
1    // 2. Check if the RemoveDataNodesProcedure conflicts with the RegionMigrateProcedure
1    Set<TConsensusGroupId> removedDataNodesRegionSet =
1        getEnv().getRemoveDataNodeHandler().getRemovedDataNodesRegionSet(dataNodeLocations);
1    Optional<Procedure<ConfigNodeProcedureEnv>> conflictRegionMigrateProcedure =
1        getExecutor().getProcedures().values().stream()
1            .filter(
1                procedure -> {
1                  if (procedure instanceof RegionMigrateProcedure) {
1                    RegionMigrateProcedure regionMigrateProcedure =
1                        (RegionMigrateProcedure) procedure;
1                    if (regionMigrateProcedure.isFinished()) {
1                      return false;
1                    }
1                    return removedDataNodesRegionSet.contains(regionMigrateProcedure.getRegionId())
1                        || dataNodeLocations.contains(regionMigrateProcedure.getDestDataNode());
1                  }
1                  return false;
1                })
1            .findAny();
1    if (conflictRegionMigrateProcedure.isPresent()) {
1      failMessage =
1          String.format(
1              "Submit RemoveDataNodesProcedure failed, "
1                  + "because another RegionMigrateProcedure %s is already in processing which conflicts with this RemoveDataNodesProcedure. "
1                  + "The RegionMigrateProcedure is migrating the region %s to the DataNode %s. "
1                  + "For further information, please search [pid%d] in log. ",
1              conflictRegionMigrateProcedure.get().getProcId(),
1              ((RegionMigrateProcedure) conflictRegionMigrateProcedure.get()).getRegionId(),
1              ((RegionMigrateProcedure) conflictRegionMigrateProcedure.get()).getDestDataNode(),
1              conflictRegionMigrateProcedure.get().getProcId());
1    }
1    // 3. Check if the RegionMigrateProcedure generated by RemoveDataNodesProcedure conflicts with
1    // each other
1    List<RegionMigrationPlan> regionMigrationPlans =
1        getEnv().getRemoveDataNodeHandler().getRegionMigrationPlans(dataNodeLocations);
1    removedDataNodesRegionSet.clear();
1    for (RegionMigrationPlan regionMigrationPlan : regionMigrationPlans) {
1      if (removedDataNodesRegionSet.contains(regionMigrationPlan.getRegionId())) {
1        failMessage =
1            String.format(
1                "Submit RemoveDataNodesProcedure failed, "
1                    + "because the RegionMigrateProcedure generated by this RemoveDataNodesProcedure conflicts with each other. "
1                    + "Only one replica of the same consensus group is allowed to be migrated at the same time."
1                    + "The conflict region id is %s . ",
1                regionMigrationPlan.getRegionId());
1        break;
1      }
1      removedDataNodesRegionSet.add(regionMigrationPlan.getRegionId());
1    }
1
1    // 4. Check if there are any other unknown or readonly DataNodes in the consensus group that are
1    // not the remove DataNodes
1
1    for (TDataNodeLocation removeDataNode : dataNodeLocations) {
1      Set<TDataNodeLocation> relatedDataNodes =
1          getEnv().getRemoveDataNodeHandler().getRelatedDataNodeLocations(removeDataNode);
1      relatedDataNodes.remove(removeDataNode);
1
1      for (TDataNodeLocation relatedDataNode : relatedDataNodes) {
1        NodeStatus nodeStatus =
1            getConfigManager().getLoadManager().getNodeStatus(relatedDataNode.getDataNodeId());
1        if (nodeStatus == NodeStatus.Unknown || nodeStatus == NodeStatus.ReadOnly) {
1          failMessage =
1              String.format(
1                  "Submit RemoveDataNodesProcedure failed, "
1                      + "because when there are other unknown or readonly nodes in the consensus group that are not remove nodes, "
1                      + "the remove operation cannot be performed for security reasons. "
1                      + "Please check the status of the node %s and ensure it is running.",
1                  relatedDataNode.getDataNodeId());
1        }
1      }
1    }
1
1    if (failMessage != null) {
1      LOGGER.warn(failMessage);
1      TSStatus failStatus = new TSStatus(TSStatusCode.REMOVE_DATANODE_ERROR.getStatusCode());
1      failStatus.setMessage(failMessage);
1      return failStatus;
1    }
1    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1  }
1
1  // region region operation related check
1
1  /**
1   * Checks whether region migration is allowed.
1   *
1   * @param migrateRegionReq the migration request details
1   * @param regionGroupId the ID of the consensus group for the region
1   * @param originalDataNode the original DataNode location from which the region is being migrated
1   * @param destDataNode the destination DataNode location to which the region is being migrated
1   * @param coordinatorForAddPeer the DataNode location acting as the coordinator for adding a peer
1   * @return the status of the migration request (TSStatus)
1   */
1  private TSStatus checkMigrateRegion(
1      TMigrateRegionReq migrateRegionReq,
1      TConsensusGroupId regionGroupId,
1      TDataNodeLocation originalDataNode,
1      TDataNodeLocation destDataNode,
1      TDataNodeLocation coordinatorForAddPeer) {
1    String failMessage;
1    if ((failMessage =
1            regionOperationCommonCheck(
1                regionGroupId,
1                destDataNode,
1                Arrays.asList(
1                    new Pair<>("Original DataNode", originalDataNode),
1                    new Pair<>("Destination DataNode", destDataNode),
1                    new Pair<>("Coordinator for add peer", coordinatorForAddPeer)),
1                migrateRegionReq.getModel()))
1        != null) {
1      // do nothing
1    } else if (configManager
1        .getPartitionManager()
1        .getAllReplicaSets(originalDataNode.getDataNodeId())
1        .stream()
1        .noneMatch(replicaSet -> replicaSet.getRegionId().equals(regionGroupId))) {
1      failMessage =
1          String.format(
1              "Submit RegionMigrateProcedure failed, because the original DataNode %s doesn't contain Region %s",
1              migrateRegionReq.getFromId(), migrateRegionReq.getRegionId());
1    } else if (configManager
1        .getPartitionManager()
1        .getAllReplicaSets(destDataNode.getDataNodeId())
1        .stream()
1        .anyMatch(replicaSet -> replicaSet.getRegionId().equals(regionGroupId))) {
1      failMessage =
1          String.format(
1              "Submit RegionMigrateProcedure failed, because the target DataNode %s already contains Region %s",
1              migrateRegionReq.getToId(), migrateRegionReq.getRegionId());
1    }
1
1    if (failMessage != null) {
1      LOGGER.warn(failMessage);
1      TSStatus failStatus = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
1      failStatus.setMessage(failMessage);
1      return failStatus;
1    }
1    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1  }
1
1  private TSStatus checkReconstructRegion(
1      TReconstructRegionReq req,
1      TConsensusGroupId regionId,
1      TDataNodeLocation targetDataNode,
1      TDataNodeLocation coordinator) {
1    String failMessage =
1        regionOperationCommonCheck(
1            regionId,
1            targetDataNode,
1            Arrays.asList(
1                new Pair<>("Target DataNode", targetDataNode),
1                new Pair<>("Coordinator", coordinator)),
1            req.getModel());
1
1    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
1    if (configManager
1            .getPartitionManager()
1            .getAllReplicaSetsMap(regionId.getType())
1            .get(regionId)
1            .getDataNodeLocationsSize()
1        == 1) {
1      failMessage = String.format("%s only has 1 replica, it cannot be reconstructed", regionId);
1    } else if (configManager
1        .getPartitionManager()
1        .getAllReplicaSets(targetDataNode.getDataNodeId())
1        .stream()
1        .noneMatch(replicaSet -> replicaSet.getRegionId().equals(regionId))) {
1      failMessage =
1          String.format(
1              "Submit ReconstructRegionProcedure failed, because the target DataNode %s doesn't contain Region %s",
1              req.getDataNodeId(), regionId);
1    }
1
1    if (failMessage != null) {
1      LOGGER.warn(failMessage);
1      TSStatus failStatus = new TSStatus(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode());
1      failStatus.setMessage(failMessage);
1      return failStatus;
1    }
1    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1  }
1
1  private TSStatus checkExtendRegion(
1      TExtendRegionReq req,
1      TConsensusGroupId regionId,
1      TDataNodeLocation targetDataNode,
1      TDataNodeLocation coordinator) {
1    String failMessage =
1        regionOperationCommonCheck(
1            regionId,
1            targetDataNode,
1            Arrays.asList(
1                new Pair<>("Target DataNode", targetDataNode),
1                new Pair<>("Coordinator", coordinator)),
1            req.getModel());
1    if (configManager
1        .getPartitionManager()
1        .getAllReplicaSets(targetDataNode.getDataNodeId())
1        .stream()
1        .anyMatch(replicaSet -> replicaSet.getRegionId().equals(regionId))) {
1      failMessage =
1          String.format(
1              "Target DataNode %s already contains region %s",
1              targetDataNode.getDataNodeId(), regionId);
1    }
1
1    if (failMessage != null) {
1      LOGGER.warn(failMessage);
1      TSStatus failStatus = new TSStatus(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode());
1      failStatus.setMessage(failMessage);
1      return failStatus;
1    }
1    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1  }
1
1  private TSStatus checkRemoveRegion(
1      TRemoveRegionReq req,
1      TConsensusGroupId regionId,
1      @Nullable TDataNodeLocation targetDataNode,
1      TDataNodeLocation coordinator) {
1    String failMessage =
1        regionOperationCommonCheck(
1            regionId,
1            targetDataNode,
1            Arrays.asList(new Pair<>("Coordinator", coordinator)),
1            req.getModel());
1
1    if (configManager
1            .getPartitionManager()
1            .getAllReplicaSetsMap(regionId.getType())
1            .get(regionId)
1            .getDataNodeLocationsSize()
1        == 1) {
1      failMessage = String.format("%s only has 1 replica, it cannot be removed", regionId);
1    } else if (targetDataNode != null
1        && configManager
1            .getPartitionManager()
1            .getAllReplicaSets(targetDataNode.getDataNodeId())
1            .stream()
1            .noneMatch(replicaSet -> replicaSet.getRegionId().equals(regionId))) {
1      failMessage =
1          String.format(
1              "Target DataNode %s doesn't contain Region %s", req.getDataNodeId(), regionId);
1    }
1
1    if (failMessage != null) {
1      LOGGER.warn(failMessage);
1      TSStatus failStatus = new TSStatus(TSStatusCode.REMOVE_REGION_PEER_ERROR.getStatusCode());
1      failStatus.setMessage(failMessage);
1      return failStatus;
1    }
1    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1  }
1
1  /**
1   * The common checks of all region operations, include migration, reconstruction, extension,
1   * removing
1   *
1   * @param regionId region group id, also called consensus group id
1   * @param targetDataNode DataNode should in Running status
1   * @param relatedDataNodes Pair<Identity, Node Location>
1   * @return The reason if check failed, or null if check pass
1   */
1  private String regionOperationCommonCheck(
1      TConsensusGroupId regionId,
1      TDataNodeLocation targetDataNode,
1      List<Pair<String, TDataNodeLocation>> relatedDataNodes,
1      Model model) {
1    String failMessage;
1    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
1
1    if (TConsensusGroupType.DataRegion == regionId.getType()
1        && ConsensusFactory.SIMPLE_CONSENSUS.equals(conf.getDataRegionConsensusProtocolClass())) {
1      failMessage = "SimpleConsensus not supports region operation.";
1    } else if (TConsensusGroupType.SchemaRegion == regionId.getType()
1        && ConsensusFactory.SIMPLE_CONSENSUS.equals(conf.getSchemaRegionConsensusProtocolClass())) {
1      failMessage = "SimpleConsensus not supports region operation.";
1    } else if ((failMessage = checkRegionOperationDuplication(regionId)) != null) {
1      // need to do nothing more
1    } else if (relatedDataNodes.stream().anyMatch(pair -> pair.getRight() == null)) {
1      Pair<String, TDataNodeLocation> nullPair =
1          relatedDataNodes.stream().filter(pair -> pair.getRight() == null).findAny().get();
1      failMessage = String.format("Cannot find %s", nullPair.getLeft());
1    } else if (targetDataNode != null
1        && !configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
1            .map(TDataNodeConfiguration::getLocation)
1            .map(TDataNodeLocation::getDataNodeId)
1            .collect(Collectors.toSet())
1            .contains(targetDataNode.getDataNodeId())) {
1      // Here we only check Running DataNode to implement migration, because removing nodes may not
1      // exist when add peer is performing
1      failMessage =
1          String.format(
1              "Target DataNode %s is not in Running status.", targetDataNode.getDataNodeId());
1    } else if ((failMessage = checkRegionOperationWithRemoveDataNode(regionId, targetDataNode))
1        != null) {
1      // need to do nothing more
1    } else if ((failMessage = checkRegionOperationModelCorrectness(regionId, model)) != null) {
1      // need to do nothing more
1    }
1
1    return failMessage;
1  }
1
1  private String checkRegionOperationWithRemoveDataNode(
1      TConsensusGroupId regionId, TDataNodeLocation targetDataNode) {
1    Optional<Procedure<ConfigNodeProcedureEnv>> conflictRemoveDataNodesProcedure =
1        getExecutor().getProcedures().values().stream()
1            .filter(
1                procedure -> {
1                  if (procedure instanceof RemoveDataNodesProcedure) {
1                    return !procedure.isFinished();
1                  }
1                  return false;
1                })
1            .findAny();
1
1    if (conflictRemoveDataNodesProcedure.isPresent()) {
1      RemoveDataNodeHandler removeDataNodeHandler = env.getRemoveDataNodeHandler();
1      List<TDataNodeLocation> removedDataNodes =
1          ((RemoveDataNodesProcedure) conflictRemoveDataNodesProcedure.get()).getRemovedDataNodes();
1      Set<TConsensusGroupId> removedDataNodesRegionSet =
1          removeDataNodeHandler.getRemovedDataNodesRegionSet(removedDataNodes);
1      if (removedDataNodesRegionSet.contains(regionId)) {
1        return String.format(
1            "Another RemoveDataNodesProcedure %s is already in processing which conflicts with this procedure. "
1                + "The RemoveDataNodesProcedure is removing the DataNodes %s which contains the region %s. "
1                + "For further information, please search [pid%d] in log. ",
1            conflictRemoveDataNodesProcedure.get().getProcId(),
1            removedDataNodes,
1            regionId,
1            conflictRemoveDataNodesProcedure.get().getProcId());
1      } else if (removedDataNodes.contains(targetDataNode)) {
1        return String.format(
1            "Another RemoveDataNodesProcedure %s is already in processing which conflicts with this procedure. "
1                + "The RemoveDataNodesProcedure is removing the target DataNode %s. "
1                + "For further information, please search [pid%d] in log. ",
1            conflictRemoveDataNodesProcedure.get().getProcId(),
1            targetDataNode,
1            conflictRemoveDataNodesProcedure.get().getProcId());
1      }
1    }
1    return null;
1  }
1
1  private String checkRegionOperationDuplication(TConsensusGroupId regionId) {
1    List<? extends RegionOperationProcedure<?>> otherRegionMemberChangeProcedures =
1        getRegionOperationProcedures()
1            .filter(
1                regionMemberChangeProcedure ->
1                    regionId.equals(regionMemberChangeProcedure.getRegionId()))
1            .collect(Collectors.toList());
1    if (!otherRegionMemberChangeProcedures.isEmpty()) {
1      return String.format(
1          "%s has some other region operation procedures in progress, their procedure id is: %s",
1          regionId, otherRegionMemberChangeProcedures);
1    }
1    return null;
1  }
1
1  public List<TConsensusGroupId> getRegionOperationConsensusIds() {
1    return getRegionOperationProcedures()
1        .map(RegionOperationProcedure::getRegionId)
1        .distinct()
1        .collect(Collectors.toList());
1  }
1
1  private Stream<RegionOperationProcedure<?>> getRegionOperationProcedures() {
1    return getExecutor().getProcedures().values().stream()
1        .filter(procedure -> !procedure.isFinished())
1        .filter(procedure -> procedure instanceof RegionOperationProcedure)
1        .map(procedure -> (RegionOperationProcedure<?>) procedure);
1  }
1
1  private String checkRegionOperationModelCorrectness(TConsensusGroupId regionId, Model model) {
1    String databaseName = configManager.getPartitionManager().getRegionDatabase(regionId);
    boolean isTreeModelDatabase = databaseName.startsWith(SqlConstant.TREE_MODEL_DATABASE_PREFIX);
    if (Model.TREE == model && isTreeModelDatabase
        || Model.TABLE == model && !isTreeModelDatabase) {
      return null;
    }
    return String.format(
        "The region's database %s is belong to %s model, but the model you are operating is %s",
        databaseName,
        isTreeModelDatabase ? Model.TREE.toString() : Model.TABLE.toString(),
        model.toString());
  }

  // end region

  public TSStatus migrateRegion(TMigrateRegionReq migrateRegionReq) {
    try (AutoCloseableLock ignoredLock =
        AutoCloseableLock.acquire(env.getSubmitRegionMigrateLock())) {
      TConsensusGroupId regionGroupId;
      Optional<TConsensusGroupId> optional =
          configManager
              .getPartitionManager()
              .generateTConsensusGroupIdByRegionId(migrateRegionReq.getRegionId());
      if (optional.isPresent()) {
        regionGroupId = optional.get();
      } else {
        LOGGER.error("get region group id fail");
        return new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode())
            .setMessage("get region group id fail");
      }

      // find original dn and dest dn
      final TDataNodeLocation originalDataNode =
          configManager
              .getNodeManager()
              .getRegisteredDataNode(migrateRegionReq.getFromId())
              .getLocation();
      final TDataNodeLocation destDataNode =
          configManager
              .getNodeManager()
              .getRegisteredDataNode(migrateRegionReq.getToId())
              .getLocation();
      // select coordinator for adding peer
      RegionMaintainHandler handler = env.getRegionMaintainHandler();
      // TODO: choose the DataNode which has lowest load
      final TDataNodeLocation coordinatorForAddPeer =
          handler
              .filterDataNodeWithOtherRegionReplica(
                  regionGroupId,
                  destDataNode,
                  NodeStatus.Running,
                  NodeStatus.Removing,
                  NodeStatus.ReadOnly)
              .orElse(null);
      // Select coordinator for removing peer
      // For now, destDataNode temporarily acts as the coordinatorForRemovePeer
      final TDataNodeLocation coordinatorForRemovePeer = destDataNode;

      TSStatus status =
          checkMigrateRegion(
              migrateRegionReq,
              regionGroupId,
              originalDataNode,
              destDataNode,
              coordinatorForAddPeer);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // finally, submit procedure
      this.executor.submitProcedure(
          new RegionMigrateProcedure(
              regionGroupId,
              originalDataNode,
              destDataNode,
              coordinatorForAddPeer,
              coordinatorForRemovePeer));
      LOGGER.info(
          "[MigrateRegion] Submit RegionMigrateProcedure successfully, Region: {}, Origin DataNode: {}, Dest DataNode: {}, Add Coordinator: {}, Remove Coordinator: {}",
          regionGroupId,
          originalDataNode,
          destDataNode,
          coordinatorForAddPeer,
          coordinatorForRemovePeer);

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
  }

  public TSStatus reconstructRegion(TReconstructRegionReq req) {
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    final TDataNodeLocation targetDataNode =
        configManager.getNodeManager().getRegisteredDataNode(req.getDataNodeId()).getLocation();
    try (AutoCloseableLock ignoredLock =
        AutoCloseableLock.acquire(env.getSubmitRegionMigrateLock())) {
      List<ReconstructRegionProcedure> procedures = new ArrayList<>();
      for (int x : req.getRegionIds()) {
        TConsensusGroupId regionId =
            configManager
                .getPartitionManager()
                .generateTConsensusGroupIdByRegionId(x)
                .orElseThrow(() -> new IllegalArgumentException("Region id " + x + " is invalid"));
        final TDataNodeLocation coordinator =
            handler
                .filterDataNodeWithOtherRegionReplica(
                    regionId,
                    targetDataNode,
                    NodeStatus.Running,
                    NodeStatus.Removing,
                    NodeStatus.ReadOnly)
                .orElse(null);
        TSStatus status = checkReconstructRegion(req, regionId, targetDataNode, coordinator);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
        procedures.add(new ReconstructRegionProcedure(regionId, targetDataNode, coordinator));
      }
      // all checks pass, submit all procedures
      procedures.forEach(
          reconstructRegionProcedure -> {
            this.executor.submitProcedure(reconstructRegionProcedure);
            LOGGER.info(
                "[ReconstructRegion] Submit ReconstructRegionProcedure successfully, {}",
                reconstructRegionProcedure);
          });
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  public TSStatus extendRegions(TExtendRegionReq req) {
    return processExtendOrRemoveRegions(
        req.getRegionId(), req, this::extendOneRegion, TSStatusCode.EXTEND_REGION_ERROR);
  }

  public TSStatus removeRegions(TRemoveRegionReq req) {
    return processExtendOrRemoveRegions(
        req.getRegionId(), req, this::removeOneRegion, TSStatusCode.REMOVE_REGION_PEER_ERROR);
  }

  private <R> TSStatus processExtendOrRemoveRegions(
      Iterable<Integer> regionIds,
      R req,
      BiFunction<Integer, R, TSStatus> regionAction,
      TSStatusCode errorCode) {
    TSStatus resp = new TSStatus();
    StringBuilder messageBuilder = new StringBuilder();

    int total = 0, success = 0;
    for (int regionId : regionIds) {
      total++;
      TSStatus subStatus = regionAction.apply(regionId, req);
      if (subStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        messageBuilder.append("region ").append(regionId).append(": Successfully submitted\n");
        success++;
      } else {
        messageBuilder
            .append("region ")
            .append(regionId)
            .append(": ")
            .append(subStatus.getMessage())
            .append('\n');
      }
      resp.addToSubStatus(subStatus);
    }

    messageBuilder.insert(
        0,
        String.format(
            "Total regions: %d, successfully submitted: %d, failed to submit: %d\n",
            total, success, total - success));

    resp.setCode(
        total == success ? TSStatusCode.SUCCESS_STATUS.getStatusCode() : errorCode.getStatusCode());
    resp.setMessage(messageBuilder.toString());
    return resp;
  }

  private TSStatus extendOneRegion(int theRegionId, TExtendRegionReq req) {
    try (AutoCloseableLock ignoredLock =
        AutoCloseableLock.acquire(env.getSubmitRegionMigrateLock())) {
      TConsensusGroupId regionId;
      Optional<TConsensusGroupId> optional =
          configManager.getPartitionManager().generateTConsensusGroupIdByRegionId(theRegionId);
      if (optional.isPresent()) {
        regionId = optional.get();
      } else {
        LOGGER.error("get region group id fail");
        return new TSStatus(TSStatusCode.EXTEND_REGION_ERROR.getStatusCode())
            .setMessage("get region group id fail");
      }

      // find target dn
      final TDataNodeLocation targetDataNode =
          configManager.getNodeManager().getRegisteredDataNode(req.getDataNodeId()).getLocation();
      // select coordinator for adding peer
      RegionMaintainHandler handler = env.getRegionMaintainHandler();
      // TODO: choose the DataNode which has lowest load
      final TDataNodeLocation coordinator =
          handler
              .filterDataNodeWithOtherRegionReplica(
                  regionId,
                  targetDataNode,
                  NodeStatus.Running,
                  NodeStatus.Removing,
                  NodeStatus.ReadOnly)
              .orElse(null);
      // do the check
      TSStatus status = checkExtendRegion(req, regionId, targetDataNode, coordinator);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
      // submit procedure
      AddRegionPeerProcedure procedure =
          new AddRegionPeerProcedure(regionId, coordinator, targetDataNode);
      this.executor.submitProcedure(procedure);
      LOGGER.info("[ExtendRegion] Submit AddRegionPeerProcedure successfully: {}", procedure);

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
  }

  private TSStatus removeOneRegion(int theRegionId, TRemoveRegionReq req) {
    try (AutoCloseableLock ignoredLock =
        AutoCloseableLock.acquire(env.getSubmitRegionMigrateLock())) {
      TConsensusGroupId regionId;
      Optional<TConsensusGroupId> optional =
          configManager.getPartitionManager().generateTConsensusGroupIdByRegionId(theRegionId);
      if (optional.isPresent()) {
        regionId = optional.get();
      } else {
        LOGGER.error("get region group id fail");
        return new TSStatus(TSStatusCode.REMOVE_REGION_PEER_ERROR.getStatusCode())
            .setMessage("get region group id fail");
      }

      // find target dn
      final TDataNodeLocation targetDataNode =
          configManager.getNodeManager().getRegisteredDataNode(req.getDataNodeId()).getLocation();

      // select coordinator for removing peer
      RegionMaintainHandler handler = env.getRegionMaintainHandler();
      final TDataNodeLocation coordinator =
          handler
              .filterDataNodeWithOtherRegionReplica(
                  regionId,
                  targetDataNode,
                  NodeStatus.Running,
                  NodeStatus.Removing,
                  NodeStatus.ReadOnly)
              .orElse(null);

      // do the check
      TSStatus status = checkRemoveRegion(req, regionId, targetDataNode, coordinator);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // SPECIAL CASE
      if (targetDataNode == null) {
        // If targetDataNode is null, it means the target DataNode does not exist in the
        // NodeManager.
        // In this case, simply clean up the partition table once and do nothing else.
        LOGGER.warn(
            "Remove region: Target DataNode {} not found, will simply clean up the partition table of region {} and do nothing else.",
            req.getDataNodeId(),
            req.getRegionId());
        this.executor
            .getEnvironment()
            .getRegionMaintainHandler()
            .removeRegionLocation(
                regionId, buildFakeDataNodeLocation(req.getDataNodeId(), "FakeIpForRemoveRegion"));
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }

      // submit procedure
      RemoveRegionPeerProcedure procedure =
          new RemoveRegionPeerProcedure(regionId, coordinator, targetDataNode);
      this.executor.submitProcedure(procedure);
      LOGGER.info(
          "[RemoveRegionPeer] Submit RemoveRegionPeerProcedure successfully: {}", procedure);

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
  }

  private static TDataNodeLocation buildFakeDataNodeLocation(int dataNodeId, String message) {
    TEndPoint fakeEndPoint = new TEndPoint(message, -1);
    return new TDataNodeLocation(
        dataNodeId, fakeEndPoint, fakeEndPoint, fakeEndPoint, fakeEndPoint, fakeEndPoint);
  }

  // endregion

  /**
   * Generate {@link CreateRegionGroupsProcedure} and wait until it finished.
   *
   * @return {@link TSStatusCode#SUCCESS_STATUS} if all RegionGroups have been created successfully,
   *     {@link TSStatusCode#CREATE_REGION_ERROR} otherwise
   */
  public TSStatus createRegionGroups(
      final TConsensusGroupType consensusGroupType,
      final CreateRegionGroupsPlan createRegionGroupsPlan) {
    final CreateRegionGroupsProcedure procedure =
        new CreateRegionGroupsProcedure(consensusGroupType, createRegionGroupsPlan);
    executor.submitProcedure(procedure);
    final TSStatus status = waitingProcedureFinished(procedure);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      return new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode())
          .setMessage(status.getMessage());
    }
  }

  /**
   * Generate {@link CreateTriggerProcedure} and wait until it finished.
   *
   * @return {@link TSStatusCode#SUCCESS_STATUS} if the trigger has been created successfully,
   *     {@link TSStatusCode#CREATE_TRIGGER_ERROR} otherwise
   */
  public TSStatus createTrigger(
      TriggerInformation triggerInformation, Binary jarFile, boolean isGeneratedByPipe) {
    final CreateTriggerProcedure createTriggerProcedure =
        new CreateTriggerProcedure(triggerInformation, jarFile, isGeneratedByPipe);
    try {
      if (jarFile != null
          && new UpdateProcedurePlan(createTriggerProcedure).getSerializedSize() > planSizeLimit) {
        return new TSStatus(TSStatusCode.CREATE_TRIGGER_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    "Fail to create trigger[%s], the size of Jar is too large, you can increase the value of property 'config_node_ratis_log_appender_buffer_size_max' on ConfigNode",
                    triggerInformation.getTriggerName()));
      }
    } catch (IOException e) {
      return new TSStatus(TSStatusCode.CREATE_TRIGGER_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }

    executor.submitProcedure(createTriggerProcedure);
    TSStatus status = waitingProcedureFinished(createTriggerProcedure);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      return new TSStatus(TSStatusCode.CREATE_TRIGGER_ERROR.getStatusCode())
          .setMessage(status.getMessage());
    }
  }

  /**
   * Generate {@link DropTriggerProcedure} and wait until it finished.
   *
   * @return {@link TSStatusCode#SUCCESS_STATUS} if the trigger has been dropped successfully,
   *     {@link TSStatusCode#DROP_TRIGGER_ERROR} otherwise
   */
  public TSStatus dropTrigger(String triggerName, boolean isGeneratedByPipe) {
    DropTriggerProcedure procedure = new DropTriggerProcedure(triggerName, isGeneratedByPipe);
    executor.submitProcedure(procedure);
    TSStatus status = waitingProcedureFinished(procedure);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      return new TSStatus(TSStatusCode.DROP_TRIGGER_ERROR.getStatusCode())
          .setMessage(status.getMessage());
    }
  }

  public TSStatus createCQ(TCreateCQReq req, ScheduledExecutorService scheduledExecutor) {
    CreateCQProcedure procedure = new CreateCQProcedure(req, scheduledExecutor);
    executor.submitProcedure(procedure);
    return waitingProcedureFinished(procedure);
  }

  public TSStatus createModel(String modelName, String uri) {
    long procedureId = executor.submitProcedure(new CreateModelProcedure(modelName, uri));
    LOGGER.info("CreateModelProcedure was submitted, procedureId: {}.", procedureId);
    return RpcUtils.SUCCESS_STATUS;
  }

  public TSStatus dropModel(String modelId) {
    DropModelProcedure procedure = new DropModelProcedure(modelId);
    executor.submitProcedure(procedure);
    TSStatus status = waitingProcedureFinished(procedure);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      return new TSStatus(TSStatusCode.DROP_MODEL_ERROR.getStatusCode())
          .setMessage(status.getMessage());
    }
  }

  public TSStatus createPipePlugin(
      PipePluginMeta pipePluginMeta, byte[] jarFile, boolean isSetIfNotExistsCondition) {
    final CreatePipePluginProcedure createPipePluginProcedure =
        new CreatePipePluginProcedure(pipePluginMeta, jarFile, isSetIfNotExistsCondition);
    try {
      if (jarFile != null
          && new UpdateProcedurePlan(createPipePluginProcedure).getSerializedSize()
              > planSizeLimit) {
        return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    "Fail to create pipe plugin[%s], the size of Jar is too large, you can increase the value of property 'config_node_ratis_log_appender_buffer_size_max' on ConfigNode",
                    pipePluginMeta.getPluginName()));
      }
    } catch (IOException e) {
      return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }

    executor.submitProcedure(createPipePluginProcedure);
    TSStatus status = waitingProcedureFinished(createPipePluginProcedure);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(status.getMessage());
    }
  }

  public TSStatus dropPipePlugin(TDropPipePluginReq req) {
    DropPipePluginProcedure procedure =
        new DropPipePluginProcedure(
            req.getPluginName(), req.isSetIfExistsCondition() && req.isIfExistsCondition());
    executor.submitProcedure(procedure);
    TSStatus status = waitingProcedureFinished(procedure);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      return new TSStatus(TSStatusCode.DROP_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(status.getMessage());
    }
  }

  public TSStatus createConsensusPipe(TCreatePipeReq req) {
    try {
      CreatePipeProcedureV2 procedure = new CreatePipeProcedureV2(req);
      executor.submitProcedure(procedure);
      return handleConsensusPipeProcedure(procedure);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus createPipe(TCreatePipeReq req) {
    try {
      CreatePipeProcedureV2 procedure = new CreatePipeProcedureV2(req);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus alterPipe(TAlterPipeReq req) {
    try {
      AlterPipeProcedureV2 procedure = new AlterPipeProcedureV2(req);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus startConsensusPipe(String pipeName) {
    try {
      StartPipeProcedureV2 procedure = new StartPipeProcedureV2(pipeName);
      executor.submitProcedure(procedure);
      return handleConsensusPipeProcedure(procedure);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus startPipe(String pipeName) {
    try {
      StartPipeProcedureV2 procedure = new StartPipeProcedureV2(pipeName);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus stopConsensusPipe(String pipeName) {
    try {
      StopPipeProcedureV2 procedure = new StopPipeProcedureV2(pipeName);
      executor.submitProcedure(procedure);
      return handleConsensusPipeProcedure(procedure);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus stopPipe(String pipeName) {
    try {
      StopPipeProcedureV2 procedure = new StopPipeProcedureV2(pipeName);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus dropConsensusPipe(String pipeName) {
    try {
      DropPipeProcedureV2 procedure = new DropPipeProcedureV2(pipeName);
      executor.submitProcedure(procedure);
      return handleConsensusPipeProcedure(procedure);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus dropPipe(String pipeName) {
    try {
      DropPipeProcedureV2 procedure = new DropPipeProcedureV2(pipeName);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  private TSStatus handleConsensusPipeProcedure(Procedure<?> procedure) {
    TSStatus status = waitingProcedureFinished(procedure);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      // if time out, optimistically believe that this procedure will execute successfully.
      if (status.getMessage().equals(PROCEDURE_TIMEOUT_MESSAGE)) {
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      // otherwise, some exceptions must have occurred, throw them.
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
    }
  }

  public void pipeHandleLeaderChange(
      Map<TConsensusGroupId, Pair<Integer, Integer>> dataRegionGroupToOldAndNewLeaderPairMap) {
    try {
      final long procedureId =
          executor.submitProcedure(
              new PipeHandleLeaderChangeProcedure(dataRegionGroupToOldAndNewLeaderPairMap));
      LOGGER.info("PipeHandleLeaderChangeProcedure was submitted, procedureId: {}.", procedureId);
    } catch (Exception e) {
      LOGGER.warn("PipeHandleLeaderChangeProcedure was failed to submit.", e);
    }
  }

  public void pipeHandleMetaChange(
      boolean needWriteConsensusOnConfigNodes, boolean needPushPipeMetaToDataNodes) {
    try {
      final long procedureId =
          executor.submitProcedure(
              new PipeHandleMetaChangeProcedure(
                  needWriteConsensusOnConfigNodes, needPushPipeMetaToDataNodes));
      LOGGER.info("PipeHandleMetaChangeProcedure was submitted, procedureId: {}.", procedureId);
    } catch (Exception e) {
      LOGGER.warn("PipeHandleMetaChangeProcedure was failed to submit.", e);
    }
  }

  public TSStatus pipeHandleMetaChangeWithBlock(
      boolean needWriteConsensusOnConfigNodes, boolean needPushPipeMetaToDataNodes) {
    try {
      PipeHandleMetaChangeProcedure procedure =
          new PipeHandleMetaChangeProcedure(
              needWriteConsensusOnConfigNodes, needPushPipeMetaToDataNodes);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus pipeMetaSync() {
    try {
      PipeMetaSyncProcedure procedure = new PipeMetaSyncProcedure();
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus createTopic(TCreateTopicReq req) {
    try {
      CreateTopicProcedure procedure = new CreateTopicProcedure(req);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus dropTopic(String topicName) {
    try {
      DropTopicProcedure procedure = new DropTopicProcedure(topicName);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.DROP_TOPIC_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.DROP_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus topicMetaSync() {
    try {
      TopicMetaSyncProcedure procedure = new TopicMetaSyncProcedure();
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus createConsumer(TCreateConsumerReq req) {
    try {
      CreateConsumerProcedure procedure = new CreateConsumerProcedure(req);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.CREATE_CONSUMER_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CREATE_CONSUMER_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus dropConsumer(TCloseConsumerReq req) {
    try {
      DropConsumerProcedure procedure = new DropConsumerProcedure(req);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.DROP_CONSUMER_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.DROP_CONSUMER_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus consumerGroupMetaSync() {
    try {
      ConsumerGroupMetaSyncProcedure procedure = new ConsumerGroupMetaSyncProcedure();
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else {
        return new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus createSubscription(TSubscribeReq req) {
    try {
      CreateSubscriptionProcedure procedure = new CreateSubscriptionProcedure(req);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else if (PROCEDURE_TIMEOUT_MESSAGE.equals(status.getMessage())) {
        // we assume that a timeout has occurred in the procedure related to the pipe in the
        // subscription procedure
        return new TSStatus(TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      } else {
        return new TSStatus(TSStatusCode.SUBSCRIPTION_SUBSCRIBE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.SUBSCRIPTION_SUBSCRIBE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus dropSubscription(TUnsubscribeReq req) {
    try {
      DropSubscriptionProcedure procedure = new DropSubscriptionProcedure(req);
      executor.submitProcedure(procedure);
      TSStatus status = waitingProcedureFinished(procedure);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      } else if (PROCEDURE_TIMEOUT_MESSAGE.equals(status.getMessage())) {
        // we assume that a timeout has occurred in the procedure related to the pipe in the
        // subscription procedure
        return new TSStatus(TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      } else {
        return new TSStatus(TSStatusCode.SUBSCRIPTION_UNSUBSCRIBE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(status.getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.SUBSCRIPTION_UNSUBSCRIBE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus operateAuthPlan(
      AuthorPlan authorPlan, List<TDataNodeConfiguration> dns, boolean isGeneratedByPipe) {
    try {
      AuthOperationProcedure procedure =
          new AuthOperationProcedure(authorPlan, dns, isGeneratedByPipe);
      executor.submitProcedure(procedure);
      return waitingProcedureFinished(procedure);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.AUTH_OPERATE_EXCEPTION.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus setTTL(SetTTLPlan setTTLPlan, final boolean isGeneratedByPipe) {
    SetTTLProcedure procedure = new SetTTLProcedure(setTTLPlan, isGeneratedByPipe);
    executor.submitProcedure(procedure);
    return waitingProcedureFinished(procedure);
  }

  private TSStatus waitingProcedureFinished(final long procedureId) {
    return waitingProcedureFinished(executor.getProcedures().get(procedureId));
  }

  /**
   * Waiting until the specific procedure finished.
   *
   * @param procedure The specific procedure
   * @return TSStatus the running result of this procedure
   */
  private TSStatus waitingProcedureFinished(Procedure<?> procedure) {
    if (procedure == null) {
      LOGGER.error("Unexpected null procedure parameters for waitingProcedureFinished");
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR);
    }
    TSStatus status;
    final long startTimeForCurrentProcedure = System.currentTimeMillis();
    while (executor.isRunning()
        && !executor.isFinished(procedure.getProcId())
        && System.currentTimeMillis() - startTimeForCurrentProcedure < PROCEDURE_WAIT_TIME_OUT) {
      sleepWithoutInterrupt(PROCEDURE_WAIT_RETRY_TIMEOUT);
    }
    if (!procedure.isFinished()) {
      // The procedure is still executing
      status =
          RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK, PROCEDURE_TIMEOUT_MESSAGE);
    } else {
      if (procedure.isSuccess()) {
        if (procedure.getResult() != null) {
          status =
              RpcUtils.getStatus(
                  TSStatusCode.SUCCESS_STATUS, Arrays.toString(procedure.getResult()));
        } else {
          status = StatusUtils.OK;
        }
      } else {
        if (procedure.getException().getCause() instanceof IoTDBException) {
          final IoTDBException e = (IoTDBException) procedure.getException().getCause();
          if (e instanceof BatchProcessException) {
            status =
                RpcUtils.getStatus(
                    Arrays.stream(((BatchProcessException) e).getFailingStatus())
                        .collect(Collectors.toList()));
          } else {
            status = RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
          }
        } else {
          status =
              StatusUtils.EXECUTE_STATEMENT_ERROR.setMessage(procedure.getException().getMessage());
        }
      }
    }
    return status;
  }

  private static String wrapTimeoutMessageForPipeProcedure(String message) {
    if (message.equals(PROCEDURE_TIMEOUT_MESSAGE)) {
      return message
          + " Please manually check later whether the procedure is executed successfully.";
    }
    return message;
  }

  public static void sleepWithoutInterrupt(final long timeToSleep) {
    long currentTime = System.currentTimeMillis();
    final long endTime = timeToSleep + currentTime;
    boolean interrupted = false;
    while (currentTime < endTime) {
      try {
        Thread.sleep(endTime - currentTime);
      } catch (final InterruptedException e) {
        interrupted = true;
      }
      currentTime = System.currentTimeMillis();
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public TSStatus createTable(final String database, final TsTable table) {
    return executeWithoutDuplicate(
        database,
        table,
        table.getTableName(),
        null,
        ProcedureType.CREATE_TABLE_PROCEDURE,
        new CreateTableProcedure(database, table, false));
  }

  public TSStatus createTableView(
      final String database, final TsTable table, final boolean replace) {
    return executeWithoutDuplicate(
        database,
        table,
        table.getTableName(),
        null,
        ProcedureType.CREATE_TABLE_VIEW_PROCEDURE,
        new CreateTableViewProcedure(database, table, replace, false));
  }

  public TSStatus alterTableAddColumn(final TAlterOrDropTableReq req) {
    final boolean isView = req.isSetIsView() && req.isIsView();
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        isView ? ProcedureType.ADD_VIEW_COLUMN_PROCEDURE : ProcedureType.ADD_TABLE_COLUMN_PROCEDURE,
        isView
            ? new AddViewColumnProcedure(
                req.database,
                req.tableName,
                req.queryId,
                TsTableColumnSchemaUtil.deserializeColumnSchemaList(req.updateInfo),
                false)
            : new AddTableColumnProcedure(
                req.database,
                req.tableName,
                req.queryId,
                TsTableColumnSchemaUtil.deserializeColumnSchemaList(req.updateInfo),
                false));
  }

  public TSStatus alterTableSetProperties(final TAlterOrDropTableReq req) {
    final boolean isView = req.isSetIsView() && req.isIsView();
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        isView
            ? ProcedureType.SET_VIEW_PROPERTIES_PROCEDURE
            : ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE,
        isView
            ? new SetViewPropertiesProcedure(
                req.database,
                req.tableName,
                req.queryId,
                ReadWriteIOUtils.readMap(req.updateInfo),
                false)
            : new SetTablePropertiesProcedure(
                req.database,
                req.tableName,
                req.queryId,
                ReadWriteIOUtils.readMap(req.updateInfo),
                false));
  }

  public TSStatus alterTableRenameColumn(final TAlterOrDropTableReq req) {
    final boolean isView = req.isSetIsView() && req.isIsView();
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        isView
            ? ProcedureType.RENAME_VIEW_COLUMN_PROCEDURE
            : ProcedureType.RENAME_TABLE_COLUMN_PROCEDURE,
        isView
            ? new RenameViewColumnProcedure(
                req.database,
                req.tableName,
                req.queryId,
                ReadWriteIOUtils.readString(req.updateInfo),
                ReadWriteIOUtils.readString(req.updateInfo),
                false)
            : new RenameTableColumnProcedure(
                req.database,
                req.tableName,
                req.queryId,
                ReadWriteIOUtils.readString(req.updateInfo),
                ReadWriteIOUtils.readString(req.updateInfo),
                false));
  }

  public TSStatus alterTableDropColumn(final TAlterOrDropTableReq req) {
    final boolean isView = req.isSetIsView() && req.isIsView();
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        isView
            ? ProcedureType.DROP_VIEW_COLUMN_PROCEDURE
            : ProcedureType.DROP_TABLE_COLUMN_PROCEDURE,
        isView
            ? new DropViewColumnProcedure(
                req.database,
                req.tableName,
                req.queryId,
                ReadWriteIOUtils.readString(req.updateInfo),
                false)
            : new DropTableColumnProcedure(
                req.database,
                req.tableName,
                req.queryId,
                ReadWriteIOUtils.readString(req.updateInfo),
                false));
  }

  public TSStatus dropTable(final TAlterOrDropTableReq req) {
    final boolean isView = req.isSetIsView() && req.isIsView();
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        isView ? ProcedureType.DROP_VIEW_PROCEDURE : ProcedureType.DROP_TABLE_PROCEDURE,
        isView
            ? new DropViewProcedure(req.database, req.tableName, req.queryId, false)
            : new DropTableProcedure(req.database, req.tableName, req.queryId, false));
  }

  public TSStatus renameTable(final TAlterOrDropTableReq req) {
    final boolean isView = req.isSetIsView() && req.isIsView();
    final String newName = ReadWriteIOUtils.readString(req.updateInfo);
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        newName,
        req.queryId,
        isView ? ProcedureType.RENAME_VIEW_PROCEDURE : ProcedureType.RENAME_TABLE_PROCEDURE,
        isView
            ? new RenameViewProcedure(req.database, req.tableName, req.queryId, newName, false)
            : new RenameTableProcedure(req.database, req.tableName, req.queryId, newName, false));
  }

  public TDeleteTableDeviceResp deleteDevices(
      final TDeleteTableDeviceReq req, final boolean isGeneratedByPipe) {
    long procedureId;
    DeleteDevicesProcedure procedure = null;
    final TSStatus status;
    synchronized (this) {
      final Pair<Long, Boolean> procedureIdDuplicatePair =
          checkDuplicateTableTask(
              req.database,
              null,
              req.tableName,
              null,
              req.queryId,
              ProcedureType.DELETE_DEVICES_PROCEDURE);
      procedureId = procedureIdDuplicatePair.getLeft();

      if (procedureId == -1) {
        if (Boolean.TRUE.equals(procedureIdDuplicatePair.getRight())) {
          return new TDeleteTableDeviceResp(
              RpcUtils.getStatus(
                  TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
                  "Some other task is operating table with same name."));
        }
        procedure =
            new DeleteDevicesProcedure(
                req.database,
                req.tableName,
                req.queryId,
                req.getPatternInfo(),
                req.getFilterInfo(),
                req.getModInfo(),
                isGeneratedByPipe);
        this.executor.submitProcedure(procedure);
        status = waitingProcedureFinished(procedure);
      } else {
        status = waitingProcedureFinished(procedureId);
      }
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return new TDeleteTableDeviceResp(StatusUtils.OK)
          .setDeletedNum(
              Optional.ofNullable(procedure)
                  .map(DeleteDevicesProcedure::getDeletedDevicesNum)
                  .orElse(-1L));
    } else {
      return new TDeleteTableDeviceResp(status);
    }
  }

  public TSStatus executeWithoutDuplicate(
      final String database,
      final TsTable table,
      final String tableName,
      final String queryId,
      final ProcedureType thisType,
      final Procedure<ConfigNodeProcedureEnv> procedure) {
    return executeWithoutDuplicate(database, table, tableName, null, queryId, thisType, procedure);
  }

  public TSStatus executeWithoutDuplicate(
      final String database,
      final TsTable table,
      final String tableName,
      final @Nullable String newName,
      final String queryId,
      final ProcedureType thisType,
      final Procedure<ConfigNodeProcedureEnv> procedure) {
    final long procedureId;
    synchronized (this) {
      final Pair<Long, Boolean> procedureIdDuplicatePair =
          checkDuplicateTableTask(database, table, tableName, newName, queryId, thisType);
      procedureId = procedureIdDuplicatePair.getLeft();

      if (procedureId == -1) {
        if (Boolean.TRUE.equals(procedureIdDuplicatePair.getRight())) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is operating table with same name.");
        }
        this.executor.submitProcedure(procedure);
      } else {
        return waitingProcedureFinished(procedureId);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  public Pair<Long, Boolean> checkDuplicateTableTask(
      final @Nonnull String database,
      final TsTable table,
      final String tableName,
      final String newName,
      final String queryId,
      final ProcedureType thisType) {
    ProcedureType type;
    for (final Procedure<?> procedure : executor.getProcedures().values()) {
      type = ProcedureFactory.getProcedureType(procedure);
      if (type == null || procedure.isFinished()) {
        continue;
      }
      // A table shall not be concurrently operated or else the dataNode cache
      // may record fake values
      switch (type) {
        case CREATE_TABLE_PROCEDURE:
        case CREATE_TABLE_VIEW_PROCEDURE:
          final CreateTableProcedure createTableProcedure = (CreateTableProcedure) procedure;
          if (type == thisType && Objects.equals(table, createTableProcedure.getTable())) {
            return new Pair<>(procedure.getProcId(), false);
          }
          // tableName == null indicates delete database procedure
          if (database.equals(createTableProcedure.getDatabase())
              && (Objects.isNull(tableName)
                  || Objects.equals(tableName, createTableProcedure.getTable().getTableName())
                  || Objects.nonNull(newName)
                      && Objects.equals(newName, createTableProcedure.getTable().getTableName()))) {
            return new Pair<>(-1L, true);
          }
          break;
        case ADD_TABLE_COLUMN_PROCEDURE:
        case ADD_VIEW_COLUMN_PROCEDURE:
        case SET_TABLE_PROPERTIES_PROCEDURE:
        case SET_VIEW_PROPERTIES_PROCEDURE:
        case RENAME_TABLE_COLUMN_PROCEDURE:
        case RENAME_VIEW_COLUMN_PROCEDURE:
        case DROP_TABLE_COLUMN_PROCEDURE:
        case DROP_VIEW_COLUMN_PROCEDURE:
        case DROP_TABLE_PROCEDURE:
        case DROP_VIEW_PROCEDURE:
        case DELETE_DEVICES_PROCEDURE:
          final AbstractAlterOrDropTableProcedure<?> alterTableProcedure =
              (AbstractAlterOrDropTableProcedure<?>) procedure;
          if (type == thisType && queryId.equals(alterTableProcedure.getQueryId())) {
            return new Pair<>(procedure.getProcId(), false);
          }
          // tableName == null indicates delete database procedure
          if (database.equals(alterTableProcedure.getDatabase())
              && (Objects.isNull(tableName)
                  || Objects.equals(tableName, alterTableProcedure.getTableName())
                  || Objects.nonNull(newName)
                      && Objects.equals(newName, alterTableProcedure.getTableName()))) {
            return new Pair<>(-1L, true);
          }
          break;
        case RENAME_TABLE_PROCEDURE:
        case RENAME_VIEW_PROCEDURE:
          final RenameTableProcedure renameTableProcedure = (RenameTableProcedure) procedure;
          if (type == thisType && queryId.equals(renameTableProcedure.getQueryId())) {
            return new Pair<>(procedure.getProcId(), false);
          }
          // tableName == null indicates delete database procedure
          if (database.equals(renameTableProcedure.getDatabase())
              && (Objects.isNull(tableName)
                  || Objects.equals(tableName, renameTableProcedure.getTableName())
                  || Objects.equals(tableName, renameTableProcedure.getNewName())
                  || Objects.nonNull(newName)
                      && (Objects.equals(newName, renameTableProcedure.getTableName())
                          || Objects.equals(newName, renameTableProcedure.getNewName())))) {
            return new Pair<>(-1L, true);
          }
          break;
        case DELETE_DATABASE_PROCEDURE:
          final DeleteDatabaseProcedure deleteDatabaseProcedure =
              (DeleteDatabaseProcedure) procedure;
          if (database.equals(deleteDatabaseProcedure.getDatabase())) {
            return new Pair<>(-1L, true);
          }
          break;
        default:
          break;
      }
    }
    return new Pair<>(-1L, false);
  }

  // ======================================================
  /*
     GET-SET Region
  */
  // ======================================================
  public IManager getConfigManager() {
    return configManager;
  }

  public ProcedureExecutor<ConfigNodeProcedureEnv> getExecutor() {
    return executor;
  }

  public void setExecutor(ProcedureExecutor<ConfigNodeProcedureEnv> executor) {
    this.executor = executor;
  }

  public ProcedureScheduler getScheduler() {
    return scheduler;
  }

  public void setScheduler(ProcedureScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public IProcedureStore getStore() {
    return store;
  }

  public ConfigNodeProcedureEnv getEnv() {
    return env;
  }

  public void setEnv(ConfigNodeProcedureEnv env) {
    this.env = env;
  }

  public void addMetrics() {
    MetricService.getInstance().addMetricSet(this.procedureMetrics);
  }

  public void removeMetrics() {
    MetricService.getInstance().removeMetricSet(this.procedureMetrics);
  }

  public ProcedureMetrics getProcedureMetrics() {
    return procedureMetrics;
  }
}
