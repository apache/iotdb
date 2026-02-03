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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.PartitionTableAutoCleaner;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.ProcedureMetrics;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveAINodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodesProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.CreatePipePluginProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.DropPipePluginProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleLeaderChangeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.AlterPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StopPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.region.AddRegionPeerProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.ReconstructRegionProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.confignode.procedure.impl.region.RegionOperationProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionPeerProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterEncodingCompressorProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterTimeSeriesDataTypeProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTTLProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AbstractAlterOrDropTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AlterTableColumnDataTypeProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DeleteDevicesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.AddViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.SetViewPropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.CreateConsumerProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.DropConsumerProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime.ConsumerGroupMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.CreateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.DropSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.CreateTopicProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.DropTopicProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.runtime.TopicMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.AuthOperationProcedure;
import org.apache.iotdb.confignode.procedure.impl.testonly.AddNeverFinishSubProcedureProcedure;
import org.apache.iotdb.confignode.procedure.impl.testonly.CreateManyDatabasesProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.confignode.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceResp;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TExtendRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TRemoveRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.util.AutoCloseableLock;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProcedureManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureManager.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONFIG =
      ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  public static final long PROCEDURE_WAIT_TIME_OUT = COMMON_CONFIG.getDnConnectionTimeoutInMS();
  public static final int PROCEDURE_WAIT_RETRY_TIMEOUT = 10;
  private static final String PROCEDURE_TIMEOUT_MESSAGE =
      "Timed out to wait for procedure return. The procedure is still running.";

  private final ConfigManager configManager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private ProcedureScheduler scheduler;
  private IProcedureStore store;
  private ConfigNodeProcedureEnv env;

  private final long planSizeLimit;
  private ProcedureMetrics procedureMetrics;

  private final PartitionTableAutoCleaner partitionTableCleaner;

  private final ReentrantLock tableLock = new ReentrantLock();

  public ProcedureManager(ConfigManager configManager, ProcedureInfo procedureInfo) {
    this.configManager = configManager;
    this.scheduler = new SimpleProcedureScheduler();
    this.store = new ConfigProcedureStore(configManager, procedureInfo);
    this.env = new ConfigNodeProcedureEnv(configManager, scheduler);
    this.executor = new ProcedureExecutor<>(env, store, scheduler);
    this.planSizeLimit =
        ConfigNodeDescriptor.getInstance()
                .getConf()
                .getConfigNodeRatisConsensusLogAppenderBufferSize()
            - IoTDBConstant.RAFT_LOG_BASIC_SIZE;
    this.procedureMetrics = new ProcedureMetrics(this);
    this.partitionTableCleaner = new PartitionTableAutoCleaner<>(configManager);
  }

  public void startExecutor() {
    if (!executor.isRunning()) {
      executor.init(CONFIG_NODE_CONFIG.getProcedureCoreWorkerThreadsCount());
      executor.startWorkers();
      executor.startCompletedCleaner(
          CONFIG_NODE_CONFIG.getProcedureCompletedCleanInterval(),
          CONFIG_NODE_CONFIG.getProcedureCompletedEvictTTL());
      executor.addInternalProcedure(partitionTableCleaner);
      store.start();
      LOGGER.info("ProcedureManager is started successfully.");
    }
  }

  public void stopExecutor() {
    if (executor.isRunning()) {
      executor.stop();
      if (!executor.isRunning()) {
        executor.join();
        store.stop();
        LOGGER.info("ProcedureManager is stopped successfully.");
      }
      executor.removeInternalProcedure(partitionTableCleaner);
    }
  }

  @TestOnly
  public TSStatus createManyDatabases() {
    this.executor.submitProcedure(new CreateManyDatabasesProcedure());
    return StatusUtils.OK;
  }

  @TestOnly
  public TSStatus testSubProcedure() {
    this.executor.submitProcedure(new AddNeverFinishSubProcedureProcedure());
    return StatusUtils.OK;
  }

  public TSStatus deleteDatabases(
      final List<TDatabaseSchema> deleteSgSchemaList, final boolean isGeneratedByPipe) {
    final List<DeleteDatabaseProcedure> procedures = new ArrayList<>();
    final long startCheckTimeForProcedures = System.currentTimeMillis();
    for (final TDatabaseSchema databaseSchema : deleteSgSchemaList) {
      final String database = databaseSchema.getName();
      boolean hasOverlappedTask = false;
      synchronized (this) {
        while (executor.isRunning()
            && System.currentTimeMillis() - startCheckTimeForProcedures < PROCEDURE_WAIT_TIME_OUT) {
          final Pair<Long, Boolean> procedureIdDuplicatePair =
              checkDuplicateTableTask(
                  database, null, null, null, null, ProcedureType.DELETE_DATABASE_PROCEDURE);
          hasOverlappedTask = procedureIdDuplicatePair.getRight();

          if (Boolean.FALSE.equals(procedureIdDuplicatePair.getRight())) {
            DeleteDatabaseProcedure procedure =
                new DeleteDatabaseProcedure(databaseSchema, isGeneratedByPipe);
            this.executor.submitProcedure(procedure);
            procedures.add(procedure);
            break;
          }
          try {
            wait(PROCEDURE_WAIT_RETRY_TIMEOUT);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              String.format(
                  "Some other task is operating table under the database %s, please retry after the procedure finishes.",
                  database));
        }
      }
    }
    List<TSStatus> results = new ArrayList<>(procedures.size());
    procedures.forEach(procedure -> results.add(waitingProcedureFinished(procedure)));
    // Clear the previously deleted regions
    final PartitionManager partitionManager = getConfigManager().getPartitionManager();
    partitionManager.getRegionMaintainer().submit(partitionManager::maintainRegionReplicas);
    if (results.stream()
        .allMatch(result -> result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
      return StatusUtils.OK;
    } else {
      return RpcUtils.getStatus(results);
    }
  }

  public TSStatus alterEncodingCompressor(
      final String queryId,
      final PathPatternTree patternTree,
      final byte encoding,
      final byte compressor,
      final boolean ifExists,
      final boolean isGeneratedByPipe,
      final boolean mayAlterAudit) {
    AlterEncodingCompressorProcedure procedure = null;
    synchronized (this) {
      ProcedureType type;
      AlterEncodingCompressorProcedure alterEncodingCompressorProcedure;
      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(runningProcedure);
        if (type == null || !type.equals(ProcedureType.ALTER_ENCODING_COMPRESSOR_PROCEDURE)) {
          continue;
        }
        alterEncodingCompressorProcedure = ((AlterEncodingCompressorProcedure) runningProcedure);
        if (queryId.equals(alterEncodingCompressorProcedure.getQueryId())) {
          procedure = alterEncodingCompressorProcedure;
          break;
        }
      }

      if (procedure == null) {
        procedure =
            new AlterEncodingCompressorProcedure(
                isGeneratedByPipe,
                queryId,
                patternTree,
                ifExists,
                encoding,
                compressor,
                mayAlterAudit);
        this.executor.submitProcedure(procedure);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  public TSStatus deleteTimeSeries(
      String queryId,
      PathPatternTree patternTree,
      boolean isGeneratedByPipe,
      boolean mayDeleteAudit) {
    DeleteTimeSeriesProcedure procedure = null;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      DeleteTimeSeriesProcedure deleteTimeSeriesProcedure;
      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(runningProcedure);
        if (type == null || !type.equals(ProcedureType.DELETE_TIMESERIES_PROCEDURE)) {
          continue;
        }
        deleteTimeSeriesProcedure = ((DeleteTimeSeriesProcedure) runningProcedure);
        if (queryId.equals(deleteTimeSeriesProcedure.getQueryId())) {
          procedure = deleteTimeSeriesProcedure;
          break;
        }
        if (patternTree.isOverlapWith(deleteTimeSeriesProcedure.getPatternTree())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedure == null) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is deleting some target timeseries.");
        }
        procedure =
            new DeleteTimeSeriesProcedure(queryId, patternTree, isGeneratedByPipe, mayDeleteAudit);
        this.executor.submitProcedure(procedure);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) {
    String queryId = req.getQueryId();
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    DeleteLogicalViewProcedure procedure = null;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      DeleteLogicalViewProcedure deleteLogicalViewProcedure;
      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(runningProcedure);
        if (type == null || !type.equals(ProcedureType.DELETE_LOGICAL_VIEW_PROCEDURE)) {
          continue;
        }
        deleteLogicalViewProcedure = ((DeleteLogicalViewProcedure) runningProcedure);
        if (queryId.equals(deleteLogicalViewProcedure.getQueryId())) {
          procedure = deleteLogicalViewProcedure;
          break;
        }
        if (patternTree.isOverlapWith(deleteLogicalViewProcedure.getPatternTree())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedure == null) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is deleting some target views.");
        }
        procedure =
            new DeleteLogicalViewProcedure(
                queryId, patternTree, req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe());
        this.executor.submitProcedure(procedure);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  public TSStatus alterLogicalView(final TAlterLogicalViewReq req) {
    final String queryId = req.getQueryId();
    final ByteBuffer byteBuffer = ByteBuffer.wrap(req.getViewBinary());
    final Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    final int size = byteBuffer.getInt();
    PartialPath path;
    ViewExpression viewExpression;
    for (int i = 0; i < size; i++) {
      path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      viewExpression = ViewExpression.deserialize(byteBuffer);
      viewPathToSourceMap.put(path, viewExpression);
    }

    AlterLogicalViewProcedure procedure = null;
    synchronized (this) {
      ProcedureType type;
      AlterLogicalViewProcedure alterLogicalViewProcedure;
      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(runningProcedure);
        if (type == null || !type.equals(ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE)) {
          continue;
        }
        alterLogicalViewProcedure = ((AlterLogicalViewProcedure) runningProcedure);
        if (queryId.equals(alterLogicalViewProcedure.getQueryId())) {
          procedure = alterLogicalViewProcedure;
          break;
        }
      }

      if (procedure == null) {
        procedure =
            new AlterLogicalViewProcedure(
                queryId,
                viewPathToSourceMap,
                req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe());
        this.executor.submitProcedure(procedure);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  public TSStatus alterTimeSeriesDataType(
      final String queryId,
      final MeasurementPath measurementPath,
      final byte operationType,
      final TSDataType tsDataType,
      final boolean isGeneratedByPipe) {
    AlterTimeSeriesDataTypeProcedure procedure;
    synchronized (this) {
      procedure =
          new AlterTimeSeriesDataTypeProcedure(
              queryId, measurementPath, operationType, tsDataType, isGeneratedByPipe);
      this.executor.submitProcedure(procedure);
    }
    return waitingProcedureFinished(procedure);
  }

  public TSStatus setSchemaTemplate(
      String queryId, String templateName, String templateSetPath, boolean isGeneratedByPipe) {
    SetTemplateProcedure procedure = null;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      SetTemplateProcedure setTemplateProcedure;
      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(runningProcedure);
        if (type == null || !type.equals(ProcedureType.SET_TEMPLATE_PROCEDURE)) {
          continue;
        }
        setTemplateProcedure = (SetTemplateProcedure) runningProcedure;
        if (queryId.equals(setTemplateProcedure.getQueryId())) {
          procedure = setTemplateProcedure;
          break;
        }
        if (templateSetPath.equals(setTemplateProcedure.getTemplateSetPath())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedure == null) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is setting template on target path.");
        }
        procedure =
            new SetTemplateProcedure(queryId, templateName, templateSetPath, isGeneratedByPipe);
        this.executor.submitProcedure(procedure);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  public TSStatus deactivateTemplate(
      String queryId, Map<PartialPath, List<Template>> templateSetInfo, boolean isGeneratedByPipe) {
    DeactivateTemplateProcedure procedure = null;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      DeactivateTemplateProcedure deactivateTemplateProcedure;
      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(runningProcedure);
        if (type == null || !type.equals(ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE)) {
          continue;
        }
        deactivateTemplateProcedure = (DeactivateTemplateProcedure) runningProcedure;
        if (queryId.equals(deactivateTemplateProcedure.getQueryId())) {
          procedure = deactivateTemplateProcedure;
          break;
        }
        for (PartialPath pattern : templateSetInfo.keySet()) {
          for (PartialPath existingPattern :
              deactivateTemplateProcedure.getTemplateSetInfo().keySet()) {
            if (pattern.overlapWith(existingPattern)) {
              hasOverlappedTask = true;
              break;
            }
          }
          if (hasOverlappedTask) {
            break;
          }
        }
        if (hasOverlappedTask) {
          break;
        }
      }

      if (procedure == null) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is deactivating some target template from target path.");
        }
        procedure = new DeactivateTemplateProcedure(queryId, templateSetInfo, isGeneratedByPipe);
        this.executor.submitProcedure(procedure);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  public TSStatus unsetSchemaTemplate(
      String queryId, Template template, PartialPath path, boolean isGeneratedByPipe) {
    UnsetTemplateProcedure procedure = null;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      UnsetTemplateProcedure unsetTemplateProcedure;
      for (Procedure<?> runningProcedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(runningProcedure);
        if (type == null || !type.equals(ProcedureType.UNSET_TEMPLATE_PROCEDURE)) {
          continue;
        }
        unsetTemplateProcedure = (UnsetTemplateProcedure) runningProcedure;
        if (queryId.equals(unsetTemplateProcedure.getQueryId())) {
          procedure = unsetTemplateProcedure;
          break;
        }
        if (template.getId() == unsetTemplateProcedure.getTemplateId()
            && path.equals(unsetTemplateProcedure.getPath())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedure == null) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is unsetting target template from target path "
                  + path.getFullPath());
        }
        procedure = new UnsetTemplateProcedure(queryId, template, path, isGeneratedByPipe);
        this.executor.submitProcedure(procedure);
      }
    }
    return waitingProcedureFinished(procedure);
  }

  /**
   * Generate an {@link AddConfigNodeProcedure}, and serially execute all the {@link
   * AddConfigNodeProcedure}s.
   */
  public void addConfigNode(TConfigNodeRegisterReq req) {
    final AddConfigNodeProcedure addConfigNodeProcedure =
        new AddConfigNodeProcedure(req.getConfigNodeLocation(), req.getVersionInfo());
    this.executor.submitProcedure(addConfigNodeProcedure);
  }

  /**
   * Generate a {@link RemoveConfigNodeProcedure}, and serially execute all the {@link
   * RemoveConfigNodeProcedure}s.
   */
  public void removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    final RemoveConfigNodeProcedure removeConfigNodeProcedure =
        new RemoveConfigNodeProcedure(removeConfigNodePlan.getConfigNodeLocation());
    this.executor.submitProcedure(removeConfigNodeProcedure);
    LOGGER.info("Submit RemoveConfigNodeProcedure successfully: {}", removeConfigNodePlan);
  }

  /**
   * Generate {@link RemoveDataNodesProcedure}s, and serially execute all the {@link
   * RemoveDataNodesProcedure}s.
   */
  public boolean removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    Map<Integer, NodeStatus> nodeStatusMap = new HashMap<>();
    removeDataNodePlan
        .getDataNodeLocations()
        .forEach(
            datanode ->
                nodeStatusMap.put(
                    datanode.getDataNodeId(),
                    configManager.getLoadManager().getNodeStatus(datanode.getDataNodeId())));
    this.executor.submitProcedure(
        new RemoveDataNodesProcedure(removeDataNodePlan.getDataNodeLocations(), nodeStatusMap));
    LOGGER.info(
        "Submit RemoveDataNodesProcedure successfully, {}",
        removeDataNodePlan.getDataNodeLocations());
    return true;
  }

  public boolean removeAINode(RemoveAINodePlan removeAINodePlan) {
    this.executor.submitProcedure(new RemoveAINodeProcedure(removeAINodePlan.getAINodeLocation()));
    LOGGER.info(
        "Submit RemoveAINodeProcedure successfully, {}", removeAINodePlan.getAINodeLocation());
    return true;
  }

  public TSStatus checkRemoveDataNodes(List<TDataNodeLocation> dataNodeLocations) {
    // 1. Only one RemoveDataNodesProcedure is allowed in the cluster
    Optional<Procedure<ConfigNodeProcedureEnv>> anotherRemoveProcedure =
        getExecutor().getProcedures().values().stream()
            .filter(
                procedure -> {
                  if (procedure instanceof RemoveDataNodesProcedure) {
                    return !procedure.isFinished();
                  }
                  return false;
                })
            .findAny();

    String failMessage = null;
    if (anotherRemoveProcedure.isPresent()) {
      List<TDataNodeLocation> anotherRemoveDataNodes =
          ((RemoveDataNodesProcedure) anotherRemoveProcedure.get()).getRemovedDataNodes();
      failMessage =
          String.format(
              "Submit RemoveDataNodesProcedure failed, "
                  + "because another RemoveDataNodesProcedure %s is already in processing. "
                  + "IoTDB is able to have at most 1 RemoveDataNodesProcedure at the same time. "
                  + "For further information, please search [pid%d] in log. ",
              anotherRemoveDataNodes, anotherRemoveProcedure.get().getProcId());
    }

    // 2. Check if the RemoveDataNodesProcedure conflicts with the RegionMigrateProcedure
    Set<TConsensusGroupId> removedDataNodesRegionSet =
        getEnv().getRemoveDataNodeHandler().getRemovedDataNodesRegionSet(dataNodeLocations);
    Optional<Procedure<ConfigNodeProcedureEnv>> conflictRegionMigrateProcedure =
        getExecutor().getProcedures().values().stream()
            .filter(
                procedure -> {
                  if (procedure instanceof RegionMigrateProcedure) {
                    RegionMigrateProcedure regionMigrateProcedure =
                        (RegionMigrateProcedure) procedure;
                    if (regionMigrateProcedure.isFinished()) {
                      return false;
                    }
                    return removedDataNodesRegionSet.contains(regionMigrateProcedure.getRegionId())
                        || dataNodeLocations.contains(regionMigrateProcedure.getDestDataNode());
                  }
                  return false;
                })
            .findAny();
    if (conflictRegionMigrateProcedure.isPresent()) {
      failMessage =
          String.format(
              "Submit RemoveDataNodesProcedure failed, "
                  + "because another RegionMigrateProcedure %s is already in processing which conflicts with this RemoveDataNodesProcedure. "
                  + "The RegionMigrateProcedure is migrating the region %s to the DataNode %s. "
                  + "For further information, please search [pid%d] in log. ",
              conflictRegionMigrateProcedure.get().getProcId(),
              ((RegionMigrateProcedure) conflictRegionMigrateProcedure.get()).getRegionId(),
              ((RegionMigrateProcedure) conflictRegionMigrateProcedure.get()).getDestDataNode(),
              conflictRegionMigrateProcedure.get().getProcId());
    }
    // 3. Check if the RegionMigrateProcedure generated by RemoveDataNodesProcedure conflicts with
    // each other
    List<RegionMigrationPlan> regionMigrationPlans =
        getEnv().getRemoveDataNodeHandler().getRegionMigrationPlans(dataNodeLocations);
    removedDataNodesRegionSet.clear();
    for (RegionMigrationPlan regionMigrationPlan : regionMigrationPlans) {
      if (removedDataNodesRegionSet.contains(regionMigrationPlan.getRegionId())) {
        failMessage =
            String.format(
                "Submit RemoveDataNodesProcedure failed, "
                    + "because the RegionMigrateProcedure generated by this RemoveDataNodesProcedure conflicts with each other. "
                    + "Only one replica of the same consensus group is allowed to be migrated at the same time."
                    + "The conflict region id is %s . ",
                regionMigrationPlan.getRegionId());
        break;
      }
      removedDataNodesRegionSet.add(regionMigrationPlan.getRegionId());
    }

    // 4. Check if there are any other unknown or readonly DataNodes in the consensus group that are
    // not the remove DataNodes

    for (TDataNodeLocation removeDataNode : dataNodeLocations) {
      Set<TDataNodeLocation> relatedDataNodes =
          getEnv().getRemoveDataNodeHandler().getRelatedDataNodeLocations(removeDataNode);
      relatedDataNodes.remove(removeDataNode);

      for (TDataNodeLocation relatedDataNode : relatedDataNodes) {
        NodeStatus nodeStatus =
            getConfigManager().getLoadManager().getNodeStatus(relatedDataNode.getDataNodeId());
        if (nodeStatus == NodeStatus.Unknown || nodeStatus == NodeStatus.ReadOnly) {
          failMessage =
              String.format(
                  "Submit RemoveDataNodesProcedure failed, "
                      + "because when there are other unknown or readonly nodes in the consensus group that are not remove nodes, "
                      + "the remove operation cannot be performed for security reasons. "
                      + "Please check the status of the node %s and ensure it is running.",
                  relatedDataNode.getDataNodeId());
        }
      }
    }

    if (failMessage != null) {
      LOGGER.warn(failMessage);
      TSStatus failStatus = new TSStatus(TSStatusCode.REMOVE_DATANODE_ERROR.getStatusCode());
      failStatus.setMessage(failMessage);
      return failStatus;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  // region region operation related check

  /**
   * Checks whether region migration is allowed.
   *
   * @param migrateRegionReq the migration request details
   * @param regionGroupId the ID of the consensus group for the region
   * @param originalDataNode the original DataNode location from which the region is being migrated
   * @param destDataNode the destination DataNode location to which the region is being migrated
   * @param coordinatorForAddPeer the DataNode location acting as the coordinator for adding a peer
   * @return the status of the migration request (TSStatus)
   */
  private TSStatus checkMigrateRegion(
      TMigrateRegionReq migrateRegionReq,
      TConsensusGroupId regionGroupId,
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TDataNodeLocation coordinatorForAddPeer) {
    String failMessage;
    if ((failMessage =
            regionOperationCommonCheck(
                regionGroupId,
                destDataNode,
                Arrays.asList(
                    new Pair<>("Original DataNode", originalDataNode),
                    new Pair<>("Destination DataNode", destDataNode),
                    new Pair<>("Coordinator for add peer", coordinatorForAddPeer)),
                migrateRegionReq.getModel()))
        != null) {
      // do nothing
    } else if (configManager
        .getPartitionManager()
        .getAllReplicaSets(originalDataNode.getDataNodeId())
        .stream()
        .noneMatch(replicaSet -> replicaSet.getRegionId().equals(regionGroupId))) {
      failMessage =
          String.format(
              "Submit RegionMigrateProcedure failed, because the original DataNode %s doesn't contain Region %s",
              migrateRegionReq.getFromId(), migrateRegionReq.getRegionId());
    } else if (configManager
        .getPartitionManager()
        .getAllReplicaSets(destDataNode.getDataNodeId())
        .stream()
        .anyMatch(replicaSet -> replicaSet.getRegionId().equals(regionGroupId))) {
      failMessage =
          String.format(
              "Submit RegionMigrateProcedure failed, because the target DataNode %s already contains Region %s",
              migrateRegionReq.getToId(), migrateRegionReq.getRegionId());
    }

    if (failMessage != null) {
      LOGGER.warn(failMessage);
      TSStatus failStatus = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      failStatus.setMessage(failMessage);
      return failStatus;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TSStatus checkReconstructRegion(
      TReconstructRegionReq req,
      TConsensusGroupId regionId,
      TDataNodeLocation targetDataNode,
      TDataNodeLocation coordinator) {
    String failMessage =
        regionOperationCommonCheck(
            regionId,
            targetDataNode,
            Arrays.asList(
                new Pair<>("Target DataNode", targetDataNode),
                new Pair<>("Coordinator", coordinator)),
            req.getModel());

    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    if (configManager
            .getPartitionManager()
            .getAllReplicaSetsMap(regionId.getType())
            .get(regionId)
            .getDataNodeLocationsSize()
        == 1) {
      failMessage = String.format("%s only has 1 replica, it cannot be reconstructed", regionId);
    } else if (configManager
        .getPartitionManager()
        .getAllReplicaSets(targetDataNode.getDataNodeId())
        .stream()
        .noneMatch(replicaSet -> replicaSet.getRegionId().equals(regionId))) {
      failMessage =
          String.format(
              "Submit ReconstructRegionProcedure failed, because the target DataNode %s doesn't contain Region %s",
              req.getDataNodeId(), regionId);
    }

    if (failMessage != null) {
      LOGGER.warn(failMessage);
      TSStatus failStatus = new TSStatus(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode());
      failStatus.setMessage(failMessage);
      return failStatus;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TSStatus checkExtendRegion(
      TExtendRegionReq req,
      TConsensusGroupId regionId,
      TDataNodeLocation targetDataNode,
      TDataNodeLocation coordinator) {
    String failMessage =
        regionOperationCommonCheck(
            regionId,
            targetDataNode,
            Arrays.asList(
                new Pair<>("Target DataNode", targetDataNode),
                new Pair<>("Coordinator", coordinator)),
            req.getModel());
    if (configManager
        .getPartitionManager()
        .getAllReplicaSets(targetDataNode.getDataNodeId())
        .stream()
        .anyMatch(replicaSet -> replicaSet.getRegionId().equals(regionId))) {
      failMessage =
          String.format(
              "Target DataNode %s already contains region %s",
              targetDataNode.getDataNodeId(), regionId);
    }

    if (failMessage != null) {
      LOGGER.warn(failMessage);
      TSStatus failStatus = new TSStatus(TSStatusCode.RECONSTRUCT_REGION_ERROR.getStatusCode());
      failStatus.setMessage(failMessage);
      return failStatus;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TSStatus checkRemoveRegion(
      TRemoveRegionReq req,
      TConsensusGroupId regionId,
      @Nullable TDataNodeLocation targetDataNode,
      TDataNodeLocation coordinator) {
    String failMessage =
        regionOperationCommonCheck(
            regionId,
            targetDataNode,
            Arrays.asList(new Pair<>("Coordinator", coordinator)),
            req.getModel());

    if (configManager
            .getPartitionManager()
            .getAllReplicaSetsMap(regionId.getType())
            .get(regionId)
            .getDataNodeLocationsSize()
        == 1) {
      failMessage = String.format("%s only has 1 replica, it cannot be removed", regionId);
    } else if (targetDataNode != null
        && configManager
            .getPartitionManager()
            .getAllReplicaSets(targetDataNode.getDataNodeId())
            .stream()
            .noneMatch(replicaSet -> replicaSet.getRegionId().equals(regionId))) {
      failMessage =
          String.format(
              "Target DataNode %s doesn't contain Region %s", req.getDataNodeId(), regionId);
    }

    if (failMessage != null) {
      LOGGER.warn(failMessage);
      TSStatus failStatus = new TSStatus(TSStatusCode.REMOVE_REGION_PEER_ERROR.getStatusCode());
      failStatus.setMessage(failMessage);
      return failStatus;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * The common checks of all region operations, include migration, reconstruction, extension,
   * removing
   *
   * @param regionId region group id, also called consensus group id
   * @param targetDataNode DataNode should in Running status
   * @param relatedDataNodes Pair<Identity, Node Location>
   * @return The reason if check failed, or null if check pass
   */
  private String regionOperationCommonCheck(
      TConsensusGroupId regionId,
      TDataNodeLocation targetDataNode,
      List<Pair<String, TDataNodeLocation>> relatedDataNodes,
      Model model) {
    String failMessage;
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

    if (TConsensusGroupType.DataRegion == regionId.getType()
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(conf.getDataRegionConsensusProtocolClass())) {
      failMessage = "SimpleConsensus not supports region operation.";
    } else if (TConsensusGroupType.SchemaRegion == regionId.getType()
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(conf.getSchemaRegionConsensusProtocolClass())) {
      failMessage = "SimpleConsensus not supports region operation.";
    } else if ((failMessage = checkRegionOperationDuplication(regionId)) != null) {
      // need to do nothing more
    } else if (relatedDataNodes.stream().anyMatch(pair -> pair.getRight() == null)) {
      Pair<String, TDataNodeLocation> nullPair =
          relatedDataNodes.stream().filter(pair -> pair.getRight() == null).findAny().get();
      failMessage = String.format("Cannot find %s", nullPair.getLeft());
    } else if (targetDataNode != null
        && !configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
            .map(TDataNodeConfiguration::getLocation)
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toSet())
            .contains(targetDataNode.getDataNodeId())) {
      // Here we only check Running DataNode to implement migration, because removing nodes may not
      // exist when add peer is performing
      failMessage =
          String.format(
              "Target DataNode %s is not in Running status.", targetDataNode.getDataNodeId());
    } else if ((failMessage = checkRegionOperationWithRemoveDataNode(regionId, targetDataNode))
        != null) {
      // need to do nothing more
    } else if ((failMessage = checkRegionOperationModelCorrectness(regionId, model)) != null) {
      // need to do nothing more
    }

    return failMessage;
  }

  private String checkRegionOperationWithRemoveDataNode(
      TConsensusGroupId regionId, TDataNodeLocation targetDataNode) {
    Optional<Procedure<ConfigNodeProcedureEnv>> conflictRemoveDataNodesProcedure =
        getExecutor().getProcedures().values().stream()
            .filter(
                procedure -> {
                  if (procedure instanceof RemoveDataNodesProcedure) {
                    return !procedure.isFinished();
                  }
                  return false;
                })
            .findAny();

    if (conflictRemoveDataNodesProcedure.isPresent()) {
      RemoveDataNodeHandler removeDataNodeHandler = env.getRemoveDataNodeHandler();
      List<TDataNodeLocation> removedDataNodes =
          ((RemoveDataNodesProcedure) conflictRemoveDataNodesProcedure.get()).getRemovedDataNodes();
      Set<TConsensusGroupId> removedDataNodesRegionSet =
          removeDataNodeHandler.getRemovedDataNodesRegionSet(removedDataNodes);
      if (removedDataNodesRegionSet.contains(regionId)) {
        return String.format(
            "Another RemoveDataNodesProcedure %s is already in processing which conflicts with this procedure. "
                + "The RemoveDataNodesProcedure is removing the DataNodes %s which contains the region %s. "
                + "For further information, please search [pid%d] in log. ",
            conflictRemoveDataNodesProcedure.get().getProcId(),
            removedDataNodes,
            regionId,
            conflictRemoveDataNodesProcedure.get().getProcId());
      } else if (removedDataNodes.contains(targetDataNode)) {
        return String.format(
            "Another RemoveDataNodesProcedure %s is already in processing which conflicts with this procedure. "
                + "The RemoveDataNodesProcedure is removing the target DataNode %s. "
                + "For further information, please search [pid%d] in log. ",
            conflictRemoveDataNodesProcedure.get().getProcId(),
            targetDataNode,
            conflictRemoveDataNodesProcedure.get().getProcId());
      }
    }
    return null;
  }

  private String checkRegionOperationDuplication(TConsensusGroupId regionId) {
    List<? extends RegionOperationProcedure<?>> otherRegionMemberChangeProcedures =
        getRegionOperationProcedures()
            .filter(
                regionMemberChangeProcedure ->
                    regionId.equals(regionMemberChangeProcedure.getRegionId()))
            .collect(Collectors.toList());
    if (!otherRegionMemberChangeProcedures.isEmpty()) {
      return String.format(
          "%s has some other region operation procedures in progress, their procedure id is: %s",
          regionId, otherRegionMemberChangeProcedures);
    }
    return null;
  }

  public List<TConsensusGroupId> getRegionOperationConsensusIds() {
    return getRegionOperationProcedures()
        .map(RegionOperationProcedure::getRegionId)
        .distinct()
        .collect(Collectors.toList());
  }

  private Stream<RegionOperationProcedure<?>> getRegionOperationProcedures() {
    return getExecutor().getProcedures().values().stream()
        .filter(procedure -> !procedure.isFinished())
        .filter(procedure -> procedure instanceof RegionOperationProcedure)
        .map(procedure -> (RegionOperationProcedure<?>) procedure);
  }

  private String checkRegionOperationModelCorrectness(TConsensusGroupId regionId, Model model) {
    String databaseName = configManager.getPartitionManager().getRegionDatabase(regionId);
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
  protected TSStatus waitingProcedureFinished(Procedure<?> procedure) {
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

  public TSStatus alterTableColumnDataType(TAlterOrDropTableReq req) {
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        ProcedureType.ALTER_TABLE_COLUMN_DATATYPE_PROCEDURE,
        new AlterTableColumnDataTypeProcedure(
            req.database,
            req.tableName,
            req.queryId,
            ReadWriteIOUtils.readVarIntString(req.updateInfo),
            TSDataType.deserialize(req.updateInfo.get()),
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

  public Map<String, List<String>> getAllExecutingTables() {
    final Map<String, List<String>> result = new HashMap<>();
    for (final Procedure<?> procedure : executor.getProcedures().values()) {
      if (procedure.isFinished()) {
        continue;
      }
      // CreateTableOrViewProcedure is covered by the default process, thus we can ignore it here
      // Note that if a table is creating there will not be a working table, and the DN will either
      // be updated by commit or fetch the CN tables
      // And it won't be committed by other procedures because:
      // if the preUpdate of other procedure has failed there will not be any commit here
      // if it succeeded then it will go to the normal process and will not leave any problems
      if (procedure instanceof AbstractAlterOrDropTableProcedure) {
        result
            .computeIfAbsent(
                ((AbstractAlterOrDropTableProcedure<?>) procedure).getDatabase(),
                k -> new ArrayList<>())
            .add(((AbstractAlterOrDropTableProcedure<?>) procedure).getTableName());
      }
      if (procedure instanceof DeleteDatabaseProcedure
          && ((DeleteDatabaseProcedure) procedure).getDeleteDatabaseSchema().isIsTableModel()) {
        result.put(((DeleteDatabaseProcedure) procedure).getDatabase(), null);
      }
    }
    return result;
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
        case ALTER_TABLE_COLUMN_DATATYPE_PROCEDURE:
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
