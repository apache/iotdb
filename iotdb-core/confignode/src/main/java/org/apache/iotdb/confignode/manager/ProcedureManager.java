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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
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
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.ProcedureMetrics;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.CreateModelProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.DropModelProcedure;
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
import org.apache.iotdb.confignode.procedure.impl.region.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTTLProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AbstractAlterOrDropTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DeleteDevicesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
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
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REGION_MIGRATE_PROCESS;

public class ProcedureManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureManager.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONFIG =
      ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  public static final long PROCEDURE_WAIT_TIME_OUT = COMMON_CONFIG.getConnectionTimeoutInMS();
  private static final int PROCEDURE_WAIT_RETRY_TIMEOUT = 10;
  private static final String PROCEDURE_TIMEOUT_MESSAGE =
      "Timed out to wait for procedure return. The procedure is still running.";

  private final ConfigManager configManager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private ProcedureScheduler scheduler;
  private IProcedureStore store;
  private ConfigNodeProcedureEnv env;

  private final long planSizeLimit;
  private ProcedureMetrics procedureMetrics;

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
  }

  public void startExecutor() {
    if (!executor.isRunning()) {
      executor.init(CONFIG_NODE_CONFIG.getProcedureCoreWorkerThreadsCount());
      executor.startWorkers();
      executor.startCompletedCleaner(
          CONFIG_NODE_CONFIG.getProcedureCompletedCleanInterval(),
          CONFIG_NODE_CONFIG.getProcedureCompletedEvictTTL());
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
    final List<Long> procedureIds = new ArrayList<>();
    final long startCheckTimeForProcedures = System.currentTimeMillis();
    for (final TDatabaseSchema databaseSchema : deleteSgSchemaList) {
      final String database = databaseSchema.getName();
      boolean hasOverlappedTask = false;
      synchronized (this) {
        while (executor.isRunning()
            && System.currentTimeMillis() - startCheckTimeForProcedures < PROCEDURE_WAIT_TIME_OUT) {
          final Pair<Long, Boolean> procedureIdDuplicatePair =
              checkDuplicateTableTask(
                  database, null, null, null, ProcedureType.DELETE_DATABASE_PROCEDURE);
          hasOverlappedTask = procedureIdDuplicatePair.getRight();

          if (Boolean.FALSE.equals(procedureIdDuplicatePair.getRight())) {
            final DeleteDatabaseProcedure deleteDatabaseProcedure =
                new DeleteDatabaseProcedure(databaseSchema, isGeneratedByPipe);
            procedureIds.add(this.executor.submitProcedure(deleteDatabaseProcedure));
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
    final List<TSStatus> procedureStatus = new ArrayList<>();
    final boolean isSucceed = waitingProcedureFinished(procedureIds, procedureStatus);
    // Clear the previously deleted regions
    final PartitionManager partitionManager = getConfigManager().getPartitionManager();
    partitionManager.getRegionMaintainer().submit(partitionManager::maintainRegionReplicas);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return RpcUtils.getStatus(procedureStatus);
    }
  }

  public TSStatus deleteTimeSeries(
      String queryId, PathPatternTree patternTree, boolean isGeneratedByPipe) {
    long procedureId = -1;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      DeleteTimeSeriesProcedure deleteTimeSeriesProcedure;
      for (Procedure<?> procedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(procedure);
        if (type == null || !type.equals(ProcedureType.DELETE_TIMESERIES_PROCEDURE)) {
          continue;
        }
        deleteTimeSeriesProcedure = ((DeleteTimeSeriesProcedure) procedure);
        if (queryId.equals(deleteTimeSeriesProcedure.getQueryId())) {
          procedureId = deleteTimeSeriesProcedure.getProcId();
          break;
        }
        if (patternTree.isOverlapWith(deleteTimeSeriesProcedure.getPatternTree())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedureId == -1) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is deleting some target timeseries.");
        }
        procedureId =
            this.executor.submitProcedure(
                new DeleteTimeSeriesProcedure(queryId, patternTree, isGeneratedByPipe));
      }
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return procedureStatus.get(0);
    }
  }

  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) {
    String queryId = req.getQueryId();
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    long procedureId = -1;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      DeleteLogicalViewProcedure deleteLogicalViewProcedure;
      for (Procedure<?> procedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(procedure);
        if (type == null || !type.equals(ProcedureType.DELETE_LOGICAL_VIEW_PROCEDURE)) {
          continue;
        }
        deleteLogicalViewProcedure = ((DeleteLogicalViewProcedure) procedure);
        if (queryId.equals(deleteLogicalViewProcedure.getQueryId())) {
          procedureId = deleteLogicalViewProcedure.getProcId();
          break;
        }
        if (patternTree.isOverlapWith(deleteLogicalViewProcedure.getPatternTree())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedureId == -1) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is deleting some target views.");
        }
        procedureId =
            this.executor.submitProcedure(
                new DeleteLogicalViewProcedure(
                    queryId,
                    patternTree,
                    req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe()));
      }
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return procedureStatus.get(0);
    }
  }

  public TSStatus alterLogicalView(TAlterLogicalViewReq req) {
    String queryId = req.getQueryId();
    ByteBuffer byteBuffer = ByteBuffer.wrap(req.getViewBinary());
    Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    int size = byteBuffer.getInt();
    PartialPath path;
    ViewExpression viewExpression;
    for (int i = 0; i < size; i++) {
      path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      viewExpression = ViewExpression.deserialize(byteBuffer);
      viewPathToSourceMap.put(path, viewExpression);
    }

    long procedureId = -1;
    synchronized (this) {
      ProcedureType type;
      AlterLogicalViewProcedure alterLogicalViewProcedure;
      for (Procedure<?> procedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(procedure);
        if (type == null || !type.equals(ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE)) {
          continue;
        }
        alterLogicalViewProcedure = ((AlterLogicalViewProcedure) procedure);
        if (queryId.equals(alterLogicalViewProcedure.getQueryId())) {
          procedureId = alterLogicalViewProcedure.getProcId();
          break;
        }
      }

      if (procedureId == -1) {
        procedureId =
            this.executor.submitProcedure(
                new AlterLogicalViewProcedure(
                    queryId,
                    viewPathToSourceMap,
                    req.isSetIsGeneratedByPipe() && req.isIsGeneratedByPipe()));
      }
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return procedureStatus.get(0);
    }
  }

  public TSStatus setSchemaTemplate(
      String queryId, String templateName, String templateSetPath, boolean isGeneratedByPipe) {
    long procedureId = -1;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      SetTemplateProcedure setTemplateProcedure;
      for (Procedure<?> procedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(procedure);
        if (type == null || !type.equals(ProcedureType.SET_TEMPLATE_PROCEDURE)) {
          continue;
        }
        setTemplateProcedure = (SetTemplateProcedure) procedure;
        if (queryId.equals(setTemplateProcedure.getQueryId())) {
          procedureId = setTemplateProcedure.getProcId();
          break;
        }
        if (templateSetPath.equals(setTemplateProcedure.getTemplateSetPath())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedureId == -1) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is setting template on target path.");
        }
        procedureId =
            this.executor.submitProcedure(
                new SetTemplateProcedure(
                    queryId, templateName, templateSetPath, isGeneratedByPipe));
      }
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return procedureStatus.get(0);
    }
  }

  public TSStatus deactivateTemplate(
      String queryId, Map<PartialPath, List<Template>> templateSetInfo, boolean isGeneratedByPipe) {
    long procedureId = -1;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      DeactivateTemplateProcedure deactivateTemplateProcedure;
      for (Procedure<?> procedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(procedure);
        if (type == null || !type.equals(ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE)) {
          continue;
        }
        deactivateTemplateProcedure = (DeactivateTemplateProcedure) procedure;
        if (queryId.equals(deactivateTemplateProcedure.getQueryId())) {
          procedureId = deactivateTemplateProcedure.getProcId();
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

      if (procedureId == -1) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is deactivating some target template from target path.");
        }
        procedureId =
            this.executor.submitProcedure(
                new DeactivateTemplateProcedure(queryId, templateSetInfo, isGeneratedByPipe));
      }
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return procedureStatus.get(0);
    }
  }

  public TSStatus unsetSchemaTemplate(
      String queryId, Template template, PartialPath path, boolean isGeneratedByPipe) {
    long procedureId = -1;
    synchronized (this) {
      boolean hasOverlappedTask = false;
      ProcedureType type;
      UnsetTemplateProcedure unsetTemplateProcedure;
      for (Procedure<?> procedure : executor.getProcedures().values()) {
        type = ProcedureFactory.getProcedureType(procedure);
        if (type == null || !type.equals(ProcedureType.UNSET_TEMPLATE_PROCEDURE)) {
          continue;
        }
        unsetTemplateProcedure = (UnsetTemplateProcedure) procedure;
        if (queryId.equals(unsetTemplateProcedure.getQueryId())) {
          procedureId = unsetTemplateProcedure.getProcId();
          break;
        }
        if (template.getId() == unsetTemplateProcedure.getTemplateId()
            && path.equals(unsetTemplateProcedure.getPath())) {
          hasOverlappedTask = true;
          break;
        }
      }

      if (procedureId == -1) {
        if (hasOverlappedTask) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is unsetting target template from target path "
                  + path.getFullPath());
        }
        procedureId =
            this.executor.submitProcedure(
                new UnsetTemplateProcedure(queryId, template, path, isGeneratedByPipe));
      }
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return procedureStatus.get(0);
    }
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
        "Submit RemoveDataNodeProcedure successfully, {}",
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
                    return removedDataNodesRegionSet.contains(
                            regionMigrateProcedure.getConsensusGroupId())
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
              ((RegionMigrateProcedure) conflictRegionMigrateProcedure.get()).getConsensusGroupId(),
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
  private TSStatus checkRegionMigrate(
      TMigrateRegionReq migrateRegionReq,
      TConsensusGroupId regionGroupId,
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TDataNodeLocation coordinatorForAddPeer) {
    String failMessage = null;
    // 1. Check if the RegionMigrateProcedure has conflict with another RegionMigrateProcedure
    Optional<Procedure<ConfigNodeProcedureEnv>> anotherMigrateProcedure =
        getExecutor().getProcedures().values().stream()
            .filter(
                procedure -> {
                  if (procedure instanceof RegionMigrateProcedure) {
                    return !procedure.isFinished()
                        && ((RegionMigrateProcedure) procedure)
                            .getConsensusGroupId()
                            .equals(regionGroupId);
                  }
                  return false;
                })
            .findAny();
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    if (TConsensusGroupType.DataRegion == regionGroupId.getType()
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(conf.getDataRegionConsensusProtocolClass())) {
      failMessage =
          "The region you are trying to migrate is using SimpleConsensus, and SimpleConsensus not supports region migration.";
    } else if (TConsensusGroupType.SchemaRegion == regionGroupId.getType()
        && ConsensusFactory.SIMPLE_CONSENSUS.equals(conf.getSchemaRegionConsensusProtocolClass())) {
      failMessage =
          "The region you are trying to migrate is using SimpleConsensus, and SimpleConsensus not supports region migration.";
    } else if (anotherMigrateProcedure.isPresent()) {
      failMessage =
          String.format(
              "Submit RegionMigrateProcedure failed, "
                  + "because another RegionMigrateProcedure of the same consensus group %d is already in processing. "
                  + "A consensus group is able to have at most 1 RegionMigrateProcedure at the same time. "
                  + "For further information, please search [pid%d] in log. ",
              regionGroupId.getId(), anotherMigrateProcedure.get().getProcId());
    } else if (originalDataNode == null) {
      failMessage =
          String.format(
              "Submit RegionMigrateProcedure failed, because no original DataNode %d",
              migrateRegionReq.getFromId());
    } else if (destDataNode == null) {
      failMessage =
          String.format(
              "Submit RegionMigrateProcedure failed, because no target DataNode %s",
              migrateRegionReq.getToId());
    } else if (coordinatorForAddPeer == null) {
      failMessage =
          String.format(
              "%s, There are no other DataNodes could be selected to perform the add peer process, "
                  + "please check RegionGroup: %s by show regions sql command",
              REGION_MIGRATE_PROCESS, regionGroupId);
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
    } else if (!configManager
        .getNodeManager()
        .filterDataNodeThroughStatus(NodeStatus.Running)
        .stream()
        .map(TDataNodeConfiguration::getLocation)
        .map(TDataNodeLocation::getDataNodeId)
        .collect(Collectors.toSet())
        .contains(migrateRegionReq.getToId())) {
      // Here we only check Running DataNode to implement migration, because removing nodes may not
      // exist when add peer is performing
      failMessage =
          String.format(
              "Submit RegionMigrateProcedure failed, because the destDataNode %s is ReadOnly or Unknown.",
              migrateRegionReq.getToId());
    }

    // 2. Check if the RegionMigrateProcedure has conflict with RemoveDataNodesProcedure
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
      if (removedDataNodesRegionSet.contains(regionGroupId)) {
        failMessage =
            String.format(
                "Submit RegionMigrateProcedure failed, "
                    + "because another RemoveDataNodesProcedure %s is already in processing which conflicts with this RegionMigrateProcedure. "
                    + "The RemoveDataNodesProcedure is removing the DataNodes %s which contains the region %s. "
                    + "For further information, please search [pid%d] in log. ",
                conflictRemoveDataNodesProcedure.get().getProcId(),
                removedDataNodes,
                regionGroupId,
                conflictRemoveDataNodesProcedure.get().getProcId());
      } else if (removedDataNodes.contains(destDataNode)) {
        failMessage =
            String.format(
                "Submit RegionMigrateProcedure failed, "
                    + "because another RemoveDataNodesProcedure %s is already in processing which conflicts with this RegionMigrateProcedure. "
                    + "The RemoveDataNodesProcedure is removing the target DataNode %s. "
                    + "For further information, please search [pid%d] in log. ",
                conflictRemoveDataNodesProcedure.get().getProcId(),
                destDataNode,
                conflictRemoveDataNodesProcedure.get().getProcId());
      }
    }

    if (failMessage != null) {
      LOGGER.warn(failMessage);
      TSStatus failStatus = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      failStatus.setMessage(failMessage);
      return failStatus;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus migrateRegion(TMigrateRegionReq migrateRegionReq) {
    env.getSubmitRegionMigrateLock().lock();
    try {
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
          checkRegionMigrate(
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
          "Submit RegionMigrateProcedure successfully, Region: {}, Origin DataNode: {}, Dest DataNode: {}, Add Coordinator: {}, Remove Coordinator: {}",
          regionGroupId,
          originalDataNode,
          destDataNode,
          coordinatorForAddPeer,
          coordinatorForRemovePeer);

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      env.getSubmitRegionMigrateLock().unlock();
    }
  }

  // endregion

  /**
   * Generate {@link CreateRegionGroupsProcedure} and wait until it finished.
   *
   * @return {@link TSStatusCode#SUCCESS_STATUS} if all RegionGroups have been created successfully,
   *     {@link TSStatusCode#CREATE_REGION_ERROR} otherwise
   */
  public TSStatus createRegionGroups(
      TConsensusGroupType consensusGroupType, CreateRegionGroupsPlan createRegionGroupsPlan) {
    final long procedureId =
        executor.submitProcedure(
            new CreateRegionGroupsProcedure(consensusGroupType, createRegionGroupsPlan));
    final List<TSStatus> statusList = new ArrayList<>();
    final boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
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

    final long procedureId = executor.submitProcedure(createTriggerProcedure);
    final List<TSStatus> statusList = new ArrayList<>();
    final boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_TRIGGER_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  /**
   * Generate {@link DropTriggerProcedure} and wait until it finished.
   *
   * @return {@link TSStatusCode#SUCCESS_STATUS} if the trigger has been dropped successfully,
   *     {@link TSStatusCode#DROP_TRIGGER_ERROR} otherwise
   */
  public TSStatus dropTrigger(String triggerName, boolean isGeneratedByPipe) {
    long procedureId =
        executor.submitProcedure(new DropTriggerProcedure(triggerName, isGeneratedByPipe));
    final List<TSStatus> statusList = new ArrayList<>();
    final boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.DROP_TRIGGER_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  public TSStatus createCQ(TCreateCQReq req, ScheduledExecutorService scheduledExecutor) {
    final long procedureId =
        executor.submitProcedure(new CreateCQProcedure(req, scheduledExecutor));
    final List<TSStatus> statusList = new ArrayList<>();
    waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    return statusList.get(0);
  }

  public TSStatus createModel(String modelName, String uri) {
    long procedureId = executor.submitProcedure(new CreateModelProcedure(modelName, uri));
    LOGGER.info("CreateModelProcedure was submitted, procedureId: {}.", procedureId);
    return RpcUtils.SUCCESS_STATUS;
  }

  public TSStatus dropModel(String modelId) {
    long procedureId = executor.submitProcedure(new DropModelProcedure(modelId));
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.DROP_MODEL_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
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

    final long procedureId = executor.submitProcedure(createPipePluginProcedure);
    final List<TSStatus> statusList = new ArrayList<>();
    final boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  public TSStatus dropPipePlugin(TDropPipePluginReq req) {
    final long procedureId =
        executor.submitProcedure(
            new DropPipePluginProcedure(
                req.getPluginName(), req.isSetIfExistsCondition() && req.isIfExistsCondition()));
    final List<TSStatus> statusList = new ArrayList<>();
    final boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.DROP_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  public TSStatus createConsensusPipe(TCreatePipeReq req) {
    try {
      final long procedureId = executor.submitProcedure(new CreatePipeProcedureV2(req));
      return handleConsensusPipeProcedure(procedureId);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus createPipe(TCreatePipeReq req) {
    try {
      final long procedureId = executor.submitProcedure(new CreatePipeProcedureV2(req));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus alterPipe(TAlterPipeReq req) {
    try {
      final long procedureId = executor.submitProcedure(new AlterPipeProcedureV2(req));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus startConsensusPipe(String pipeName) {
    try {
      final long procedureId = executor.submitProcedure(new StartPipeProcedureV2(pipeName));
      return handleConsensusPipeProcedure(procedureId);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus startPipe(String pipeName) {
    try {
      final long procedureId = executor.submitProcedure(new StartPipeProcedureV2(pipeName));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus stopConsensusPipe(String pipeName) {
    try {
      final long procedureId = executor.submitProcedure(new StopPipeProcedureV2(pipeName));
      return handleConsensusPipeProcedure(procedureId);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus stopPipe(String pipeName) {
    try {
      final long procedureId = executor.submitProcedure(new StopPipeProcedureV2(pipeName));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus dropConsensusPipe(String pipeName) {
    try {
      final long procedureId = executor.submitProcedure(new DropPipeProcedureV2(pipeName));
      return handleConsensusPipeProcedure(procedureId);
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus dropPipe(String pipeName) {
    try {
      final long procedureId = executor.submitProcedure(new DropPipeProcedureV2(pipeName));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  private TSStatus handleConsensusPipeProcedure(final long procedureId) {
    final List<TSStatus> statusList = new ArrayList<>();
    final boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return statusList.get(0);
    } else {
      // if time out, optimistically believe that this procedure will execute successfully.
      if (statusList.get(0).getMessage().equals(PROCEDURE_TIMEOUT_MESSAGE)) {
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      // otherwise, some exceptions must have occurred, throw them.
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
          .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
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
      final long procedureId =
          executor.submitProcedure(
              new PipeHandleMetaChangeProcedure(
                  needWriteConsensusOnConfigNodes, needPushPipeMetaToDataNodes));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus pipeMetaSync() {
    try {
      final long procedureId = executor.submitProcedure(new PipeMetaSyncProcedure());
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus createTopic(TCreateTopicReq req) {
    try {
      final long procedureId = executor.submitProcedure(new CreateTopicProcedure(req));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus dropTopic(String topicName) {
    try {
      final long procedureId = executor.submitProcedure(new DropTopicProcedure(topicName));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.DROP_TOPIC_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.DROP_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus topicMetaSync() {
    try {
      final long procedureId = executor.submitProcedure(new TopicMetaSyncProcedure());
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.TOPIC_PUSH_META_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus createConsumer(TCreateConsumerReq req) {
    try {
      final long procedureId = executor.submitProcedure(new CreateConsumerProcedure(req));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.CREATE_CONSUMER_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CREATE_CONSUMER_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus dropConsumer(TCloseConsumerReq req) {
    try {
      final long procedureId = executor.submitProcedure(new DropConsumerProcedure(req));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else {
        return new TSStatus(TSStatusCode.DROP_CONSUMER_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.DROP_CONSUMER_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus consumerGroupMetaSync() {
    try {
      final long procedureId = executor.submitProcedure(new ConsumerGroupMetaSyncProcedure());
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus createSubscription(TSubscribeReq req) {
    try {
      final long procedureId = executor.submitProcedure(new CreateSubscriptionProcedure(req));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else if (PROCEDURE_TIMEOUT_MESSAGE.equals(statusList.get(0).getMessage())) {
        // we assume that a timeout has occurred in the procedure related to the pipe in the
        // subscription procedure
        return new TSStatus(TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      } else {
        return new TSStatus(TSStatusCode.SUBSCRIPTION_SUBSCRIBE_ERROR.getStatusCode());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.SUBSCRIPTION_SUBSCRIBE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus dropSubscription(TUnsubscribeReq req) {
    try {
      final long procedureId = executor.submitProcedure(new DropSubscriptionProcedure(req));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return statusList.get(0);
      } else if (PROCEDURE_TIMEOUT_MESSAGE.equals(statusList.get(0).getMessage())) {
        // we assume that a timeout has occurred in the procedure related to the pipe in the
        // subscription procedure
        return new TSStatus(TSStatusCode.SUBSCRIPTION_PIPE_TIMEOUT_ERROR.getStatusCode())
            .setMessage(wrapTimeoutMessageForPipeProcedure(statusList.get(0).getMessage()));
      } else {
        return new TSStatus(TSStatusCode.SUBSCRIPTION_UNSUBSCRIBE_ERROR.getStatusCode());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.SUBSCRIPTION_UNSUBSCRIBE_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus operateAuthPlan(
      AuthorPlan authorPlan, List<TDataNodeConfiguration> dns, boolean isGeneratedByPipe) {
    try {
      final long procedureId =
          executor.submitProcedure(new AuthOperationProcedure(authorPlan, dns, isGeneratedByPipe));
      final List<TSStatus> statusList = new ArrayList<>();
      final boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(statusList.get(0).getCode()).setMessage(statusList.get(0).getMessage());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.AUTH_OPERATE_EXCEPTION.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus setTTL(SetTTLPlan setTTLPlan, final boolean isGeneratedByPipe) {
    long procedureId = executor.submitProcedure(new SetTTLProcedure(setTTLPlan, isGeneratedByPipe));

    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return procedureStatus.get(0);
    }
  }

  /**
   * Waiting until the specific procedures finished.
   *
   * @param procedureIds The specific procedures' index
   * @param statusList The corresponding running results of these procedures
   * @return True if all Procedures finished successfully, false otherwise
   */
  private boolean waitingProcedureFinished(List<Long> procedureIds, List<TSStatus> statusList) {
    boolean isSucceed = true;
    for (long procedureId : procedureIds) {
      final long startTimeForCurrentProcedure = System.currentTimeMillis();
      while (executor.isRunning()
          && !executor.isFinished(procedureId)
          && System.currentTimeMillis() - startTimeForCurrentProcedure < PROCEDURE_WAIT_TIME_OUT) {
        sleepWithoutInterrupt(PROCEDURE_WAIT_RETRY_TIMEOUT);
      }
      final Procedure<ConfigNodeProcedureEnv> finishedProcedure =
          executor.getResultOrProcedure(procedureId);
      if (!finishedProcedure.isFinished()) {
        // The procedure is still executing
        statusList.add(
            RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK, PROCEDURE_TIMEOUT_MESSAGE));
        isSucceed = false;
        continue;
      }
      if (finishedProcedure.isSuccess()) {
        if (Objects.nonNull(finishedProcedure.getResult())) {
          statusList.add(
              RpcUtils.getStatus(
                  TSStatusCode.SUCCESS_STATUS, Arrays.toString(finishedProcedure.getResult())));
        } else {
          statusList.add(StatusUtils.OK);
        }
      } else {
        if (finishedProcedure.getException().getCause() instanceof IoTDBException) {
          final IoTDBException e = (IoTDBException) finishedProcedure.getException().getCause();
          if (e instanceof BatchProcessException) {
            statusList.add(
                RpcUtils.getStatus(
                    Arrays.stream(((BatchProcessException) e).getFailingStatus())
                        .collect(Collectors.toList())));
          } else {
            statusList.add(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
          }
        } else {
          statusList.add(
              StatusUtils.EXECUTE_STATEMENT_ERROR.setMessage(
                  finishedProcedure.getException().getMessage()));
        }
        isSucceed = false;
      }
    }
    return isSucceed;
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
        new CreateTableProcedure(database, table));
  }

  public TSStatus alterTableAddColumn(final TAlterOrDropTableReq req) {
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        ProcedureType.ADD_TABLE_COLUMN_PROCEDURE,
        new AddTableColumnProcedure(
            req.database,
            req.tableName,
            req.queryId,
            TsTableColumnSchemaUtil.deserializeColumnSchemaList(req.updateInfo)));
  }

  public TSStatus alterTableSetProperties(final TAlterOrDropTableReq req) {
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE,
        new SetTablePropertiesProcedure(
            req.database, req.tableName, req.queryId, ReadWriteIOUtils.readMap(req.updateInfo)));
  }

  public TSStatus alterTableRenameColumn(final TAlterOrDropTableReq req) {
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        ProcedureType.RENAME_TABLE_COLUMN_PROCEDURE,
        new RenameTableColumnProcedure(
            req.database,
            req.tableName,
            req.queryId,
            ReadWriteIOUtils.readString(req.updateInfo),
            ReadWriteIOUtils.readString(req.updateInfo)));
  }

  public TSStatus alterTableDropColumn(final TAlterOrDropTableReq req) {
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        ProcedureType.DROP_TABLE_COLUMN_PROCEDURE,
        new DropTableColumnProcedure(
            req.database, req.tableName, req.queryId, ReadWriteIOUtils.readString(req.updateInfo)));
  }

  public TSStatus dropTable(final TAlterOrDropTableReq req) {
    return executeWithoutDuplicate(
        req.database,
        null,
        req.tableName,
        req.queryId,
        ProcedureType.DROP_TABLE_PROCEDURE,
        new DropTableProcedure(req.database, req.tableName, req.queryId));
  }

  public TDeleteTableDeviceResp deleteDevices(final TDeleteTableDeviceReq req) {
    long procedureId;
    DeleteDevicesProcedure procedure = null;
    synchronized (this) {
      final Pair<Long, Boolean> procedureIdDuplicatePair =
          checkDuplicateTableTask(
              req.database,
              null,
              req.tableName,
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
                req.getModInfo());
        procedureId = this.executor.submitProcedure(procedure);
      }
    }
    executor.getResultOrProcedure(procedureId);
    final List<TSStatus> procedureStatus = new ArrayList<>();
    if (waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus)) {
      if (Objects.isNull(procedure)) {
        procedure = ((DeleteDevicesProcedure) executor.getResultOrProcedure(procedureId));
      }
      return new TDeleteTableDeviceResp(StatusUtils.OK)
          .setDeletedNum(Objects.nonNull(procedure) ? procedure.getDeletedDevicesNum() : -1);
    } else {
      return new TDeleteTableDeviceResp(procedureStatus.get(0));
    }
  }

  private TSStatus executeWithoutDuplicate(
      final String database,
      final TsTable table,
      final String tableName,
      final String queryId,
      final ProcedureType thisType,
      final Procedure<ConfigNodeProcedureEnv> procedure) {
    long procedureId;
    synchronized (this) {
      final Pair<Long, Boolean> procedureIdDuplicatePair =
          checkDuplicateTableTask(database, table, tableName, queryId, thisType);
      procedureId = procedureIdDuplicatePair.getLeft();

      if (procedureId == -1) {
        if (Boolean.TRUE.equals(procedureIdDuplicatePair.getRight())) {
          return RpcUtils.getStatus(
              TSStatusCode.OVERLAP_WITH_EXISTING_TASK,
              "Some other task is operating table with same name.");
        }
        procedureId = this.executor.submitProcedure(procedure);
      }
    }
    final List<TSStatus> procedureStatus = new ArrayList<>();
    return waitingProcedureFinished(Collections.singletonList(procedureId), procedureStatus)
        ? StatusUtils.OK
        : procedureStatus.get(0);
  }

  public Pair<Long, Boolean> checkDuplicateTableTask(
      final @Nonnull String database,
      final TsTable table,
      final String tableName,
      final String queryId,
      final ProcedureType thisType) {
    ProcedureType type;
    for (final Procedure<?> procedure : executor.getProcedures().values()) {
      type = ProcedureFactory.getProcedureType(procedure);
      if (type == null) {
        continue;
      }
      // A table shall not be concurrently operated or else the dataNode cache
      // may record fake values
      switch (type) {
        case CREATE_TABLE_PROCEDURE:
          final CreateTableProcedure createTableProcedure = (CreateTableProcedure) procedure;
          if (type == thisType && Objects.equals(table, createTableProcedure.getTable())) {
            return new Pair<>(procedure.getProcId(), false);
          }
          // tableName == null indicates delete database procedure
          if (database.equals(createTableProcedure.getDatabase())
              && (Objects.isNull(tableName)
                  || Objects.equals(tableName, createTableProcedure.getTable().getTableName()))) {
            return new Pair<>(-1L, true);
          }
          break;
        case ADD_TABLE_COLUMN_PROCEDURE:
        case SET_TABLE_PROPERTIES_PROCEDURE:
        case RENAME_TABLE_COLUMN_PROCEDURE:
        case DROP_TABLE_COLUMN_PROCEDURE:
        case DROP_TABLE_PROCEDURE:
        case DELETE_DEVICES_PROCEDURE:
          final AbstractAlterOrDropTableProcedure<?> alterTableProcedure =
              (AbstractAlterOrDropTableProcedure<?>) procedure;
          if (type == thisType && queryId.equals(alterTableProcedure.getQueryId())) {
            return new Pair<>(procedure.getProcId(), false);
          }
          // tableName == null indicates delete database procedure
          if (database.equals(alterTableProcedure.getDatabase())
              && (Objects.isNull(tableName)
                  || Objects.equals(tableName, alterTableProcedure.getTableName()))) {
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
