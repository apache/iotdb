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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.CreateModelProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.DropModelProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.CreatePipePluginProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.DropPipePluginProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleLeaderChangeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StopPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.statemachine.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.statemachine.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.confignode.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureStore;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ProcedureManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureManager.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONFIG =
      ConfigNodeDescriptor.getInstance().getConf();

  private static final int PROCEDURE_WAIT_TIME_OUT = 30;
  private static final int PROCEDURE_WAIT_RETRY_TIMEOUT = 250;

  private final ConfigManager configManager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private ProcedureScheduler scheduler;
  private IProcedureStore store;
  private ConfigNodeProcedureEnv env;

  private final long planSizeLimit;

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
  }

  public void shiftExecutor(boolean running) {
    if (running) {
      if (!executor.isRunning()) {
        executor.init(CONFIG_NODE_CONFIG.getProcedureCoreWorkerThreadsCount());
        executor.startWorkers();
        executor.startCompletedCleaner(
            CONFIG_NODE_CONFIG.getProcedureCompletedCleanInterval(),
            CONFIG_NODE_CONFIG.getProcedureCompletedEvictTTL());
        store.start();
        LOGGER.info("ProcedureManager is started successfully.");
      }
    } else {
      if (executor.isRunning()) {
        executor.stop();
        if (!executor.isRunning()) {
          executor.join();
          store.stop();
          LOGGER.info("ProcedureManager is stopped successfully.");
        }
      }
    }
  }

  public TSStatus deleteDatabases(ArrayList<TDatabaseSchema> deleteSgSchemaList) {
    List<Long> procedureIds = new ArrayList<>();
    for (TDatabaseSchema storageGroupSchema : deleteSgSchemaList) {
      DeleteDatabaseProcedure deleteDatabaseProcedure =
          new DeleteDatabaseProcedure(storageGroupSchema);
      long procedureId = this.executor.submitProcedure(deleteDatabaseProcedure);
      procedureIds.add(procedureId);
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed = waitingProcedureFinished(procedureIds, procedureStatus);
    // clear the previously deleted regions
    final PartitionManager partitionManager = getConfigManager().getPartitionManager();
    partitionManager.getRegionMaintainer().submit(partitionManager::maintainRegionReplicas);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return RpcUtils.getStatus(procedureStatus);
    }
  }

  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) {
    String queryId = req.getQueryId();
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
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
            this.executor.submitProcedure(new DeleteTimeSeriesProcedure(queryId, patternTree));
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
            this.executor.submitProcedure(new DeleteLogicalViewProcedure(queryId, patternTree));
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
                new AlterLogicalViewProcedure(queryId, viewPathToSourceMap));
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

  public TSStatus setSchemaTemplate(String queryId, String templateName, String templateSetPath) {
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
                new SetTemplateProcedure(queryId, templateName, templateSetPath));
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
      String queryId, Map<PartialPath, List<Template>> templateSetInfo) {
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
                new DeactivateTemplateProcedure(queryId, templateSetInfo));
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

  public TSStatus unsetSchemaTemplate(String queryId, Template template, PartialPath path) {
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
            this.executor.submitProcedure(new UnsetTemplateProcedure(queryId, template, path));
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
    AddConfigNodeProcedure addConfigNodeProcedure =
        new AddConfigNodeProcedure(req.getConfigNodeLocation(), req.getVersionInfo());
    this.executor.submitProcedure(addConfigNodeProcedure);
  }

  /**
   * Generate a {@link RemoveConfigNodeProcedure}, and serially execute all the {@link
   * RemoveConfigNodeProcedure}s.
   */
  public void removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    RemoveConfigNodeProcedure removeConfigNodeProcedure =
        new RemoveConfigNodeProcedure(removeConfigNodePlan.getConfigNodeLocation());
    this.executor.submitProcedure(removeConfigNodeProcedure);
    LOGGER.info("Submit RemoveConfigNodeProcedure successfully: {}", removeConfigNodePlan);
  }

  /**
   * Generate {@link RemoveDataNodeProcedure}s, and serially execute all the {@link
   * RemoveDataNodeProcedure}s.
   */
  public boolean removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    removeDataNodePlan
        .getDataNodeLocations()
        .forEach(
            tDataNodeLocation -> {
              this.executor.submitProcedure(new RemoveDataNodeProcedure(tDataNodeLocation));
              LOGGER.info("Submit RemoveDataNodeProcedure successfully, {}", tDataNodeLocation);
            });
    return true;
  }

  public TSStatus migrateRegion(TMigrateRegionReq migrateRegionReq) {
    TConsensusGroupId regionGroupId;
    if (configManager
        .getPartitionManager()
        .isRegionGroupExists(
            new TConsensusGroupId(
                TConsensusGroupType.SchemaRegion, migrateRegionReq.getRegionId()))) {
      regionGroupId =
          new TConsensusGroupId(TConsensusGroupType.SchemaRegion, migrateRegionReq.getRegionId());
    } else if (configManager
        .getPartitionManager()
        .isRegionGroupExists(
            new TConsensusGroupId(
                TConsensusGroupType.DataRegion, migrateRegionReq.getRegionId()))) {
      regionGroupId =
          new TConsensusGroupId(TConsensusGroupType.DataRegion, migrateRegionReq.getRegionId());
    } else {
      LOGGER.warn(
          "Submit RegionMigrateProcedure failed, because RegionGroup: {} doesn't exist",
          migrateRegionReq.getRegionId());
      TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          String.format(
              "Submit RegionMigrateProcedure failed, because RegionGroup: %s doesn't exist",
              migrateRegionReq.getRegionId()));
      return status;
    }

    TDataNodeLocation originalDataNode =
        configManager
            .getNodeManager()
            .getRegisteredDataNode(migrateRegionReq.getFromId())
            .getLocation();
    TDataNodeLocation destDataNode =
        configManager
            .getNodeManager()
            .getRegisteredDataNode(migrateRegionReq.getToId())
            .getLocation();

    if (originalDataNode == null) {
      LOGGER.warn(
          "Submit RegionMigrateProcedure failed, because no original DataNode {}",
          migrateRegionReq.getFromId());
      TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "Submit RegionMigrateProcedure failed, because no original DataNode "
              + migrateRegionReq.getFromId());
      return status;
    } else if (destDataNode == null) {
      LOGGER.warn(
          "Submit RegionMigrateProcedure failed, because no target DataNode {}",
          migrateRegionReq.getToId());
      TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "Submit RegionMigrateProcedure failed, because no target DataNode "
              + migrateRegionReq.getToId());
      return status;
    } else if (configManager.getPartitionManager()
        .getAllReplicaSets(originalDataNode.getDataNodeId()).stream()
        .noneMatch(replicaSet -> replicaSet.getRegionId().equals(regionGroupId))) {
      LOGGER.warn(
          "Submit RegionMigrateProcedure failed, because the original DataNode {} doesn't contain Region {}",
          migrateRegionReq.getFromId(),
          migrateRegionReq.getRegionId());
      TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "Submit RegionMigrateProcedure failed, because the original DataNode "
              + migrateRegionReq.getFromId()
              + " doesn't contain Region "
              + migrateRegionReq.getRegionId());
      return status;
    } else if (configManager.getPartitionManager().getAllReplicaSets(destDataNode.getDataNodeId())
        .stream()
        .anyMatch(replicaSet -> replicaSet.getRegionId().equals(regionGroupId))) {
      LOGGER.warn(
          "Submit RegionMigrateProcedure failed, because the target DataNode {} already contains Region {}",
          migrateRegionReq.getToId(),
          migrateRegionReq.getRegionId());
      TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "Submit RegionMigrateProcedure failed, because the target DataNode "
              + migrateRegionReq.getToId()
              + " already contains Region "
              + migrateRegionReq.getRegionId());
      return status;
    }
    // Here we only check Running DataNode to implement migration, because removing nodes may not
    // exist when add peer is performing
    Set<Integer> aliveDataNodes =
        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
            .map(TDataNodeConfiguration::getLocation)
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toSet());
    if (NodeStatus.Unknown.equals(
        configManager.getLoadManager().getNodeStatus(migrateRegionReq.getFromId()))) {
      LOGGER.warn(
          "Submit RegionMigrateProcedure failed, because the sourceDataNode {} is Unknown.",
          migrateRegionReq.getFromId());
      TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "Submit RegionMigrateProcedure failed, because the sourceDataNode "
              + migrateRegionReq.getFromId()
              + " is Unknown.");
      return status;
    }

    if (!aliveDataNodes.contains(migrateRegionReq.getToId())) {
      LOGGER.warn(
          "Submit RegionMigrateProcedure failed, because the destDataNode {} is ReadOnly or Unknown.",
          migrateRegionReq.getToId());
      TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "Submit RegionMigrateProcedure failed, because the destDataNode "
              + migrateRegionReq.getToId()
              + " is ReadOnly or Unknown.");
      return status;
    }
    this.executor.submitProcedure(
        new RegionMigrateProcedure(regionGroupId, originalDataNode, destDataNode));
    LOGGER.info(
        "Submit RegionMigrateProcedure successfully, Region: {}, From: {}, To: {}",
        migrateRegionReq.getRegionId(),
        migrateRegionReq.getFromId(),
        migrateRegionReq.getToId());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Generate CreateRegionGroupsProcedure and wait for it finished.
   *
   * @return SUCCESS_STATUS if all RegionGroups created successfully, CREATE_REGION_ERROR otherwise
   */
  public TSStatus createRegionGroups(
      TConsensusGroupType consensusGroupType, CreateRegionGroupsPlan createRegionGroupsPlan) {
    long procedureId =
        executor.submitProcedure(
            new CreateRegionGroupsProcedure(consensusGroupType, createRegionGroupsPlan));
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  /**
   * Generate CreateTriggerProcedure and wait for it finished.
   *
   * @return SUCCESS_STATUS if trigger created successfully, CREATE_TRIGGER_ERROR otherwise
   */
  public TSStatus createTrigger(TriggerInformation triggerInformation, Binary jarFile) {
    final CreateTriggerProcedure createTriggerProcedure =
        new CreateTriggerProcedure(triggerInformation, jarFile);
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

    long procedureId = executor.submitProcedure(createTriggerProcedure);
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_TRIGGER_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  /**
   * Generate DropTriggerProcedure and wait for it finished.
   *
   * @return SUCCESS_STATUS if trigger dropped successfully, DROP_TRIGGER_ERROR otherwise
   */
  public TSStatus dropTrigger(String triggerName) {
    long procedureId = executor.submitProcedure(new DropTriggerProcedure(triggerName));
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.DROP_TRIGGER_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  public TSStatus createCQ(TCreateCQReq req, ScheduledExecutorService scheduledExecutor) {
    long procedureId = executor.submitProcedure(new CreateCQProcedure(req, scheduledExecutor));
    List<TSStatus> statusList = new ArrayList<>();
    waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    return statusList.get(0);
  }

  public TSStatus createModel(ModelInformation modelInformation, Map<String, String> modelConfigs) {
    long procedureId =
        executor.submitProcedure(new CreateModelProcedure(modelInformation, modelConfigs));
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_MODEL_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
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

  public TSStatus createPipePlugin(PipePluginMeta pipePluginMeta, byte[] jarFile) {
    final CreatePipePluginProcedure createPipePluginProcedure =
        new CreatePipePluginProcedure(pipePluginMeta, jarFile);
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

    long procedureId = executor.submitProcedure(createPipePluginProcedure);
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  public TSStatus dropPipePlugin(String pluginName) {
    long procedureId = executor.submitProcedure(new DropPipePluginProcedure(pluginName));
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.DROP_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  public TSStatus createPipe(TCreatePipeReq req) {
    try {
      long procedureId = executor.submitProcedure(new CreatePipeProcedureV2(req));
      List<TSStatus> statusList = new ArrayList<>();
      boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(statusList.get(0).getMessage());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus startPipe(String pipeName) {
    try {
      long procedureId = executor.submitProcedure(new StartPipeProcedureV2(pipeName));
      List<TSStatus> statusList = new ArrayList<>();
      boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(statusList.get(0).getMessage());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus stopPipe(String pipeName) {
    try {
      long procedureId = executor.submitProcedure(new StopPipeProcedureV2(pipeName));
      List<TSStatus> statusList = new ArrayList<>();
      boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(statusList.get(0).getMessage());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus dropPipe(String pipeName) {
    try {
      long procedureId = executor.submitProcedure(new DropPipeProcedureV2(pipeName));
      List<TSStatus> statusList = new ArrayList<>();
      boolean isSucceed =
          waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
      if (isSucceed) {
        return RpcUtils.SUCCESS_STATUS;
      } else {
        return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode())
            .setMessage(statusList.get(0).getMessage());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
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
            .setMessage(statusList.get(0).getMessage());
      }
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage());
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
      long startTimeForCurrentProcedure = System.currentTimeMillis();
      while (executor.isRunning()
          && !executor.isFinished(procedureId)
          && TimeUnit.MILLISECONDS.toSeconds(
                  System.currentTimeMillis() - startTimeForCurrentProcedure)
              < PROCEDURE_WAIT_TIME_OUT) {
        sleepWithoutInterrupt(PROCEDURE_WAIT_RETRY_TIMEOUT);
      }
      Procedure<ConfigNodeProcedureEnv> finishedProcedure =
          executor.getResultOrProcedure(procedureId);
      if (!finishedProcedure.isFinished()) {
        // the procedure is still executing
        statusList.add(RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK));
        isSucceed = false;
        continue;
      }
      if (finishedProcedure.isSuccess()) {
        statusList.add(StatusUtils.OK);
      } else {
        if (finishedProcedure.getException().getCause() instanceof IoTDBException) {
          IoTDBException e = (IoTDBException) finishedProcedure.getException().getCause();
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

  public static void sleepWithoutInterrupt(final long timeToSleep) {
    long currentTime = System.currentTimeMillis();
    long endTime = timeToSleep + currentTime;
    boolean interrupted = false;
    while (currentTime < endTime) {
      try {
        Thread.sleep(endTime - currentTime);
      } catch (InterruptedException e) {
        interrupted = true;
      }
      currentTime = System.currentTimeMillis();
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
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

  public void setStore(ProcedureStore store) {
    this.store = store;
  }

  public ConfigNodeProcedureEnv getEnv() {
    return env;
  }

  public void setEnv(ConfigNodeProcedureEnv env) {
    this.env = env;
  }

  public void reportRegionMigrateResult(TRegionMigrateResultReportReq req) {

    this.executor
        .getProcedures()
        .values()
        .forEach(
            procedure -> {
              if (procedure instanceof RegionMigrateProcedure) {
                RegionMigrateProcedure regionMigrateProcedure = (RegionMigrateProcedure) procedure;
                if (regionMigrateProcedure.getConsensusGroupId().equals(req.getRegionId())) {
                  regionMigrateProcedure.notifyTheRegionMigrateFinished(req);
                }
              }
            });
  }
}
