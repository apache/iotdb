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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningFilter;
import org.apache.iotdb.db.pipe.metric.PipeExtractorMetrics;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.task.builder.PipeDataNodeBuilder;
import org.apache.iotdb.db.pipe.task.builder.PipeDataNodeTaskBuilder;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeDataNodeTaskAgent extends PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeTaskAgent.class);

  protected static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  ////////////////////////// Pipe Task Management Entry //////////////////////////

  @Override
  protected boolean isShutdown() {
    return PipeAgent.runtime().isShutdown();
  }

  @Override
  protected Map<Integer, PipeTask> buildPipeTasks(PipeMeta pipeMetaFromConfigNode)
      throws IllegalPathException {
    return new PipeDataNodeBuilder(pipeMetaFromConfigNode).build();
  }

  ///////////////////////// Manage by regionGroupId /////////////////////////

  @Override
  protected void createPipeTask(
      int consensusGroupId, PipeStaticMeta pipeStaticMeta, PipeTaskMeta pipeTaskMeta)
      throws IllegalPathException {
    if (pipeTaskMeta.getLeaderNodeId() == CONFIG.getDataNodeId()) {
      final PipeParameters extractorParameters = pipeStaticMeta.getExtractorParameters();
      final boolean needConstructDataRegionTask =
          StorageEngine.getInstance()
                  .getAllDataRegionIds()
                  .contains(new DataRegionId(consensusGroupId))
              && DataRegionListeningFilter.shouldDataRegionBeListened(extractorParameters);
      final boolean needConstructSchemaRegionTask =
          SchemaEngine.getInstance()
                  .getAllSchemaRegionIds()
                  .contains(new SchemaRegionId(consensusGroupId))
              && !SchemaRegionListeningFilter.parseListeningPlanTypeSet(extractorParameters)
                  .isEmpty();

      // Advance the extractor parameters parsing logic to avoid creating un-relevant pipeTasks
      if (needConstructDataRegionTask || needConstructSchemaRegionTask) {
        final PipeDataNodeTask pipeTask =
            new PipeDataNodeTaskBuilder(pipeStaticMeta, consensusGroupId, pipeTaskMeta).build();
        pipeTask.create();
        pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
      }
    }

    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .put(consensusGroupId, pipeTaskMeta);
  }

  @Override
  public List<TPushPipeMetaRespExceptionMessage> handlePipeMetaChangesInternal(
      List<PipeMeta> pipeMetaListFromCoordinator) {
    // Do nothing if the node is removing or removed
    if (isShutdown()) {
      return Collections.emptyList();
    }

    final List<TPushPipeMetaRespExceptionMessage> exceptionMessages =
        super.handlePipeMetaChangesInternal(pipeMetaListFromCoordinator);

    try {
      final Set<Integer> validSchemaRegionIds =
          clearSchemaRegionListeningQueueIfNecessary(pipeMetaListFromCoordinator);
      closeSchemaRegionListeningQueueIfNecessary(validSchemaRegionIds, exceptionMessages);
    } catch (Exception e) {
      throw new PipeException("Failed to clear/close schema region listening queue.", e);
    }

    return exceptionMessages;
  }

  private Set<Integer> clearSchemaRegionListeningQueueIfNecessary(
      List<PipeMeta> pipeMetaListFromCoordinator) throws IllegalPathException {
    final Map<Integer, Long> schemaRegionId2ListeningQueueNewFirstIndex = new HashMap<>();

    // Check each pipe
    for (final PipeMeta pipeMetaFromCoordinator : pipeMetaListFromCoordinator) {
      if (SchemaRegionListeningFilter.parseListeningPlanTypeSet(
              pipeMetaFromCoordinator.getStaticMeta().getExtractorParameters())
          .isEmpty()) {
        continue;
      }

      // Check each schema region in a pipe
      final Map<Integer, PipeTaskMeta> groupId2TaskMetaMap =
          pipeMetaFromCoordinator.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();
      for (final SchemaRegionId regionId : SchemaEngine.getInstance().getAllSchemaRegionIds()) {
        final int id = regionId.getId();
        final PipeTaskMeta pipeTaskMeta = groupId2TaskMetaMap.get(id);
        if (pipeTaskMeta == null) {
          continue;
        }

        final ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
        if (progressIndex instanceof MetaProgressIndex) {
          if (((MetaProgressIndex) progressIndex).getIndex() + 1
              < schemaRegionId2ListeningQueueNewFirstIndex.getOrDefault(id, Long.MAX_VALUE)) {
            schemaRegionId2ListeningQueueNewFirstIndex.put(
                id, ((MetaProgressIndex) progressIndex).getIndex() + 1);
          }
        } else {
          // Do not clear "minimumProgressIndex"s related queues to avoid clearing
          // the queue when there are schema tasks just started and transferring
          schemaRegionId2ListeningQueueNewFirstIndex.put(id, 0L);
        }
      }
    }

    schemaRegionId2ListeningQueueNewFirstIndex.forEach(
        (schemaRegionId, listeningQueueNewFirstIndex) ->
            PipeAgent.runtime()
                .schemaListener(new SchemaRegionId(schemaRegionId))
                .removeBefore(listeningQueueNewFirstIndex));

    return schemaRegionId2ListeningQueueNewFirstIndex.keySet();
  }

  private void closeSchemaRegionListeningQueueIfNecessary(
      Set<Integer> validSchemaRegionIds,
      List<TPushPipeMetaRespExceptionMessage> exceptionMessages) {
    if (!exceptionMessages.isEmpty()) {
      return;
    }

    PipeAgent.runtime()
        .listeningSchemaRegionIds()
        .forEach(
            schemaRegionId -> {
              if (!validSchemaRegionIds.contains(schemaRegionId.getId())
                  && PipeAgent.runtime().isSchemaLeaderReady(schemaRegionId)) {
                try {
                  SchemaRegionConsensusImpl.getInstance()
                      .write(
                          schemaRegionId,
                          new PipeOperateSchemaQueueNode(new PlanNodeId(""), false));
                } catch (ConsensusException e) {
                  throw new PipeException(
                      "Failed to close listening queue for SchemaRegion " + schemaRegionId, e);
                }
              }
            });
  }

  public void stopAllPipesWithCriticalException() {
    super.stopAllPipesWithCriticalException(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
  }

  ///////////////////////// Heartbeat /////////////////////////

  public void collectPipeMetaList(TDataNodeHeartbeatResp resp) throws TException {
    // Try the lock instead of directly acquire it to prevent the block of the cluster heartbeat
    // 10s is the half of the HEARTBEAT_TIMEOUT_TIME defined in class BaseNodeCache in ConfigNode
    if (!tryReadLockWithTimeOut(10)) {
      return;
    }
    try {
      collectPipeMetaListInternal(resp);
    } finally {
      releaseReadLock();
    }
  }

  private void collectPipeMetaListInternal(TDataNodeHeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      final Optional<Logger> logger =
          PipeResourceManager.log()
              .schedule(
                  PipeDataNodeTaskAgent.class,
                  PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                  PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                  pipeMetaKeeper.getPipeMetaCount());
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        logger.ifPresent(l -> l.info("Reporting pipe meta: {}", pipeMeta.coreReportMessage()));
      }
      LOGGER.info("Reported {} pipe metas.", pipeMetaBinaryList.size());
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
  }

  @Override
  protected void collectPipeMetaListInternal(TPipeHeartbeatReq req, TPipeHeartbeatResp resp)
      throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }
    LOGGER.info("Received pipe heartbeat request {} from config node.", req.heartbeatId);

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      final Optional<Logger> logger =
          PipeResourceManager.log()
              .schedule(
                  PipeDataNodeTaskAgent.class,
                  PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                  PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                  pipeMetaKeeper.getPipeMetaCount());
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        logger.ifPresent(l -> l.info("Reporting pipe meta: {}", pipeMeta.coreReportMessage()));
      }
      LOGGER.info("Reported {} pipe metas.", pipeMetaBinaryList.size());
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);

    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(true);
  }

  ///////////////////////// Restart Logic /////////////////////////

  public void restartAllStuckPipes() {
    if (!tryWriteLockWithTimeOut(5)) {
      return;
    }
    try {
      restartAllStuckPipesInternal();
    } finally {
      releaseWriteLock();
    }
  }

  private void restartAllStuckPipesInternal() {
    final Map<String, IoTDBDataRegionExtractor> taskId2ExtractorMap =
        PipeExtractorMetrics.getInstance().getExtractorMap();

    final Set<PipeMeta> stuckPipes = new HashSet<>();
    for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
      final String pipeName = pipeMeta.getStaticMeta().getPipeName();
      final List<IoTDBDataRegionExtractor> extractors =
          taskId2ExtractorMap.values().stream()
              .filter(e -> e.getPipeName().equals(pipeName))
              .collect(Collectors.toList());
      if (extractors.isEmpty()
          || !extractors.get(0).isStreamMode()
          || extractors.stream()
              .noneMatch(IoTDBDataRegionExtractor::hasConsumedAllHistoricalTsFiles)) {
        continue;
      }

      if (mayMemTablePinnedCountReachDangerousThreshold() || mayWalSizeReachThrottleThreshold()) {
        LOGGER.warn("Pipe {} may be stuck.", pipeMeta.getStaticMeta());
        stuckPipes.add(pipeMeta);
      }
    }

    // Restart all stuck pipes
    stuckPipes.parallelStream().forEach(this::restartStuckPipe);
  }

  private boolean mayMemTablePinnedCountReachDangerousThreshold() {
    return PipeResourceManager.wal().getPinnedWalCount()
        >= 10 * PipeConfig.getInstance().getPipeMaxAllowedPinnedMemTableCount();
  }

  private boolean mayWalSizeReachThrottleThreshold() {
    return 3 * WALManager.getInstance().getTotalDiskUsage()
        > 2 * IoTDBDescriptor.getInstance().getConfig().getThrottleThreshold();
  }

  private void restartStuckPipe(PipeMeta pipeMeta) {
    LOGGER.warn("Pipe {} will be restarted because of stuck.", pipeMeta.getStaticMeta());
    final long startTime = System.currentTimeMillis();
    changePipeStatusBeforeRestart(pipeMeta.getStaticMeta().getPipeName());
    handleSinglePipeMetaChangesInternal(pipeMeta);
    LOGGER.warn(
        "Pipe {} was restarted because of stuck, time cost: {} ms.",
        pipeMeta.getStaticMeta(),
        System.currentTimeMillis() - startTime);
  }

  private void changePipeStatusBeforeRestart(String pipeName) {
    final PipeMeta pipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    final Map<Integer, PipeTask> pipeTasks = pipeTaskManager.getPipeTasks(pipeMeta.getStaticMeta());
    final Set<Integer> taskRegionIds = new HashSet<>(pipeTasks.keySet());
    final Set<Integer> dataRegionIds =
        StorageEngine.getInstance().getAllDataRegionIds().stream()
            .map(DataRegionId::getId)
            .collect(Collectors.toSet());
    final Set<PipeTask> dataRegionPipeTasks =
        taskRegionIds.stream()
            .filter(dataRegionIds::contains)
            .map(regionId -> pipeTaskManager.removePipeTask(pipeMeta.getStaticMeta(), regionId))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    // Drop data region tasks
    dataRegionPipeTasks.parallelStream().forEach(PipeTask::drop);

    // Stop schema region tasks
    pipeTaskManager
        .getPipeTasks(pipeMeta.getStaticMeta())
        .values()
        .parallelStream()
        .forEach(PipeTask::stop);

    // Re-create data region tasks
    dataRegionPipeTasks
        .parallelStream()
        .forEach(
            pipeTask -> {
              final PipeTask newPipeTask =
                  new PipeDataNodeTaskBuilder(
                          pipeMeta.getStaticMeta(),
                          ((PipeDataNodeTask) pipeTask).getRegionId(),
                          pipeMeta
                              .getRuntimeMeta()
                              .getConsensusGroupId2TaskMetaMap()
                              .get(((PipeDataNodeTask) pipeTask).getRegionId()))
                      .build();
              newPipeTask.create();
              pipeTaskManager.addPipeTask(
                  pipeMeta.getStaticMeta(),
                  ((PipeDataNodeTask) pipeTask).getRegionId(),
                  newPipeTask);
            });

    // Set pipe meta status to STOPPED
    pipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
  }

  ///////////////////////// Utils /////////////////////////

  public Set<Integer> getPipeTaskRegionIdSet(String pipeName, long creationTime) {
    final PipeMeta pipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    return pipeMeta == null || pipeMeta.getStaticMeta().getCreationTime() != creationTime
        ? Collections.emptySet()
        : pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().keySet();
  }
}
