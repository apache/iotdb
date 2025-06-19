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
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.builder.PipeDataNodeBuilder;
import org.apache.iotdb.db.pipe.agent.task.builder.PipeDataNodeTaskBuilder;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningFilter;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeRemainingEventAndTimeMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionExtractorMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_QUERY_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_SNAPSHOT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_SNAPSHOT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_SNAPSHOT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODE_SNAPSHOT_KEY;

public class PipeDataNodeTaskAgent extends PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeTaskAgent.class);

  protected static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final AtomicLong LAST_FORCED_RESTART_TIME =
      new AtomicLong(System.currentTimeMillis());
  private static final Map<String, AtomicLong> PIPE_NAME_TO_LAST_RESTART_TIME_MAP =
      new ConcurrentHashMap<>();

  ////////////////////////// Pipe Task Management Entry //////////////////////////

  @Override
  protected boolean isShutdown() {
    return PipeDataNodeAgent.runtime().isShutdown();
  }

  @Override
  protected Map<Integer, PipeTask> buildPipeTasks(final PipeMeta pipeMetaFromConfigNode)
      throws IllegalPathException {
    return pipeMetaFromConfigNode.getStaticMeta().isSourceExternal()
        ? new PipeDataNodeBuilder(pipeMetaFromConfigNode).buildTasksWithExternalSource()
        : new PipeDataNodeBuilder(pipeMetaFromConfigNode).buildTasksWithInternalSource();
  }

  ////////////////////////// Manage by Pipe Name //////////////////////////

  @Override
  protected void startPipe(final String pipeName, final long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    final PipeStatus status = existedPipeMeta.getRuntimeMeta().getStatus().get();
    if (PipeStatus.STOPPED.equals(status) || status == null) {
      restartPipeToReloadResourceIfNeeded(existedPipeMeta);
    }

    super.startPipe(pipeName, creationTime);
  }

  private void restartPipeToReloadResourceIfNeeded(final PipeMeta pipeMeta) {
    if (System.currentTimeMillis() - pipeMeta.getStaticMeta().getCreationTime()
        < PipeConfig.getInstance().getPipeStuckRestartMinIntervalMs()) {
      return;
    }

    final AtomicLong lastRestartTime =
        PIPE_NAME_TO_LAST_RESTART_TIME_MAP.get(pipeMeta.getStaticMeta().getPipeName());
    if (lastRestartTime != null
        && System.currentTimeMillis() - lastRestartTime.get()
            < PipeConfig.getInstance().getPipeStuckRestartMinIntervalMs()) {
      LOGGER.info(
          "Skipping reload resource for stopped pipe {} before starting it because reloading resource is too frequent.",
          pipeMeta.getStaticMeta().getPipeName());
      return;
    }

    if (PIPE_NAME_TO_LAST_RESTART_TIME_MAP.isEmpty()) {
      LOGGER.info(
          "Flushing storage engine before restarting pipe {}.",
          pipeMeta.getStaticMeta().getPipeName());
      final long currentTime = System.currentTimeMillis();
      StorageEngine.getInstance().syncCloseAllProcessor();
      WALManager.getInstance().syncDeleteOutdatedFilesInWALNodes();
      LOGGER.info(
          "Finished flushing storage engine, time cost: {} ms.",
          System.currentTimeMillis() - currentTime);
    }

    restartStuckPipe(pipeMeta);
    LOGGER.info(
        "Reloaded resource for stopped pipe {} before starting it.",
        pipeMeta.getStaticMeta().getPipeName());
  }

  ///////////////////////// Manage by regionGroupId /////////////////////////

  @Override
  protected void createPipeTask(
      final int consensusGroupId,
      final PipeStaticMeta pipeStaticMeta,
      final PipeTaskMeta pipeTaskMeta)
      throws IllegalPathException {
    if (pipeTaskMeta.getLeaderNodeId() == CONFIG.getDataNodeId()) {
      final PipeParameters extractorParameters = pipeStaticMeta.getExtractorParameters();
      final DataRegionId dataRegionId = new DataRegionId(consensusGroupId);
      final boolean needConstructDataRegionTask =
          StorageEngine.getInstance().getAllDataRegionIds().contains(dataRegionId)
              && DataRegionListeningFilter.shouldDataRegionBeListened(
                  extractorParameters, dataRegionId);
      final boolean needConstructSchemaRegionTask =
          SchemaEngine.getInstance()
                  .getAllSchemaRegionIds()
                  .contains(new SchemaRegionId(consensusGroupId))
              && SchemaRegionListeningFilter.shouldSchemaRegionBeListened(
                  consensusGroupId, extractorParameters);

      // Advance the extractor parameters parsing logic to avoid creating un-relevant pipeTasks
      if (
      // For external source
      PipeRuntimeMeta.isSourceExternal(consensusGroupId)
          // For internal source
          || needConstructDataRegionTask
          || needConstructSchemaRegionTask) {
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
      final List<PipeMeta> pipeMetaListFromCoordinator) {
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
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to clear/close the schema region listening queue, because {}. Will wait until success or the region's state machine is stopped.",
          e.getMessage());
      // Do not use null pipe name to retain the field "required" to be compatible with the lower
      // versions
      exceptionMessages.add(
          new TPushPipeMetaRespExceptionMessage("", e.getMessage(), System.currentTimeMillis()));
    }

    return exceptionMessages;
  }

  private Set<Integer> clearSchemaRegionListeningQueueIfNecessary(
      final List<PipeMeta> pipeMetaListFromCoordinator) throws IllegalPathException {
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
            PipeDataNodeAgent.runtime()
                .schemaListener(new SchemaRegionId(schemaRegionId))
                .removeBefore(listeningQueueNewFirstIndex));

    return schemaRegionId2ListeningQueueNewFirstIndex.keySet();
  }

  private void closeSchemaRegionListeningQueueIfNecessary(
      final Set<Integer> validSchemaRegionIds,
      final List<TPushPipeMetaRespExceptionMessage> exceptionMessages) {
    if (!exceptionMessages.isEmpty()) {
      return;
    }

    PipeDataNodeAgent.runtime().listeningSchemaRegionIds().stream()
        .filter(
            schemaRegionId ->
                !validSchemaRegionIds.contains(schemaRegionId.getId())
                    && PipeDataNodeAgent.runtime().isSchemaLeaderReady(schemaRegionId))
        .forEach(
            schemaRegionId -> {
              try {
                SchemaRegionConsensusImpl.getInstance()
                    .write(
                        schemaRegionId, new PipeOperateSchemaQueueNode(new PlanNodeId(""), false));
              } catch (final ConsensusException e) {
                throw new PipeException(
                    "Failed to close listening queue for SchemaRegion "
                        + schemaRegionId
                        + ", because "
                        + e.getMessage(),
                    e);
              }
            });
  }

  @Override
  protected void thawRate(final String pipeName, final long creationTime) {
    PipeDataNodeRemainingEventAndTimeMetrics.getInstance().thawRate(pipeName + "_" + creationTime);
  }

  @Override
  protected void freezeRate(final String pipeName, final long creationTime) {
    PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
        .freezeRate(pipeName + "_" + creationTime);
  }

  @Override
  protected boolean dropPipe(final String pipeName, final long creationTime) {
    if (!super.dropPipe(pipeName, creationTime)) {
      return false;
    }

    final String taskId = pipeName + "_" + creationTime;
    PipeTsFileToTabletsMetrics.getInstance().deregister(taskId);
    PipeDataNodeRemainingEventAndTimeMetrics.getInstance().deregister(taskId);

    return true;
  }

  @Override
  protected boolean dropPipe(final String pipeName) {
    // Get the pipe meta first because it is removed after super#dropPipe(pipeName)
    final PipeMeta pipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    // Record whether there are pipe tasks before dropping the pipe
    final boolean hasPipeTasks;
    if (Objects.nonNull(pipeMeta)) {
      final Map<Integer, PipeTask> pipeTaskMap =
          pipeTaskManager.getPipeTasks(pipeMeta.getStaticMeta());
      hasPipeTasks = Objects.nonNull(pipeTaskMap) && !pipeTaskMap.isEmpty();
    } else {
      hasPipeTasks = false;
    }

    if (!super.dropPipe(pipeName)) {
      return false;
    }

    if (Objects.nonNull(pipeMeta)) {
      final long creationTime = pipeMeta.getStaticMeta().getCreationTime();
      final String taskId = pipeName + "_" + creationTime;
      PipeTsFileToTabletsMetrics.getInstance().deregister(taskId);
      PipeDataNodeRemainingEventAndTimeMetrics.getInstance().deregister(taskId);
      // When the pipe contains no pipe tasks, there is no corresponding prefetching queue for the
      // subscribed pipe, so the subscription needs to be manually marked as completed.
      if (!hasPipeTasks && PipeStaticMeta.isSubscriptionPipe(pipeName)) {
        final String topicName =
            pipeMeta
                .getStaticMeta()
                .getConnectorParameters()
                .getString(PipeConnectorConstant.SINK_TOPIC_KEY);
        final String consumerGroupId =
            pipeMeta
                .getStaticMeta()
                .getConnectorParameters()
                .getString(PipeConnectorConstant.SINK_CONSUMER_GROUP_KEY);
        SubscriptionAgent.broker().updateCompletedTopicNames(consumerGroupId, topicName);
      }
    }

    return true;
  }

  public void stopAllPipesWithCriticalException() {
    super.stopAllPipesWithCriticalException(CONFIG.getDataNodeId());
  }

  ///////////////////////// Heartbeat /////////////////////////

  public void collectPipeMetaList(final TDataNodeHeartbeatResp resp) throws TException {
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

  private void collectPipeMetaListInternal(final TDataNodeHeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeDataNodeAgent.runtime().isShutdown()) {
      return;
    }

    final Set<Integer> dataRegionIds =
        StorageEngine.getInstance().getAllDataRegionIds().stream()
            .map(DataRegionId::getId)
            .collect(Collectors.toSet());

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    final List<Boolean> pipeCompletedList = new ArrayList<>();
    final List<Long> pipeRemainingEventCountList = new ArrayList<>();
    final List<Double> pipeRemainingTimeList = new ArrayList<>();
    try {
      final Optional<Logger> logger =
          PipeDataNodeResourceManager.log()
              .schedule(
                  PipeDataNodeTaskAgent.class,
                  PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                  PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                  pipeMetaKeeper.getPipeMetaCount());
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());

        final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();

        final Map<Integer, PipeTask> pipeTaskMap = pipeTaskManager.getPipeTasks(staticMeta);
        final boolean isAllDataRegionCompleted =
            pipeTaskMap == null
                || pipeTaskMap.entrySet().stream()
                    .filter(entry -> dataRegionIds.contains(entry.getKey()))
                    .allMatch(entry -> ((PipeDataNodeTask) entry.getValue()).isCompleted());
        final String extractorModeValue =
            pipeMeta
                .getStaticMeta()
                .getExtractorParameters()
                .getStringOrDefault(
                    Arrays.asList(
                        PipeExtractorConstant.EXTRACTOR_MODE_KEY,
                        PipeExtractorConstant.SOURCE_MODE_KEY),
                    PipeExtractorConstant.EXTRACTOR_MODE_DEFAULT_VALUE);
        final boolean includeDataAndNeedDrop =
            DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(
                        pipeMeta.getStaticMeta().getExtractorParameters())
                    .getLeft()
                && (extractorModeValue.equalsIgnoreCase(
                        PipeExtractorConstant.EXTRACTOR_MODE_QUERY_VALUE)
                    || extractorModeValue.equalsIgnoreCase(
                        PipeExtractorConstant.EXTRACTOR_MODE_SNAPSHOT_VALUE));

        final boolean isCompleted = isAllDataRegionCompleted && includeDataAndNeedDrop;
        final Pair<Long, Double> remainingEventAndTime =
            PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
                .getRemainingEventAndTime(staticMeta.getPipeName(), staticMeta.getCreationTime());
        pipeCompletedList.add(isCompleted);
        pipeRemainingEventCountList.add(remainingEventAndTime.getLeft());
        pipeRemainingTimeList.add(remainingEventAndTime.getRight());

        logger.ifPresent(
            l ->
                l.info(
                    "Reporting pipe meta: {}, isCompleted: {}, remainingEventCount: {}, estimatedRemainingTime: {}",
                    pipeMeta.coreReportMessage(),
                    isCompleted,
                    remainingEventAndTime.getLeft(),
                    remainingEventAndTime.getRight()));
      }
      LOGGER.info("Reported {} pipe metas.", pipeMetaBinaryList.size());
    } catch (final IOException | IllegalPathException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
    resp.setPipeCompletedList(pipeCompletedList);
    resp.setPipeRemainingEventCountList(pipeRemainingEventCountList);
    resp.setPipeRemainingTimeList(pipeRemainingTimeList);
    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(true);
  }

  @Override
  protected void collectPipeMetaListInternal(
      final TPipeHeartbeatReq req, final TPipeHeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeDataNodeAgent.runtime().isShutdown()) {
      return;
    }
    LOGGER.info("Received pipe heartbeat request {} from config node.", req.heartbeatId);

    final Set<Integer> dataRegionIds =
        StorageEngine.getInstance().getAllDataRegionIds().stream()
            .map(DataRegionId::getId)
            .collect(Collectors.toSet());

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    final List<Boolean> pipeCompletedList = new ArrayList<>();
    final List<Long> pipeRemainingEventCountList = new ArrayList<>();
    final List<Double> pipeRemainingTimeList = new ArrayList<>();
    try {
      final Optional<Logger> logger =
          PipeDataNodeResourceManager.log()
              .schedule(
                  PipeDataNodeTaskAgent.class,
                  PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                  PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                  pipeMetaKeeper.getPipeMetaCount());
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());

        final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();

        final Map<Integer, PipeTask> pipeTaskMap = pipeTaskManager.getPipeTasks(staticMeta);
        final boolean isAllDataRegionCompleted =
            pipeTaskMap == null
                || pipeTaskMap.entrySet().stream()
                    .filter(entry -> dataRegionIds.contains(entry.getKey()))
                    .allMatch(entry -> ((PipeDataNodeTask) entry.getValue()).isCompleted());

        final boolean includeDataAndNeedDrop =
            DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(
                        pipeMeta.getStaticMeta().getExtractorParameters())
                    .getLeft()
                && isSnapshotMode(pipeMeta.getStaticMeta().getExtractorParameters());

        final boolean isCompleted = isAllDataRegionCompleted && includeDataAndNeedDrop;
        final Pair<Long, Double> remainingEventAndTime =
            PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
                .getRemainingEventAndTime(staticMeta.getPipeName(), staticMeta.getCreationTime());
        pipeCompletedList.add(isCompleted);
        pipeRemainingEventCountList.add(remainingEventAndTime.getLeft());
        pipeRemainingTimeList.add(remainingEventAndTime.getRight());

        logger.ifPresent(
            l ->
                l.info(
                    "Reporting pipe meta: {}, isCompleted: {}, remainingEventCount: {}, estimatedRemainingTime: {}",
                    pipeMeta.coreReportMessage(),
                    isCompleted,
                    remainingEventAndTime.getLeft(),
                    remainingEventAndTime.getRight()));
      }
      LOGGER.info("Reported {} pipe metas.", pipeMetaBinaryList.size());
    } catch (final IOException | IllegalPathException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
    resp.setPipeCompletedList(pipeCompletedList);
    resp.setPipeRemainingEventCountList(pipeRemainingEventCountList);
    resp.setPipeRemainingTimeList(pipeRemainingTimeList);
    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(true);
  }

  ///////////////////////// Restart Logic /////////////////////////

  public void restartAllStuckPipes() {
    final List<String> removedPipeName = removeOutdatedPipeInfoFromLastRestartTimeMap();
    if (!removedPipeName.isEmpty()) {
      final long currentTime = System.currentTimeMillis();
      LOGGER.info(
          "Pipes {} now can dynamically adjust their extraction strategies. "
              + "Start to flush storage engine to trigger the adjustment.",
          removedPipeName);
      StorageEngine.getInstance().syncCloseAllProcessor();
      WALManager.getInstance().syncDeleteOutdatedFilesInWALNodes();
      LOGGER.info(
          "Finished flushing storage engine, time cost: {} ms.",
          System.currentTimeMillis() - currentTime);
      LOGGER.info("Skipping restarting pipes this round because of the dynamic flushing.");
      return;
    }

    if (!tryWriteLockWithTimeOut(5)) {
      return;
    }

    final Set<PipeMeta> stuckPipes;
    try {
      stuckPipes = findAllStuckPipes();
    } finally {
      releaseWriteLock();
    }

    // If the pipe has been restarted recently, skip it.
    stuckPipes.removeIf(
        pipeMeta -> {
          final AtomicLong lastRestartTime =
              PIPE_NAME_TO_LAST_RESTART_TIME_MAP.get(pipeMeta.getStaticMeta().getPipeName());
          return lastRestartTime != null
              && System.currentTimeMillis() - lastRestartTime.get()
                  < PipeConfig.getInstance().getPipeStuckRestartMinIntervalMs();
        });

    // Restart all stuck pipes.
    // Note that parallelStream cannot be used here. The method PipeTaskAgent#dropPipe also uses
    // parallelStream. If parallelStream is used here, the subtasks generated inside the dropPipe
    // may not be scheduled by the worker thread of ForkJoinPool because of less available threads,
    // and the parent task will wait for the completion of the subtasks in ForkJoinPool forever,
    // causing the deadlock.
    stuckPipes.forEach(this::restartStuckPipe);
  }

  private List<String> removeOutdatedPipeInfoFromLastRestartTimeMap() {
    final List<String> removedPipeName = new ArrayList<>();
    PIPE_NAME_TO_LAST_RESTART_TIME_MAP
        .entrySet()
        .removeIf(
            entry -> {
              final AtomicLong lastRestartTime = entry.getValue();
              final boolean shouldRemove =
                  lastRestartTime == null
                      || PipeConfig.getInstance().getPipeStuckRestartMinIntervalMs()
                          <= System.currentTimeMillis() - lastRestartTime.get();
              if (shouldRemove) {
                removedPipeName.add(entry.getKey());
              }
              return shouldRemove;
            });
    return removedPipeName;
  }

  private Set<PipeMeta> findAllStuckPipes() {
    final Set<PipeMeta> stuckPipes = new HashSet<>();

    if (System.currentTimeMillis() - LAST_FORCED_RESTART_TIME.get()
        > PipeConfig.getInstance().getPipeSubtaskExecutorForcedRestartIntervalMs()) {
      LAST_FORCED_RESTART_TIME.set(System.currentTimeMillis());
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        stuckPipes.add(pipeMeta);
      }
      if (!stuckPipes.isEmpty()) {
        LOGGER.warn(
            "All {} pipe(s) will be restarted because of forced restart policy.",
            stuckPipes.size());
      }
      return stuckPipes;
    }

    final long totalLinkedButDeletedTsFileResourceRamSize =
        PipeDataNodeResourceManager.tsfile().getTotalLinkedButDeletedTsFileResourceRamSize();
    final long totalInsertNodeFloatingMemoryUsageInBytes = getAllFloatingMemoryUsageInByte();
    final long totalFloatingMemorySizeInBytes =
        PipeDataNodeResourceManager.memory().getTotalFloatingMemorySizeInBytes();
    if (totalInsertNodeFloatingMemoryUsageInBytes + totalLinkedButDeletedTsFileResourceRamSize
        >= totalFloatingMemorySizeInBytes) {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        stuckPipes.add(pipeMeta);
      }
      if (!stuckPipes.isEmpty()) {
        LOGGER.warn(
            "All {} pipe(s) will be restarted because linked but deleted tsFiles' resource size {} and all insertNode's size {} exceeds limit {}.",
            stuckPipes.size(),
            totalLinkedButDeletedTsFileResourceRamSize,
            totalInsertNodeFloatingMemoryUsageInBytes,
            totalFloatingMemorySizeInBytes);
      }
      return stuckPipes;
    }

    final Map<String, IoTDBDataRegionExtractor> taskId2ExtractorMap =
        PipeDataRegionExtractorMetrics.getInstance().getExtractorMap();
    for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
      final String pipeName = pipeMeta.getStaticMeta().getPipeName();
      final List<IoTDBDataRegionExtractor> extractors =
          taskId2ExtractorMap.values().stream()
              .filter(e -> e.getPipeName().equals(pipeName) && e.shouldExtractInsertion())
              .collect(Collectors.toList());

      if (extractors.isEmpty()) {
        continue;
      }

      // Extractors of this pipe might not pin too much MemTables,
      // still need to check if linked-and-deleted TsFile count exceeds limit.
      // Typically, if deleted tsFiles are too abundant all pipes may need to restart.
      if ((CONFIG.isEnableSeqSpaceCompaction()
              || CONFIG.isEnableUnseqSpaceCompaction()
              || CONFIG.isEnableCrossSpaceCompaction())
          && mayDeletedTsFileSizeReachDangerousThreshold()) {
        LOGGER.warn(
            "Pipe {} needs to restart because too many TsFiles are out-of-date.",
            pipeMeta.getStaticMeta());
        stuckPipes.add(pipeMeta);
        continue;
      }

      // Try to restart the stream mode pipes for releasing memTables.
      if (extractors.get(0).isStreamMode()) {
        if (extractors.stream().anyMatch(IoTDBDataRegionExtractor::hasConsumedAllHistoricalTsFiles)
            && (mayMemTablePinnedCountReachDangerousThreshold()
                || mayWalSizeReachThrottleThreshold())) {
          // Extractors of this pipe may be stuck and is pinning too many MemTables.
          LOGGER.warn(
              "Pipe {} needs to restart because too many memTables are pinned or the WAL size is too large. mayMemTablePinnedCountReachDangerousThreshold: {}, mayWalSizeReachThrottleThreshold: {}",
              pipeMeta.getStaticMeta(),
              mayMemTablePinnedCountReachDangerousThreshold(),
              mayWalSizeReachThrottleThreshold());
          stuckPipes.add(pipeMeta);
        }
      }
    }

    return stuckPipes;
  }

  private boolean mayDeletedTsFileSizeReachDangerousThreshold() {
    try {
      final long linkedButDeletedTsFileSize =
          PipeDataNodeResourceManager.tsfile().getTotalLinkedButDeletedTsfileSize();
      final double totalDisk =
          MetricService.getInstance()
              .getAutoGauge(
                  SystemMetric.SYS_DISK_TOTAL_SPACE.toString(),
                  MetricLevel.CORE,
                  Tag.NAME.toString(),
                  // This "system" should stay the same with the one in
                  // DataNodeInternalRPCServiceImpl.
                  "system")
              .getValue();
      return linkedButDeletedTsFileSize > 0
          && totalDisk > 0
          && linkedButDeletedTsFileSize
              > PipeConfig.getInstance().getPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage()
                  * totalDisk;
    } catch (final Exception e) {
      LOGGER.warn("Failed to judge if deleted TsFile size reaches dangerous threshold.", e);
      return false;
    }
  }

  private boolean mayMemTablePinnedCountReachDangerousThreshold() {
    return PipeConfig.getInstance().getPipeMaxAllowedPinnedMemTableCount() != Integer.MAX_VALUE
        && PipeDataNodeResourceManager.wal().getPinnedWalCount()
            >= 5
                * PipeConfig.getInstance().getPipeMaxAllowedPinnedMemTableCount()
                * StorageEngine.getInstance().getDataRegionNumber();
  }

  private boolean mayWalSizeReachThrottleThreshold() {
    return 3 * WALManager.getInstance().getTotalDiskUsage() > 2 * CONFIG.getThrottleThreshold();
  }

  private void restartStuckPipe(final PipeMeta pipeMeta) {
    LOGGER.warn(
        "Pipe {} will be restarted because it is stuck or has encountered issues such as data backlog or being stopped for too long.",
        pipeMeta.getStaticMeta());
    acquireWriteLock();
    try {
      final long startTime = System.currentTimeMillis();
      final PipeMeta originalPipeMeta = pipeMeta.deepCopy4TaskAgent();
      handleDropPipe(pipeMeta.getStaticMeta().getPipeName());

      final long restartTime = System.currentTimeMillis();
      PIPE_NAME_TO_LAST_RESTART_TIME_MAP
          .computeIfAbsent(pipeMeta.getStaticMeta().getPipeName(), k -> new AtomicLong(restartTime))
          .set(restartTime);
      handleSinglePipeMetaChanges(originalPipeMeta);

      LOGGER.warn(
          "Pipe {} was restarted because of stuck or data backlog, time cost: {} ms.",
          originalPipeMeta.getStaticMeta(),
          System.currentTimeMillis() - startTime);
    } catch (final Exception e) {
      LOGGER.warn("Failed to restart stuck pipe {}.", pipeMeta.getStaticMeta(), e);
    } finally {
      releaseWriteLock();
    }
  }

  public boolean isPipeTaskCurrentlyRestarted(final String pipeName) {
    return PIPE_NAME_TO_LAST_RESTART_TIME_MAP.containsKey(pipeName);
  }

  ///////////////////////// Terminate Logic /////////////////////////

  public void markCompleted(final String pipeName, final int regionId) {
    acquireWriteLock();
    try {
      if (pipeMetaKeeper.containsPipeMeta(pipeName)) {
        final PipeDataNodeTask pipeDataNodeTask =
            ((PipeDataNodeTask)
                pipeTaskManager.getPipeTask(
                    pipeMetaKeeper.getPipeMeta(pipeName).getStaticMeta(), regionId));
        if (Objects.nonNull(pipeDataNodeTask)) {
          pipeDataNodeTask.markCompleted();
        }
      }
    } finally {
      releaseWriteLock();
    }
  }

  ///////////////////////// Utils /////////////////////////

  public Set<Integer> getPipeTaskRegionIdSet(final String pipeName, final long creationTime) {
    final PipeMeta pipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    return pipeMeta == null || pipeMeta.getStaticMeta().getCreationTime() != creationTime
        ? Collections.emptySet()
        : pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().keySet();
  }

  public boolean hasPipeReleaseRegionRelatedResource(final int consensusGroupId) {
    if (!tryReadLockWithTimeOut(10)) {
      LOGGER.warn(
          "Failed to check if pipe has release region related resource with consensus group id: {}.",
          consensusGroupId);
      return false;
    }

    try {
      return !pipeTaskManager.hasPipeTaskInConsensusGroup(consensusGroupId);
    } finally {
      releaseReadLock();
    }
  }

  private boolean isSnapshotMode(final PipeParameters parameters) {
    final boolean isSnapshotMode;
    if (parameters.hasAnyAttributes(EXTRACTOR_MODE_SNAPSHOT_KEY, SOURCE_MODE_SNAPSHOT_KEY)) {
      isSnapshotMode =
          parameters.getBooleanOrDefault(
              Arrays.asList(EXTRACTOR_MODE_SNAPSHOT_KEY, SOURCE_MODE_SNAPSHOT_KEY),
              EXTRACTOR_MODE_SNAPSHOT_DEFAULT_VALUE);
    } else {
      final String extractorModeValue =
          parameters.getStringOrDefault(
              Arrays.asList(EXTRACTOR_MODE_KEY, SOURCE_MODE_KEY), EXTRACTOR_MODE_DEFAULT_VALUE);
      isSnapshotMode =
          extractorModeValue.equalsIgnoreCase(EXTRACTOR_MODE_SNAPSHOT_VALUE)
              || extractorModeValue.equalsIgnoreCase(EXTRACTOR_MODE_QUERY_VALUE);
    }
    return isSnapshotMode;
  }

  ///////////////////////// Shutdown Logic /////////////////////////

  public void persistAllProgressIndexLocally() {
    if (!PipeConfig.getInstance().isPipeProgressIndexPersistEnabled()) {
      LOGGER.info(
          "Pipe progress index persist disabled. Skipping persist all progress index locally.");
      return;
    }
    if (!tryReadLockWithTimeOut(10)) {
      LOGGER.info("Failed to persist all progress index locally because of timeout.");
      return;
    }
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMeta.getRuntimeMeta().persistProgressIndex();
      }
      LOGGER.info("Persist all progress index locally successfully.");
    } catch (final Exception e) {
      LOGGER.warn("Failed to record all progress index locally, because {}.", e.getMessage(), e);
    } finally {
      releaseReadLock();
    }
  }

  ///////////////////////// Pipe Consensus /////////////////////////

  public ProgressIndex getPipeTaskProgressIndex(final String pipeName, final int consensusGroupId) {
    if (!tryReadLockWithTimeOut(10)) {
      throw new PipeException(
          String.format(
              "Failed to get pipe task progress index with pipe name: %s, consensus group id %s.",
              pipeName, consensusGroupId));
    }

    try {
      if (!pipeMetaKeeper.containsPipeMeta(pipeName)) {
        throw new PipeException("Pipe meta not found: " + pipeName);
      }

      return pipeMetaKeeper
          .getPipeMeta(pipeName)
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .get(consensusGroupId)
          .getProgressIndex();
    } finally {
      releaseReadLock();
    }
  }

  public Map<ConsensusPipeName, PipeStatus> getAllConsensusPipe() {
    if (!tryReadLockWithTimeOut(10)) {
      throw new PipeException("Failed to get all consensus pipe.");
    }

    try {
      return StreamSupport.stream(pipeMetaKeeper.getPipeMetaList().spliterator(), false)
          .filter(pipeMeta -> PipeType.CONSENSUS.equals(pipeMeta.getStaticMeta().getPipeType()))
          .collect(
              ImmutableMap.toImmutableMap(
                  pipeMeta -> new ConsensusPipeName(pipeMeta.getStaticMeta().getPipeName()),
                  pipeMeta -> pipeMeta.getRuntimeMeta().getStatus().get()));
    } finally {
      releaseReadLock();
    }
  }
}
