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

import org.apache.iotdb.common.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.builder.PipeDataNodeBuilder;
import org.apache.iotdb.db.pipe.agent.task.builder.PipeDataNodeTaskBuilder;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.source.schemaregion.SchemaRegionListeningFilter;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY;

public class PipeDataNodeTaskAgent extends PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeTaskAgent.class);

  protected static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final ExecutorService pipeExecutor =
      new WrappedThreadPoolExecutor(
          0,
          IoTDBDescriptor.getInstance().getConfig().getPipeTaskThreadCount(),
          0L,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(
              IoTDBDescriptor.getInstance().getConfig().getPipeTaskThreadCount()),
          new IoTThreadFactory(ThreadName.PIPE_PARALLEL_EXECUTION_POOL.getName()),
          ThreadName.PIPE_PARALLEL_EXECUTION_POOL.getName(),
          new ThreadPoolExecutor.CallerRunsPolicy());

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

  ///////////////////////// Manage by regionGroupId /////////////////////////

  @Override
  protected void createPipeTask(
      final int consensusGroupId,
      final PipeStaticMeta pipeStaticMeta,
      final PipeTaskMeta pipeTaskMeta)
      throws IllegalPathException {
    if (pipeTaskMeta.getLeaderNodeId() == CONFIG.getDataNodeId()) {
      final PipeParameters sourceParameters = pipeStaticMeta.getSourceParameters();
      final DataRegionId dataRegionId = new DataRegionId(consensusGroupId);
      final boolean needConstructDataRegionTask =
          StorageEngine.getInstance().getAllDataRegionIds().contains(dataRegionId)
              && DataRegionListeningFilter.shouldDataRegionBeListened(
                  sourceParameters, dataRegionId);
      final boolean needConstructSchemaRegionTask =
          SchemaEngine.getInstance()
                  .getAllSchemaRegionIds()
                  .contains(new SchemaRegionId(consensusGroupId))
              && SchemaRegionListeningFilter.shouldSchemaRegionBeListened(
                  consensusGroupId, sourceParameters);

      // Advance the source parameters parsing logic to avoid creating un-relevant pipeTasks
      if (
      // For external source
      PipeRuntimeMeta.isSourceExternal(consensusGroupId)
          // For internal source
          || needConstructDataRegionTask
          || needConstructSchemaRegionTask) {
        calculateMemoryUsage(
            pipeStaticMeta, Collections.singletonList(new Pair<>(consensusGroupId, pipeTaskMeta)));

        final PipeDataNodeTask pipeTask =
            new PipeDataNodeTaskBuilder(pipeStaticMeta, consensusGroupId, pipeTaskMeta).build();
        pipeTask.create();
        pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
      }
    }

    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta)
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
      LOGGER.warn(DataNodePipeMessages.FAILED_TO_CLEAR_CLOSE_THE_SCHEMA_REGION, e.getMessage());
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
              pipeMetaFromCoordinator.getStaticMeta().getSourceParameters())
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
                    String.format(
                        DataNodePipeMessages
                            .FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION_BECAUSE_FMT,
                        schemaRegionId,
                        e.getMessage()),
                    e);
              }
            });
  }

  @Override
  protected void thawRate(final String pipeName, final long creationTime) {
    PipeDataNodeSinglePipeMetrics.getInstance().thawRate(pipeName + "_" + creationTime);
  }

  @Override
  protected void freezeRate(final String pipeName, final long creationTime) {
    PipeDataNodeSinglePipeMetrics.getInstance().freezeRate(pipeName + "_" + creationTime);
  }

  @Override
  protected boolean dropPipe(final String pipeName, final long creationTime) {
    if (!super.dropPipe(pipeName, creationTime)) {
      return false;
    }

    final String taskId = pipeName + "_" + creationTime;
    PipeTsFileToTabletsMetrics.getInstance().deregister(taskId);
    PipeDataNodeSinglePipeMetrics.getInstance().deregister(taskId);

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
      PipeDataNodeSinglePipeMetrics.getInstance().deregister(taskId);
      // When the pipe contains no pipe tasks, there is no corresponding prefetching queue for the
      // subscribed pipe, so the subscription needs to be manually marked as completed.
      if (!hasPipeTasks && PipeStaticMeta.isSubscriptionPipe(pipeName)) {
        final String topicName =
            pipeMeta.getStaticMeta().getSinkParameters().getString(PipeSinkConstant.SINK_TOPIC_KEY);
        final String consumerGroupId =
            pipeMeta
                .getStaticMeta()
                .getSinkParameters()
                .getString(PipeSinkConstant.SINK_CONSUMER_GROUP_KEY);
        SubscriptionAgent.broker().updateCompletedTopicNames(consumerGroupId, topicName);
      }
    }

    return true;
  }

  public void stopAllPipesWithCriticalExceptionAndTrackException(
      final PipeTaskMeta pipeTaskMeta, final PipeRuntimeException pipeRuntimeException) {
    super.stopAllPipesWithCriticalException(
        CONFIG.getDataNodeId(), pipeTaskMeta, pipeRuntimeException);
  }

  ///////////////////////// Heartbeat /////////////////////////

  public void collectPipeMetaList(final TDataNodeHeartbeatResp resp) throws TException {
    if (!tryReadLockWithTimeOutInMs(
        CommonDescriptor.getInstance().getConfig().getDnConnectionTimeoutInMS() * 2L / 3)) {
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
    final Optional<Logger> logger =
        PipeDataNodeResourceManager.log()
            .schedule(
                PipeDataNodeTaskAgent.class,
                PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                pipeMetaKeeper.getPipeMetaCount());

    collectPipeMetaReport(logger, true).setTo(resp);
    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(true);
  }

  @Override
  protected void collectPipeMetaListInternal(
      final TPipeHeartbeatReq req, final TPipeHeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    // If the heartbeatId == Long.MIN_VALUE then it's shutdown report and shall not be skipped
    if (PipeDataNodeAgent.runtime().isShutdown() && req.heartbeatId != Long.MIN_VALUE) {
      return;
    }
    final Optional<Logger> logger =
        PipeDataNodeResourceManager.log()
            .schedule(
                PipeDataNodeTaskAgent.class,
                PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                pipeMetaKeeper.getPipeMetaCount());
    LOGGER.debug(
        DataNodePipeMessages.RECEIVED_PIPE_HEARTBEAT_REQUEST_FROM_CONFIG_NODE, req.heartbeatId);

    collectPipeMetaReport(logger, false).setTo(resp);
    PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(true);
  }

  private PipeMetaReport collectPipeMetaReport(
      final Optional<Logger> logger, final boolean includeQueryMode) throws TException {
    final Set<Integer> dataRegionIds =
        StorageEngine.getInstance().getAllDataRegionIds().stream()
            .map(DataRegionId::getId)
            .collect(Collectors.toSet());

    final PipeMetaReport report = new PipeMetaReport();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        report.pipeMetaBinaryList.add(pipeMeta.serialize());

        final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();

        final Map<Integer, PipeTask> pipeTaskMap = pipeTaskManager.getPipeTasks(staticMeta);
        final boolean isAllDataRegionCompleted =
            pipeTaskMap == null
                || pipeTaskMap.entrySet().stream()
                    .filter(entry -> dataRegionIds.contains(entry.getKey()))
                    .allMatch(entry -> ((PipeDataNodeTask) entry.getValue()).isCompleted());
        final boolean isCompleted =
            isAllDataRegionCompleted && includeDataAndNeedDrop(pipeMeta, includeQueryMode);
        final Pair<Long, Double> remainingEventAndTime =
            PipeDataNodeSinglePipeMetrics.getInstance()
                .getRemainingEventAndTime(staticMeta.getPipeName(), staticMeta.getCreationTime());
        report.pipeCompletedList.add(isCompleted);
        report.pipeRemainingEventCountList.add(remainingEventAndTime.getLeft());
        report.pipeRemainingTimeList.add(remainingEventAndTime.getRight());
        report.pipeDegradedStatusList.add(
            PipeTemporaryMeta.encodeTsFileEpochDegradedStatus(
                ((PipeTemporaryMetaInAgent) pipeMeta.getTemporaryMeta())
                    .getGlobalTsFileEpochDegraded()));

        logger.ifPresent(
            l ->
                PipeLogger.log(
                    l::info,
                    DataNodePipeMessages
                        .LOG_REPORTING_PIPE_META_ARG_ISCOMPLETED_ARG_REMAININGEVENTCOUNT_ARG_8F996DF3,
                    pipeMeta.coreReportMessage(),
                    isCompleted,
                    remainingEventAndTime.getLeft()));
      }
      logger.ifPresent(
          l ->
              PipeLogger.log(
                  l::info,
                  DataNodePipeMessages.LOG_REPORTED_ARG_PIPE_METAS_12068FC6,
                  report.pipeMetaBinaryList.size()));
    } catch (final IOException | IllegalPathException e) {
      throw new TException(e);
    }
    return report;
  }

  private boolean includeDataAndNeedDrop(final PipeMeta pipeMeta, final boolean includeQueryMode)
      throws IllegalPathException {
    final PipeParameters sourceParameters = pipeMeta.getStaticMeta().getSourceParameters();
    if (!DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(sourceParameters)
        .getLeft()) {
      return false;
    }
    if (!includeQueryMode) {
      return isSnapshotMode(sourceParameters);
    }

    final String sourceModeValue =
        sourceParameters.getStringOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_MODE_KEY, PipeSourceConstant.SOURCE_MODE_KEY),
            PipeSourceConstant.EXTRACTOR_MODE_DEFAULT_VALUE);
    return sourceModeValue.equalsIgnoreCase(PipeSourceConstant.EXTRACTOR_MODE_QUERY_VALUE)
        || sourceModeValue.equalsIgnoreCase(PipeSourceConstant.EXTRACTOR_MODE_SNAPSHOT_VALUE);
  }

  private static class PipeMetaReport {
    private final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    private final List<Boolean> pipeCompletedList = new ArrayList<>();
    private final List<Long> pipeRemainingEventCountList = new ArrayList<>();
    private final List<Double> pipeRemainingTimeList = new ArrayList<>();
    private final List<Integer> pipeDegradedStatusList = new ArrayList<>();

    private void setTo(final TDataNodeHeartbeatResp resp) {
      resp.setPipeMetaList(pipeMetaBinaryList);
      resp.setPipeCompletedList(pipeCompletedList);
      resp.setPipeRemainingEventCountList(pipeRemainingEventCountList);
      resp.setPipeRemainingTimeList(pipeRemainingTimeList);
      resp.setPipeDegradedStatusList(pipeDegradedStatusList);
    }

    private void setTo(final TPipeHeartbeatResp resp) {
      resp.setPipeMetaList(pipeMetaBinaryList);
      resp.setPipeCompletedList(pipeCompletedList);
      resp.setPipeRemainingEventCountList(pipeRemainingEventCountList);
      resp.setPipeRemainingTimeList(pipeRemainingTimeList);
      resp.setPipeDegradedStatusList(pipeDegradedStatusList);
    }
  }

  ///////////////////////// Terminate Logic /////////////////////////

  public void markCompleted(final String pipeName, final int regionId) {
    markCompleted(pipeName, 0, regionId);
  }

  public void markCompleted(final String pipeName, final long creationTime, final int regionId) {
    acquireWriteLock();
    try {
      final PipeMeta pipeMeta =
          creationTime == 0
              ? pipeMetaKeeper.getPipeMeta(pipeName)
              : pipeMetaKeeper.getPipeMeta(pipeName, creationTime);
      if (pipeMeta != null) {
        final PipeDataNodeTask pipeDataNodeTask =
            ((PipeDataNodeTask) pipeTaskManager.getPipeTask(pipeMeta.getStaticMeta(), regionId));
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
    final PipeMeta pipeMeta = pipeMetaKeeper.getPipeMeta(pipeName, creationTime);
    return pipeMeta == null
        ? Collections.emptySet()
        : pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().keySet();
  }

  public boolean hasPipeReleaseRegionRelatedResource(final int consensusGroupId) {
    if (!tryReadLockWithTimeOut(10)) {
      LOGGER.warn(DataNodePipeMessages.FAILED_TO_CHECK_IF_PIPE_HAS_RELEASE, consensusGroupId);
      return false;
    }

    try {
      return !pipeTaskManager.hasPipeTaskInConsensusGroup(consensusGroupId);
    } finally {
      releaseReadLock();
    }
  }

  public boolean isFullSync(final PipeParameters parameters) throws IllegalPathException {
    if (isSnapshotMode(parameters)) {
      return false;
    }

    final boolean isHistoryEnable =
        parameters.getBooleanOrDefault(
            Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
            EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);
    final boolean isRealtimeEnable =
        parameters.getBooleanOrDefault(
            Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
            EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE);

    return isHistoryEnable
        && isRealtimeEnable
        && DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters)
            .getLeft();
  }

  @Override
  public void runPipeTasks(
      final Collection<PipeTask> pipeTasks, final Consumer<PipeTask> runSingle) {
    final Set<Future<?>> pipeFuture = new HashSet<>();

    pipeTasks.forEach(
        pipeTask -> pipeFuture.add(pipeExecutor.submit(() -> runSingle.accept(pipeTask))));

    for (final Future<?> future : pipeFuture) {
      try {
        future.get();
      } catch (final ExecutionException | InterruptedException e) {
        LOGGER.warn(DataNodePipeMessages.EXCEPTION_OCCURS_WHEN_EXECUTING_PIPE_TASK, e);
        throw new PipeException(e.toString());
      }
    }
  }

  ///////////////////////// Shutdown Logic /////////////////////////

  public long getShutdownProgressPersistTimeoutInMs() {
    return Math.max(
        1_000L,
        (long) CommonDescriptor.getInstance().getConfig().getCnConnectionTimeoutInMS()
            + CommonDescriptor.getInstance().getConfig().getDnConnectionTimeoutInMS());
  }

  public boolean persistAllProgressIndex(final long timeoutInMs) {
    final long normalizedTimeoutInMs = Math.max(1L, timeoutInMs);
    final long startTime = System.currentTimeMillis();
    final AtomicBoolean isConfirmed = new AtomicBoolean(false);
    final Thread persistThread =
        new Thread(
            () -> isConfirmed.set(persistAllProgressIndexInternal()),
            ThreadName.PIPE_RUNTIME_META_SYNCER.getName() + "-Shutdown-Persist");
    persistThread.setDaemon(true);

    LOGGER.info(
        DataNodePipeMessages.START_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN,
        getPipeCount(),
        normalizedTimeoutInMs);
    persistThread.start();
    try {
      final long deadlineInMs = startTime + normalizedTimeoutInMs;
      while (persistThread.isAlive()) {
        final long remainingTimeInMs = deadlineInMs - System.currentTimeMillis();
        if (remainingTimeInMs <= 0) {
          break;
        }
        persistThread.join(remainingTimeInMs);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.info(
          DataNodePipeMessages
              .INTERRUPTED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN);
      return false;
    }

    if (persistThread.isAlive()) {
      LOGGER.warn(
          DataNodePipeMessages.TIMED_OUT_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN,
          System.currentTimeMillis() - startTime);
      return false;
    }

    if (!isConfirmed.get()) {
      LOGGER.warn(
          DataNodePipeMessages.FAILED_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN,
          System.currentTimeMillis() - startTime);
    }
    return isConfirmed.get();
  }

  public void persistAllProgressIndex() {
    persistAllProgressIndex(getShutdownProgressPersistTimeoutInMs());
  }

  private boolean persistAllProgressIndexInternal() {
    final long collectStartTime = System.currentTimeMillis();
    final int pipeCount = getPipeCount();
    try {
      final TPipeHeartbeatResp resp = new TPipeHeartbeatResp(new ArrayList<>());
      collectPipeMetaList(new TPipeHeartbeatReq(Long.MIN_VALUE), resp);
      final int pipeMetaCount = resp.getPipeMetaList().size();
      final int pipeMetaSizeInBytes =
          resp.getPipeMetaList().stream()
              .filter(Objects::nonNull)
              .mapToInt(ByteBuffer::remaining)
              .sum();
      LOGGER.info(
          DataNodePipeMessages.COLLECTED_PIPE_METAS_FOR_SHUTDOWN_PROGRESS_PERSIST,
          pipeCount,
          pipeMetaCount,
          pipeMetaSizeInBytes,
          System.currentTimeMillis() - collectStartTime);

      if (resp.getPipeMetaList().isEmpty()) {
        if (pipeCount != 0) {
          LOGGER.info(DataNodePipeMessages.COLLECTED_EMPTY_PIPE_METAS_DURING_SHUTDOWN, pipeCount);
          return false;
        }
        return true;
      }

      try (final ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        LOGGER.info(
            DataNodePipeMessages.START_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE,
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            pipeCount,
            pipeMetaCount,
            pipeMetaSizeInBytes);
        final long pushStartTime = System.currentTimeMillis();
        final TSStatus result =
            configNodeClient.pushHeartbeat(
                IoTDBDescriptor.getInstance().getConfig().getDataNodeId(), resp);
        final long pushCostTime = System.currentTimeMillis() - pushStartTime;
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != result.getCode()) {
          LOGGER.warn(DataNodePipeMessages.FAILED_TO_PERSIST_PROGRESS_INDEX_TO_CONFIGNODE, result);
          LOGGER.warn(
              DataNodePipeMessages.FAILED_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE,
              result,
              pushCostTime);
          return false;
        } else {
          LOGGER.info(
              DataNodePipeMessages
                  .SUCCESSFULLY_FINISHED_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE,
              pipeCount,
              pipeMetaCount,
              pipeMetaSizeInBytes,
              pushCostTime);
          LOGGER.info(DataNodePipeMessages.SUCCESSFULLY_PERSISTED_ALL_PIPE_S_INFO_TO);
          return true;
        }
      }
    } catch (final Exception e) {
      LOGGER.warn(
          DataNodePipeMessages
              .EXCEPTION_OCCURRED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN,
          e);
      return false;
    }
  }

  ///////////////////////// Pipe Consensus /////////////////////////

  public ProgressIndex getPipeTaskProgressIndex(final String pipeName, final int consensusGroupId) {
    if (!tryReadLockWithTimeOut(10)) {
      throw new PipeException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FAILED_TO_GET_PIPE_TASK_PROGRESS_INDEX_WITH_PIPE_NAME_S_CFE9DE7C,
              pipeName,
              consensusGroupId));
    }

    try {
      if (!pipeMetaKeeper.containsPipeMeta(pipeName)) {
        throw new PipeException(DataNodePipeMessages.PIPE_META_NOT_FOUND + pipeName);
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

  @Override
  protected void calculateMemoryUsage(final PipeMeta pipeMetaFromCoordinator)
      throws IllegalPathException {
    final PipeStaticMeta staticMeta = pipeMetaFromCoordinator.getStaticMeta();
    if (!PipeConfig.getInstance().isPipeEnableMemoryCheck()
        || !isInnerSource(staticMeta.getSourceParameters())
        || !PipeType.USER.equals(staticMeta.getPipeType())) {
      return;
    }

    calculateMemoryUsage(staticMeta, collectPipeTasksToBeCreated(pipeMetaFromCoordinator));
  }

  private void calculateMemoryUsage(
      final PipeStaticMeta staticMeta, final List<Pair<Integer, PipeTaskMeta>> pipeTasksToBeCreated)
      throws IllegalPathException {
    if (!PipeConfig.getInstance().isPipeEnableMemoryCheck()
        || !isInnerSource(staticMeta.getSourceParameters())
        || !PipeType.USER.equals(staticMeta.getPipeType())) {
      return;
    }

    if (pipeTasksToBeCreated.isEmpty()) {
      calculateInsertNodeQueueMemory(staticMeta.getSourceParameters(), 1);
      return;
    }

    final MemoryEstimation memoryEstimation =
        calculateIncrementalMemoryUsage(staticMeta, pipeTasksToBeCreated);
    calculateInsertNodeQueueMemory(
        staticMeta.getSourceParameters(), memoryEstimation.dataRegionTaskCount);

    final long needMemory = memoryEstimation.nonFloatingMemoryInBytes;

    PipeMemoryManager pipeMemoryManager = PipeDataNodeResourceManager.memory();
    final long freeMemorySizeInBytes = pipeMemoryManager.getFreeMemorySizeInBytes();
    final long reservedMemorySizeInBytes =
        (long)
            (PipeDataNodeResourceManager.memory().getTotalMemorySizeInBytes()
                * PipeConfig.getInstance().getReservedMemoryPercentage());
    if (freeMemorySizeInBytes < needMemory + reservedMemorySizeInBytes) {
      final String message =
          String.format(
              PipeMessages.NOT_ENOUGH_MEMORY_FOR_PIPE_FORMAT,
              needMemory,
              freeMemorySizeInBytes,
              reservedMemorySizeInBytes,
              PipeDataNodeResourceManager.memory().getTotalMemorySizeInBytes());
      LOGGER.warn(message);
      throw new PipeException(message);
    }
  }

  private List<Pair<Integer, PipeTaskMeta>> collectPipeTasksToBeCreated(
      final PipeMeta pipeMetaFromCoordinator) throws IllegalPathException {
    final PipeStaticMeta pipeStaticMeta = pipeMetaFromCoordinator.getStaticMeta();
    final PipeParameters sourceParameters = pipeStaticMeta.getSourceParameters();
    final Set<DataRegionId> dataRegionIds =
        new HashSet<>(StorageEngine.getInstance().getAllDataRegionIds());
    final Set<SchemaRegionId> schemaRegionIds =
        new HashSet<>(SchemaEngine.getInstance().getAllSchemaRegionIds());
    final List<Pair<Integer, PipeTaskMeta>> pipeTasksToBeCreated = new ArrayList<>();

    for (final Map.Entry<Integer, PipeTaskMeta> consensusGroupIdToPipeTaskMeta :
        pipeMetaFromCoordinator.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().entrySet()) {
      final int consensusGroupId = consensusGroupIdToPipeTaskMeta.getKey();
      final PipeTaskMeta pipeTaskMeta = consensusGroupIdToPipeTaskMeta.getValue();
      if (pipeTaskMeta.getLeaderNodeId() != CONFIG.getDataNodeId()) {
        continue;
      }

      final boolean needConstructTask;
      if (pipeStaticMeta.isSourceExternal()) {
        needConstructTask = true;
      } else {
        final DataRegionId dataRegionId = new DataRegionId(consensusGroupId);
        final boolean needConstructDataRegionTask =
            dataRegionIds.contains(dataRegionId)
                && DataRegionListeningFilter.shouldDataRegionBeListened(
                    sourceParameters, dataRegionId);
        final boolean needConstructSchemaRegionTask =
            schemaRegionIds.contains(new SchemaRegionId(consensusGroupId))
                && SchemaRegionListeningFilter.shouldSchemaRegionBeListened(
                    consensusGroupId, sourceParameters);
        needConstructTask = needConstructDataRegionTask || needConstructSchemaRegionTask;
      }

      if (needConstructTask) {
        pipeTasksToBeCreated.add(new Pair<>(consensusGroupId, pipeTaskMeta));
      }
    }
    return pipeTasksToBeCreated;
  }

  private MemoryEstimation calculateIncrementalMemoryUsage(
      final PipeStaticMeta staticMeta,
      final List<Pair<Integer, PipeTaskMeta>> pipeTasksToBeCreated) {
    int dataRegionTaskCount = 0;

    for (final Pair<Integer, PipeTaskMeta> regionIdAndTaskMeta : pipeTasksToBeCreated) {
      if (isDataRegionTask(regionIdAndTaskMeta.getLeft())) {
        dataRegionTaskCount++;
      }
    }

    // TsFile parser, sink batch, and TsFile read buffer memory are allocated dynamically
    // from PipeMemoryManager only while they are active.
    final long needMemory =
        dataRegionTaskCount > 0 ? calculateAssignerMemory(staticMeta.getSourceParameters()) : 0;
    return new MemoryEstimation(needMemory, dataRegionTaskCount);
  }

  private boolean isDataRegionTask(final int regionId) {
    return StorageEngine.getInstance().getAllDataRegionIds().contains(new DataRegionId(regionId))
        || PipeRuntimeMeta.isSourceExternal(regionId);
  }

  private static class MemoryEstimation {
    private final long nonFloatingMemoryInBytes;
    private final int dataRegionTaskCount;

    private MemoryEstimation(final long nonFloatingMemoryInBytes, final int dataRegionTaskCount) {
      this.nonFloatingMemoryInBytes = nonFloatingMemoryInBytes;
      this.dataRegionTaskCount = dataRegionTaskCount;
    }
  }

  private boolean isInnerSource(final PipeParameters sourceParameters) {
    final String pluginName =
        sourceParameters
            .getStringOrDefault(
                Arrays.asList(PipeSourceConstant.EXTRACTOR_KEY, PipeSourceConstant.SOURCE_KEY),
                BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
            .toLowerCase();

    return pluginName.equals(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
        || pluginName.equals(BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName());
  }

  private void calculateInsertNodeQueueMemory(
      final PipeParameters sourceParameters, final int dataRegionTaskCount) {
    if (dataRegionTaskCount <= 0) {
      return;
    }

    // Realtime source is enabled by default, so we only need to check the source realtime
    if (!sourceParameters.getBooleanOrDefault(
        Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
        EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)) {
      return;
    }

    // If the realtime mode is batch or file, we do not need to allocate memory
    final String realtimeMode =
        sourceParameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY,
            PipeSourceConstant.SOURCE_REALTIME_MODE_KEY);
    if (PipeSourceConstant.EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE.equals(realtimeMode)
        || PipeSourceConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE.equals(realtimeMode)) {
      return;
    }

    final long needFloatingMemory =
        PipeConfig.getInstance().getPipeInsertNodeQueueMemory() * dataRegionTaskCount;
    final long allocatedMemorySizeInBytes = this.getAllFloatingMemoryUsageInByte();
    final long remainingMemory =
        PipeDataNodeResourceManager.memory().getTotalFloatingMemorySizeInBytes()
            - allocatedMemorySizeInBytes;
    if (remainingMemory < needFloatingMemory) {
      final String message =
          String.format(
              PipeMessages.NOT_ENOUGH_FLOATING_MEMORY_FOR_PIPE_FORMAT,
              needFloatingMemory,
              remainingMemory);
      LOGGER.warn(message);
      throw new PipeException(message);
    }
  }

  private long calculateAssignerMemory(final PipeParameters sourceParameters) {
    try {
      if (!PipeInsertionDataNodeListener.getInstance().isEmpty()
          || !DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(sourceParameters)
              .getLeft()) {
        return 0;
      }
      return PipeConfig.getInstance().getPipeSourceAssignerDisruptorRingBufferSize()
          * PipeConfig.getInstance().getPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes()
          * Math.min(StorageEngine.getInstance().getDataRegionNumber(), 10);
    } catch (final IllegalPathException e) {
      return 0;
    }
  }
}
