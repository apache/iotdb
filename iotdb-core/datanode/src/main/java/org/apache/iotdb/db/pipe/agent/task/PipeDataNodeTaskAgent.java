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

import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.builder.PipeDataNodeBuilder;
import org.apache.iotdb.db.pipe.agent.task.builder.PipeDataNodeTaskBuilder;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningFilter;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_START_TIME_KEY;

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
              IoTDBDescriptor.getInstance().getConfig().getSchemaThreadCount()),
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
    return new PipeDataNodeBuilder(pipeMetaFromConfigNode).build();
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
    if (!tryReadLockWithTimeOut(
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
            PipeDataNodeSinglePipeMetrics.getInstance()
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
            PipeDataNodeSinglePipeMetrics.getInstance()
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
        LOGGER.warn("Exception occurs when executing pipe task: ", e);
        throw new PipeException(e.toString());
      }
    }
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

  @Override
  protected void calculateMemoryUsage(
      final PipeParameters extractorParameters,
      final PipeParameters processorParameters,
      final PipeParameters connectorParameters) {
    if (!PipeConfig.getInstance().isPipeEnableMemoryCheck()) {
      return;
    }

    calculateInsertNodeQueueMemory(extractorParameters, processorParameters, connectorParameters);

    long needMemory = 0;

    needMemory +=
        calculateTsFileParserMemory(extractorParameters, processorParameters, connectorParameters);
    needMemory +=
        calculateSinkBatchMemory(extractorParameters, processorParameters, connectorParameters);
    needMemory +=
        calculateSendTsFileReadBufferMemory(
            extractorParameters, processorParameters, connectorParameters);

    PipeMemoryManager pipeMemoryManager = PipeDataNodeResourceManager.memory();
    final long freeMemorySizeInBytes = pipeMemoryManager.getFreeMemorySizeInBytes();
    final long reservedMemorySizeInBytes =
        (long)
            (PipeMemoryManager.getTotalMemorySizeInBytes()
                * PipeConfig.getInstance().getReservedMemoryPercentage());
    if (freeMemorySizeInBytes < needMemory + reservedMemorySizeInBytes) {
      final String message =
          String.format(
              "Not enough memory for pipe. Need memory: %d bytes, free memory: %d bytes, reserved memory: %d bytes, total memory: %d bytes",
              needMemory,
              freeMemorySizeInBytes,
              freeMemorySizeInBytes,
              PipeMemoryManager.getTotalMemorySizeInBytes());
      LOGGER.warn(message);
      throw new PipeException(message);
    }
  }

  private void calculateInsertNodeQueueMemory(
      final PipeParameters extractorParameters,
      final PipeParameters processorParameters,
      final PipeParameters connectorParameters) {

    // Realtime extractor is enabled by default, so we only need to check the source realtime
    if (!extractorParameters.getBooleanOrDefault(
        Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
        EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)) {
      return;
    }

    // If the realtime mode is batch or file, we do not need to allocate memory
    final String realtimeMode =
        extractorParameters.getStringByKeys(
            PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_KEY,
            PipeExtractorConstant.SOURCE_REALTIME_MODE_KEY);
    if (PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE.equals(realtimeMode)
        || PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE.equals(realtimeMode)) {
      return;
    }

    final long allocatedMemorySizeInBytes = this.getAllFloatingMemoryUsageInByte();
    final long remainingMemory =
        PipeMemoryManager.getTotalFloatingMemorySizeInBytes() - allocatedMemorySizeInBytes;
    if (remainingMemory < PipeConfig.getInstance().PipeInsertNodeQueueMemory()) {
      final String message =
          String.format(
              "Not enough memory for pipe. Need Floating memory: %d  bytes, free Floating memory: %d bytes",
              PipeConfig.getInstance().PipeInsertNodeQueueMemory(), remainingMemory);
      LOGGER.warn(message);
      throw new PipeException(message);
    }
  }

  private long calculateTsFileParserMemory(
      final PipeParameters extractorParameters,
      final PipeParameters processorParameters,
      final PipeParameters connectorParameters) {

    // If the extractor is not history, we do not need to allocate memory
    boolean isExtractorHistory =
        extractorParameters.getBooleanOrDefault(
                SystemConstant.RESTART_KEY, SystemConstant.RESTART_DEFAULT_VALUE)
            || extractorParameters.getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);

    // If the extractor is history, and has start/end time, we need to allocate memory
    boolean isTSFileParser =
        isExtractorHistory
            && extractorParameters.hasAnyAttributes(
                EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY);

    isTSFileParser =
        isTSFileParser
            || (isExtractorHistory
                && extractorParameters.hasAnyAttributes(
                    EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY));

    // if the extractor has start/end time, we need to allocate memory
    isTSFileParser =
        isTSFileParser
            || extractorParameters.hasAnyAttributes(
                SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY);

    isTSFileParser =
        isTSFileParser
            || extractorParameters.hasAnyAttributes(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY);

    // If the extractor has pattern or path, we need to allocate memory
    isTSFileParser =
        isTSFileParser
            || extractorParameters.hasAnyAttributes(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY);

    isTSFileParser =
        isTSFileParser || extractorParameters.hasAnyAttributes(EXTRACTOR_PATH_KEY, SOURCE_PATH_KEY);

    // If the extractor is not hybrid, we do need to allocate memory
    isTSFileParser =
        isTSFileParser
            || !PipeConnectorConstant.CONNECTOR_FORMAT_HYBRID_VALUE.equals(
                connectorParameters.getStringOrDefault(
                    Arrays.asList(
                        PipeConnectorConstant.CONNECTOR_FORMAT_KEY,
                        PipeConnectorConstant.SINK_FORMAT_KEY),
                    PipeConnectorConstant.CONNECTOR_FORMAT_HYBRID_VALUE));

    if (!isTSFileParser) {
      return 0;
    }

    return PipeConfig.getInstance().getTsFileParserMemory();
  }

  private long calculateSinkBatchMemory(
      final PipeParameters extractorParameters,
      final PipeParameters processorParameters,
      final PipeParameters connectorParameters) {

    // If the connector format is tsfile , we need to use batch
    boolean needUseBatch =
        PipeConnectorConstant.CONNECTOR_FORMAT_TS_FILE_VALUE.equals(
            connectorParameters.getStringOrDefault(
                Arrays.asList(
                    PipeConnectorConstant.CONNECTOR_FORMAT_KEY,
                    PipeConnectorConstant.SINK_FORMAT_KEY),
                PipeConnectorConstant.CONNECTOR_FORMAT_HYBRID_VALUE));

    if (needUseBatch) {
      return PipeConfig.getInstance().getSinkBatchMemoryTsFile();
    }

    // If the connector is batch mode, we need to use batch
    needUseBatch =
        connectorParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY,
                PipeConnectorConstant.SINK_IOTDB_BATCH_MODE_ENABLE_KEY),
            PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_MODE_ENABLE_DEFAULT_VALUE);

    if (!needUseBatch) {
      return 0;
    }

    return PipeConfig.getInstance().getSinkBatchMemoryInsertNode();
  }

  private long calculateSendTsFileReadBufferMemory(
      final PipeParameters extractorParameters,
      final PipeParameters processorParameters,
      final PipeParameters connectorParameters) {
    // If the extractor is history enable, we need to transfer tsfile
    boolean needTransferTsFile =
        extractorParameters.getBooleanOrDefault(
                SystemConstant.RESTART_KEY, SystemConstant.RESTART_DEFAULT_VALUE)
            || extractorParameters.getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);

    String format =
        connectorParameters.getStringOrDefault(
            Arrays.asList(
                PipeConnectorConstant.CONNECTOR_FORMAT_KEY, PipeConnectorConstant.SINK_FORMAT_KEY),
            PipeConnectorConstant.CONNECTOR_FORMAT_HYBRID_VALUE);

    // If the connector format is tsfile and hybrid, we need to transfer tsfile
    needTransferTsFile =
        needTransferTsFile
            || PipeConnectorConstant.CONNECTOR_FORMAT_HYBRID_VALUE.equals(format)
            || PipeConnectorConstant.CONNECTOR_FORMAT_TS_FILE_VALUE.equals(format);

    if (!needTransferTsFile) {
      return 0;
    }

    return PipeConfig.getInstance().getSendTsFileReadBuffer();
  }
}
