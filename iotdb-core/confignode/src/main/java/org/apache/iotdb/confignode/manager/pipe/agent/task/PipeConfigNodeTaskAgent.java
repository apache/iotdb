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

package org.apache.iotdb.confignode.manager.pipe.agent.task;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.extractor.ConfigRegionListeningFilter;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigNodeRemainingTimeMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigRegionExtractorMetrics;
import org.apache.iotdb.confignode.manager.pipe.resource.PipeConfigNodeResourceManager;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PipeConfigNodeTaskAgent extends PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigNodeTaskAgent.class);

  @Override
  protected boolean isShutdown() {
    return PipeConfigNodeAgent.runtime().isShutdown();
  }

  @Override
  protected void thawRate(final String pipeName, final long creationTime) {
    PipeConfigNodeRemainingTimeMetrics.getInstance().thawRate(pipeName + "_" + creationTime);
  }

  @Override
  protected void freezeRate(final String pipeName, final long creationTime) {
    PipeConfigNodeRemainingTimeMetrics.getInstance().freezeRate(pipeName + "_" + creationTime);
  }

  @Override
  protected Map<Integer, PipeTask> buildPipeTasks(final PipeMeta pipeMetaFromConfigNode)
      throws IllegalPathException {
    return new PipeConfigNodeTaskBuilder(pipeMetaFromConfigNode).build();
  }

  @Override
  protected void createPipeTask(
      final int consensusGroupId,
      final PipeStaticMeta pipeStaticMeta,
      final PipeTaskMeta pipeTaskMeta)
      throws IllegalPathException {
    // Advance the extractor parameters parsing logic to avoid creating un-relevant pipeTasks
    if (consensusGroupId == Integer.MIN_VALUE
        && pipeTaskMeta.getLeaderNodeId()
            == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()
        && !ConfigRegionListeningFilter.parseListeningPlanTypeSet(
                pipeStaticMeta.getExtractorParameters())
            .isEmpty()) {
      final PipeConfigNodeTask pipeTask =
          new PipeConfigNodeTask(
              new PipeConfigNodeTaskStage(
                  pipeStaticMeta.getPipeName(),
                  pipeStaticMeta.getCreationTime(),
                  pipeStaticMeta.getExtractorParameters().getAttribute(),
                  pipeStaticMeta.getProcessorParameters().getAttribute(),
                  pipeStaticMeta.getConnectorParameters().getAttribute(),
                  pipeTaskMeta));
      pipeTask.create();
      pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
    }

    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .put(consensusGroupId, pipeTaskMeta);
  }

  public void stopAllPipesWithCriticalException() {
    super.stopAllPipesWithCriticalException(
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId());
  }

  @Override
  protected TPushPipeMetaRespExceptionMessage handleSinglePipeMetaChangesInternal(
      final PipeMeta pipeMetaFromCoordinator) {
    try {
      return PipeConfigNodeAgent.runtime().isLeaderReady()
          ? super.handleSinglePipeMetaChangesInternal(pipeMetaFromCoordinator.deepCopy4TaskAgent())
          : null;
    } catch (final Exception e) {
      return new TPushPipeMetaRespExceptionMessage(
          pipeMetaFromCoordinator.getStaticMeta().getPipeName(),
          e.getMessage(),
          System.currentTimeMillis());
    }
  }

  @Override
  protected TPushPipeMetaRespExceptionMessage handleDropPipeInternal(final String pipeName) {
    return PipeConfigNodeAgent.runtime().isLeaderReady()
        ? super.handleDropPipeInternal(pipeName)
        : null;
  }

  @Override
  protected List<TPushPipeMetaRespExceptionMessage> handlePipeMetaChangesInternal(
      final List<PipeMeta> pipeMetaListFromCoordinator) {
    if (isShutdown() || !PipeConfigNodeAgent.runtime().isLeaderReady()) {
      return Collections.emptyList();
    }

    try {
      final List<TPushPipeMetaRespExceptionMessage> exceptionMessages =
          super.handlePipeMetaChangesInternal(
              pipeMetaListFromCoordinator.stream()
                  .map(
                      pipeMeta -> {
                        try {
                          return pipeMeta.deepCopy4TaskAgent();
                        } catch (Exception e) {
                          throw new PipeException("failed to deep copy pipeMeta", e);
                        }
                      })
                  .collect(Collectors.toList()));
      clearConfigRegionListeningQueueIfNecessary(pipeMetaListFromCoordinator);
      return exceptionMessages;
    } catch (final Exception e) {
      throw new PipeException("failed to handle pipe meta changes", e);
    }
  }

  private void clearConfigRegionListeningQueueIfNecessary(
      final List<PipeMeta> pipeMetaListFromCoordinator) {
    final AtomicLong listeningQueueNewFirstIndex = new AtomicLong(Long.MAX_VALUE);

    // Check each pipe
    for (final PipeMeta pipeMetaFromCoordinator : pipeMetaListFromCoordinator) {
      // Check each region in a pipe
      final Map<Integer, PipeTaskMeta> groupId2TaskMetaMap =
          pipeMetaFromCoordinator.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();
      for (final Integer regionId : groupId2TaskMetaMap.keySet()) {
        if (regionId != Integer.MIN_VALUE) {
          continue;
        }

        final ProgressIndex progressIndex = groupId2TaskMetaMap.get(regionId).getProgressIndex();
        if (progressIndex instanceof MetaProgressIndex) {
          if (((MetaProgressIndex) progressIndex).getIndex() + 1
              < listeningQueueNewFirstIndex.get()) {
            listeningQueueNewFirstIndex.set(((MetaProgressIndex) progressIndex).getIndex() + 1);
          }
        } else {
          // Do not clear "minimumProgressIndex"s related queues to avoid clearing
          // the queue when there are schema tasks just started and transferring
          listeningQueueNewFirstIndex.set(0);
        }
      }
    }

    if (listeningQueueNewFirstIndex.get() < Long.MAX_VALUE) {
      PipeConfigNodeAgent.runtime().listener().removeBefore(listeningQueueNewFirstIndex.get());
    }
  }

  @Override
  protected void collectPipeMetaListInternal(
      final TPipeHeartbeatReq req, final TPipeHeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (isShutdown() || !PipeConfigNodeAgent.runtime().isLeaderReady()) {
      return;
    }

    LOGGER.info("Received pipe heartbeat request {} from config coordinator.", req.heartbeatId);

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    final List<Long> pipeRemainingEventCountList = new ArrayList<>();
    final List<Double> pipeRemainingTimeList = new ArrayList<>();
    try {
      final Optional<Logger> logger =
          PipeConfigNodeResourceManager.log()
              .schedule(
                  PipeConfigNodeTaskAgent.class,
                  PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                  PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                  pipeMetaKeeper.getPipeMetaCount());

      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());

        final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();
        final long remainingEventCount =
            PipeConfigRegionExtractorMetrics.getInstance()
                .getRemainingEventCount(staticMeta.getPipeName(), staticMeta.getCreationTime());
        final double estimatedRemainingTime =
            PipeConfigNodeRemainingTimeMetrics.getInstance()
                .getRemainingTime(staticMeta.getPipeName(), staticMeta.getCreationTime());

        pipeRemainingEventCountList.add(remainingEventCount);
        pipeRemainingTimeList.add(estimatedRemainingTime);

        logger.ifPresent(
            l ->
                l.info(
                    "Reporting pipe meta: {}, remainingEventCount: {}, estimatedRemainingTime: {}",
                    pipeMeta.coreReportMessage(),
                    remainingEventCount,
                    estimatedRemainingTime));
      }
      LOGGER.info("Reported {} pipe metas.", pipeMetaBinaryList.size());
    } catch (final IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
    resp.setPipeRemainingEventCountList(pipeRemainingEventCountList);
    resp.setPipeRemainingTimeList(pipeRemainingTimeList);
  }
}
