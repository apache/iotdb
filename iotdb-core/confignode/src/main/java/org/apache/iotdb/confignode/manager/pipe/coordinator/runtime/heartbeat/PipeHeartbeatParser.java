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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.resource.PipeConfigNodeResourceManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PipeHeartbeatParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatParser.class);

  private final ConfigManager configManager;

  private long heartbeatCounter;
  private int registeredNodeNumber;

  private final AtomicBoolean needWriteConsensusOnConfigNodes;
  private final AtomicBoolean needPushPipeMetaToDataNodes;

  PipeHeartbeatParser(final ConfigManager configManager) {
    this.configManager = configManager;

    heartbeatCounter = 0;
    registeredNodeNumber = getExpectedHeartbeatNodeCount();

    needWriteConsensusOnConfigNodes = new AtomicBoolean(false);
    needPushPipeMetaToDataNodes = new AtomicBoolean(false);
  }

  synchronized void parseHeartbeat(final int nodeId, final PipeHeartbeat pipeHeartbeat) {
    final long heartbeatCount = ++heartbeatCounter;

    final AtomicBoolean canSubmitHandleMetaChangeProcedure = new AtomicBoolean(false);
    // registeredNodeNumber can not be 0 when the method is called
    if (heartbeatCount % registeredNodeNumber == 0) {
      canSubmitHandleMetaChangeProcedure.set(true);

      // The expected reporter set may be changed, update it at the end of the current round.
      registeredNodeNumber = getExpectedHeartbeatNodeCount();
    }

    if (pipeHeartbeat.isEmpty()
        && !(canSubmitHandleMetaChangeProcedure.get()
            && (needWriteConsensusOnConfigNodes.get() || needPushPipeMetaToDataNodes.get()))) {
      return;
    }

    configManager
        .getPipeManager()
        .getPipeRuntimeCoordinator()
        .getProcedureSubmitter()
        .submit(
            () -> {
              final AtomicReference<PipeTaskInfo> pipeTaskInfo =
                  configManager.getPipeManager().getPipeTaskCoordinator().tryLock();
              if (pipeTaskInfo == null) {
                PipeLogger.log(
                    LOGGER::warn,
                    ManagerMessages.FAILED_TO_ACQUIRE_LOCK_WHEN_PARSEHEARTBEAT_FROM_NODE_ID,
                    nodeId);
                return;
              }

              try {
                if (!pipeHeartbeat.isEmpty()) {
                  parseHeartbeatAndSaveMetaChangeLocally(pipeTaskInfo, nodeId, pipeHeartbeat);
                }

                if (canSubmitHandleMetaChangeProcedure.get()
                    && (needWriteConsensusOnConfigNodes.get()
                        || needPushPipeMetaToDataNodes.get())) {
                  if (configManager
                      .getProcedureManager()
                      .pipeHandleMetaChange(
                          needWriteConsensusOnConfigNodes.get(),
                          needPushPipeMetaToDataNodes.get())) {
                    needWriteConsensusOnConfigNodes.set(false);
                    needPushPipeMetaToDataNodes.set(false);
                  }
                }
              } finally {
                configManager.getPipeManager().getPipeTaskCoordinator().unlock();
              }
            });
  }

  private int getExpectedHeartbeatNodeCount() {
    final int expectedNodeCount =
        configManager.getNodeManager().getRegisteredDataNodeCount()
            + (PipeConfig.getInstance().isSeperatedPipeHeartbeatEnabled() ? 1 : 0);
    if (expectedNodeCount <= 0) {
      PipeLogger.log(
          LOGGER::warn,
          ManagerMessages.EXPECTED_PIPE_HEARTBEAT_NODE_COUNT_IS_FALLBACK_TO_1,
          expectedNodeCount);
      return 1;
    }
    return expectedNodeCount;
  }

  private void parseHeartbeatAndSaveMetaChangeLocally(
      final AtomicReference<PipeTaskInfo> pipeTaskInfo,
      final int nodeId,
      final PipeHeartbeat pipeHeartbeat) {
    for (final PipeMeta pipeMetaFromCoordinator : pipeTaskInfo.get().getPipeMetaList()) {
      if (PipeStatus.PRE_DELETE.equals(
          pipeMetaFromCoordinator.getRuntimeMeta().getStatus().get())) {
        continue;
      }

      final PipeStaticMeta staticMeta = pipeMetaFromCoordinator.getStaticMeta();
      final PipeMeta pipeMetaFromAgent = pipeHeartbeat.getPipeMeta(staticMeta);
      if (pipeMetaFromAgent == null) {
        continue;
      }

      final PipeTemporaryMetaInCoordinator temporaryMeta =
          (PipeTemporaryMetaInCoordinator) pipeMetaFromCoordinator.getTemporaryMeta();

      // Aggregate completed DataRegion ids reported by DataNodes. Only the DataNodes that own the
      // target region can report it, so the coordinator can compare the union against all required
      // DataRegion ids without trusting any DataNode's single per-pipe completion boolean.
      if (pipeHeartbeat.hasCompletedDataRegionReport(staticMeta)) {
        for (final Integer completedDataRegionId :
            pipeHeartbeat.getCompletedDataRegionIds(staticMeta)) {
          temporaryMeta.markDataRegionCompleted(completedDataRegionId);
        }
      }

      // Align with copyAndFilterOutNonWorkingDataRegionPipeTasks: CN's task table contains every
      // user-visible DataRegion, but DataNodes only create / complete tasks for regions that match
      // the source pattern. Waiting on unmatched regions would prevent snapshot pipes from
      // dropping.
      final Set<Integer> requiredDataRegionIds =
          collectRequiredDataRegionIds(pipeMetaFromCoordinator);

      // A history-only internal pipe is finite and may be removed when all required DataRegions
      // complete, or when CN determines that no DataRegion matched at creation time. An explicit
      // region-level report proves that a DataNode has received the pipe meta, preventing an empty
      // task map from completing the pipe before its initial push. Realtime and external-source
      // pipes must remain alive because they may receive work in the future.
      final boolean isFiniteInternalPipe =
          !staticMeta.isSourceExternal()
              && PipeTaskAgent.isHistoryOnlyPipe(staticMeta.getSourceParameters());
      final boolean hasReliableDataRegionReport =
          pipeHeartbeat.hasCompletedDataRegionReport(staticMeta);
      if (isFiniteInternalPipe
          && hasReliableDataRegionReport
          && temporaryMeta.getCompletedDataRegionIds().containsAll(requiredDataRegionIds)) {
        PipeLogger.log(
            LOGGER::info,
            ManagerMessages.ALL_DATANODES_REPORTED_HISTORICAL_PIPE_COMPLETED,
            staticMeta.getPipeName(),
            temporaryMeta.getGlobalRemainingEvents(),
            temporaryMeta.getGlobalRemainingTime(),
            staticMeta);
        pipeTaskInfo.get().removePipeMeta(staticMeta);
        PipeLogger.log(
            LOGGER::info,
            ManagerMessages.DETECTED_COMPLETION_OF_PIPE_STATIC_META_REMOVE_IT,
            staticMeta.getPipeName(),
            staticMeta);
        needWriteConsensusOnConfigNodes.set(true);
        needPushPipeMetaToDataNodes.set(true);
        continue;
      }

      // Record statistics
      temporaryMeta.setRemainingEvent(nodeId, pipeHeartbeat.getRemainingEventCount(staticMeta));
      temporaryMeta.setRemainingTime(nodeId, pipeHeartbeat.getRemainingTime(staticMeta));
      temporaryMeta.setDegraded(nodeId, pipeHeartbeat.getDegraded(staticMeta));
      temporaryMeta.setRecentFailures(nodeId, pipeHeartbeat.getRecentFailures(staticMeta));

      final Map<Integer, PipeTaskMeta> pipeTaskMetaMapFromCoordinator =
          pipeMetaFromCoordinator.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();
      final Map<Integer, PipeTaskMeta> pipeTaskMetaMapFromAgent =
          pipeMetaFromAgent.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();
      for (final Map.Entry<Integer, PipeTaskMeta> runtimeMetaFromCoordinator :
          pipeTaskMetaMapFromCoordinator.entrySet()) {
        if (runtimeMetaFromCoordinator.getValue().getLeaderNodeId() != nodeId) {
          continue;
        }

        final PipeTaskMeta runtimeMetaFromAgent =
            pipeTaskMetaMapFromAgent.get(runtimeMetaFromCoordinator.getKey());
        if (runtimeMetaFromAgent == null) {
          LOGGER.debug(
              ManagerMessages
                  .NO_CORRESPONDING_PIPE_IS_RUNNING_IN_THE_REPORTED_DATAREGION_RUNTIMEMETAFROMAGENT,
              runtimeMetaFromCoordinator);
          continue;
        }

        // Update progress index
        if (!(runtimeMetaFromCoordinator
                .getValue()
                .getProgressIndex()
                .isAfter(runtimeMetaFromAgent.getProgressIndex())
            || runtimeMetaFromCoordinator
                .getValue()
                .getProgressIndex()
                .equals(runtimeMetaFromAgent.getProgressIndex()))) {
          final ProgressIndex updatedProgressIndex =
              runtimeMetaFromCoordinator
                  .getValue()
                  .updateProgressIndex(runtimeMetaFromAgent.getProgressIndex());
          PipeConfigNodeResourceManager.log()
              .schedule(
                  PipeHeartbeatParser.class,
                  PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                  PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                  pipeHeartbeat.getPipeMetaSize())
              .ifPresent(
                  l ->
                      l.info(
                          ManagerMessages
                                  .LOG_UPDATED_PROGRESS_INDEX_PIPE_NAME_ARG_CONSENSUS_GROUP_ID_ARG_DF112F4F
                              + ManagerMessages
                                  .LOG_PROGRESS_INDEX_COORDINATOR_ARG_PROGRESS_INDEX_AGENT_ARG_UPDATED_PROGRESSINDEX_1A22ABC5,
                          pipeMetaFromCoordinator.getStaticMeta().getPipeName(),
                          runtimeMetaFromCoordinator.getKey(),
                          runtimeMetaFromCoordinator.getValue().getProgressIndex(),
                          runtimeMetaFromAgent.getProgressIndex(),
                          updatedProgressIndex));

          needWriteConsensusOnConfigNodes.set(true);
        }

        // Update runtime exception
        final PipeTaskMeta pipeTaskMetaFromCoordinator = runtimeMetaFromCoordinator.getValue();
        final PipeRuntimeMeta pipeRuntimeMeta = pipeMetaFromCoordinator.getRuntimeMeta();
        pipeTaskMetaFromCoordinator.clearExceptionMessages();
        for (final PipeRuntimeException exception : runtimeMetaFromAgent.getExceptionMessages()) {
          if (exception.getTimeStamp() <= pipeRuntimeMeta.getExceptionsClearTime()) {
            needPushPipeMetaToDataNodes.set(true);
            continue;
          }

          pipeTaskMetaFromCoordinator.trackExceptionMessage(exception);

          if (exception instanceof PipeRuntimeCriticalException) {
            final String pipeName = pipeMetaFromCoordinator.getStaticMeta().getPipeName();
            if (!pipeRuntimeMeta.getStatus().get().equals(PipeStatus.STOPPED)) {
              pipeRuntimeMeta.getStatus().set(PipeStatus.STOPPED);
              pipeRuntimeMeta.setIsStoppedByRuntimeException(true);

              needWriteConsensusOnConfigNodes.set(true);
              needPushPipeMetaToDataNodes.set(false);

              PipeLogger.log(
                  LOGGER::warn,
                  exception,
                  ManagerMessages.DETECT_PIPERUNTIMECRITICALEXCEPTION_FROM_AGENT_STOP_PIPE,
                  exception,
                  pipeName);
            }

            if (exception instanceof PipeRuntimeSinkCriticalException) {
              pipeTaskInfo
                  .get()
                  .getPipeMetaList()
                  .forEach(
                      pipeMeta -> {
                        final PipeStaticMeta affectedStaticMeta = pipeMeta.getStaticMeta();
                        if (!affectedStaticMeta
                                .getSinkParameters()
                                .equals(pipeMetaFromCoordinator.getStaticMeta().getSinkParameters())
                            || affectedStaticMeta.equals(pipeMetaFromCoordinator.getStaticMeta())) {
                          return;
                        }

                        final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
                        if (PipeStatus.PRE_DELETE.equals(runtimeMeta.getStatus().get())) {
                          return;
                        }
                        if (!runtimeMeta.getStatus().get().equals(PipeStatus.STOPPED)) {
                          // Record the connector exception for each pipe affected
                          Map<Integer, PipeRuntimeException> exceptionMap =
                              runtimeMeta.getNodeId2PipeRuntimeExceptionMap();
                          if (!exceptionMap.containsKey(nodeId)
                              || exceptionMap.get(nodeId).getTimeStamp()
                                  < exception.getTimeStamp()) {
                            exceptionMap.put(nodeId, exception);
                          }
                          runtimeMeta.getStatus().set(PipeStatus.STOPPED);
                          runtimeMeta.setIsStoppedByRuntimeException(true);

                          needWriteConsensusOnConfigNodes.set(true);
                          needPushPipeMetaToDataNodes.set(false);

                          PipeLogger.log(
                              LOGGER::warn,
                              exception,
                              ManagerMessages
                                  .DETECT_PIPERUNTIMESINKCRITICALEXCEPTION_FROM_AGENT_STOP_PIPE,
                              exception,
                              pipeName);
                        }
                      });
            }
          }
        }
      }
    }
  }

  /**
   * Collect DataRegion ids that this pipe must wait on before auto-drop. Schema / Config regions
   * are skipped; DataRegions that the source pattern will not listen to are skipped as well, using
   * the same {@link DataRegionListeningFilter} as task push-down.
   *
   * <p>If database / schema lookup fails, the region is kept (same conservative behavior as {@code
   * copyAndFilterOutNonWorkingDataRegionPipeTasks}).
   */
  private Set<Integer> collectRequiredDataRegionIds(final PipeMeta pipeMetaFromCoordinator) {
    final PipeStaticMeta staticMeta = pipeMetaFromCoordinator.getStaticMeta();
    final Set<Integer> requiredDataRegionIds = new HashSet<>();
    for (final Map.Entry<Integer, PipeTaskMeta> entry :
        pipeMetaFromCoordinator.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().entrySet()) {
      final TConsensusGroupId dataRegionId =
          new TConsensusGroupId(TConsensusGroupType.DataRegion, entry.getKey());
      if (!configManager.getPartitionManager().isRegionGroupExists(dataRegionId)) {
        continue;
      }
      if (shouldKeepDataRegionAsRequired(staticMeta, dataRegionId)) {
        requiredDataRegionIds.add(entry.getKey());
      }
    }
    return requiredDataRegionIds;
  }

  private boolean shouldKeepDataRegionAsRequired(
      final PipeStaticMeta staticMeta, final TConsensusGroupId dataRegionId) {
    if (staticMeta.isSourceExternal()) {
      return true;
    }

    final String database;
    try {
      database = configManager.getPartitionManager().getRegionDatabase(dataRegionId);
      if (database == null) {
        return true;
      }
    } catch (final Exception ignored) {
      return true;
    }

    final boolean isTableModel;
    try {
      final TDatabaseSchema schema =
          configManager.getClusterSchemaManager().getDatabaseSchemaByName(database);
      if (schema == null) {
        return true;
      }
      isTableModel = schema.isIsTableModel();
    } catch (final Exception ignored) {
      return true;
    }

    try {
      return DataRegionListeningFilter.shouldDatabaseBeListened(
          staticMeta.getSourceParameters(), isTableModel, database, staticMeta.getPipeType());
    } catch (final Exception ignored) {
      return true;
    }
  }
}
