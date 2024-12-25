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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.resource.PipeConfigNodeResourceManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    registeredNodeNumber = 1;

    needWriteConsensusOnConfigNodes = new AtomicBoolean(false);
    needPushPipeMetaToDataNodes = new AtomicBoolean(false);
  }

  synchronized void parseHeartbeat(final int nodeId, final PipeHeartbeat pipeHeartbeat) {
    final long heartbeatCount = ++heartbeatCounter;

    final AtomicBoolean canSubmitHandleMetaChangeProcedure = new AtomicBoolean(false);
    // registeredNodeNumber can not be 0 when the method is called
    if (heartbeatCount % registeredNodeNumber == 0) {
      canSubmitHandleMetaChangeProcedure.set(true);

      // registeredNodeNumber may be changed, update it here when we can submit procedure
      registeredNodeNumber = configManager.getNodeManager().getRegisteredNodeCount();
      if (registeredNodeNumber <= 0) {
        LOGGER.warn(
            "registeredNodeNumber is {} when parseHeartbeat from node (id={}).",
            registeredNodeNumber,
            nodeId);
        // registeredNodeNumber can not be set to 0 in this class, otherwise may cause
        // DivideByZeroException
        registeredNodeNumber = 1;
      }
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
                LOGGER.warn(
                    "Failed to acquire lock when parseHeartbeat from node (id={}).", nodeId);
                return;
              }

              try {
                if (!pipeHeartbeat.isEmpty()) {
                  parseHeartbeatAndSaveMetaChangeLocally(pipeTaskInfo, nodeId, pipeHeartbeat);
                }

                if (canSubmitHandleMetaChangeProcedure.get()
                    && (needWriteConsensusOnConfigNodes.get()
                        || needPushPipeMetaToDataNodes.get())) {
                  configManager
                      .getProcedureManager()
                      .pipeHandleMetaChange(
                          needWriteConsensusOnConfigNodes.get(), needPushPipeMetaToDataNodes.get());

                  // Reset flags after procedure is submitted
                  needWriteConsensusOnConfigNodes.set(false);
                  needPushPipeMetaToDataNodes.set(false);
                }
              } finally {
                configManager.getPipeManager().getPipeTaskCoordinator().unlock();
              }
            });
  }

  private void parseHeartbeatAndSaveMetaChangeLocally(
      final AtomicReference<PipeTaskInfo> pipeTaskInfo,
      final int nodeId,
      final PipeHeartbeat pipeHeartbeat) {
    for (final PipeMeta pipeMetaFromCoordinator : pipeTaskInfo.get().getPipeMetaList()) {
      final PipeStaticMeta staticMeta = pipeMetaFromCoordinator.getStaticMeta();
      final PipeMeta pipeMetaFromAgent = pipeHeartbeat.getPipeMeta(staticMeta);
      if (pipeMetaFromAgent == null) {
        LOGGER.info(
            "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                + "pipeMetaFromAgent is null, pipeMetaFromCoordinator: {}",
            pipeMetaFromCoordinator);
        continue;
      }

      final PipeTemporaryMetaInCoordinator temporaryMeta =
          (PipeTemporaryMetaInCoordinator) pipeMetaFromCoordinator.getTemporaryMeta();

      // Remove completed pipes
      final Boolean isPipeCompletedFromAgent = pipeHeartbeat.isCompleted(staticMeta);
      if (Boolean.TRUE.equals(isPipeCompletedFromAgent)) {

        temporaryMeta.markDataNodeCompleted(nodeId);

        final Set<Integer> uncompletedDataNodeIds =
            configManager.getNodeManager().getRegisteredDataNodeLocations().keySet();
        uncompletedDataNodeIds.removeAll(temporaryMeta.getCompletedDataNodeIds());
        if (uncompletedDataNodeIds.isEmpty()) {
          pipeTaskInfo.get().removePipeMeta(staticMeta.getPipeName());
          LOGGER.info(
              "Detected completion of pipe {}, static meta: {}, remove it.",
              staticMeta.getPipeName(),
              staticMeta);
          needWriteConsensusOnConfigNodes.set(true);
          needPushPipeMetaToDataNodes.set(true);
          continue;
        }
      }

      // Record statistics
      temporaryMeta.setRemainingEvent(nodeId, pipeHeartbeat.getRemainingEventCount(staticMeta));
      temporaryMeta.setRemainingTime(nodeId, pipeHeartbeat.getRemainingTime(staticMeta));

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
          LOGGER.warn(
              "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                  + "runtimeMetaFromAgent is null, runtimeMetaFromCoordinator: {}",
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
                          "Updated progress index for (pipe name: {}, consensus group id: {}) ... "
                              + "Progress index on coordinator: {}, progress index from agent: {}, updated progressIndex: {}",
                          pipeMetaFromCoordinator.getStaticMeta().getPipeName(),
                          runtimeMetaFromCoordinator.getKey(),
                          runtimeMetaFromCoordinator.getValue().getProgressIndex(),
                          runtimeMetaFromAgent.getProgressIndex(),
                          updatedProgressIndex));

          needWriteConsensusOnConfigNodes.set(true);
        }

        // Update runtime exception
        final PipeTaskMeta pipeTaskMetaFromCoordinator = runtimeMetaFromCoordinator.getValue();
        pipeTaskMetaFromCoordinator.clearExceptionMessages();
        for (final PipeRuntimeException exception : runtimeMetaFromAgent.getExceptionMessages()) {

          // Do not judge the exception's clear time to avoid the restart process
          // being ended after the failure of some pipe

          pipeTaskMetaFromCoordinator.trackExceptionMessage(exception);

          if (exception instanceof PipeRuntimeCriticalException) {
            final String pipeName = pipeMetaFromCoordinator.getStaticMeta().getPipeName();
            if (!pipeMetaFromCoordinator
                .getRuntimeMeta()
                .getStatus()
                .get()
                .equals(PipeStatus.STOPPED)) {
              PipeRuntimeMeta runtimeMeta = pipeMetaFromCoordinator.getRuntimeMeta();
              runtimeMeta.getStatus().set(PipeStatus.STOPPED);
              runtimeMeta.setIsStoppedByRuntimeException(true);

              needWriteConsensusOnConfigNodes.set(true);
              needPushPipeMetaToDataNodes.set(false);

              LOGGER.warn(
                  "Detect PipeRuntimeCriticalException {} from agent, stop pipe {}.",
                  exception,
                  pipeName);
            }

            if (exception instanceof PipeRuntimeConnectorCriticalException) {
              ((PipeTableResp) pipeTaskInfo.get().showPipes())
                  .filter(true, pipeName).getAllPipeMeta().stream()
                      .filter(pipeMeta -> !pipeMeta.getStaticMeta().getPipeName().equals(pipeName))
                      .map(PipeMeta::getRuntimeMeta)
                      .filter(
                          runtimeMeta -> !runtimeMeta.getStatus().get().equals(PipeStatus.STOPPED))
                      .forEach(
                          runtimeMeta -> {
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

                            LOGGER.warn(
                                String.format(
                                    "Detect PipeRuntimeConnectorCriticalException %s "
                                        + "from agent, stop pipe %s.",
                                    exception, pipeName));
                          });
            }
          }
        }
      }
    }
  }
}
