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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;

import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PipeHeartbeatParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatParser.class);

  private final ConfigManager configManager;

  private long heartbeatCounter;
  private int registeredNodeNumber;

  private final AtomicBoolean needWriteConsensusOnConfigNodes;
  private final AtomicBoolean needPushPipeMetaToDataNodes;

  PipeHeartbeatParser(ConfigManager configManager) {
    this.configManager = configManager;

    heartbeatCounter = 0;
    registeredNodeNumber = 1;

    needWriteConsensusOnConfigNodes = new AtomicBoolean(false);
    needPushPipeMetaToDataNodes = new AtomicBoolean(false);
  }

  public synchronized void parseHeartbeat(
      int nodeId, @NotNull List<ByteBuffer> pipeMetaByteBufferListFromAgent) {
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

    if (pipeMetaByteBufferListFromAgent.isEmpty()
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
                if (!pipeMetaByteBufferListFromAgent.isEmpty()) {
                  parseHeartbeatAndSaveMetaChangeLocally(
                      pipeTaskInfo, nodeId, pipeMetaByteBufferListFromAgent);
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
      @NotNull final List<ByteBuffer> pipeMetaByteBufferListFromAgent) {
    final Map<PipeStaticMeta, PipeMeta> pipeMetaMapFromAgent = new HashMap<>();
    for (ByteBuffer byteBuffer : pipeMetaByteBufferListFromAgent) {
      final PipeMeta pipeMeta = PipeMeta.deserialize(byteBuffer);
      pipeMetaMapFromAgent.put(pipeMeta.getStaticMeta(), pipeMeta);
    }

    for (final PipeMeta pipeMetaFromCoordinator : pipeTaskInfo.get().getPipeMetaList()) {
      final PipeMeta pipeMetaFromAgent =
          pipeMetaMapFromAgent.get(pipeMetaFromCoordinator.getStaticMeta());
      if (pipeMetaFromAgent == null) {
        LOGGER.info(
            "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                + "pipeMetaFromAgent is null, pipeMetaFromCoordinator: {}",
            pipeMetaFromCoordinator);
        continue;
      }

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
          LOGGER.info(
              "Updating progress index for (pipe name: {}, consensus group id: {}) ... "
                  + "Progress index on coordinator: {}, progress index from agent: {}",
              pipeMetaFromCoordinator.getStaticMeta().getPipeName(),
              runtimeMetaFromCoordinator.getKey(),
              runtimeMetaFromCoordinator.getValue().getProgressIndex(),
              runtimeMetaFromAgent.getProgressIndex());
          LOGGER.info(
              "Progress index for (pipe name: {}, consensus group id: {}) is updated to {}",
              pipeMetaFromCoordinator.getStaticMeta().getPipeName(),
              runtimeMetaFromCoordinator.getKey(),
              runtimeMetaFromCoordinator
                  .getValue()
                  .updateProgressIndex(runtimeMetaFromAgent.getProgressIndex()));

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
