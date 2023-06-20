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

package org.apache.iotdb.confignode.manager.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeHeartbeatParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatParser.class);

  private final ConfigManager configManager;

  private long heartbeatCounter;
  private int registeredDataNodeNumber;

  private final AtomicBoolean needWriteConsensusOnConfigNodes;
  private final AtomicBoolean needPushPipeMetaToDataNodes;

  PipeHeartbeatParser(ConfigManager configManager) {
    this.configManager = configManager;

    heartbeatCounter = 0;
    registeredDataNodeNumber = 1;

    needWriteConsensusOnConfigNodes = new AtomicBoolean(false);
    needPushPipeMetaToDataNodes = new AtomicBoolean(false);
  }

  public synchronized void parseHeartbeat(
      int dataNodeId, @NotNull List<ByteBuffer> pipeMetaByteBufferListFromDataNode) {
    final long heartbeatCount = ++heartbeatCounter;

    final AtomicBoolean canSubmitHandleMetaChangeProcedure = new AtomicBoolean(false);
    // registeredDataNodeNumber can not be 0 when the method is called
    if (heartbeatCount % registeredDataNodeNumber == 0) {
      canSubmitHandleMetaChangeProcedure.set(true);

      // registeredDataNodeNumber may be changed, update it here when we can submit procedure
      registeredDataNodeNumber = configManager.getNodeManager().getRegisteredDataNodeCount();
      if (registeredDataNodeNumber <= 0) {
        LOGGER.warn(
            "registeredDataNodeNumber is {} when parseHeartbeat from data node (id={}).",
            registeredDataNodeNumber,
            dataNodeId);
        // registeredDataNodeNumber can not be set to 0 in this class, otherwise may cause
        // DivideByZeroException
        registeredDataNodeNumber = 1;
      }
    }

    if (pipeMetaByteBufferListFromDataNode.isEmpty()
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
              if (!pipeMetaByteBufferListFromDataNode.isEmpty()) {
                parseHeartbeatAndSaveMetaChangeLocally(
                    dataNodeId, pipeMetaByteBufferListFromDataNode);
              }

              if (canSubmitHandleMetaChangeProcedure.get()
                  && (needWriteConsensusOnConfigNodes.get() || needPushPipeMetaToDataNodes.get())) {
                configManager
                    .getProcedureManager()
                    .pipeHandleMetaChange(
                        needWriteConsensusOnConfigNodes.get(), needPushPipeMetaToDataNodes.get());

                // reset flags after procedure is submitted
                needWriteConsensusOnConfigNodes.set(false);
                needPushPipeMetaToDataNodes.set(false);
              }
            });
  }

  private void parseHeartbeatAndSaveMetaChangeLocally(
      int dataNodeId, @NotNull List<ByteBuffer> pipeMetaByteBufferListFromDataNode) {
    final Map<PipeStaticMeta, PipeMeta> pipeMetaMapFromDataNode = new HashMap<>();
    for (ByteBuffer byteBuffer : pipeMetaByteBufferListFromDataNode) {
      final PipeMeta pipeMeta = PipeMeta.deserialize(byteBuffer);
      pipeMetaMapFromDataNode.put(pipeMeta.getStaticMeta(), pipeMeta);
    }

    for (final PipeMeta pipeMetaOnConfigNode :
        configManager
            .getPipeManager()
            .getPipeTaskCoordinator()
            .getPipeTaskInfo()
            .getPipeMetaList()) {
      final PipeMeta pipeMetaFromDataNode =
          pipeMetaMapFromDataNode.get(pipeMetaOnConfigNode.getStaticMeta());
      if (pipeMetaFromDataNode == null) {
        LOGGER.info(
            "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                + "pipeMetaFromDataNode is null, pipeMetaOnConfigNode: {}",
            pipeMetaOnConfigNode);
        continue;
      }

      final Map<TConsensusGroupId, PipeTaskMeta> pipeTaskMetaMapOnConfigNode =
          pipeMetaOnConfigNode.getRuntimeMeta().getConsensusGroupIdToTaskMetaMap();
      final Map<TConsensusGroupId, PipeTaskMeta> pipeTaskMetaMapFromDataNode =
          pipeMetaFromDataNode.getRuntimeMeta().getConsensusGroupIdToTaskMetaMap();
      for (final Map.Entry<TConsensusGroupId, PipeTaskMeta> runtimeMetaOnConfigNode :
          pipeTaskMetaMapOnConfigNode.entrySet()) {
        if (runtimeMetaOnConfigNode.getValue().getLeaderDataNodeId() != dataNodeId) {
          continue;
        }

        final PipeTaskMeta runtimeMetaFromDataNode =
            pipeTaskMetaMapFromDataNode.get(runtimeMetaOnConfigNode.getKey());
        if (runtimeMetaFromDataNode == null) {
          LOGGER.warn(
              "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                  + "runtimeMetaFromDataNode is null, runtimeMetaOnConfigNode: {}",
              runtimeMetaOnConfigNode);
          continue;
        }

        // update progress index
        if (!runtimeMetaOnConfigNode
            .getValue()
            .getProgressIndex()
            .isAfter(runtimeMetaFromDataNode.getProgressIndex())) {
          LOGGER.info(
              "Updating progress index for (pipe name: {}, consensus group id: {}) ... Progress index on config node: {}, progress index from data node: {}",
              pipeMetaOnConfigNode.getStaticMeta().getPipeName(),
              runtimeMetaOnConfigNode.getKey(),
              runtimeMetaOnConfigNode.getValue().getProgressIndex(),
              runtimeMetaFromDataNode.getProgressIndex());
          LOGGER.info(
              "Progress index for (pipe name: {}, consensus group id: {}) is updated to {}",
              pipeMetaOnConfigNode.getStaticMeta().getPipeName(),
              runtimeMetaOnConfigNode.getKey(),
              runtimeMetaOnConfigNode
                  .getValue()
                  .updateProgressIndex(runtimeMetaFromDataNode.getProgressIndex()));

          needWriteConsensusOnConfigNodes.set(true);
        }

        // update runtime exception
        final PipeTaskMeta pipeTaskMetaOnConfigNode = runtimeMetaOnConfigNode.getValue();
        pipeTaskMetaOnConfigNode.clearExceptionMessages();
        for (final PipeRuntimeException exception :
            runtimeMetaFromDataNode.getExceptionMessages()) {

          pipeTaskMetaOnConfigNode.trackExceptionMessage(exception);

          if (exception instanceof PipeRuntimeCriticalException) {
            final String pipeName = pipeMetaOnConfigNode.getStaticMeta().getPipeName();
            if (!pipeMetaOnConfigNode
                .getRuntimeMeta()
                .getStatus()
                .get()
                .equals(PipeStatus.STOPPED)) {
              pipeMetaOnConfigNode.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);

              needWriteConsensusOnConfigNodes.set(true);
              needPushPipeMetaToDataNodes.set(true);

              LOGGER.warn(
                  "Detect PipeRuntimeCriticalException {} from DataNode, stop pipe {}.",
                  exception,
                  pipeName);
            }

            if (exception instanceof PipeRuntimeConnectorCriticalException) {
              ((PipeTableResp)
                      configManager
                          .getPipeManager()
                          .getPipeTaskCoordinator()
                          .getPipeTaskInfo()
                          .showPipes())
                  .filter(true, pipeName).getAllPipeMeta().stream()
                      .map(pipeMeta -> pipeMeta.getRuntimeMeta().getStatus())
                      .filter(status -> !status.get().equals(PipeStatus.STOPPED))
                      .forEach(
                          status -> {
                            status.set(PipeStatus.STOPPED);

                            needWriteConsensusOnConfigNodes.set(true);
                            needPushPipeMetaToDataNodes.set(true);

                            LOGGER.warn(
                                String.format(
                                    "Detect PipeRuntimeConnectorCriticalException %s from DataNode, stop pipe %s.",
                                    exception, pipeName));
                          });
            }
          }
        }
      }
    }
  }
}
