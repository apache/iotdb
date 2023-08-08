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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.task.PipeBuilder;
import org.apache.iotdb.db.pipe.task.PipeTask;
import org.apache.iotdb.db.pipe.task.PipeTaskBuilder;
import org.apache.iotdb.db.pipe.task.PipeTaskManager;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * State transition diagram of a pipe task:
 *
 * <p><code>
 * |----------------|                     |---------| --> start pipe --> |---------|                   |---------|
 * | initial status | --> create pipe --> | STOPPED |                    | RUNNING | --> drop pipe --> | DROPPED |
 * |----------------|                     |---------| <-- stop  pipe <-- |---------|                   |---------|
 *                                             |                                                            |
 *                                             | ----------------------> drop pipe -----------------------> |
 * </code>
 *
 * <p>Other transitions are not allowed, will be ignored when received in the pipe task agent.
 */
public class PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskAgent.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final String MESSAGE_UNKNOWN_PIPE_STATUS = "Unknown pipe status %s for pipe %s";
  private static final String MESSAGE_UNEXPECTED_PIPE_STATUS = "Unexpected pipe status %s: ";

  private final PipeMetaKeeper pipeMetaKeeper;
  private final PipeTaskManager pipeTaskManager;

  public PipeTaskAgent() {
    pipeMetaKeeper = new PipeMetaKeeper();
    pipeTaskManager = new PipeTaskManager();
  }

  ////////////////////////// PipeMeta Lock Control //////////////////////////

  private void acquireReadLock() {
    pipeMetaKeeper.acquireReadLock();
  }

  public boolean tryReadLockWithTimeOut(long timeOutInSeconds) {
    try {
      return pipeMetaKeeper.tryReadLock(timeOutInSeconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interruption during requiring pipeMetaKeeper lock.", e);
      return false;
    }
  }

  private void releaseReadLock() {
    pipeMetaKeeper.releaseReadLock();
  }

  private void acquireWriteLock() {
    pipeMetaKeeper.acquireWriteLock();
  }

  private void releaseWriteLock() {
    pipeMetaKeeper.releaseWriteLock();
  }

  ////////////////////////// Pipe Task Management Entry //////////////////////////

  public synchronized List<TPushPipeMetaRespExceptionMessage> handlePipeMetaChanges(
      List<PipeMeta> pipeMetaListFromConfigNode) {
    acquireWriteLock();
    try {
      return handlePipeMetaChangesInternal(pipeMetaListFromConfigNode);
    } finally {
      releaseWriteLock();
    }
  }

  private List<TPushPipeMetaRespExceptionMessage> handlePipeMetaChangesInternal(
      List<PipeMeta> pipeMetaListFromConfigNode) {
    // Do nothing if data node is removing or removed
    if (PipeAgent.runtime().isShutdown()) {
      return Collections.emptyList();
    }

    final List<TPushPipeMetaRespExceptionMessage> exceptionMessages = new ArrayList<>();

    // Iterate through pipe meta list from config node, check if pipe meta exists on data node
    // or has changed
    for (final PipeMeta metaFromConfigNode : pipeMetaListFromConfigNode) {
      final String pipeName = metaFromConfigNode.getStaticMeta().getPipeName();

      try {
        final PipeMeta metaOnDataNode = pipeMetaKeeper.getPipeMeta(pipeName);

        // If pipe meta does not exist on data node, create a new pipe
        if (metaOnDataNode == null) {
          if (createPipe(metaFromConfigNode)) {
            // If the status recorded in config node is RUNNING, start the pipe
            startPipe(pipeName, metaFromConfigNode.getStaticMeta().getCreationTime());
          }
          // If the status recorded in config node is STOPPED or DROPPED, do nothing
          continue;
        }

        // If pipe meta exists on data node, check if it has changed
        final PipeStaticMeta staticMetaOnDataNode = metaOnDataNode.getStaticMeta();
        final PipeStaticMeta staticMetaFromConfigNode = metaFromConfigNode.getStaticMeta();

        // First check if pipe static meta has changed, if so, drop the pipe and create a new one
        if (!staticMetaOnDataNode.equals(staticMetaFromConfigNode)) {
          dropPipe(pipeName);
          if (createPipe(metaFromConfigNode)) {
            startPipe(pipeName, metaFromConfigNode.getStaticMeta().getCreationTime());
          }
          // If the status is STOPPED or DROPPED, do nothing
          continue;
        }

        // Then check if pipe runtime meta has changed, if so, update the pipe
        final PipeRuntimeMeta runtimeMetaOnDataNode = metaOnDataNode.getRuntimeMeta();
        final PipeRuntimeMeta runtimeMetaFromConfigNode = metaFromConfigNode.getRuntimeMeta();
        handlePipeRuntimeMetaChanges(
            staticMetaFromConfigNode, runtimeMetaFromConfigNode, runtimeMetaOnDataNode);
      } catch (Exception e) {
        final String errorMessage =
            String.format(
                "Failed to handle pipe meta changes for %s, because %s", pipeName, e.getMessage());
        LOGGER.warn("Failed to handle pipe meta changes for {}", pipeName, e);
        exceptionMessages.add(
            new TPushPipeMetaRespExceptionMessage(
                pipeName, errorMessage, System.currentTimeMillis()));
      }
    }

    // Check if there are pipes on data node that do not exist on config node, if so, drop them
    final Set<String> pipeNamesFromConfigNode =
        pipeMetaListFromConfigNode.stream()
            .map(meta -> meta.getStaticMeta().getPipeName())
            .collect(Collectors.toSet());
    for (final PipeMeta metaOnDataNode : pipeMetaKeeper.getPipeMetaList()) {
      final String pipeName = metaOnDataNode.getStaticMeta().getPipeName();

      try {
        if (!pipeNamesFromConfigNode.contains(pipeName)) {
          dropPipe(metaOnDataNode.getStaticMeta().getPipeName());
        }
      } catch (Exception e) {
        // Do not record the error messages for the pipes don't exist on ConfigNode.
        LOGGER.warn("Failed to handle pipe meta changes for {}", pipeName, e);
      }
    }

    return exceptionMessages;
  }

  private void handlePipeRuntimeMetaChanges(
      @NotNull PipeStaticMeta pipeStaticMeta,
      @NotNull PipeRuntimeMeta runtimeMetaFromConfigNode,
      @NotNull PipeRuntimeMeta runtimeMetaOnDataNode) {
    // 1. Handle data region group leader changed first
    final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapFromConfigNode =
        runtimeMetaFromConfigNode.getConsensusGroupId2TaskMetaMap();
    final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapOnDataNode =
        runtimeMetaOnDataNode.getConsensusGroupId2TaskMetaMap();

    // 1.1 Iterate over all consensus group ids in config node's pipe runtime meta, decide if we
    // need to drop and create a new task for each consensus group id
    for (final Map.Entry<TConsensusGroupId, PipeTaskMeta> entryFromConfigNode :
        consensusGroupIdToTaskMetaMapFromConfigNode.entrySet()) {
      final TConsensusGroupId consensusGroupIdFromConfigNode = entryFromConfigNode.getKey();

      final PipeTaskMeta taskMetaFromConfigNode = entryFromConfigNode.getValue();
      final PipeTaskMeta taskMetaOnDataNode =
          consensusGroupIdToTaskMetaMapOnDataNode.get(consensusGroupIdFromConfigNode);

      // If task meta does not exist on data node, create a new task
      if (taskMetaOnDataNode == null) {
        createPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta, taskMetaFromConfigNode);
        // We keep the new created task's status consistent with the status recorded in data node's
        // pipe runtime meta. please note that the status recorded in data node's pipe runtime meta
        // is not reliable, but we will have a check later to make sure the status is correct.
        if (runtimeMetaOnDataNode.getStatus().get() == PipeStatus.RUNNING) {
          startPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta);
        }
        continue;
      }

      // If task meta exists on data node, check if it has changed
      final int dataNodeIdFromConfigNode = taskMetaFromConfigNode.getLeaderDataNodeId();
      final int dataNodeIdOnDataNode = taskMetaOnDataNode.getLeaderDataNodeId();

      if (dataNodeIdFromConfigNode != dataNodeIdOnDataNode) {
        dropPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta);
        createPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta, taskMetaFromConfigNode);
        // We keep the new created task's status consistent with the status recorded in data node's
        // pipe runtime meta. please note that the status recorded in data node's pipe runtime meta
        // is not reliable, but we will have a check later to make sure the status is correct.
        if (runtimeMetaOnDataNode.getStatus().get() == PipeStatus.RUNNING) {
          startPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta);
        }
      }
    }

    // 1.2 Iterate over all consensus group ids on data node's pipe runtime meta, decide if we need
    // to drop any task. we do not need to create any new task here because we have already done
    // that in 1.1.
    for (final Map.Entry<TConsensusGroupId, PipeTaskMeta> entryOnDataNode :
        consensusGroupIdToTaskMetaMapOnDataNode.entrySet()) {
      final TConsensusGroupId consensusGroupIdOnDataNode = entryOnDataNode.getKey();
      final PipeTaskMeta taskMetaFromConfigNode =
          consensusGroupIdToTaskMetaMapFromConfigNode.get(consensusGroupIdOnDataNode);
      if (taskMetaFromConfigNode == null) {
        dropPipeTask(consensusGroupIdOnDataNode, pipeStaticMeta);
      }
    }

    // 2. Handle pipe runtime meta status changes
    final PipeStatus statusFromConfigNode = runtimeMetaFromConfigNode.getStatus().get();
    final PipeStatus statusOnDataNode = runtimeMetaOnDataNode.getStatus().get();
    if (statusFromConfigNode == statusOnDataNode) {
      return;
    }

    switch (statusFromConfigNode) {
      case RUNNING:
        if (Objects.requireNonNull(statusOnDataNode) == PipeStatus.STOPPED) {
          startPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        } else {
          throw new IllegalStateException(
              String.format(
                  MESSAGE_UNKNOWN_PIPE_STATUS, statusOnDataNode, pipeStaticMeta.getPipeName()));
        }
        break;
      case STOPPED:
        if (Objects.requireNonNull(statusOnDataNode) == PipeStatus.RUNNING) {
          stopPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        } else {
          throw new IllegalStateException(
              String.format(
                  MESSAGE_UNKNOWN_PIPE_STATUS, statusOnDataNode, pipeStaticMeta.getPipeName()));
        }
        break;
      case DROPPED:
        // This should not happen, but we still handle it here
        dropPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        break;
      default:
        throw new IllegalStateException(
            String.format(
                MESSAGE_UNKNOWN_PIPE_STATUS, statusFromConfigNode, pipeStaticMeta.getPipeName()));
    }
  }

  public synchronized void dropAllPipeTasks() {
    acquireWriteLock();
    try {
      dropAllPipeTasksInternal();
    } finally {
      releaseWriteLock();
    }
  }

  private void dropAllPipeTasksInternal() {
    for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
      try {
        dropPipe(
            pipeMeta.getStaticMeta().getPipeName(), pipeMeta.getStaticMeta().getCreationTime());
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to drop pipe {} with creation time {}",
            pipeMeta.getStaticMeta().getPipeName(),
            pipeMeta.getStaticMeta().getCreationTime(),
            e);
      }
    }
  }

  public synchronized void stopAllPipesWithCriticalException() {
    acquireWriteLock();
    try {
      stopAllPipesWithCriticalExceptionInternal();
    } finally {
      releaseWriteLock();
    }
  }

  private void stopAllPipesWithCriticalExceptionInternal() {
    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();
              final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();

              if (runtimeMeta.getStatus().get() == PipeStatus.RUNNING) {
                runtimeMeta
                    .getConsensusGroupId2TaskMetaMap()
                    .values()
                    .forEach(
                        pipeTaskMeta -> {
                          for (final PipeRuntimeException e : pipeTaskMeta.getExceptionMessages()) {
                            if (e instanceof PipeRuntimeCriticalException) {
                              stopPipe(staticMeta.getPipeName(), staticMeta.getCreationTime());
                              return;
                            }
                          }
                        });
              }
            });
  }

  ////////////////////////// Manage by Pipe Name //////////////////////////

  /**
   * Create a new pipe. If the pipe already exists, do nothing and return false. Otherwise, create
   * the pipe and return true.
   *
   * @param pipeMetaFromConfigNode pipe meta from config node
   * @return true if the pipe is created successfully and should be started, false if the pipe
   *     already exists or is created but should not be started
   * @throws IllegalStateException if the status is illegal
   */
  private boolean createPipe(PipeMeta pipeMetaFromConfigNode) {
    final String pipeName = pipeMetaFromConfigNode.getStaticMeta().getPipeName();
    final long creationTime = pipeMetaFromConfigNode.getStaticMeta().getCreationTime();

    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    if (existedPipeMeta != null) {
      if (existedPipeMeta.getStaticMeta().getCreationTime() == creationTime) {
        final PipeStatus status = existedPipeMeta.getRuntimeMeta().getStatus().get();
        switch (status) {
          case STOPPED:
          case RUNNING:
            if (LOGGER.isInfoEnabled()) {
              LOGGER.info(
                  "Pipe {} (creation time = {}) has already been created. "
                      + "Current status = {}. Skip creating.",
                  pipeName,
                  creationTime,
                  status.name());
            }
            return false;
          case DROPPED:
            if (LOGGER.isInfoEnabled()) {
              LOGGER.info(
                  "Pipe {} (creation time = {}) has already been dropped, "
                      + "but the pipe task meta has not been cleaned up. "
                      + "Current status = {}. Try dropping the pipe and recreating it.",
                  pipeName,
                  creationTime,
                  status.name());
            }
            // Break to drop the pipe and recreate it
            break;
          default:
            throw new IllegalStateException(
                MESSAGE_UNEXPECTED_PIPE_STATUS
                    + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        }
      }

      // Drop the pipe if
      // 1. The pipe with the same name but with different creation time has been created before
      // 2. The pipe with the same name and the same creation time has been dropped before, but the
      //  pipe task meta has not been cleaned up
      dropPipe(pipeName, existedPipeMeta.getStaticMeta().getCreationTime());
    }

    // Create pipe tasks and trigger create() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        new PipeBuilder(pipeMetaFromConfigNode).build();
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.create();
    }
    pipeTaskManager.addPipeTasks(pipeMetaFromConfigNode.getStaticMeta(), pipeTasks);

    // No matter the pipe status from config node is RUNNING or STOPPED, we always set the status
    // of pipe meta to STOPPED when it is created. The STOPPED status should always be the initial
    // status of a pipe, which makes the status transition logic simpler.
    final AtomicReference<PipeStatus> pipeStatusFromConfigNode =
        pipeMetaFromConfigNode.getRuntimeMeta().getStatus();
    final boolean needToStartPipe = pipeStatusFromConfigNode.get() == PipeStatus.RUNNING;
    pipeStatusFromConfigNode.set(PipeStatus.STOPPED);

    pipeMetaKeeper.addPipeMeta(pipeName, pipeMetaFromConfigNode);

    // If the pipe status from config node is RUNNING, we will start the pipe later.
    return needToStartPipe;
  }

  private void dropPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip dropping.",
          pipeName,
          creationTime);
      return;
    }
    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match "
              + "the creation time ({}) in dropPipe request. Skip dropping.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return;
    }

    // Mark pipe meta as dropped first. This will help us detect if the pipe meta has been dropped
    // but the pipe task meta has not been cleaned up (in case of failure when executing
    // dropPipeTaskByConsensusGroup).
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

    // Drop pipe tasks and trigger drop() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.removePipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip dropping.",
          pipeName,
          creationTime);
      return;
    }
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.drop();
    }

    // Remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  private void dropPipe(String pipeName) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} has already been dropped or has not been created. Skip dropping.", pipeName);
      return;
    }

    // Mark pipe meta as dropped first. This will help us detect if the pipe meta has been dropped
    // but the pipe task meta has not been cleaned up (in case of failure when executing
    // dropPipeTaskByConsensusGroup).
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

    // Drop pipe tasks and trigger drop() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.removePipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} has already been dropped or has not been created. Skip dropping.", pipeName);
      return;
    }
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.drop();
    }

    // Remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  private void startPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip starting.",
          pipeName,
          creationTime);
      return;
    }
    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match "
              + "the creation time ({}) in startPipe request. Skip starting.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return;
    }

    final PipeStatus status = existedPipeMeta.getRuntimeMeta().getStatus().get();
    switch (status) {
      case STOPPED:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has been created. Current status = {}. Starting.",
              pipeName,
              creationTime,
              status.name());
        }
        break;
      case RUNNING:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has already been started. Current status = {}. "
                  + "Skip starting.",
              pipeName,
              creationTime,
              status.name());
        }
        return;
      case DROPPED:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has already been dropped. Current status = {}. "
                  + "Skip starting.",
              pipeName,
              creationTime,
              status.name());
        }
        return;
      default:
        throw new IllegalStateException(
            MESSAGE_UNEXPECTED_PIPE_STATUS
                + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
    }

    // Trigger start() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.getPipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip starting.",
          pipeName,
          creationTime);
      return;
    }
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.start();
    }

    // Set pipe meta status to RUNNING
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    // Clear exception messages if started successfully
    existedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .values()
        .forEach(PipeTaskMeta::clearExceptionMessages);
  }

  private void stopPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip stopping.",
          pipeName,
          creationTime);
      return;
    }
    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match "
              + "the creation time ({}) in stopPipe request. Skip stopping.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return;
    }

    final PipeStatus status = existedPipeMeta.getRuntimeMeta().getStatus().get();
    switch (status) {
      case STOPPED:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has already been stopped. Current status = {}. "
                  + "Skip stopping.",
              pipeName,
              creationTime,
              status.name());
        }
        return;
      case RUNNING:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has been started. Current status = {}. Stopping.",
              pipeName,
              creationTime,
              status.name());
        }
        break;
      case DROPPED:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has already been dropped. Current status = {}. "
                  + "Skip stopping.",
              pipeName,
              creationTime,
              status.name());
        }
        return;
      default:
        throw new IllegalStateException(MESSAGE_UNEXPECTED_PIPE_STATUS + status.name());
    }

    // Trigger stop() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.getPipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip stopping.",
          pipeName,
          creationTime);
      return;
    }
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.stop();
    }

    // Set pipe meta status to STOPPED
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
  }

  ///////////////////////// Manage by dataRegionGroupId /////////////////////////

  private void createPipeTask(
      TConsensusGroupId consensusGroupId,
      PipeStaticMeta pipeStaticMeta,
      PipeTaskMeta pipeTaskMeta) {
    if (pipeTaskMeta.getLeaderDataNodeId() == CONFIG.getDataNodeId()) {
      final PipeTask pipeTask =
          new PipeTaskBuilder(pipeStaticMeta, consensusGroupId, pipeTaskMeta).build();
      pipeTask.create();
      pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
    }
    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .put(consensusGroupId, pipeTaskMeta);
  }

  private void dropPipeTask(TConsensusGroupId dataRegionGroupId, PipeStaticMeta pipeStaticMeta) {
    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .remove(dataRegionGroupId);
    final PipeTask pipeTask = pipeTaskManager.removePipeTask(pipeStaticMeta, dataRegionGroupId);
    if (pipeTask != null) {
      pipeTask.drop();
    }
  }

  private void startPipeTask(TConsensusGroupId dataRegionGroupId, PipeStaticMeta pipeStaticMeta) {
    final PipeTask pipeTask = pipeTaskManager.getPipeTask(pipeStaticMeta, dataRegionGroupId);
    if (pipeTask != null) {
      pipeTask.start();
    }
  }

  ///////////////////////// Heartbeat /////////////////////////

  public synchronized void collectPipeMetaList(THeartbeatReq req, THeartbeatResp resp)
      throws TException {
    // If the pipe heartbeat is separated from the cluster heartbeat, then the lock doesn't
    // need to be acquired
    if (!req.isNeedPipeMetaList()) {
      return;
    }
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

  private void collectPipeMetaListInternal(THeartbeatResp resp) throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        LOGGER.info("Reporting pipe meta: {}", pipeMeta);
      }
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
  }

  public synchronized void collectPipeMetaList(TPipeHeartbeatReq req, TPipeHeartbeatResp resp)
      throws TException {
    acquireReadLock();
    try {
      collectPipeMetaListInternal(req, resp);
    } finally {
      releaseReadLock();
    }
  }

  private void collectPipeMetaListInternal(TPipeHeartbeatReq req, TPipeHeartbeatResp resp)
      throws TException {
    // Do nothing if data node is removing or removed, or request does not need pipe meta list
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }
    LOGGER.info("Received pipe heartbeat request {} from config node.", req.heartbeatId);

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
        LOGGER.info("Reporting pipe meta: {}", pipeMeta);
      }
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
  }
}
