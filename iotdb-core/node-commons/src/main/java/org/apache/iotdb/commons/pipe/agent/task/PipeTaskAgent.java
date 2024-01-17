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

package org.apache.iotdb.commons.pipe.agent.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.PipeTaskManager;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * |----------------|                     |---------| --> stop  pipe --> |---------|                   |---------|
 * | initial status | --> create pipe --> | RUNNING |                    | STOPPED | --> drop pipe --> | DROPPED |
 * |----------------|                     |---------| <-- start pipe <-- |---------|                   |---------|
 *                                             |                                                            |
 *                                             | ----------------------> drop pipe -----------------------> |
 * </code>
 *
 * <p>Other transitions are not allowed, will be ignored when received in the pipe task agent.
 */
public abstract class PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskAgent.class);

  protected static final String MESSAGE_UNKNOWN_PIPE_STATUS = "Unknown pipe status %s for pipe %s";
  protected static final String MESSAGE_UNEXPECTED_PIPE_STATUS = "Unexpected pipe status %s: ";

  protected final PipeMetaKeeper pipeMetaKeeper;
  protected final PipeTaskManager pipeTaskManager;

  public PipeTaskAgent() {
    pipeMetaKeeper = new PipeMetaKeeper();
    pipeTaskManager = new PipeTaskManager();
  }

  ////////////////////////// PipeMeta Lock Control //////////////////////////

  protected void acquireReadLock() {
    pipeMetaKeeper.acquireReadLock();
  }

  protected boolean tryReadLockWithTimeOut(long timeOutInSeconds) {
    try {
      return pipeMetaKeeper.tryReadLock(timeOutInSeconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interruption during requiring pipeMetaKeeper read lock.", e);
      return false;
    }
  }

  protected void releaseReadLock() {
    pipeMetaKeeper.releaseReadLock();
  }

  protected void acquireWriteLock() {
    pipeMetaKeeper.acquireWriteLock();
  }

  protected boolean tryWriteLockWithTimeOut(long timeOutInSeconds) {
    try {
      return pipeMetaKeeper.tryWriteLock(timeOutInSeconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interruption during requiring pipeMetaKeeper write lock.", e);
      return false;
    }
  }

  protected void releaseWriteLock() {
    pipeMetaKeeper.releaseWriteLock();
  }

  ////////////////////////// Pipe Task Management Entry //////////////////////////

  public TPushPipeMetaRespExceptionMessage handleSinglePipeMetaChanges(
      PipeMeta pipeMetaFromCoordinator) {
    acquireWriteLock();
    try {
      return handleSinglePipeMetaChangesInternal(pipeMetaFromCoordinator);
    } finally {
      releaseWriteLock();
    }
  }

  private TPushPipeMetaRespExceptionMessage handleSinglePipeMetaChangesInternal(
      PipeMeta pipeMetaFromCoordinator) {
    // Do nothing if node is removing or removed
    if (isShutdown()) {
      return null;
    }

    try {
      executeSinglePipeMetaChanges(pipeMetaFromCoordinator);
      return null;
    } catch (Exception e) {
      final String pipeName = pipeMetaFromCoordinator.getStaticMeta().getPipeName();
      final String errorMessage =
          String.format(
              "Failed to handle single pipe meta changes for %s, because %s",
              pipeName, e.getMessage());
      LOGGER.warn("Failed to handle single pipe meta changes for {}", pipeName, e);
      return new TPushPipeMetaRespExceptionMessage(
          pipeName, errorMessage, System.currentTimeMillis());
    }
  }

  protected abstract boolean isShutdown();

  private void executeSinglePipeMetaChanges(final PipeMeta metaFromCoordinator) {
    final String pipeName = metaFromCoordinator.getStaticMeta().getPipeName();
    final PipeMeta metaInAgent = pipeMetaKeeper.getPipeMeta(pipeName);

    // If pipe meta does not exist on local agent, create a new pipe
    if (metaInAgent == null) {
      if (createPipe(metaFromCoordinator)) {
        // If the status recorded in coordinator is RUNNING, start the pipe
        startPipe(pipeName, metaFromCoordinator.getStaticMeta().getCreationTime());
      }
      // If the status recorded in coordinator is STOPPED or DROPPED, do nothing
      return;
    }

    // If pipe meta exists on local agent, check if it has changed
    final PipeStaticMeta staticMetaInAgent = metaInAgent.getStaticMeta();
    final PipeStaticMeta staticMetaFromCoordinator = metaFromCoordinator.getStaticMeta();

    // First check if pipe static meta has changed, if so, drop the pipe and create a new one
    if (!staticMetaInAgent.equals(staticMetaFromCoordinator)) {
      dropPipe(pipeName);
      if (createPipe(metaFromCoordinator)) {
        startPipe(pipeName, metaFromCoordinator.getStaticMeta().getCreationTime());
      }
      // If the status is STOPPED or DROPPED, do nothing
      return;
    }

    // Then check if pipe runtime meta has changed, if so, update the pipe
    final PipeRuntimeMeta runtimeMetaInAgent = metaInAgent.getRuntimeMeta();
    final PipeRuntimeMeta runtimeMetaFromCoordinator = metaFromCoordinator.getRuntimeMeta();
    executeSinglePipeRuntimeMetaChanges(
        staticMetaFromCoordinator, runtimeMetaFromCoordinator, runtimeMetaInAgent);
  }

  private void executeSinglePipeRuntimeMetaChanges(
      /* @NotNull */ PipeStaticMeta pipeStaticMeta,
      /* @NotNull */ PipeRuntimeMeta runtimeMetaFromCoordinator,
      /* @NotNull */ PipeRuntimeMeta runtimeMetaInAgent) {
    // 1. Handle region group leader changed first
    final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapFromCoordinator =
        runtimeMetaFromCoordinator.getConsensusGroupId2TaskMetaMap();
    final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapInAgent =
        runtimeMetaInAgent.getConsensusGroupId2TaskMetaMap();

    // 1.1 Iterate over all consensus group ids in coordinator's pipe runtime meta, decide if we
    // need to drop and create a new task for each consensus group id
    for (final Map.Entry<TConsensusGroupId, PipeTaskMeta> entryFromCoordinator :
        consensusGroupIdToTaskMetaMapFromCoordinator.entrySet()) {
      final TConsensusGroupId consensusGroupIdFromCoordinator = entryFromCoordinator.getKey();

      final PipeTaskMeta taskMetaFromCoordinator = entryFromCoordinator.getValue();
      final PipeTaskMeta taskMetaInAgent =
          consensusGroupIdToTaskMetaMapInAgent.get(consensusGroupIdFromCoordinator);

      // If task meta does not exist on local agent, create a new task
      if (taskMetaInAgent == null) {
        createPipeTask(consensusGroupIdFromCoordinator, pipeStaticMeta, taskMetaFromCoordinator);
        // We keep the new created task's status consistent with the status recorded in local
        // agent's
        // pipe runtime meta. please note that the status recorded in local agent's pipe runtime
        // meta
        // is not reliable, but we will have a check later to make sure the status is correct.
        if (runtimeMetaInAgent.getStatus().get() == PipeStatus.RUNNING) {
          startPipeTask(consensusGroupIdFromCoordinator, pipeStaticMeta);
        }
        continue;
      }

      // If task meta exists on local agent, check if it has changed
      final int nodeIdFromCoordinator = taskMetaFromCoordinator.getLeaderDataNodeId();
      final int nodeIdInAgent = taskMetaInAgent.getLeaderDataNodeId();

      if (nodeIdFromCoordinator != nodeIdInAgent) {
        dropPipeTask(consensusGroupIdFromCoordinator, pipeStaticMeta);
        createPipeTask(consensusGroupIdFromCoordinator, pipeStaticMeta, taskMetaFromCoordinator);
        // We keep the new created task's status consistent with the status recorded in local
        // agent's
        // pipe runtime meta. please note that the status recorded in local agent's pipe runtime
        // meta
        // is not reliable, but we will have a check later to make sure the status is correct.
        if (runtimeMetaInAgent.getStatus().get() == PipeStatus.RUNNING) {
          startPipeTask(consensusGroupIdFromCoordinator, pipeStaticMeta);
        }
      }
    }

    // 1.2 Iterate over all consensus group ids on local agent's pipe runtime meta, decide if we
    // need
    // to drop any task. we do not need to create any new task here because we have already done
    // that in 1.1.
    for (final Map.Entry<TConsensusGroupId, PipeTaskMeta> entryInAgent :
        consensusGroupIdToTaskMetaMapInAgent.entrySet()) {
      final TConsensusGroupId consensusGroupIdInAgent = entryInAgent.getKey();
      final PipeTaskMeta taskMetaFromCoordinator =
          consensusGroupIdToTaskMetaMapFromCoordinator.get(consensusGroupIdInAgent);
      if (taskMetaFromCoordinator == null) {
        dropPipeTask(consensusGroupIdInAgent, pipeStaticMeta);
      }
    }

    // 2. Handle pipe runtime meta status changes
    final PipeStatus statusFromCoordinator = runtimeMetaFromCoordinator.getStatus().get();
    final PipeStatus statusInAgent = runtimeMetaInAgent.getStatus().get();
    if (statusFromCoordinator == statusInAgent) {
      return;
    }

    switch (statusFromCoordinator) {
      case RUNNING:
        if (Objects.requireNonNull(statusInAgent) == PipeStatus.STOPPED) {
          startPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        } else {
          throw new IllegalStateException(
              String.format(
                  MESSAGE_UNKNOWN_PIPE_STATUS, statusInAgent, pipeStaticMeta.getPipeName()));
        }
        break;
      case STOPPED:
        if (Objects.requireNonNull(statusInAgent) == PipeStatus.RUNNING) {
          stopPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        } else {
          throw new IllegalStateException(
              String.format(
                  MESSAGE_UNKNOWN_PIPE_STATUS, statusInAgent, pipeStaticMeta.getPipeName()));
        }
        break;
      case DROPPED:
        // This should not happen, but we still handle it here
        dropPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        break;
      default:
        throw new IllegalStateException(
            String.format(
                MESSAGE_UNKNOWN_PIPE_STATUS, statusFromCoordinator, pipeStaticMeta.getPipeName()));
    }
  }

  public TPushPipeMetaRespExceptionMessage handleDropPipe(String pipeName) {
    acquireWriteLock();
    try {
      return handleDropPipeInternal(pipeName);
    } finally {
      releaseWriteLock();
    }
  }

  private TPushPipeMetaRespExceptionMessage handleDropPipeInternal(String pipeName) {
    // Do nothing if node is removing or removed
    if (isShutdown()) {
      return null;
    }

    try {
      dropPipe(pipeName);
      return null;
    } catch (Exception e) {
      final String errorMessage =
          String.format("Failed to drop pipe %s, because %s", pipeName, e.getMessage());
      LOGGER.warn("Failed to drop pipe {}", pipeName, e);
      return new TPushPipeMetaRespExceptionMessage(
          pipeName, errorMessage, System.currentTimeMillis());
    }
  }

  public List<TPushPipeMetaRespExceptionMessage> handlePipeMetaChanges(
      List<PipeMeta> pipeMetaListFromCoordinator) {
    acquireWriteLock();
    try {
      return handlePipeMetaChangesInternal(pipeMetaListFromCoordinator);
    } finally {
      releaseWriteLock();
    }
  }

  private List<TPushPipeMetaRespExceptionMessage> handlePipeMetaChangesInternal(
      List<PipeMeta> pipeMetaListFromCoordinator) {
    // Do nothing if node is removing or removed
    if (isShutdown()) {
      return Collections.emptyList();
    }

    final List<TPushPipeMetaRespExceptionMessage> exceptionMessages = new ArrayList<>();

    // Iterate through pipe meta list from coordinator, check if pipe meta exists on local agent
    // or has changed
    for (final PipeMeta metaFromCoordinator : pipeMetaListFromCoordinator) {
      try {
        executeSinglePipeMetaChanges(metaFromCoordinator);
      } catch (Exception e) {
        final String pipeName = metaFromCoordinator.getStaticMeta().getPipeName();
        final String errorMessage =
            String.format(
                "Failed to handle pipe meta changes for %s, because %s", pipeName, e.getMessage());
        LOGGER.warn("Failed to handle pipe meta changes for {}", pipeName, e);
        exceptionMessages.add(
            new TPushPipeMetaRespExceptionMessage(
                pipeName, errorMessage, System.currentTimeMillis()));
      }
    }

    // Check if there are pipes on local agent that do not exist on coordinator, if so, drop them
    final Set<String> pipeNamesFromCoordinator =
        pipeMetaListFromCoordinator.stream()
            .map(meta -> meta.getStaticMeta().getPipeName())
            .collect(Collectors.toSet());
    for (final PipeMeta metaInAgent : pipeMetaKeeper.getPipeMetaList()) {
      final String pipeName = metaInAgent.getStaticMeta().getPipeName();

      try {
        if (!pipeNamesFromCoordinator.contains(pipeName)) {
          dropPipe(metaInAgent.getStaticMeta().getPipeName());
        }
      } catch (Exception e) {
        // Do not record the error messages for the pipes don't exist on coordinator.
        LOGGER.warn("Failed to handle pipe meta changes for {}", pipeName, e);
      }
    }

    return exceptionMessages;
  }

  public void dropAllPipeTasks() {
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

  ////////////////////////// Manage by Pipe Name //////////////////////////

  /**
   * Create a new pipe. If the pipe already exists, do nothing and return false. Otherwise, create
   * the pipe and return true.
   *
   * @param pipeMetaFromCoordinator pipe meta from coordinator
   * @return true if the pipe is created successfully and should be started, false if the pipe
   *     already exists or is created but should not be started
   * @throws IllegalStateException if the status is illegal
   */
  private boolean createPipe(PipeMeta pipeMetaFromCoordinator) {
    final String pipeName = pipeMetaFromCoordinator.getStaticMeta().getPipeName();
    final long creationTime = pipeMetaFromCoordinator.getStaticMeta().getCreationTime();

    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    if (existedPipeMeta != null) {
      if (!checkBeforeCreatePipe(existedPipeMeta, pipeName, creationTime)) {
        return false;
      }

      // Drop the pipe if
      // 1. The pipe with the same name but with different creation time has been created before
      // 2. The pipe with the same name and the same creation time has been dropped before, but the
      //  pipe task meta has not been cleaned up
      dropPipe(pipeName, existedPipeMeta.getStaticMeta().getCreationTime());
    }

    // Create pipe tasks
    final Map<TConsensusGroupId, PipeTask> pipeTasks = buildPipeTasks(pipeMetaFromCoordinator);

    // Trigger create() method for each pipe task by parallel stream
    final long startTime = System.currentTimeMillis();
    pipeTasks.values().parallelStream().forEach(PipeTask::create);
    LOGGER.info(
        "Create all pipe tasks on Pipe {} successfully within {} ms",
        pipeName,
        System.currentTimeMillis() - startTime);

    pipeTaskManager.addPipeTasks(pipeMetaFromCoordinator.getStaticMeta(), pipeTasks);

    // No matter the pipe status from coordinator is RUNNING or STOPPED, we always set the status
    // of pipe meta to STOPPED when it is created. The STOPPED status should always be the initial
    // status of a pipe, which makes the status transition logic simpler.
    final AtomicReference<PipeStatus> pipeStatusFromCoordinator =
        pipeMetaFromCoordinator.getRuntimeMeta().getStatus();
    final boolean needToStartPipe = pipeStatusFromCoordinator.get() == PipeStatus.RUNNING;
    pipeStatusFromCoordinator.set(PipeStatus.STOPPED);

    pipeMetaKeeper.addPipeMeta(pipeName, pipeMetaFromCoordinator);

    // If the pipe status from coordinator is RUNNING, we will start the pipe later.
    return needToStartPipe;
  }

  protected abstract Map<TConsensusGroupId, PipeTask> buildPipeTasks(
      PipeMeta pipeMetaFromCoordinator);

  private void dropPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (!checkBeforeDropPipe(existedPipeMeta, pipeName, creationTime)) {
      return;
    }

    // Mark pipe meta as dropped first. This will help us detect if the pipe meta has been dropped
    // but the pipe task meta has not been cleaned up (in case of failure when executing
    // dropPipeTaskByConsensusGroup).
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

    // Drop pipe tasks
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

    // Trigger drop() method for each pipe task by parallel stream
    final long startTime = System.currentTimeMillis();
    pipeTasks.values().parallelStream().forEach(PipeTask::drop);
    LOGGER.info(
        "Drop all pipe tasks on Pipe {} successfully within {} ms",
        pipeName,
        System.currentTimeMillis() - startTime);

    // Remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  private void dropPipe(String pipeName) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (!checkBeforeDropPipe(existedPipeMeta, pipeName)) {
      return;
    }

    // Mark pipe meta as dropped first. This will help us detect if the pipe meta has been dropped
    // but the pipe task meta has not been cleaned up (in case of failure when executing
    // dropPipeTaskByConsensusGroup).
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

    // Drop pipe tasks
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.removePipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} has already been dropped or has not been created. Skip dropping.", pipeName);
      return;
    }

    // Trigger drop() method for each pipe task by parallel stream
    final long startTime = System.currentTimeMillis();
    pipeTasks.values().parallelStream().forEach(PipeTask::drop);
    LOGGER.info(
        "Drop all pipe tasks on Pipe {} successfully within {} ms",
        pipeName,
        System.currentTimeMillis() - startTime);

    // Remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  private void startPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (!checkBeforeStartPipe(existedPipeMeta, pipeName, creationTime)) {
      return;
    }

    // Get pipe tasks
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

    // Trigger start() method for each pipe task by parallel stream
    final long startTime = System.currentTimeMillis();
    pipeTasks.values().parallelStream().forEach(PipeTask::start);
    LOGGER.info(
        "Start all pipe tasks on Pipe {} successfully within {} ms",
        pipeName,
        System.currentTimeMillis() - startTime);

    // Set pipe meta status to RUNNING
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    // Clear exception messages if started successfully
    existedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .values()
        .forEach(PipeTaskMeta::clearExceptionMessages);
  }

  protected void stopPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (!checkBeforeStopPipe(existedPipeMeta, pipeName, creationTime)) {
      return;
    }

    // Get pipe tasks
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

    // Trigger stop() method for each pipe task by parallel stream
    final long startTime = System.currentTimeMillis();
    pipeTasks.values().parallelStream().forEach(PipeTask::stop);
    LOGGER.info(
        "Stop all pipe tasks on Pipe {} successfully within {} ms",
        pipeName,
        System.currentTimeMillis() - startTime);

    // Set pipe meta status to STOPPED
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
  }

  ////////////////////////// Checker //////////////////////////

  /**
   * Check if we need to create pipe tasks.
   *
   * @return {@code true} if need to create pipe tasks, {@code false} if no need to create.
   * @throws IllegalStateException if current pipe status is illegal.
   */
  protected boolean checkBeforeCreatePipe(
      PipeMeta existedPipeMeta, String pipeName, long creationTime) throws IllegalStateException {
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
          // Need to drop the pipe and recreate it
          return true;
        default:
          throw new IllegalStateException(
              MESSAGE_UNEXPECTED_PIPE_STATUS
                  + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
      }
    }

    return true;
  }

  /**
   * Check if we need to actually start the pipe tasks.
   *
   * @return {@code true} if need to start the pipe tasks, {@code false} if no need to start.
   * @throws IllegalStateException if current pipe status is illegal.
   */
  protected boolean checkBeforeStartPipe(
      PipeMeta existedPipeMeta, String pipeName, long creationTime) throws IllegalStateException {
    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip starting.",
          pipeName,
          creationTime);
      return false;
    }

    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match "
              + "the creation time ({}) in startPipe request. Skip starting.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return false;
    }

    final PipeStatus status = existedPipeMeta.getRuntimeMeta().getStatus().get();
    switch (status) {
      case STOPPED:
        // Only need to start the pipe tasks when current status is STOPPED.
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has been created. Current status = {}. Starting.",
              pipeName,
              creationTime,
              status.name());
        }
        return true;
      case RUNNING:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has already been started. Current status = {}. "
                  + "Skip starting.",
              pipeName,
              creationTime,
              status.name());
        }
        return false;
      case DROPPED:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has already been dropped. Current status = {}. "
                  + "Skip starting.",
              pipeName,
              creationTime,
              status.name());
        }
        return false;
      default:
        throw new IllegalStateException(
            MESSAGE_UNEXPECTED_PIPE_STATUS
                + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
    }
  }

  /**
   * Check if we need to actually stop the pipe tasks.
   *
   * @return {@code true} if need to stop the pipe tasks, {@code false} if no need to stop.
   * @throws IllegalStateException if current pipe status is illegal.
   */
  protected boolean checkBeforeStopPipe(
      PipeMeta existedPipeMeta, String pipeName, long creationTime) throws IllegalStateException {
    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip stopping.",
          pipeName,
          creationTime);
      return false;
    }

    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match "
              + "the creation time ({}) in stopPipe request. Skip stopping.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return false;
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
        return false;
      case RUNNING:
        // Only need to start the pipe tasks when current status is RUNNING.
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has been started. Current status = {}. Stopping.",
              pipeName,
              creationTime,
              status.name());
        }
        return true;
      case DROPPED:
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Pipe {} (creation time = {}) has already been dropped. Current status = {}. "
                  + "Skip stopping.",
              pipeName,
              creationTime,
              status.name());
        }
        return false;
      default:
        throw new IllegalStateException(MESSAGE_UNEXPECTED_PIPE_STATUS + status.name());
    }
  }

  /**
   * Check if we need to drop pipe tasks.
   *
   * @return {@code true} if need to drop pipe tasks, {@code false} if no need to drop.
   * @throws IllegalStateException if current pipe status is illegal.
   */
  protected boolean checkBeforeDropPipe(
      PipeMeta existedPipeMeta, String pipeName, long creationTime) throws IllegalStateException {
    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. "
              + "Skip dropping.",
          pipeName,
          creationTime);
      return false;
    }

    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match "
              + "the creation time ({}) in dropPipe request. Skip dropping.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return false;
    }

    return true;
  }

  /**
   * Check if we need to drop pipe tasks.
   *
   * @return {@code true} if need to drop pipe tasks, {@code false} if no need to drop.
   * @throws IllegalStateException if current pipe status is illegal.
   */
  protected boolean checkBeforeDropPipe(PipeMeta existedPipeMeta, String pipeName)
      throws IllegalStateException {
    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} has already been dropped or has not been created. Skip dropping.", pipeName);
      return false;
    }

    return true;
  }

  ///////////////////////// Manage by consensusGroupId /////////////////////////

  protected abstract void createPipeTask(
      TConsensusGroupId consensusGroupId, PipeStaticMeta pipeStaticMeta, PipeTaskMeta pipeTaskMeta);

  private void dropPipeTask(TConsensusGroupId consensusGroupId, PipeStaticMeta pipeStaticMeta) {
    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .remove(consensusGroupId);
    final PipeTask pipeTask = pipeTaskManager.removePipeTask(pipeStaticMeta, consensusGroupId);
    if (pipeTask != null) {
      pipeTask.drop();
    }
  }

  private void startPipeTask(TConsensusGroupId consensusGroupId, PipeStaticMeta pipeStaticMeta) {
    final PipeTask pipeTask = pipeTaskManager.getPipeTask(pipeStaticMeta, consensusGroupId);
    if (pipeTask != null) {
      pipeTask.start();
    }
  }
}
