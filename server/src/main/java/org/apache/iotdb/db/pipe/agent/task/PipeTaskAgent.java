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
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.task.PipeBuilder;
import org.apache.iotdb.db.pipe.task.PipeTask;
import org.apache.iotdb.db.pipe.task.PipeTaskBuilder;
import org.apache.iotdb.db.pipe.task.PipeTaskManager;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

  private final PipeMetaKeeper pipeMetaKeeper;
  private final PipeTaskManager pipeTaskManager;

  public PipeTaskAgent() {
    pipeMetaKeeper = new PipeMetaKeeper();
    pipeTaskManager = new PipeTaskManager();
  }

  ////////////////////////// Pipe Task Management Entry //////////////////////////

  // TODO: handle progress index
  public synchronized void handlePipeMetaChanges(List<PipeMeta> pipeMetaListFromConfigNode) {
    // do nothing if data node is removing or removed
    if (PipeAgent.runtime().isShutdown()) {
      return;
    }

    // iterate through pipe meta list from config node, check if pipe meta exists on data node
    // or has changed
    for (final PipeMeta metaFromConfigNode : pipeMetaListFromConfigNode) {
      final String pipeName = metaFromConfigNode.getStaticMeta().getPipeName();

      final PipeMeta metaOnDataNode = pipeMetaKeeper.getPipeMeta(pipeName);

      // if pipe meta does not exist on data node, create a new pipe
      if (metaOnDataNode == null) {
        createPipe(metaFromConfigNode);
        if (metaFromConfigNode.getRuntimeMeta().getStatus().get() == PipeStatus.RUNNING) {
          startPipe(pipeName, metaFromConfigNode.getStaticMeta().getCreationTime());
        }
        // if the status is STOPPED or DROPPED, do nothing
        continue;
      }

      // if pipe meta exists on data node, check if it has changed
      final PipeStaticMeta staticMetaOnDataNode = metaOnDataNode.getStaticMeta();
      final PipeStaticMeta staticMetaFromConfigNode = metaFromConfigNode.getStaticMeta();

      // first check if pipe static meta has changed, if so, drop the pipe and create a new one
      if (!staticMetaOnDataNode.equals(staticMetaFromConfigNode)) {
        dropPipe(pipeName);
        createPipe(metaFromConfigNode);
        if (metaFromConfigNode.getRuntimeMeta().getStatus().get() == PipeStatus.RUNNING) {
          startPipe(pipeName, metaFromConfigNode.getStaticMeta().getCreationTime());
        }
        // if the status is STOPPED or DROPPED, do nothing
        continue;
      }

      // then check if pipe runtime meta has changed, if so, update the pipe
      final PipeRuntimeMeta runtimeMetaOnDataNode = metaOnDataNode.getRuntimeMeta();
      final PipeRuntimeMeta runtimeMetaFromConfigNode = metaFromConfigNode.getRuntimeMeta();
      handlePipeRuntimeMetaChanges(
          staticMetaFromConfigNode, runtimeMetaFromConfigNode, runtimeMetaOnDataNode);
    }

    // check if there are pipes on data node that do not exist on config node, if so, drop them
    final Set<String> pipeNamesFromConfigNode =
        pipeMetaListFromConfigNode.stream()
            .map(meta -> meta.getStaticMeta().getPipeName())
            .collect(Collectors.toSet());
    for (final PipeMeta metaOnDataNode : pipeMetaKeeper.getPipeMetaList()) {
      if (!pipeNamesFromConfigNode.contains(metaOnDataNode.getStaticMeta().getPipeName())) {
        dropPipe(metaOnDataNode.getStaticMeta().getPipeName());
      }
    }
  }

  private void handlePipeRuntimeMetaChanges(
      @NotNull PipeStaticMeta pipeStaticMeta,
      @NotNull PipeRuntimeMeta runtimeMetaFromConfigNode,
      @NotNull PipeRuntimeMeta runtimeMetaOnDataNode) {
    // 1. handle data region group leader changed first
    final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapFromConfigNode =
        runtimeMetaFromConfigNode.getConsensusGroupIdToTaskMetaMap();
    final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMapOnDataNode =
        runtimeMetaOnDataNode.getConsensusGroupIdToTaskMetaMap();

    // 1.1 iterate over all consensus group ids in config node's pipe runtime meta, decide if we
    // need to drop and create a new task for each consensus group id
    for (final Map.Entry<TConsensusGroupId, PipeTaskMeta> entryFromConfigNode :
        consensusGroupIdToTaskMetaMapFromConfigNode.entrySet()) {
      final TConsensusGroupId consensusGroupIdFromConfigNode = entryFromConfigNode.getKey();

      final PipeTaskMeta taskMetaFromConfigNode = entryFromConfigNode.getValue();
      final PipeTaskMeta taskMetaOnDataNode =
          consensusGroupIdToTaskMetaMapOnDataNode.get(consensusGroupIdFromConfigNode);

      // if task meta does not exist on data node, create a new task
      if (taskMetaOnDataNode == null) {
        createPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta, taskMetaFromConfigNode);
        // we keep the new created task's status consistent with the status recorded in data node's
        // pipe runtime meta. please note that the status recorded in data node's pipe runtime meta
        // is not reliable, but we will have a check later to make sure the status is correct.
        if (runtimeMetaOnDataNode.getStatus().get() == PipeStatus.RUNNING) {
          startPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta);
        }
        continue;
      }

      // if task meta exists on data node, check if it has changed
      final int regionLeaderFromConfigNode = taskMetaFromConfigNode.getRegionLeader();
      final int regionLeaderOnDataNode = taskMetaOnDataNode.getRegionLeader();

      if (regionLeaderFromConfigNode != regionLeaderOnDataNode) {
        dropPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta);
        createPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta, taskMetaFromConfigNode);
        // we keep the new created task's status consistent with the status recorded in data node's
        // pipe runtime meta. please note that the status recorded in data node's pipe runtime meta
        // is not reliable, but we will have a check later to make sure the status is correct.
        if (runtimeMetaOnDataNode.getStatus().get() == PipeStatus.RUNNING) {
          startPipeTask(consensusGroupIdFromConfigNode, pipeStaticMeta);
        }
      }
    }

    // 1.2 iterate over all consensus group ids on data node's pipe runtime meta, decide if we need
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

    // 2. handle pipe runtime meta status changes
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
                  "Unknown pipe status %s for pipe %s",
                  statusOnDataNode, pipeStaticMeta.getPipeName()));
        }
        break;
      case STOPPED:
        if (Objects.requireNonNull(statusOnDataNode) == PipeStatus.RUNNING) {
          stopPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        } else {
          throw new IllegalStateException(
              String.format(
                  "Unknown pipe status %s for pipe %s",
                  statusOnDataNode, pipeStaticMeta.getPipeName()));
        }
        break;
      case DROPPED:
        // this should not happen, but we still handle it here
        dropPipe(pipeStaticMeta.getPipeName(), pipeStaticMeta.getCreationTime());
        break;
      default:
        throw new IllegalStateException(
            String.format(
                "Unknown pipe status %s for pipe %s",
                statusFromConfigNode, pipeStaticMeta.getPipeName()));
    }
  }

  public synchronized void dropAllPipeTasks() {
    for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
      dropPipe(pipeMeta.getStaticMeta().getPipeName(), pipeMeta.getStaticMeta().getCreationTime());
    }
  }

  ////////////////////////// Manage by Pipe Name //////////////////////////

  private void createPipe(PipeMeta pipeMeta) {
    final String pipeName = pipeMeta.getStaticMeta().getPipeName();
    final long creationTime = pipeMeta.getStaticMeta().getCreationTime();

    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    if (existedPipeMeta != null) {
      if (existedPipeMeta.getStaticMeta().getCreationTime() == creationTime) {
        switch (existedPipeMeta.getRuntimeMeta().getStatus().get()) {
          case STOPPED:
          case RUNNING:
            LOGGER.info(
                "Pipe {} (creation time = {}) has already been created. Current status = {}. Skip creating.",
                pipeName,
                creationTime,
                existedPipeMeta.getRuntimeMeta().getStatus().get().name());
            return;
          case DROPPED:
            LOGGER.info(
                "Pipe {} (creation time = {}) has already been dropped, but the pipe task meta has not been cleaned up. "
                    + "Current status = {}. Try dropping the pipe and recreating it.",
                pipeName,
                creationTime,
                existedPipeMeta.getRuntimeMeta().getStatus().get().name());
            // break to drop the pipe and recreate it
            break;
          default:
            throw new IllegalStateException(
                "Unexpected status: " + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        }
      }

      // drop the pipe if
      // 1. the pipe with the same name but with different creation time has been created before
      // 2. the pipe with the same name and the same creation time has been dropped before, but the
      //  pipe task meta has not been cleaned up
      dropPipe(pipeName, existedPipeMeta.getStaticMeta().getCreationTime());
    }

    // create pipe tasks and trigger create() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks = new PipeBuilder(pipeMeta).build();
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.create();
    }
    pipeTaskManager.addPipeTasks(pipeMeta.getStaticMeta(), pipeTasks);

    // add pipe meta to pipe meta keeper
    // note that we do not need to set the status of pipe meta here, because the status of pipe meta
    // is already set to STOPPED when it is created
    pipeMetaKeeper.addPipeMeta(pipeName, pipeMeta);
  }

  private void dropPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. Skip dropping.",
          pipeName,
          creationTime);
      return;
    }
    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match the creation time ({}) in dropPipe request. Skip dropping.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return;
    }

    // mark pipe meta as dropped first. this will help us detect if the pipe meta has been dropped
    // but the pipe task meta has not been cleaned up (in case of failure when executing
    // dropPipeTaskByConsensusGroup).
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

    // drop pipe tasks and trigger drop() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.removePipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. Skip dropping.",
          pipeName,
          creationTime);
      return;
    }
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.drop();
    }

    // remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  private void dropPipe(String pipeName) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} has already been dropped or has not been created. Skip dropping.", pipeName);
      return;
    }

    // mark pipe meta as dropped first. this will help us detect if the pipe meta has been dropped
    // but the pipe task meta has not been cleaned up (in case of failure when executing
    // dropPipeTaskByConsensusGroup).
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

    // drop pipe tasks and trigger drop() method for each pipe task
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

    // remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  private void startPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. Skip starting.",
          pipeName,
          creationTime);
      return;
    }
    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match the creation time ({}) in startPipe request. Skip starting.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return;
    }

    switch (existedPipeMeta.getRuntimeMeta().getStatus().get()) {
      case STOPPED:
        LOGGER.info(
            "Pipe {} (creation time = {}) has been created. Current status = {}. Starting.",
            pipeName,
            creationTime,
            existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        break;
      case RUNNING:
        LOGGER.info(
            "Pipe {} (creation time = {}) has already been started. Current status = {}. Skip starting.",
            pipeName,
            creationTime,
            existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        return;
      case DROPPED:
        LOGGER.info(
            "Pipe {} (creation time = {}) has already been dropped. Current status = {}. Skip starting.",
            pipeName,
            creationTime,
            existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        return;
      default:
        throw new IllegalStateException(
            "Unexpected status: " + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
    }

    // trigger start() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.getPipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. Skip starting.",
          pipeName,
          creationTime);
      return;
    }
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.start();
    }

    // set pipe meta status to RUNNING
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    // clear exception messages if started successfully
    existedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .values()
        .forEach(PipeTaskMeta::clearExceptionMessages);
  }

  private void stopPipe(String pipeName, long creationTime) {
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

    if (existedPipeMeta == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. Skip stopping.",
          pipeName,
          creationTime);
      return;
    }
    if (existedPipeMeta.getStaticMeta().getCreationTime() != creationTime) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has been created but does not match the creation time ({}) in stopPipe request. Skip stopping.",
          pipeName,
          existedPipeMeta.getStaticMeta().getCreationTime(),
          creationTime);
      return;
    }

    switch (existedPipeMeta.getRuntimeMeta().getStatus().get()) {
      case STOPPED:
        LOGGER.info(
            "Pipe {} (creation time = {}) has already been stopped. Current status = {}. Skip stopping.",
            pipeName,
            creationTime,
            existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        return;
      case RUNNING:
        LOGGER.info(
            "Pipe {} (creation time = {}) has been started. Current status = {}. Stopping.",
            pipeName,
            creationTime,
            existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        break;
      case DROPPED:
        LOGGER.info(
            "Pipe {} (creation time = {}) has already been dropped. Current status = {}. Skip stopping.",
            pipeName,
            creationTime,
            existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        return;
      default:
        throw new IllegalStateException(
            "Unexpected status: " + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
    }

    // trigger stop() method for each pipe task
    final Map<TConsensusGroupId, PipeTask> pipeTasks =
        pipeTaskManager.getPipeTasks(existedPipeMeta.getStaticMeta());
    if (pipeTasks == null) {
      LOGGER.info(
          "Pipe {} (creation time = {}) has already been dropped or has not been created. Skip stopping.",
          pipeName,
          creationTime);
      return;
    }
    for (PipeTask pipeTask : pipeTasks.values()) {
      pipeTask.stop();
    }

    // set pipe meta status to STOPPED
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
  }

  ///////////////////////// Manage by dataRegionGroupId /////////////////////////

  private void createPipeTask(
      TConsensusGroupId consensusGroupId,
      PipeStaticMeta pipeStaticMeta,
      PipeTaskMeta pipeTaskMeta) {
    final PipeTask pipeTask =
        new PipeTaskBuilder(consensusGroupId, pipeTaskMeta, pipeStaticMeta).build();
    pipeTask.create();
    pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .put(
            consensusGroupId,
            new PipeTaskMeta(pipeTaskMeta.getProgressIndex(), pipeTaskMeta.getRegionLeader()));
  }

  private void dropPipeTask(TConsensusGroupId dataRegionGroupId, PipeStaticMeta pipeStaticMeta) {
    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
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

  private void stopPipeTask(TConsensusGroupId dataRegionGroupId, PipeStaticMeta pipeStaticMeta) {
    final PipeTask pipeTask = pipeTaskManager.getPipeTask(pipeStaticMeta, dataRegionGroupId);
    if (pipeTask != null) {
      pipeTask.stop();
    }
  }

  ///////////////////////// Heartbeat /////////////////////////

  public synchronized void collectPipeMetaList(THeartbeatReq req, THeartbeatResp resp)
      throws TException {
    if (!req.isNeedPipeMetaList()) {
      return;
    }

    final List<ByteBuffer> pipeMetaBinaryList = new ArrayList<>();
    try {
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        pipeMetaBinaryList.add(pipeMeta.serialize());
      }
    } catch (IOException e) {
      throw new TException(e);
    }
    resp.setPipeMetaList(pipeMetaBinaryList);
  }
}
