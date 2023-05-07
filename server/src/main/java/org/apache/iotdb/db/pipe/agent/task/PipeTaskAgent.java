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
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public PipeTaskAgent() {
    pipeMetaKeeper = new PipeMetaKeeper();
  }

  ////////////////////////// Pipe Task Management //////////////////////////

  public void createPipe(PipeMeta pipeMeta) {
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

    // build pipe task by consensus group
    pipeMeta
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .forEach(
            ((consensusGroupId, pipeTaskMeta) -> {
              createPipeTaskByConsensusGroup(
                  pipeName, creationTime, consensusGroupId, pipeTaskMeta);
            }));
    // add pipe meta to pipe meta keeper
    // note that we do not need to set the status of pipe meta here, because the status of pipe meta
    // is already set to STOPPED when it is created
    pipeMetaKeeper.addPipeMeta(pipeName, pipeMeta);
  }

  public void createPipeTaskByConsensusGroup(
      String pipeName,
      long creationTime,
      TConsensusGroupId consensusGroupId,
      PipeTaskMeta pipeTaskMeta) {}

  public void dropPipe(String pipeName, long creationTime) {
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
    // drop pipe task by consensus group
    existedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .forEach(
            ((consensusGroupId, pipeTaskMeta) -> {
              dropPipeTaskByConsensusGroup(pipeName, creationTime, consensusGroupId);
            }));
    // remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  public void dropPipeTaskByConsensusGroup(
      String pipeName, long creationTime, TConsensusGroupId consensusGroupId) {}

  public void dropPipe(String pipeName) {
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
    // drop pipe task by consensus group
    existedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .forEach(
            ((consensusGroupId, pipeTaskMeta) -> {
              dropPipeTaskByConsensusGroup(pipeName, consensusGroupId);
            }));
    // remove pipe meta from pipe meta keeper
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  public void dropPipeTaskByConsensusGroup(String pipeName, TConsensusGroupId consensusGroupId) {}

  public void startPipe(String pipeName, long creationTime) {
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

    // start pipe task by consensus group
    existedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .forEach(
            ((consensusGroupId, pipeTaskMeta) -> {
              startPipeTaskByConsensusGroup(pipeName, creationTime, consensusGroupId);
            }));
    // set pipe meta status to RUNNING
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
  }

  public void startPipeTaskByConsensusGroup(
      String pipeName, long creationTime, TConsensusGroupId consensusGroupId) {}

  public void stopPipe(String pipeName, long creationTime) {
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

    // stop pipe task by consensus group
    existedPipeMeta
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .forEach(
            ((consensusGroupId, pipeTaskMeta) -> {
              stopPipeTaskByConsensusGroup(pipeName, creationTime, consensusGroupId);
            }));
    // set pipe meta status to STOPPED
    existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
  }

  public void stopPipeTaskByConsensusGroup(
      String pipeName, long creationTime, TConsensusGroupId consensusGroupId) {}
}
