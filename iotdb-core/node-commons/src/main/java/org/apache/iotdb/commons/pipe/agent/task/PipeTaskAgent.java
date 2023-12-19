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
import org.apache.iotdb.commons.pipe.task.PipeTaskManager;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskAgent.class);

  protected static final String MESSAGE_UNKNOWN_PIPE_STATUS = "Unknown pipe status %s for pipe %s";
  protected static final String MESSAGE_UNEXPECTED_PIPE_STATUS = "Unexpected pipe status %s: ";

  protected final PipeTaskManager pipeTaskManager;

  protected PipeTaskAgent() {
    pipeTaskManager = new PipeTaskManager();
  }

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
      PipeMetaKeeper metaKeeper,
      TConsensusGroupId consensusGroupId,
      PipeStaticMeta pipeStaticMeta,
      PipeTaskMeta pipeTaskMeta);

  protected abstract void dropPipeTask(
      PipeMetaKeeper metaKeeper, TConsensusGroupId consensusGroupId, PipeStaticMeta pipeStaticMeta);

  protected abstract void startPipeTask(
      TConsensusGroupId consensusGroupId, PipeStaticMeta pipeStaticMeta);
}
