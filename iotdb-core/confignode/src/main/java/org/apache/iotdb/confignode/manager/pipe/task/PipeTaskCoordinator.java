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

package org.apache.iotdb.confignode.manager.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.read.pipe.task.ShowPipePlanV2;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class PipeTaskCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinator.class);

  private final ConfigManager configManager;

  // NEVER EXPOSE THIS DIRECTLY TO THE OUTSIDE
  private final PipeTaskInfo pipeTaskInfo;

  private final ReentrantLock pipeTaskCoordinatorLock;
  private AtomicReference<PipeTaskInfo> pipeTaskInfoHolder;

  public PipeTaskCoordinator(ConfigManager configManager, PipeTaskInfo pipeTaskInfo) {
    this.configManager = configManager;
    this.pipeTaskInfo = pipeTaskInfo;
    this.pipeTaskCoordinatorLock = new ReentrantLock(true);
  }

  /**
   * Lock the pipe task coordinator.
   *
   * @return the pipe task info holder, which can be used to get the pipe task info. The holder is
   *     null if the lock is not acquired.
   */
  public AtomicReference<PipeTaskInfo> lock() {
    pipeTaskCoordinatorLock.lock();
    LOGGER.info("Pipe task coordinator locked.");

    pipeTaskInfoHolder = new AtomicReference<>(pipeTaskInfo);
    return pipeTaskInfoHolder;
  }

  /**
   * Unlock the pipe task coordinator. Calling this method will clear the pipe task info holder,
   * which means that the holder will be null after calling this method.
   *
   * @return true if successfully unlocked, false if current thread is not holding the lock.
   */
  public boolean unlock() {
    if (pipeTaskInfoHolder != null) {
      pipeTaskInfoHolder.set(null);
      pipeTaskInfoHolder = null;
    }

    try {
      pipeTaskCoordinatorLock.unlock();
      LOGGER.info("Pipe task coordinator unlocked.");
      return true;
    } catch (IllegalMonitorStateException ignored) {
      // This is thrown if unlock() is called without lock() called first.
      LOGGER.warn("This thread is not holding the lock.");
      return false;
    }
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus createPipe(TCreatePipeReq req) {
    return configManager.getProcedureManager().createPipe(req);
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus startPipe(String pipeName) {
    final boolean hasException = pipeTaskInfo.hasExceptions(pipeName);
    final TSStatus status = configManager.getProcedureManager().startPipe(pipeName);
    if (status == RpcUtils.SUCCESS_STATUS && hasException) {
      LOGGER.info("Pipe {} has started successfully, clear its exceptions.", pipeName);
      configManager.getProcedureManager().pipeHandleMetaChange(true, true);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus stopPipe(String pipeName) {
    final boolean isStoppedByRuntimeException = pipeTaskInfo.isStoppedByRuntimeException(pipeName);
    final TSStatus status = configManager.getProcedureManager().stopPipe(pipeName);
    if (status == RpcUtils.SUCCESS_STATUS && isStoppedByRuntimeException) {
      LOGGER.info(
          "Pipe {} has stopped successfully manually, stop its auto restart process.", pipeName);
      pipeTaskInfo.setIsStoppedByRuntimeExceptionToFalse(pipeName);
      configManager.getProcedureManager().pipeHandleMetaChange(true, true);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus dropPipe(String pipeName) {
    final boolean isPipeExistedBeforeDrop = pipeTaskInfo.isPipeExisted(pipeName);
    final TSStatus status = configManager.getProcedureManager().dropPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to drop pipe {}. Result status: {}.", pipeName, status);
    }
    return isPipeExistedBeforeDrop
        ? status
        : RpcUtils.getStatus(
            TSStatusCode.PIPE_NOT_EXIST_ERROR,
            String.format(
                "Failed to drop pipe %s. Failures: %s does not exist.", pipeName, pipeName));
  }

  public TShowPipeResp showPipes(TShowPipeReq req) {
    lock();
    try {
      return ((PipeTableResp)
              configManager.getConsensusManager().read(new ShowPipePlanV2()).getDataset())
          .filter(req.whereClause, req.pipeName)
          .convertToTShowPipeResp();
    } finally {
      unlock();
    }
  }

  public TGetAllPipeInfoResp getAllPipeInfo() {
    lock();
    try {
      return ((PipeTableResp)
              configManager.getConsensusManager().read(new ShowPipePlanV2()).getDataset())
          .convertToTGetAllPipeInfoResp();
    } catch (IOException e) {
      LOGGER.warn("Failed to get all pipe info.", e);
      return new TGetAllPipeInfoResp(
          new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage()),
          Collections.emptyList());
    } finally {
      unlock();
    }
  }

  public boolean hasAnyPipe() {
    lock();
    try {
      return !pipeTaskInfo.isEmpty();
    } finally {
      unlock();
    }
  }
}
