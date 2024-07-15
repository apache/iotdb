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

package org.apache.iotdb.confignode.manager.pipe.coordinator.task;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.read.pipe.task.ShowPipePlanV2;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTaskCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinator.class);

  private final ConfigManager configManager;

  // NEVER EXPOSE THIS DIRECTLY TO THE OUTSIDE
  private final PipeTaskInfo pipeTaskInfo;

  private final PipeTaskCoordinatorLock pipeTaskCoordinatorLock;
  private AtomicReference<PipeTaskInfo> pipeTaskInfoHolder;

  public PipeTaskCoordinator(ConfigManager configManager, PipeTaskInfo pipeTaskInfo) {
    this.configManager = configManager;
    this.pipeTaskInfo = pipeTaskInfo;
    this.pipeTaskCoordinatorLock = new PipeTaskCoordinatorLock();
  }

  /**
   * Lock the pipe task coordinator.
   *
   * @return the pipe task info holder, which can be used to get the pipe task info. The holder is
   *     null if the lock is not acquired.
   */
  public AtomicReference<PipeTaskInfo> tryLock() {
    if (pipeTaskCoordinatorLock.tryLock()) {
      pipeTaskInfoHolder = new AtomicReference<>(pipeTaskInfo);
      return pipeTaskInfoHolder;
    }

    return null;
  }

  /**
   * Lock the pipe task coordinator.
   *
   * @return the {@link PipeTaskInfo} holder, which can be used to get the {@link PipeTaskInfo}.
   *     Wait until lock is acquired
   */
  public AtomicReference<PipeTaskInfo> lock() {
    pipeTaskCoordinatorLock.lock();
    pipeTaskInfoHolder = new AtomicReference<>(pipeTaskInfo);
    return pipeTaskInfoHolder;
  }

  /**
   * Unlock the pipe task coordinator. Calling this method will clear the pipe task info holder,
   * which means that the holder will be null after calling this method.
   *
   * @return {@code true} if successfully unlocked, {@code false} if current thread is not holding
   *     the lock.
   */
  public boolean unlock() {
    if (pipeTaskInfoHolder != null) {
      pipeTaskInfoHolder.set(null);
      pipeTaskInfoHolder = null;
    }

    try {
      pipeTaskCoordinatorLock.unlock();
      return true;
    } catch (IllegalMonitorStateException ignored) {
      // This is thrown if unlock() is called without lock() called first.
      LOGGER.warn("This thread is not holding the lock.");
      return false;
    }
  }

  public boolean isLocked() {
    return pipeTaskCoordinatorLock.isLocked();
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus createPipe(TCreatePipeReq req) {
    final TSStatus status = configManager.getProcedureManager().createPipe(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to create pipe {}. Result status: {}.", req.getPipeName(), status);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus alterPipe(TAlterPipeReq req) {
    final TSStatus status = configManager.getProcedureManager().alterPipe(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to alter pipe {}. Result status: {}.", req.getPipeName(), status);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus startPipe(String pipeName) {
    final TSStatus status = configManager.getProcedureManager().startPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to start pipe {}. Result status: {}.", pipeName, status);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus stopPipe(String pipeName) {
    final boolean isStoppedByRuntimeException = pipeTaskInfo.isStoppedByRuntimeException(pipeName);
    final TSStatus status = configManager.getProcedureManager().stopPipe(pipeName);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (isStoppedByRuntimeException) {
        // Even if the return status is success, it doesn't imply the success of the
        // `executeFromOperateOnDataNodes` phase of stopping pipe. However, we still need to set
        // `isStoppedByRuntimeException` to false to avoid auto-restart. Meanwhile,
        // `isStoppedByRuntimeException` does not need to be synchronized with DNs.
        LOGGER.info("Pipe {} has stopped manually, stop its auto restart process.", pipeName);
        pipeTaskInfo.setIsStoppedByRuntimeExceptionToFalse(pipeName);
        configManager.getProcedureManager().pipeHandleMetaChange(true, false);
      }
    } else {
      LOGGER.warn("Failed to stop pipe {}. Result status: {}.", pipeName, status);
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
    try {
      return ((PipeTableResp) configManager.getConsensusManager().read(new ShowPipePlanV2()))
          .filter(req.whereClause, req.pipeName)
          .convertToTShowPipeResp();
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new PipeTableResp(res, Collections.emptyList()).convertToTShowPipeResp();
    }
  }

  public TGetAllPipeInfoResp getAllPipeInfo() {
    try {
      return ((PipeTableResp) configManager.getConsensusManager().read(new ShowPipePlanV2()))
          .convertToTGetAllPipeInfoResp();
    } catch (IOException | ConsensusException e) {
      LOGGER.warn("Failed to get all pipe info.", e);
      return new TGetAllPipeInfoResp(
          new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public boolean hasAnyPipe() {
    return !pipeTaskInfo.isEmpty();
  }

  /** Caller should ensure that the method is called in the write lock of {@link #pipeTaskInfo}. */
  public void updateLastSyncedVersion() {
    pipeTaskInfo.updateLastSyncedVersion();
  }

  public boolean canSkipNextSync() {
    return pipeTaskInfo.canSkipNextSync();
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public long runningPipeCount() {
    return pipeTaskInfo.runningPipeCount();
  }

  public long droppedPipeCount() {
    return pipeTaskInfo.droppedPipeCount();
  }

  public long userStoppedPipeCount() {
    return pipeTaskInfo.userStoppedPipeCount();
  }

  public long exceptionStoppedPipeCount() {
    return pipeTaskInfo.exceptionStoppedPipeCount();
  }
}
