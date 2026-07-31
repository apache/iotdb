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
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.confignode.consensus.request.read.pipe.task.ShowPipePlanV2;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopPipeReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTaskCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinator.class);

  private final ConfigManager configManager;

  // NEVER EXPOSE THIS DIRECTLY TO THE OUTSIDE
  private final PipeTaskInfo pipeTaskInfo;

  private final PipeTaskCoordinatorLock pipeTaskCoordinatorLock;

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
    return pipeTaskCoordinatorLock.tryLock() ? new AtomicReference<>(pipeTaskInfo) : null;
  }

  /**
   * Lock the pipe task coordinator.
   *
   * @return the {@link PipeTaskInfo} holder, which can be used to get the {@link PipeTaskInfo}.
   *     Wait until lock is acquired
   */
  public AtomicReference<PipeTaskInfo> lock() {
    pipeTaskCoordinatorLock.lock();
    return new AtomicReference<>(pipeTaskInfo);
  }

  /**
   * Unlock the pipe task coordinator. Calling this method will clear the pipe task info holder,
   * which means that the holder will be null after calling this method.
   */
  public void unlock() {
    pipeTaskCoordinatorLock.unlock();
  }

  public boolean isLocked() {
    return pipeTaskCoordinatorLock.isLocked();
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus createPipe(TCreatePipeReq req) {
    final TSStatus status;
    if (req.getPipeName().startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      status = configManager.getProcedureManager().createConsensusPipe(req);
    } else if (isDoubleLivingPipe(req)) {
      status = createDoubleLivingPipe(req);
    } else {
      status = configManager.getProcedureManager().createPipe(req);
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(ManagerMessages.FAILED_TO_CREATE_PIPE_RESULT_STATUS, req.getPipeName(), status);
    }
    return status;
  }

  private boolean isDoubleLivingPipe(final TCreatePipeReq req) {
    return PipeSourceConstant.isDoubleLiving(new PipeParameters(req.getExtractorAttributes()));
  }

  private TSStatus createDoubleLivingPipe(final TCreatePipeReq req) {
    final PipeParameters sourceParameters = new PipeParameters(req.getExtractorAttributes());
    final TSStatus validationStatus = validateDoubleLivingPipeParameters(sourceParameters);
    if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != validationStatus.getCode()) {
      return validationStatus;
    }
    final String currentDialect =
        sourceParameters.getStringOrDefault(
            SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final String firstDialect =
        SystemConstant.SQL_DIALECT_TREE_VALUE.equals(currentDialect)
            ? SystemConstant.SQL_DIALECT_TREE_VALUE
            : SystemConstant.SQL_DIALECT_TABLE_VALUE;
    final String secondDialect =
        SystemConstant.SQL_DIALECT_TREE_VALUE.equals(firstDialect)
            ? SystemConstant.SQL_DIALECT_TABLE_VALUE
            : SystemConstant.SQL_DIALECT_TREE_VALUE;

    final TCreatePipeReq firstReq =
        cloneCreatePipeRequestWithDialect(req, sourceParameters, firstDialect);
    final TCreatePipeReq secondReq =
        cloneCreatePipeRequestWithDialect(req, sourceParameters, secondDialect);
    final boolean shouldCreateFirstPipe;
    final boolean shouldCreateSecondPipe;
    try {
      shouldCreateFirstPipe = pipeTaskInfo.checkBeforeCreatePipe(firstReq);
      shouldCreateSecondPipe = pipeTaskInfo.checkBeforeCreatePipe(secondReq);
    } catch (final Exception e) {
      return RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, e.getMessage());
    }

    TSStatus firstStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    if (shouldCreateFirstPipe) {
      firstStatus = configManager.getProcedureManager().createPipe(firstReq);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != firstStatus.getCode()) {
        return firstStatus;
      }
    }
    if (!shouldCreateSecondPipe) {
      return firstStatus;
    }

    final TSStatus secondStatus = configManager.getProcedureManager().createPipe(secondReq);
    if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == secondStatus.getCode()) {
      return secondStatus;
    }
    if (shouldCreateFirstPipe) {
      configManager
          .getProcedureManager()
          .dropPipe(
              firstReq.getPipeName(), SystemConstant.SQL_DIALECT_TABLE_VALUE.equals(firstDialect));
    }
    return secondStatus;
  }

  private TSStatus validateDoubleLivingPipeParameters(final PipeParameters sourceParameters) {
    final Boolean isForwardingPipeRequests =
        sourceParameters.getBooleanByKeys(
            PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
            PipeSourceConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY);
    return Boolean.TRUE.equals(isForwardingPipeRequests)
        ? RpcUtils.getStatus(
            TSStatusCode.PIPE_ERROR,
            PipeMessages
                .EXCEPTION_FORWARDING_PIPE_REQUESTS_CAN_NOT_SPECIFIED_TRUE_DOUBLE_LIVING_ENABLED_B000E8A1)
        : RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  private TCreatePipeReq cloneCreatePipeRequestWithDialect(
      final TCreatePipeReq req, final PipeParameters sourceParameters, final String sqlDialect) {
    final Map<String, String> sourceAttributes = new HashMap<>(sourceParameters.getAttribute());
    PipeSourceConstant.removeDoubleLivingAttributes(sourceAttributes);
    PipeSourceConstant.disableForwardingPipeRequests(sourceAttributes);
    sourceAttributes.put(SystemConstant.SQL_DIALECT_KEY, sqlDialect);
    sourceAttributes.put(
        SystemConstant.PIPE_VISIBILITY_KEY, SystemConstant.PIPE_VISIBILITY_STRICT_VALUE);

    final TCreatePipeReq clonedReq =
        new TCreatePipeReq()
            .setPipeName(req.getPipeName())
            .setExtractorAttributes(sourceAttributes)
            .setProcessorAttributes(cloneAttributes(req.getProcessorAttributes()))
            .setConnectorAttributes(cloneAttributes(req.getConnectorAttributes()));
    if (req.isSetIfNotExistsCondition()) {
      clonedReq.setIfNotExistsCondition(req.isIfNotExistsCondition());
    }
    if (req.isSetNeedManuallyStart()) {
      clonedReq.setNeedManuallyStart(req.isNeedManuallyStart());
    }
    return clonedReq;
  }

  private Map<String, String> cloneAttributes(final Map<String, String> attributes) {
    return new HashMap<>(attributes == null ? Collections.emptyMap() : attributes);
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus alterPipe(TAlterPipeReq req) {
    final String pipeName = req.getPipeName();
    final boolean isSetIfExistsCondition =
        req.isSetIfExistsCondition() && req.isIfExistsCondition();
    final boolean isTableModel =
        resolveIsTableModel(pipeName, req.isSetIsTableModel(), req.isTableModel);
    if (!pipeTaskInfo.isPipeExisted(pipeName, isTableModel)) {
      return isSetIfExistsCondition
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
          : RpcUtils.getStatus(
              TSStatusCode.PIPE_NOT_EXIST_ERROR,
              String.format(
                  "Failed to alter pipe %s. Failures: %s does not exist.", pipeName, pipeName));
    }
    req.setIsTableModel(isTableModel);
    final TSStatus status = configManager.getProcedureManager().alterPipe(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(ManagerMessages.FAILED_TO_ALTER_PIPE_RESULT_STATUS, req.getPipeName(), status);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  private TSStatus startPipe(String pipeName) {
    return startPipe(pipeName, false);
  }

  private TSStatus startPipe(String pipeName, boolean isTableModel) {
    final TSStatus status;
    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      status = configManager.getProcedureManager().startConsensusPipe(pipeName);
    } else {
      status = configManager.getProcedureManager().startPipe(pipeName, isTableModel);
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(ManagerMessages.FAILED_TO_START_PIPE_RESULT_STATUS, pipeName, status);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus startPipe(TStartPipeReq req) {
    final String pipeName = req.getPipeName();
    final boolean isTableModel =
        resolveIsTableModel(pipeName, req.isSetIsTableModel(), req.isTableModel);
    if (!pipeTaskInfo.isPipeExisted(pipeName, isTableModel)) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_NOT_EXIST_ERROR,
          String.format(
              "Failed to start pipe %s. Failures: %s does not exist.", pipeName, pipeName));
    }
    return startPipe(pipeName, isTableModel);
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  private TSStatus stopPipe(String pipeName) {
    return stopPipe(pipeName, false);
  }

  private TSStatus stopPipe(String pipeName, boolean isTableModel) {
    final TSStatus status;
    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      status = configManager.getProcedureManager().stopConsensusPipe(pipeName);
    } else {
      status = configManager.getProcedureManager().stopPipe(pipeName, isTableModel);
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(ManagerMessages.FAILED_TO_STOP_PIPE_RESULT_STATUS, pipeName, status);
    }
    return status;
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus stopPipe(TStopPipeReq req) {
    final String pipeName = req.getPipeName();
    final boolean isTableModel =
        resolveIsTableModel(pipeName, req.isSetIsTableModel(), req.isTableModel);
    if (!pipeTaskInfo.isPipeExisted(pipeName, isTableModel)) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_NOT_EXIST_ERROR,
          String.format(
              "Failed to stop pipe %s. Failures: %s does not exist.", pipeName, pipeName));
    }
    return stopPipe(pipeName, isTableModel);
  }

  /** Caller should ensure that the method is called in the lock {@link #lock()}. */
  public TSStatus dropPipe(TDropPipeReq req) {
    final String pipeName = req.getPipeName();
    final boolean isSetIfExistsCondition =
        req.isSetIfExistsCondition() && req.isIfExistsCondition();
    final boolean isTableModel =
        resolveIsTableModel(pipeName, req.isSetIsTableModel(), req.isTableModel);
    if (!pipeTaskInfo.isPipeExisted(pipeName, isTableModel)) {
      return isSetIfExistsCondition
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
          : RpcUtils.getStatus(
              TSStatusCode.PIPE_NOT_EXIST_ERROR,
              String.format(
                  "Failed to drop pipe %s. Failures: %s does not exist.", pipeName, pipeName));
    }
    final TSStatus status;
    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      status = configManager.getProcedureManager().dropConsensusPipe(pipeName);
    } else {
      status = configManager.getProcedureManager().dropPipe(pipeName, isTableModel);
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(ManagerMessages.FAILED_TO_DROP_PIPE_RESULT_STATUS, pipeName, status);
    }
    return status;
  }

  private boolean resolveIsTableModel(
      final String pipeName, final boolean isSetIsTableModel, final boolean isTableModel) {
    return isSetIsTableModel
        ? isTableModel
        : !pipeTaskInfo.isPipeExisted(pipeName, false)
            && pipeTaskInfo.isPipeExisted(pipeName, true);
  }

  public boolean resolveIsTableModel(final String pipeName) {
    return resolveIsTableModel(pipeName, false, false);
  }

  public TShowPipeResp showPipes(final TShowPipeReq req) {
    try {
      final PipeTableResp pipeTableResp =
          (PipeTableResp) configManager.getConsensusManager().read(new ShowPipePlanV2());
      return (req.isSetIsTableModel()
              ? pipeTableResp.filter(req.whereClause, req.pipeName, req.isTableModel, req.userName)
              : pipeTableResp.filter(req.whereClause, req.pipeName, req.userName))
          .convertToTShowPipeResp();
    } catch (final ConsensusException e) {
      PipeLogger.log(
          LOGGER::warn,
          e,
          ConfigNodeMessages.FAILED_IN_THE_READ_API_EXECUTING_THE_CONSENSUS_LAYER_DUE);
      final TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new PipeTableResp(res, Collections.emptyList()).convertToTShowPipeResp();
    }
  }

  public TGetAllPipeInfoResp getAllPipeInfo() {
    try {
      return ((PipeTableResp) configManager.getConsensusManager().read(new ShowPipePlanV2()))
          .convertToTGetAllPipeInfoResp();
    } catch (IOException | ConsensusException e) {
      PipeLogger.log(LOGGER::warn, e, ManagerMessages.FAILED_TO_GET_ALL_PIPE_INFO);
      return new TGetAllPipeInfoResp(
          new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public boolean hasAnyPipe() {
    return !pipeTaskInfo.isEmpty();
  }

  public Map<String, PipeStatus> getConsensusPipeStatusMap() {
    return pipeTaskInfo.getConsensusPipeStatusMap();
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
