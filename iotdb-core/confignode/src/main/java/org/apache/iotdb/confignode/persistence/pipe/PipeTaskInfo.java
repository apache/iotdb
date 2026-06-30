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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusWithStoppedByRuntimeExceptionPlanV2;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.manager.pipe.resource.PipeConfigNodeResourceManager;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR;
import static org.apache.iotdb.commons.pipe.config.constant.PipeRPCMessageConstant.PIPE_ALREADY_EXIST_MSG;
import static org.apache.iotdb.commons.pipe.config.constant.PipeRPCMessageConstant.PIPE_NOT_EXIST_MSG;

public class PipeTaskInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskInfo.class);
  private static final String SNAPSHOT_FILE_NAME = "pipe_task_info.bin";

  private final PipeMetaKeeper pipeMetaKeeper;

  // Pure in-memory object, not involved in snapshot serialization and deserialization.
  private final PipeTaskInfoVersion pipeTaskInfoVersion;
  // Accepts a username and returns its current stored password for pipe authentication.
  private final Function<String, String> pipeUserCurrentPasswordProvider;

  public PipeTaskInfo() {
    this(null);
  }

  public PipeTaskInfo(final Function<String, String> pipeUserCurrentPasswordProvider) {
    this.pipeMetaKeeper = new PipeMetaKeeper();
    this.pipeTaskInfoVersion = new PipeTaskInfoVersion();
    this.pipeUserCurrentPasswordProvider = pipeUserCurrentPasswordProvider;
  }

  /////////////////////////////// Lock ///////////////////////////////

  private void acquireReadLock() {
    pipeMetaKeeper.acquireReadLock();
  }

  private void releaseReadLock() {
    pipeMetaKeeper.releaseReadLock();
  }

  private void acquireWriteLock() {
    pipeMetaKeeper.acquireWriteLock();
    // We use the number of times obtaining the write lock of PipeMetaKeeper as the version number
    // of PipeTaskInfo.
    pipeTaskInfoVersion.increaseLatestVersion();
  }

  private void releaseWriteLock() {
    pipeMetaKeeper.releaseWriteLock();
  }

  /////////////////////////////// Version ///////////////////////////////

  private class PipeTaskInfoVersion {

    private final AtomicLong latestVersion;
    private long lastSyncedVersion;
    private boolean isLastSyncedPipeTaskInfoEmpty;

    public PipeTaskInfoVersion() {
      this.latestVersion = new AtomicLong(0);
      this.lastSyncedVersion = 0;
      this.isLastSyncedPipeTaskInfoEmpty = false;
    }

    public void increaseLatestVersion() {
      latestVersion.incrementAndGet();
    }

    public void updateLastSyncedVersion() {
      lastSyncedVersion = latestVersion.get();
      isLastSyncedPipeTaskInfoEmpty = pipeMetaKeeper.isEmpty();
    }

    public boolean canSkipNextSync() {
      return isLastSyncedPipeTaskInfoEmpty
          && pipeMetaKeeper.isEmpty()
          && lastSyncedVersion == latestVersion.get();
    }
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock}. */
  public void updateLastSyncedVersion() {
    pipeTaskInfoVersion.updateLastSyncedVersion();
  }

  public boolean canSkipNextSync() {
    return pipeTaskInfoVersion.canSkipNextSync();
  }

  /////////////////////////////// Validator ///////////////////////////////

  public boolean checkBeforeCreatePipe(final TCreatePipeReq createPipeRequest)
      throws PipeException {
    acquireReadLock();
    try {
      return checkBeforeCreatePipeInternal(createPipeRequest);
    } finally {
      releaseReadLock();
    }
  }

  private boolean checkBeforeCreatePipeInternal(final TCreatePipeReq createPipeRequest)
      throws PipeException {
    final PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            createPipeRequest.getPipeName(),
            System.currentTimeMillis(),
            createPipeRequest.getExtractorAttributes(),
            createPipeRequest.getProcessorAttributes(),
            createPipeRequest.getConnectorAttributes());
    if (!pipeMetaKeeper.containsPipeMetaOverlapped(pipeStaticMeta)) {
      return true;
    }

    if (createPipeRequest.isSetIfNotExistsCondition()
        && createPipeRequest.isIfNotExistsCondition()) {
      return false;
    }

    final String exceptionMessage =
        String.format(
            "Failed to create pipe %s, %s",
            createPipeRequest.getPipeName(), PIPE_ALREADY_EXIST_MSG);
    LOGGER.info(exceptionMessage);
    throw new PipeException(exceptionMessage);
  }

  public void checkAndUpdateRequestBeforeAlterPipe(final TAlterPipeReq alterPipeRequest)
      throws PipeException {
    acquireReadLock();
    try {
      checkAndUpdateRequestBeforeAlterPipeInternal(alterPipeRequest);
    } finally {
      releaseReadLock();
    }
  }

  private void checkAndUpdateRequestBeforeAlterPipeInternal(final TAlterPipeReq alterPipeRequest)
      throws PipeException {
    if (!isPipeExisted(alterPipeRequest.getPipeName(), alterPipeRequest.isTableModel)) {
      final String exceptionMessage =
          String.format(
              "Failed to alter pipe %s, %s", alterPipeRequest.getPipeName(), PIPE_NOT_EXIST_MSG);
      LOGGER.info(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStaticMeta pipeStaticMetaFromCoordinator =
        getPipeMetaByPipeName(alterPipeRequest.getPipeName(), alterPipeRequest.isTableModel)
            .getStaticMeta();
    // deep copy current pipe static meta
    final PipeStaticMeta copiedPipeStaticMetaFromCoordinator =
        new PipeStaticMeta(
            pipeStaticMetaFromCoordinator.getPipeName(),
            pipeStaticMetaFromCoordinator.getCreationTime(),
            new HashMap<>(pipeStaticMetaFromCoordinator.getSourceParameters().getAttribute()),
            new HashMap<>(pipeStaticMetaFromCoordinator.getProcessorParameters().getAttribute()),
            new HashMap<>(pipeStaticMetaFromCoordinator.getSinkParameters().getAttribute()));

    // 1. In modify mode, based on the passed attributes:
    //   1.1. if they are empty, the original attributes are filled directly.
    //   1.2. Otherwise, corresponding updates on original attributes are performed.
    // 2. In replace mode, do nothing here.
    if (!alterPipeRequest.isReplaceAllExtractorAttributes) { // modify mode
      if (alterPipeRequest.getExtractorAttributes().isEmpty()) {
        alterPipeRequest.setExtractorAttributes(
            copiedPipeStaticMetaFromCoordinator.getSourceParameters().getAttribute());
      } else {
        alterPipeRequest.setExtractorAttributes(
            copiedPipeStaticMetaFromCoordinator
                .getSourceParameters()
                .addOrReplaceEquivalentAttributes(
                    new PipeParameters(alterPipeRequest.getExtractorAttributes()))
                .getAttribute());
      }
    }
    keepExtractorDialectConsistentWithTargetModel(
        alterPipeRequest, pipeStaticMetaFromCoordinator.getSourceParameters());

    if (!alterPipeRequest.isReplaceAllProcessorAttributes) { // modify mode
      if (alterPipeRequest.getProcessorAttributes().isEmpty()) {
        alterPipeRequest.setProcessorAttributes(
            copiedPipeStaticMetaFromCoordinator.getProcessorParameters().getAttribute());
      } else {
        alterPipeRequest.setProcessorAttributes(
            copiedPipeStaticMetaFromCoordinator
                .getProcessorParameters()
                .addOrReplaceEquivalentAttributes(
                    new PipeParameters(alterPipeRequest.getProcessorAttributes()))
                .getAttribute());
      }
    }

    if (!alterPipeRequest.isReplaceAllConnectorAttributes) { // modify mode
      if (alterPipeRequest.getConnectorAttributes().isEmpty()) {
        alterPipeRequest.setConnectorAttributes(
            copiedPipeStaticMetaFromCoordinator.getSinkParameters().getAttribute());
      } else {
        alterPipeRequest.setConnectorAttributes(
            copiedPipeStaticMetaFromCoordinator
                .getSinkParameters()
                .addOrReplaceEquivalentAttributes(
                    new PipeParameters(alterPipeRequest.getConnectorAttributes()))
                .getAttribute());
      }
    }

    final PipeStaticMeta updatedPipeStaticMeta =
        new PipeStaticMeta(
            alterPipeRequest.getPipeName(),
            System.currentTimeMillis(),
            alterPipeRequest.getExtractorAttributes(),
            alterPipeRequest.getProcessorAttributes(),
            alterPipeRequest.getConnectorAttributes());
    if (pipeMetaKeeper.containsPipeMetaOverlapped(
        updatedPipeStaticMeta, pipeStaticMetaFromCoordinator)) {
      final String exceptionMessage =
          String.format(
              "Failed to alter pipe %s, %s",
              alterPipeRequest.getPipeName(), PIPE_ALREADY_EXIST_MSG);
      LOGGER.info(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  private void keepExtractorDialectConsistentWithTargetModel(
      final TAlterPipeReq alterPipeRequest, final PipeParameters currentSourceParameters) {
    final boolean isStrictVisibility = VisibilityUtils.isStrictVisibility(currentSourceParameters);
    if (!isStrictVisibility) {
      final Visibility currentVisibility =
          VisibilityUtils.calculateFromExtractorParameters(currentSourceParameters);
      if (Objects.equals(Visibility.BOTH, currentVisibility)
          || Objects.equals(Visibility.NONE, currentVisibility)) {
        return;
      }
    }

    alterPipeRequest
        .getExtractorAttributes()
        .put(
            SystemConstant.SQL_DIALECT_KEY,
            alterPipeRequest.isTableModel
                ? SystemConstant.SQL_DIALECT_TABLE_VALUE
                : SystemConstant.SQL_DIALECT_TREE_VALUE);
    if (isStrictVisibility) {
      alterPipeRequest
          .getExtractorAttributes()
          .put(SystemConstant.PIPE_VISIBILITY_KEY, SystemConstant.PIPE_VISIBILITY_STRICT_VALUE);
    }
  }

  public void checkBeforeStartPipe(final String pipeName) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeStartPipeInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  public void checkBeforeStartPipe(final String pipeName, final boolean isTableModel)
      throws PipeException {
    acquireReadLock();
    try {
      checkBeforeStartPipeInternal(pipeName, isTableModel);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeStartPipeInternal(final String pipeName) throws PipeException {
    if (!isPipeExisted(pipeName)) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_START_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName);
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_START_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  private void checkBeforeStartPipeInternal(final String pipeName, final boolean isTableModel)
      throws PipeException {
    if (!isPipeExisted(pipeName, isTableModel)) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_START_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName, isTableModel);
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_START_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  public void checkBeforeStopPipe(final String pipeName) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeStopPipeInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  public void checkBeforeStopPipe(final String pipeName, final boolean isTableModel)
      throws PipeException {
    acquireReadLock();
    try {
      checkBeforeStopPipeInternal(pipeName, isTableModel);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeStopPipeInternal(final String pipeName) throws PipeException {
    if (!isPipeExisted(pipeName)) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_STOP_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName);
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_STOP_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  private void checkBeforeStopPipeInternal(final String pipeName, final boolean isTableModel)
      throws PipeException {
    if (!isPipeExisted(pipeName, isTableModel)) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_STOP_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName, isTableModel);
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format(
              ConfigNodeMessages.FAILED_TO_STOP_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED, pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  public void checkBeforeDropPipe(final String pipeName) {
    acquireReadLock();
    try {
      checkBeforeDropPipeInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeDropPipeInternal(final String pipeName) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          ConfigNodeMessages.CHECK_BEFORE_DROP_PIPE_PIPE_EXISTS, pipeName, isPipeExisted(pipeName));
    }
    // No matter whether the pipe exists, we allow the drop operation executed on all nodes to
    // ensure the consistency.
    // DO NOTHING HERE!
  }

  public boolean isPipeExisted(final String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  public boolean isPipeExisted(final String pipeName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName, isTableModel);
    } finally {
      releaseReadLock();
    }
  }

  private PipeStatus getPipeStatus(final String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().getStatus().get();
    } finally {
      releaseReadLock();
    }
  }

  private PipeStatus getPipeStatus(final String pipeName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMeta(pipeName, isTableModel).getRuntimeMeta().getStatus().get();
    } finally {
      releaseReadLock();
    }
  }

  public boolean isPipeRunning(final String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName)
          && PipeStatus.RUNNING.equals(getPipeStatus(pipeName));
    } finally {
      releaseReadLock();
    }
  }

  public boolean isPipeRunning(final String pipeName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName, isTableModel)
          && PipeStatus.RUNNING.equals(getPipeStatus(pipeName, isTableModel));
    } finally {
      releaseReadLock();
    }
  }

  public boolean isPipeStoppedByUser(final String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName)
          && PipeStatus.STOPPED.equals(getPipeStatus(pipeName))
          && !isStoppedByRuntimeException(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  public boolean isPipeStoppedByUser(final String pipeName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName, isTableModel)
          && PipeStatus.STOPPED.equals(getPipeStatus(pipeName, isTableModel))
          && !isStoppedByRuntimeException(pipeName, isTableModel);
    } finally {
      releaseReadLock();
    }
  }

  public void validatePipePluginUsageByPipe(String pluginName) {
    acquireReadLock();
    try {
      validatePipePluginUsageByPipeInternal(pluginName);
    } finally {
      releaseReadLock();
    }
  }

  private void validatePipePluginUsageByPipeInternal(String pluginName) {
    Iterable<PipeMeta> pipeMetas = getPipeMetaList();
    for (PipeMeta pipeMeta : pipeMetas) {
      PipeParameters extractorParameters = pipeMeta.getStaticMeta().getSourceParameters();
      final String extractorPluginName =
          extractorParameters.getStringOrDefault(
              Arrays.asList(PipeSourceConstant.EXTRACTOR_KEY, PipeSourceConstant.SOURCE_KEY),
              BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName());
      if (pluginName.equals(extractorPluginName)) {
        String exceptionMessage =
            String.format(
                "PipePlugin '%s' is already used by Pipe '%s' as a source.",
                pluginName, pipeMeta.getStaticMeta().getPipeName());
        throw new PipeException(exceptionMessage);
      }

      PipeParameters processorParameters = pipeMeta.getStaticMeta().getProcessorParameters();
      final String processorPluginName =
          processorParameters.getString(PipeProcessorConstant.PROCESSOR_KEY);
      if (pluginName.equals(processorPluginName)) {
        String exceptionMessage =
            String.format(
                "PipePlugin '%s' is already used by Pipe '%s' as a processor.",
                pluginName, pipeMeta.getStaticMeta().getPipeName());
        throw new PipeException(exceptionMessage);
      }

      PipeParameters connectorParameters = pipeMeta.getStaticMeta().getSinkParameters();
      final String connectorPluginName =
          connectorParameters.getStringOrDefault(
              Arrays.asList(PipeSinkConstant.CONNECTOR_KEY, PipeSinkConstant.SINK_KEY),
              IOTDB_THRIFT_CONNECTOR.getPipePluginName());
      if (pluginName.equals(connectorPluginName)) {
        String exceptionMessage =
            String.format(
                "PipePlugin '%s' is already used by Pipe '%s' as a sink.",
                pluginName, pipeMeta.getStaticMeta().getPipeName());
        throw new PipeException(exceptionMessage);
      }
    }
  }

  /////////////////////////////// Pipe Task Management ///////////////////////////////

  public TSStatus createPipe(final CreatePipePlanV2 plan) {
    acquireWriteLock();
    try {
      enrichPipeMetaWithRootUserForCompatibility(plan.getPipeStaticMeta());
      pipeMetaKeeper.addPipeMeta(new PipeMeta(plan.getPipeStaticMeta(), plan.getPipeRuntimeMeta()));
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus operateMultiplePipes(final OperateMultiplePipesPlanV2 plan) {
    acquireWriteLock();
    try {
      if (plan.getSubPlans() == null || plan.getSubPlans().isEmpty()) {
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }

      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      // We use sub-status to record the status of each subPlan
      status.setSubStatus(new ArrayList<>());

      for (final ConfigPhysicalPlan subPlan : plan.getSubPlans()) {
        try {
          if (subPlan instanceof CreatePipePlanV2) {
            createPipe((CreatePipePlanV2) subPlan);
          } else if (subPlan instanceof AlterPipePlanV2) {
            alterPipe((AlterPipePlanV2) subPlan);
          } else if (subPlan instanceof SetPipeStatusPlanV2) {
            setPipeStatus((SetPipeStatusPlanV2) subPlan);
          } else if (subPlan instanceof DropPipePlanV2) {
            dropPipe((DropPipePlanV2) subPlan);
          } else {
            throw new PipeException(
                String.format(
                    ConfigNodeMessages.UNSUPPORTED_SUBPLAN_TYPE, subPlan.getClass().getName()));
          }
          status.getSubStatus().add(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
        } catch (final Exception e) {
          // If one of the subPlan fails, we stop operating the rest of the pipes
          LOGGER.error(ConfigNodeMessages.FAILED_TO_OPERATE_PIPE, e);
          status.setCode(TSStatusCode.PIPE_ERROR.getStatusCode());
          status.getSubStatus().add(new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()));
          break;
        }
      }

      // If all the subPlans are successful, we return the success status and clear sub-status.
      // Otherwise, we return the error status with sub-status to record the failing index.
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        status.setSubStatus(null);
      }
      return status;
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus alterPipe(final AlterPipePlanV2 plan) {
    acquireWriteLock();
    try {
      enrichPipeMetaWithRootUserForCompatibility(plan.getPipeStaticMeta());
      final PipeTemporaryMeta temporaryMeta =
          pipeMetaKeeper.getPipeMeta(plan.getCurrentPipeStaticMeta()).getTemporaryMeta();
      pipeMetaKeeper.removePipeMeta(plan.getCurrentPipeStaticMeta());
      pipeMetaKeeper.addPipeMeta(
          new PipeMeta(plan.getPipeStaticMeta(), plan.getPipeRuntimeMeta(), temporaryMeta));
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus setPipeStatus(final SetPipeStatusPlanV2 plan) {
    acquireWriteLock();
    try {
      pipeMetaKeeper
          .getPipeMeta(plan.getPipeName(), plan.isTableModelSet(), plan.isTableModel())
          .getRuntimeMeta()
          .getStatus()
          .set(plan.getPipeStatus());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus setPipeStatusWithStoppedByRuntimeException(
      final SetPipeStatusWithStoppedByRuntimeExceptionPlanV2 plan) {
    acquireWriteLock();
    try {
      pipeMetaKeeper
          .getPipeMeta(plan.getPipeName(), plan.isTableModelSet(), plan.isTableModel())
          .getRuntimeMeta()
          .getStatus()
          .set(plan.getPipeStatus());
      pipeMetaKeeper
          .getPipeMeta(plan.getPipeName(), plan.isTableModelSet(), plan.isTableModel())
          .getRuntimeMeta()
          .setIsStoppedByRuntimeException(plan.isStoppedByRuntimeException());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus dropPipe(final DropPipePlanV2 plan) {
    acquireWriteLock();
    try {
      if (plan.isTableModelSet()) {
        pipeMetaKeeper.removePipeMeta(plan.getPipeName(), plan.isTableModel());
      } else {
        pipeMetaKeeper.removePipeMeta(plan.getPipeName());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public DataSet showPipes() {
    acquireReadLock();
    try {
      return new PipeTableResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          StreamSupport.stream(getPipeMetaList().spliterator(), false)
              .collect(Collectors.toList()));
    } finally {
      releaseReadLock();
    }
  }

  public Iterable<PipeMeta> getPipeMetaList() {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMetaList();
    } finally {
      releaseReadLock();
    }
  }

  public PipeMeta getPipeMetaByPipeName(final String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMetaByPipeName(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  public PipeMeta getPipeMetaByPipeName(final String pipeName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMetaByPipeName(pipeName, isTableModel);
    } finally {
      releaseReadLock();
    }
  }

  public PipeMeta getPipeMetaByPipeStaticMeta(final PipeStaticMeta pipeStaticMeta) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMeta(pipeStaticMeta);
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Creation time is part of a pipe's identity on DataNodes. The same pipe name may coexist in
   * different visibility scopes, and alter pushes the dropped old meta and the updated meta
   * together, so same-name metas must have distinct creation times.
   */
  public long generateUniqueCreationTime(final String pipeName) {
    acquireReadLock();
    try {
      long creationTime = System.currentTimeMillis();
      for (final PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        if (pipeMeta.getStaticMeta().getPipeName().equals(pipeName)) {
          creationTime = Math.max(creationTime, pipeMeta.getStaticMeta().getCreationTime() + 1);
        }
      }
      return creationTime;
    } finally {
      releaseReadLock();
    }
  }

  public Map<String, PipeStatus> getConsensusPipeStatusMap() {
    acquireReadLock();
    try {
      return StreamSupport.stream(pipeMetaKeeper.getPipeMetaList().spliterator(), false)
          .filter(pipeMeta -> PipeType.CONSENSUS.equals(pipeMeta.getStaticMeta().getPipeType()))
          .collect(
              Collectors.toMap(
                  pipeMeta -> pipeMeta.getStaticMeta().getPipeName(),
                  pipeMeta -> pipeMeta.getRuntimeMeta().getStatus().get()));
    } finally {
      releaseReadLock();
    }
  }

  public boolean isEmpty() {
    acquireReadLock();
    try {
      return pipeMetaKeeper.isEmpty();
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////// Pipe Runtime Management ///////////////////////////////

  /** Handle the region leader change event and update the pipe task meta accordingly. */
  public TSStatus handleLeaderChange(final PipeHandleLeaderChangePlan plan) {
    acquireWriteLock();
    try {
      return handleLeaderChangeInternal(plan);
    } finally {
      releaseWriteLock();
    }
  }

  private TSStatus handleLeaderChangeInternal(final PipeHandleLeaderChangePlan plan) {
    plan.getConsensusGroupId2NewLeaderIdMap()
        .forEach(
            (consensusGroupId, newLeader) ->
                pipeMetaKeeper
                    .getPipeMetaList()
                    .forEach(
                        pipeMeta -> {
                          if (PipeType.CONSENSUS.equals(pipeMeta.getStaticMeta().getPipeType())) {
                            return; // pipe consensus pipe task will not change
                          }

                          if (pipeMeta.getStaticMeta().isSourceExternal()) {
                            // external source pipe tasks are not balanced here since non-leaders
                            // don't know about RegionLeader Map and will be balanced in the meta
                            // sync procedure
                            PipeLogger.log(
                                LOGGER::info,
                                ConfigNodeMessages.PIPE_IS_USING_EXTERNAL_SOURCE_SKIP_REGION,
                                pipeMeta.getStaticMeta().getPipeName(),
                                plan.getConsensusGroupId2NewLeaderIdMap());
                            return;
                          }

                          final Map<Integer, PipeTaskMeta> consensusGroupIdToTaskMetaMap =
                              pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();

                          if (consensusGroupIdToTaskMetaMap.containsKey(consensusGroupId.getId())) {
                            // If the region leader is -1, it means the region is
                            // removed
                            if (newLeader != -1) {
                              consensusGroupIdToTaskMetaMap
                                  .get(consensusGroupId.getId())
                                  .setLeaderNodeId(newLeader);
                              // New region leader may contain un-transferred events
                              ((PipeTemporaryMetaInCoordinator) pipeMeta.getTemporaryMeta())
                                  .markDataNodeUncompleted(newLeader);
                            } else {
                              consensusGroupIdToTaskMetaMap.remove(consensusGroupId.getId());
                            }
                          } else if (!PipeTaskAgent.isHistoryOnlyPipe(
                                  pipeMeta.getStaticMeta().getSourceParameters())
                              || !consensusGroupId
                                  .getType()
                                  .equals(TConsensusGroupType.DataRegion)) {
                            // If CN does not contain the region group, it means the data
                            // region group is newly added.
                            // We do not handle history only pipes for new data regions

                            // Newly added leader
                            if (newLeader != -1) {
                              consensusGroupIdToTaskMetaMap.put(
                                  consensusGroupId.getId(),
                                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, newLeader)
                                      .markAsNewlyAdded());
                            }
                            // else:
                            // "The pipe task meta does not contain the data region group {} or
                            // the data region group has already been removed"
                          }
                        }));

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Replace the local {@link PipeMeta}s by the {@link PipeMeta}s from the leader {@link
   * ConfigNode}.
   *
   * @param plan The plan containing all the {@link PipeMeta}s from leader {@link ConfigNode}
   * @return {@link TSStatusCode#SUCCESS_STATUS}
   */
  public TSStatus handleMetaChanges(final PipeHandleMetaChangePlan plan) {
    acquireWriteLock();
    try {
      return handleMetaChangesInternal(plan);
    } finally {
      releaseWriteLock();
    }
  }

  private TSStatus handleMetaChangesInternal(final PipeHandleMetaChangePlan plan) {
    LOGGER.debug(ConfigNodeMessages.HANDLING_PIPE_META_CHANGES);

    pipeMetaKeeper.clear();

    // This method is only triggered by pipe sync / meta report currently
    // And is guaranteed to print log finally
    final Optional<Logger> logger =
        PipeConfigNodeResourceManager.log()
            .schedule(
                PipeTaskInfo.class,
                PipeConfig.getInstance().getPipeMetaReportMaxLogNumPerRound(),
                PipeConfig.getInstance().getPipeMetaReportMaxLogIntervalRounds(),
                pipeMetaKeeper.getPipeMetaCount());
    plan.getPipeMetaList()
        .forEach(
            pipeMeta -> {
              enrichPipeMetaWithRootUserForCompatibility(pipeMeta.getStaticMeta());
              pipeMetaKeeper.addPipeMeta(pipeMeta);
              logger.ifPresent(l -> l.debug(ConfigNodeMessages.RECORDING_PIPE_META, pipeMeta));
            });

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public boolean isStoppedByRuntimeException(final String pipeName) {
    acquireReadLock();
    try {
      return isStoppedByRuntimeExceptionInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  public boolean isStoppedByRuntimeException(final String pipeName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return isStoppedByRuntimeExceptionInternal(pipeName, isTableModel);
    } finally {
      releaseReadLock();
    }
  }

  private boolean isStoppedByRuntimeExceptionInternal(final String pipeName) {
    return pipeMetaKeeper.containsPipeMeta(pipeName)
        && pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().getIsStoppedByRuntimeException();
  }

  private boolean isStoppedByRuntimeExceptionInternal(
      final String pipeName, final boolean isTableModel) {
    return pipeMetaKeeper.containsPipeMeta(pipeName, isTableModel)
        && pipeMetaKeeper
            .getPipeMeta(pipeName, isTableModel)
            .getRuntimeMeta()
            .getIsStoppedByRuntimeException();
  }

  /**
   * Clear the exceptions of a pipe locally after it starts successfully.
   *
   * <p>If there are exceptions cleared or flag changed, the messages will then be updated to all
   * the nodes through {@link PipeHandleMetaChangeProcedure}.
   *
   * @param pipeName The name of the pipe to be clear exception
   */
  public void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(final String pipeName) {
    acquireWriteLock();
    try {
      clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
          pipeName, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  public void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(
      final String pipeName, final long exceptionsClearTime) {
    acquireWriteLock();
    try {
      clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
          pipeName, exceptionsClearTime);
    } finally {
      releaseWriteLock();
    }
  }

  public void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(
      final String pipeName, final boolean isTableModel) {
    acquireWriteLock();
    try {
      clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(pipeName, isTableModel);
    } finally {
      releaseWriteLock();
    }
  }

  public void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(
      final String pipeName, final boolean isTableModel, final long exceptionsClearTime) {
    acquireWriteLock();
    try {
      clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
          pipeName, isTableModel, exceptionsClearTime);
    } finally {
      releaseWriteLock();
    }
  }

  private void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
      final String pipeName, final long exceptionsClearTime) {
    clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
        pipeMetaKeeper.getPipeMeta(pipeName), exceptionsClearTime);
  }

  private void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
      final String pipeName, final boolean isTableModel) {
    clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
        pipeMetaKeeper.getPipeMeta(pipeName, isTableModel), System.currentTimeMillis());
  }

  private void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
      final String pipeName, final boolean isTableModel, final long exceptionsClearTime) {
    clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
        pipeMetaKeeper.getPipeMeta(pipeName, isTableModel), exceptionsClearTime);
  }

  private void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
      final PipeMeta pipeMeta, final long exceptionsClearTime) {
    if (pipeMeta == null) {
      return;
    }

    final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();

    // To avoid unnecessary retries, we set the isStoppedByRuntimeException flag to false
    runtimeMeta.setIsStoppedByRuntimeException(false);

    runtimeMeta.setExceptionsClearTime(exceptionsClearTime);

    final Map<Integer, PipeRuntimeException> exceptionMap =
        runtimeMeta.getNodeId2PipeRuntimeExceptionMap();
    if (!exceptionMap.isEmpty()) {
      exceptionMap.clear();
    }

    runtimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .values()
        .forEach(
            pipeTaskMeta -> {
              if (pipeTaskMeta.getExceptionMessages().iterator().hasNext()) {
                pipeTaskMeta.clearExceptionMessages();
              }
            });
  }

  public void setIsStoppedByRuntimeExceptionToFalse(final String pipeName) {
    acquireWriteLock();
    try {
      setIsStoppedByRuntimeExceptionToFalseInternal(pipeName);
    } finally {
      releaseWriteLock();
    }
  }

  private void setIsStoppedByRuntimeExceptionToFalseInternal(final String pipeName) {
    if (!pipeMetaKeeper.containsPipeMeta(pipeName)) {
      return;
    }

    pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().setIsStoppedByRuntimeException(false);
  }

  /**
   * Record the exceptions of all pipes locally if they encountered failure when pushing {@link
   * PipeMeta}s to dataNodes.
   *
   * <p>If there are exceptions recorded, the related pipes will be stopped, and the exception
   * messages will then be updated to all the nodes through {@link PipeHandleMetaChangeProcedure}.
   *
   * @param respMap The responseMap after pushing pipe meta
   * @return {@code true} if there are exceptions encountered
   */
  public boolean recordDataNodePushPipeMetaExceptions(
      final Map<Integer, TPushPipeMetaResp> respMap) {
    acquireWriteLock();
    try {
      return recordDataNodePushPipeMetaExceptionsInternal(respMap);
    } finally {
      releaseWriteLock();
    }
  }

  private boolean recordDataNodePushPipeMetaExceptionsInternal(
      final Map<Integer, TPushPipeMetaResp> respMap) {
    boolean hasException = false;

    for (final Map.Entry<Integer, TPushPipeMetaResp> respEntry : respMap.entrySet()) {
      final int dataNodeId = respEntry.getKey();
      final TPushPipeMetaResp resp = respEntry.getValue();

      if (resp.getStatus().getCode() == TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()) {
        hasException = true;

        if (!resp.isSetExceptionMessages()) {
          // The pushPipeMeta process on dataNode encountered internal errors
          continue;
        }

        for (final TPushPipeMetaRespExceptionMessage message : resp.getExceptionMessages()) {
          final PipeMeta pipeMeta =
              message.isSetCreationTime()
                  ? pipeMetaKeeper.getPipeMeta(message.getPipeName(), message.getCreationTime())
                  : pipeMetaKeeper.getPipeMeta(message.getPipeName());
          if (pipeMeta == null) {
            continue;
          }

          final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();

          // Keep user-stopped pipes out of the auto-restart flow. Otherwise, a failed STOPPED meta
          // sync can turn a manually stopped pipe into a runtime-stopped one and the next
          // PipeMetaSyncer round will restart it automatically.
          if (PipeStatus.STOPPED.equals(runtimeMeta.getStatus().get())
              && !runtimeMeta.getIsStoppedByRuntimeException()) {
            continue;
          }

          // Mark the status of the pipe with exception as stopped
          runtimeMeta.getStatus().set(PipeStatus.STOPPED);
          runtimeMeta.setIsStoppedByRuntimeException(true);

          final Map<Integer, PipeRuntimeException> exceptionMap =
              runtimeMeta.getNodeId2PipeRuntimeExceptionMap();
          if (!exceptionMap.containsKey(dataNodeId)
              || exceptionMap.get(dataNodeId).getTimeStamp() < message.getTimeStamp()) {
            exceptionMap.put(
                dataNodeId,
                new PipeRuntimeCriticalException(message.getMessage(), message.getTimeStamp()));
          }
        }
      }
    }

    return hasException;
  }

  public boolean autoRestart() {
    acquireWriteLock();
    try {
      return autoRestartInternal();
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Set the statuses of all the pipes stopped automatically because of critical exceptions to
   * {@link PipeStatus#RUNNING} in order to restart them.
   *
   * @return {@code true} if there are pipes need restarting
   */
  private boolean autoRestartInternal() {
    final AtomicBoolean needRestart = new AtomicBoolean(false);
    final long exceptionsClearTime = System.currentTimeMillis();
    final List<String> pipeToRestart = new LinkedList<>();

    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
              if (runtimeMeta.getIsStoppedByRuntimeException()) {
                runtimeMeta.setExceptionsClearTime(exceptionsClearTime);
                runtimeMeta.getStatus().set(PipeStatus.RUNNING);

                needRestart.set(true);
                pipeToRestart.add(pipeMeta.getStaticMeta().getPipeName());
              }
            });

    if (needRestart.get()) {
      PipeLogger.log(
          LOGGER::info,
          ConfigNodeMessages.PIPEMETASYNCER_IS_TRYING_TO_RESTART_THE_PIPES,
          pipeToRestart);
    }
    return needRestart.get();
  }

  public void handleSuccessfulRestart() {
    acquireWriteLock();
    try {
      handleSuccessfulRestartInternal();
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Clear the exceptions to, and set the isAutoStopped flag to false for the successfully restarted
   * pipe.
   */
  private void handleSuccessfulRestartInternal() {
    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
              if (runtimeMeta.getStatus().get().equals(PipeStatus.RUNNING)
                  && runtimeMeta.getIsStoppedByRuntimeException()) {
                clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(
                    pipeMeta, runtimeMeta.getExceptionsClearTime());
              }
            });
  }

  public void removePipeMeta(final String pipeName) {
    acquireWriteLock();
    try {
      removePipeMetaInternal(pipeName);
    } finally {
      releaseWriteLock();
    }
  }

  public void removePipeMeta(final PipeStaticMeta pipeStaticMeta) {
    acquireWriteLock();
    try {
      removePipeMetaInternal(pipeStaticMeta);
    } finally {
      releaseWriteLock();
    }
  }

  private void removePipeMetaInternal(final String pipeName) {
    pipeMetaKeeper.removePipeMeta(pipeName);
  }

  private void removePipeMetaInternal(final PipeStaticMeta pipeStaticMeta) {
    pipeMetaKeeper.removePipeMeta(pipeStaticMeta);
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  @Override
  public boolean processTakeSnapshot(final File snapshotDir) throws IOException {
    acquireReadLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (snapshotFile.exists() && snapshotFile.isFile()) {
        LOGGER.error(
            ConfigNodeMessages.FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY_EXIST,
            snapshotFile.getAbsolutePath());
        return false;
      }

      try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
        pipeMetaKeeper.processTakeSnapshot(fileOutputStream);
        fileOutputStream.getFD().sync();
      }
      return true;
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void processLoadSnapshot(final File snapshotDir) throws IOException {
    acquireWriteLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (!snapshotFile.exists() || !snapshotFile.isFile()) {
        LOGGER.error(
            ConfigNodeMessages.FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST_2,
            snapshotFile.getAbsolutePath());
        return;
      }

      try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
        pipeMetaKeeper.processLoadSnapshot(fileInputStream);
      }
      normalizeRecoveredConsensusPipeStatus();
    } finally {
      releaseWriteLock();
    }
  }

  public void enrichPipeMetasWithRootUserForCompatibility() {
    acquireWriteLock();
    try {
      pipeMetaKeeper
          .getPipeMetaList()
          .forEach(
              pipeMeta -> enrichPipeMetaWithRootUserForCompatibility(pipeMeta.getStaticMeta()));
    } finally {
      releaseWriteLock();
    }
  }

  private void enrichPipeMetaWithRootUserForCompatibility(final PipeStaticMeta pipeStaticMeta) {
    if (pipeUserCurrentPasswordProvider == null) {
      return;
    }
    final boolean shouldEnrichSource = pipeStaticMeta.mayNeedCompatibleRootUserForIoTDBSource();
    final boolean shouldEnrichSink = pipeStaticMeta.mayNeedCompatibleRootUserForWriteBackSink();
    if (!shouldEnrichSource && !shouldEnrichSink) {
      return;
    }

    final String rootUserName = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
    final String password = pipeUserCurrentPasswordProvider.apply(rootUserName);
    if (Objects.isNull(password)) {
      throw new PipeException(
          String.format(
              ConfigNodeMessages
                  .FAILED_TO_ENRICH_PIPE_WITH_ROOT_USER_FOR_COMPATIBILITY_BECAUSE_ROOT_USER_DOES_NOT_EXIST,
              pipeStaticMeta.getPipeName(),
              rootUserName));
    }

    if (shouldEnrichSource) {
      pipeStaticMeta.enrichSourceWithRootUserForCompatibility(rootUserName, password);
    }
    if (shouldEnrichSink) {
      pipeStaticMeta.enrichWriteBackSinkWithRootUserForCompatibility(rootUserName, password);
    }
  }

  private void normalizeRecoveredConsensusPipeStatus() {
    final List<String> restartedConsensusPipes = new ArrayList<>();

    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
              if (!PipeType.CONSENSUS.equals(pipeMeta.getStaticMeta().getPipeType())
                  || !PipeStatus.STOPPED.equals(runtimeMeta.getStatus().get())
                  || runtimeMeta.getIsStoppedByRuntimeException()) {
                return;
              }

              runtimeMeta.getStatus().set(PipeStatus.RUNNING);
              restartedConsensusPipes.add(pipeMeta.getStaticMeta().getPipeName());
            });

    if (!restartedConsensusPipes.isEmpty()) {
      LOGGER.info(
          ConfigNodeMessages.RECOVERED_CONSENSUS_PIPES_AS_RUNNING_DURING_SNAPSHOT_LOAD,
          restartedConsensusPipes);
    }
  }

  /////////////////////////////// hashCode & equals ///////////////////////////////

  @Override
  public int hashCode() {
    return pipeMetaKeeper.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTaskInfo other = (PipeTaskInfo) obj;
    return pipeMetaKeeper.equals(other.pipeMetaKeeper);
  }

  @Override
  public String toString() {
    return pipeMetaKeeper.toString();
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public long runningPipeCount() {
    return pipeMetaKeeper.runningPipeCount();
  }

  public long droppedPipeCount() {
    return pipeMetaKeeper.droppedPipeCount();
  }

  public long userStoppedPipeCount() {
    return pipeMetaKeeper.userStoppedPipeCount();
  }

  public long exceptionStoppedPipeCount() {
    return pipeMetaKeeper.exceptionStoppedPipeCount();
  }
}
