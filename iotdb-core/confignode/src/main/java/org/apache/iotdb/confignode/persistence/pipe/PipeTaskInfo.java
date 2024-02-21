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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.transfer.extractor.ConfigPlanListeningQueue;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.service.ConfigNode;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PipeTaskInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskInfo.class);
  private static final String SNAPSHOT_FILE_NAME = "pipe_task_info.bin";

  private final PipeMetaKeeper pipeMetaKeeper;

  // Pure in-memory object, not involved in snapshot serialization and deserialization.
  private final PipeTaskInfoVersion pipeTaskInfoVersion;

  public PipeTaskInfo() {
    this.pipeMetaKeeper = new PipeMetaKeeper();
    this.pipeTaskInfoVersion = new PipeTaskInfoVersion();
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

  public void checkBeforeCreatePipe(TCreatePipeReq createPipeRequest) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeCreatePipeInternal(createPipeRequest);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeCreatePipeInternal(TCreatePipeReq createPipeRequest)
      throws PipeException {
    if (!isPipeExisted(createPipeRequest.getPipeName())) {
      return;
    }

    final String exceptionMessage =
        String.format(
            "Failed to create pipe %s, the pipe with the same name has been created",
            createPipeRequest.getPipeName());
    LOGGER.warn(exceptionMessage);
    throw new PipeException(exceptionMessage);
  }

  public void checkAndUpdateRequestBeforeAlterPipe(TAlterPipeReq alterPipeRequest)
      throws PipeException {
    acquireReadLock();
    try {
      checkAndUpdateRequestBeforeAlterPipeInternal(alterPipeRequest);
    } finally {
      releaseReadLock();
    }
  }

  private void checkAndUpdateRequestBeforeAlterPipeInternal(TAlterPipeReq alterPipeRequest)
      throws PipeException {
    if (!isPipeExisted(alterPipeRequest.getPipeName())) {
      final String exceptionMessage =
          String.format(
              "Failed to alter pipe %s, the pipe does not exist", alterPipeRequest.getPipeName());
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    PipeStaticMeta pipeStaticMetaFromCoordinator =
        getPipeMetaByPipeName(alterPipeRequest.getPipeName()).getStaticMeta();
    // deep copy current pipe static meta
    PipeStaticMeta copiedPipeStaticMetaFromCoordinator =
        new PipeStaticMeta(
            pipeStaticMetaFromCoordinator.getPipeName(),
            pipeStaticMetaFromCoordinator.getCreationTime(),
            new HashMap<>(pipeStaticMetaFromCoordinator.getExtractorParameters().getAttribute()),
            new HashMap<>(pipeStaticMetaFromCoordinator.getProcessorParameters().getAttribute()),
            new HashMap<>(pipeStaticMetaFromCoordinator.getConnectorParameters().getAttribute()));

    // 1. In modify mode, based on the passed attributes:
    //   1.1. if they are empty, the original attributes are filled directly.
    //   1.2. Otherwise, corresponding updates on original attributes are performed.
    // 2. In replace mode, do nothing here.
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
            copiedPipeStaticMetaFromCoordinator.getConnectorParameters().getAttribute());
      } else {
        alterPipeRequest.setConnectorAttributes(
            copiedPipeStaticMetaFromCoordinator
                .getConnectorParameters()
                .addOrReplaceEquivalentAttributes(
                    new PipeParameters(alterPipeRequest.getConnectorAttributes()))
                .getAttribute());
      }
    }
  }

  public void checkBeforeStartPipe(String pipeName) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeStartPipeInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeStartPipeInternal(String pipeName) throws PipeException {
    if (!isPipeExisted(pipeName)) {
      final String exceptionMessage =
          String.format("Failed to start pipe %s, the pipe does not exist", pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName);
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format("Failed to start pipe %s, the pipe is already dropped", pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  public void checkBeforeStopPipe(String pipeName) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeStopPipeInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeStopPipeInternal(String pipeName) throws PipeException {
    if (!isPipeExisted(pipeName)) {
      final String exceptionMessage =
          String.format("Failed to stop pipe %s, the pipe does not exist", pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName);
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format("Failed to stop pipe %s, the pipe is already dropped", pipeName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  public void checkBeforeDropPipe(String pipeName) {
    acquireReadLock();
    try {
      checkBeforeDropPipeInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeDropPipeInternal(String pipeName) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Check before drop pipe {}, pipe exists: {}.",
          pipeName,
          isPipeExisted(pipeName) ? "true" : "false");
    }
    // No matter whether the pipe exists, we allow the drop operation executed on all nodes to
    // ensure the consistency.
    // DO NOTHING HERE!
  }

  public boolean isPipeExisted(String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private PipeStatus getPipeStatus(String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().getStatus().get();
    } finally {
      releaseReadLock();
    }
  }

  public boolean isPipeRunning(String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName)
          && PipeStatus.RUNNING.equals(getPipeStatus(pipeName));
    } finally {
      releaseReadLock();
    }
  }

  public boolean isPipeStoppedByUser(String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.containsPipeMeta(pipeName)
          && PipeStatus.STOPPED.equals(getPipeStatus(pipeName))
          && !shouldBeRunning(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////// Pipe Task Management ///////////////////////////////

  public TSStatus createPipe(CreatePipePlanV2 plan) {
    acquireWriteLock();
    try {
      pipeMetaKeeper.addPipeMeta(
          plan.getPipeStaticMeta().getPipeName(),
          new PipeMeta(plan.getPipeStaticMeta(), plan.getPipeRuntimeMeta()));
      handleSinglePipeMetaChangeOnConfigTaskAgent(
          new PipeMeta(plan.getPipeStaticMeta(), plan.getPipeRuntimeMeta()));
      ConfigPlanListeningQueue.getInstance()
          .increaseReferenceCountForListeningPipe(
              plan.getPipeStaticMeta().getExtractorParameters());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Failed to create pipe, because " + e.getMessage());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus alterPipe(AlterPipePlanV2 plan) {
    acquireWriteLock();
    try {
      pipeMetaKeeper.removePipeMeta(plan.getPipeStaticMeta().getPipeName());
      pipeMetaKeeper.addPipeMeta(
          plan.getPipeStaticMeta().getPipeName(),
          new PipeMeta(plan.getPipeStaticMeta(), plan.getPipeRuntimeMeta()));
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus setPipeStatus(SetPipeStatusPlanV2 plan) {
    acquireWriteLock();
    try {
      PipeMeta pipeMeta = pipeMetaKeeper.getPipeMeta(plan.getPipeName());
      PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
      runtimeMeta.getStatus().set(plan.getPipeStatus());
      runtimeMeta.setShouldBeRunning(plan.getPipeStatus() == PipeStatus.RUNNING);
      handleSinglePipeMetaChangeOnConfigTaskAgent(pipeMeta);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Failed to set pipe status, because " + e.getMessage());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus dropPipe(DropPipePlanV2 plan) {
    acquireWriteLock();
    try {
      String pipeName = plan.getPipeName();
      if (pipeMetaKeeper.containsPipeMeta(pipeName)) {
        ConfigPlanListeningQueue.getInstance()
            .decreaseReferenceCountForListeningPipe(
                pipeMetaKeeper
                    .getPipeMetaByPipeName(pipeName)
                    .getStaticMeta()
                    .getExtractorParameters());
        dropPipeOnConfigTaskAgent(pipeName);
      }
      pipeMetaKeeper.removePipeMeta(pipeName);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Failed to drop pipe, because " + e.getMessage());
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

  public PipeMeta getPipeMetaByPipeName(String pipeName) {
    acquireReadLock();
    try {
      return pipeMetaKeeper.getPipeMetaByPipeName(pipeName);
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
  public TSStatus handleLeaderChange(PipeHandleLeaderChangePlan plan) {
    acquireWriteLock();
    try {
      return handleLeaderChangeInternal(plan);
    } finally {
      releaseWriteLock();
    }
  }

  private TSStatus handleLeaderChangeInternal(PipeHandleLeaderChangePlan plan) {
    plan.getConsensusGroupId2NewLeaderIdMap()
        .forEach(
            (consensusGroupId, newLeader) ->
                pipeMetaKeeper
                    .getPipeMetaList()
                    .forEach(
                        pipeMeta -> {
                          final Map<Integer, PipeTaskMeta> consensusGroupIdToTaskMetaMap =
                              pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();

                          if (consensusGroupIdToTaskMetaMap.containsKey(consensusGroupId.getId())) {
                            // If the region leader is -1, it means the region is
                            // removed
                            if (newLeader != -1) {
                              consensusGroupIdToTaskMetaMap
                                  .get(consensusGroupId.getId())
                                  .setLeaderNodeId(newLeader);
                            } else {
                              consensusGroupIdToTaskMetaMap.remove(consensusGroupId.getId());
                            }
                          } else {
                            // If CN does not contain the region group, it means the data
                            // region group is newly added.
                            if (newLeader != -1) {
                              consensusGroupIdToTaskMetaMap.put(
                                  consensusGroupId.getId(),
                                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, newLeader));
                            }
                            // else:
                            // "The pipe task meta does not contain the data region group {} or
                            // the data region group has already been removed"
                          }
                        }));

    try {
      // Notify configNode agent to handle config leader change
      handlePipeMetaChangesOnConfigTaskAgent();
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Failed to handle leader change on configNode, because " + e.getMessage());
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Replace the local {@link PipeMeta}s by the {@link PipeMeta}s from the leader {@link
   * ConfigNode}.
   *
   * @param plan The plan containing all the {@link PipeMeta}s from leader {@link ConfigNode}
   * @return {@link TSStatusCode#SUCCESS_STATUS}
   */
  public TSStatus handleMetaChanges(PipeHandleMetaChangePlan plan) {
    acquireWriteLock();
    try {
      return handleMetaChangesInternal(plan);
    } finally {
      releaseWriteLock();
    }
  }

  private TSStatus handleMetaChangesInternal(PipeHandleMetaChangePlan plan) {
    LOGGER.info("Handling pipe meta changes ...");

    pipeMetaKeeper.clear();

    AtomicLong newFirstIndex = new AtomicLong(Long.MAX_VALUE);

    plan.getPipeMetaList()
        .forEach(
            pipeMeta -> {
              pipeMetaKeeper.addPipeMeta(pipeMeta.getStaticMeta().getPipeName(), pipeMeta);
              LOGGER.info("Recording pipe meta: {}", pipeMeta);

              ProgressIndex configIndex =
                  pipeMeta
                      .getRuntimeMeta()
                      .getConsensusGroupId2TaskMetaMap()
                      .get(Integer.MIN_VALUE)
                      .getProgressIndex();
              if (configIndex instanceof MetaProgressIndex
                  && ((MetaProgressIndex) configIndex).getIndex() + 1 < newFirstIndex.get()) {
                // The index itself is committed, thus can be removed
                newFirstIndex.set(((MetaProgressIndex) configIndex).getIndex() + 1);
              } else {
                // Do not clear "minimumProgressIndex"s related queues to avoid clearing
                // the queue when there are schema tasks just started and transferring
                newFirstIndex.set(0);
              }
            });

    ConfigPlanListeningQueue.getInstance().removeBefore(newFirstIndex.get());

    // No need to handle meta changes on configNodeAgent here since pipeMetas here only change on
    // follower
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public boolean shouldBeRunning(String pipeName) {
    acquireReadLock();
    try {
      return shouldBeRunningInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private boolean shouldBeRunningInternal(String pipeName) {
    return pipeMetaKeeper.containsPipeMeta(pipeName)
        && pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().getShouldBeRunning();
  }

  /**
   * Clear the exceptions of a pipe locally after it starts successfully.
   *
   * <p>If there are exceptions cleared or flag changed, the messages will then be updated to all
   * the nodes through {@link PipeHandleMetaChangeProcedure}.
   *
   * @param pipeName The name of the pipe to be clear exception
   */
  public void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(String pipeName) {
    acquireWriteLock();
    try {
      clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(pipeName);
    } finally {
      releaseWriteLock();
    }
  }

  private void clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalseInternal(String pipeName) {
    if (!pipeMetaKeeper.containsPipeMeta(pipeName)) {
      return;
    }

    final PipeRuntimeMeta runtimeMeta = pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta();

    runtimeMeta.setExceptionsClearTime(System.currentTimeMillis());

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

  /**
   * Record the exceptions of all pipes locally if they encountered failure when pushing {@link
   * PipeMeta}s to dataNodes.
   *
   * <p>If there are exceptions recorded, the related pipes will be stopped, and the exception
   * messages will then be updated to all the nodes through {@link PipeHandleMetaChangeProcedure}.
   *
   * @param respMap The responseMap after pushing pipe meta
   * @return {@link true} if there are exceptions encountered
   */
  public boolean recordDataNodePushPipeMetaExceptions(Map<Integer, TPushPipeMetaResp> respMap) {
    acquireWriteLock();
    try {
      return recordDataNodePushPipeMetaExceptionsInternal(respMap);
    } finally {
      releaseWriteLock();
    }
  }

  private boolean recordDataNodePushPipeMetaExceptionsInternal(
      Map<Integer, TPushPipeMetaResp> respMap) {
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

        resp.getExceptionMessages()
            .forEach(
                message -> {
                  if (pipeMetaKeeper.containsPipeMeta(message.getPipeName())) {
                    final PipeRuntimeMeta runtimeMeta =
                        pipeMetaKeeper.getPipeMeta(message.getPipeName()).getRuntimeMeta();

                    // Mark the status of the pipe with exception as stopped
                    runtimeMeta.getStatus().set(PipeStatus.STOPPED);

                    final Map<Integer, PipeRuntimeException> exceptionMap =
                        runtimeMeta.getNodeId2PipeRuntimeExceptionMap();
                    if (!exceptionMap.containsKey(dataNodeId)
                        || exceptionMap.get(dataNodeId).getTimeStamp() < message.getTimeStamp()) {
                      exceptionMap.put(
                          dataNodeId,
                          new PipeRuntimeCriticalException(
                              message.getMessage(), message.getTimeStamp()));
                    }
                  }
                });
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
    final List<String> pipeToRestart = new LinkedList<>();

    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
              if (runtimeMeta.getShouldBeRunning()
                  && runtimeMeta.getStatus().get().equals(PipeStatus.STOPPED)) {
                runtimeMeta.getStatus().set(PipeStatus.RUNNING);

                needRestart.set(true);
                pipeToRestart.add(pipeMeta.getStaticMeta().getPipeName());
              }
            });

    if (needRestart.get()) {
      LOGGER.info("PipeMetaSyncer is trying to restart the pipes: {}", pipeToRestart);
    }

    handlePipeMetaChangesOnConfigTaskAgent();
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
              if (pipeMeta.getRuntimeMeta().getStatus().get().equals(PipeStatus.RUNNING)) {
                clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(
                    pipeMeta.getStaticMeta().getPipeName());
              }
            });
  }

  /////////////////////////////// ConfigTask ///////////////////////////////

  private void dropPipeOnConfigTaskAgent(String pipeName) {
    // Operate tasks only after leader gets ready
    if (!ConfigPlanListeningQueue.getInstance().isLeaderReady()) {
      return;
    }
    TPushPipeMetaRespExceptionMessage message = PipeConfigNodeAgent.task().handleDropPipe(pipeName);
    if (message != null) {
      pipeMetaKeeper
          .getPipeMeta(message.getPipeName())
          .getRuntimeMeta()
          .getNodeId2PipeRuntimeExceptionMap()
          .put(
              ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
              new PipeRuntimeCriticalException(message.getMessage(), message.getTimeStamp()));
    }
  }

  private void handleSinglePipeMetaChangeOnConfigTaskAgent(PipeMeta pipeMeta) {
    // Operate tasks only after leader gets ready
    if (!ConfigPlanListeningQueue.getInstance().isLeaderReady()) {
      return;
    }
    // The new agent meta has separated status to enable control by diff
    // Yet the taskMetaMap is reused to let configNode pipe report directly to the
    // original meta. No lock is needed since the configNode taskMeta is only
    // altered by one pipe.
    PipeMeta agentMeta =
        new PipeMeta(
            pipeMeta.getStaticMeta(),
            new PipeRuntimeMeta(pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap()));
    agentMeta.getRuntimeMeta().getStatus().set(pipeMeta.getRuntimeMeta().getStatus().get());

    TPushPipeMetaRespExceptionMessage message =
        PipeConfigNodeAgent.task().handleSinglePipeMetaChanges(agentMeta);
    if (message != null) {
      pipeMetaKeeper
          .getPipeMeta(message.getPipeName())
          .getRuntimeMeta()
          .getNodeId2PipeRuntimeExceptionMap()
          .put(
              ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
              new PipeRuntimeCriticalException(message.getMessage(), message.getTimeStamp()));
    }
  }

  public void handlePipeMetaChangesOnConfigTaskAgent() {
    // Operate tasks only after leader get ready
    if (!ConfigPlanListeningQueue.getInstance().isLeaderReady()) {
      return;
    }
    List<PipeMeta> pipeMetas = new ArrayList<>();
    for (PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
      // The new agent meta has separated status to enable control by diff
      // Yet the taskMetaMap is reused to let configNode pipe report directly to the
      // original meta. No lock is needed since the configNode taskMeta is only
      // altered by one pipe.
      PipeMeta agentMeta =
          new PipeMeta(
              pipeMeta.getStaticMeta(),
              new PipeRuntimeMeta(pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap()));
      agentMeta.getRuntimeMeta().getStatus().set(pipeMeta.getRuntimeMeta().getStatus().get());
      pipeMetas.add(agentMeta);
    }
    PipeConfigNodeAgent.task()
        .handlePipeMetaChanges(pipeMetas)
        .forEach(
            message ->
                pipeMetaKeeper
                    .getPipeMeta(message.getPipeName())
                    .getRuntimeMeta()
                    .getNodeId2PipeRuntimeExceptionMap()
                    .put(
                        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
                        new PipeRuntimeCriticalException(
                            message.getMessage(), message.getTimeStamp())));
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    acquireReadLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (snapshotFile.exists() && snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to take snapshot, because snapshot file [{}] is already exist.",
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
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    acquireWriteLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (!snapshotFile.exists() || !snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to load snapshot,snapshot file [{}] is not exist.",
            snapshotFile.getAbsolutePath());
        return;
      }

      try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
        pipeMetaKeeper.processLoadSnapshot(fileInputStream);
      }
      // We initialize reference count of listening pipes here to avoid separate
      // serialization of it
      for (PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
        ConfigPlanListeningQueue.getInstance()
            .increaseReferenceCountForListeningPipe(
                pipeMeta.getStaticMeta().getExtractorParameters());
      }
    } catch (IllegalPathException e) {
      LOGGER.warn(
          "Failed to increase reference count for listening pipe, PipeMetas: {}",
          pipeMetaKeeper.getPipeMetaList());
    } finally {
      releaseWriteLock();
    }
  }

  /////////////////////////////// hashCode & equals ///////////////////////////////

  @Override
  public int hashCode() {
    return pipeMetaKeeper.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeTaskInfo other = (PipeTaskInfo) obj;
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
