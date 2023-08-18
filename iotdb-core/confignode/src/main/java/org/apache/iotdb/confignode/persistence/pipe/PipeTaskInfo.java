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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PipeTaskInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskInfo.class);
  private static final String SNAPSHOT_FILE_NAME = "pipe_task_info.bin";

  private final PipeMetaKeeper pipeMetaKeeper;

  public PipeTaskInfo() {
    this.pipeMetaKeeper = new PipeMetaKeeper();
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
  }

  private void releaseWriteLock() {
    pipeMetaKeeper.releaseWriteLock();
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
    LOGGER.info(exceptionMessage);
    throw new PipeException(exceptionMessage);
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
      LOGGER.info(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName);
    if (pipeStatus == PipeStatus.RUNNING) {
      final String exceptionMessage =
          String.format("Failed to start pipe %s, the pipe is already running", pipeName);
      LOGGER.info(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format("Failed to start pipe %s, the pipe is already dropped", pipeName);
      LOGGER.info(exceptionMessage);
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
      LOGGER.info(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeStatus pipeStatus = getPipeStatus(pipeName);
    if (pipeStatus == PipeStatus.STOPPED) {
      final String exceptionMessage =
          String.format("Failed to stop pipe %s, the pipe is already stop", pipeName);
      LOGGER.info(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
    if (pipeStatus == PipeStatus.DROPPED) {
      final String exceptionMessage =
          String.format("Failed to stop pipe %s, the pipe is already dropped", pipeName);
      LOGGER.info(exceptionMessage);
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

  /////////////////////////////// Pipe Task Management ///////////////////////////////

  public TSStatus createPipe(CreatePipePlanV2 plan) {
    acquireWriteLock();
    try {
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
      pipeMetaKeeper
          .getPipeMeta(plan.getPipeName())
          .getRuntimeMeta()
          .getStatus()
          .set(plan.getPipeStatus());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseWriteLock();
    }
  }

  public TSStatus dropPipe(DropPipePlanV2 plan) {
    acquireWriteLock();
    try {
      pipeMetaKeeper.removePipeMeta(plan.getPipeName());
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

  /** Handle the data region leader change event and update the pipe task meta accordingly. */
  public TSStatus handleLeaderChange(PipeHandleLeaderChangePlan plan) {
    acquireWriteLock();
    try {
      return handleLeaderChangeInternal(plan);
    } finally {
      releaseWriteLock();
    }
  }

  private TSStatus handleLeaderChangeInternal(PipeHandleLeaderChangePlan plan) {
    plan.getConsensusGroupId2NewDataRegionLeaderIdMap()
        .forEach(
            (dataRegionGroupId, newDataRegionLeader) ->
                pipeMetaKeeper
                    .getPipeMetaList()
                    .forEach(
                        pipeMeta -> {
                          final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMap =
                              pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap();

                          if (consensusGroupIdToTaskMetaMap.containsKey(dataRegionGroupId)) {
                            // If the data region leader is -1, it means the data region is
                            // removed
                            if (newDataRegionLeader != -1) {
                              consensusGroupIdToTaskMetaMap
                                  .get(dataRegionGroupId)
                                  .setLeaderDataNodeId(newDataRegionLeader);
                            } else {
                              consensusGroupIdToTaskMetaMap.remove(dataRegionGroupId);
                            }
                          } else {
                            // If CN does not contain the data region group, it means the data
                            // region group is newly added.
                            if (newDataRegionLeader != -1) {
                              consensusGroupIdToTaskMetaMap.put(
                                  dataRegionGroupId,
                                  new PipeTaskMeta(
                                      new MinimumProgressIndex(), newDataRegionLeader));
                            }
                            // else:
                            // "The pipe task meta does not contain the data region group {} or
                            // the data region group has already been removed"
                          }
                        }));

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Replace the local pipeMetas by the pipeMetas from the leader ConfigNode.
   *
   * @param plan The plan containing all the pipeMetas from leader ConfigNode
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

    plan.getPipeMetaList()
        .forEach(
            pipeMeta -> {
              pipeMetaKeeper.addPipeMeta(pipeMeta.getStaticMeta().getPipeName(), pipeMeta);
              LOGGER.info("Recording pipe meta: {}", pipeMeta);
            });

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public boolean hasExceptions(String pipeName) {
    acquireReadLock();
    try {
      return hasExceptionsInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private boolean hasExceptionsInternal(String pipeName) {
    if (!pipeMetaKeeper.containsPipeMeta(pipeName)) {
      return false;
    }

    final PipeRuntimeMeta runtimeMeta = pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta();
    final Map<Integer, PipeRuntimeException> exceptionMap =
        runtimeMeta.getDataNodeId2PipeRuntimeExceptionMap();

    if (!exceptionMap.isEmpty()) {
      return true;
    }

    final AtomicBoolean hasException = new AtomicBoolean(false);
    runtimeMeta
        .getConsensusGroupId2TaskMetaMap()
        .values()
        .forEach(
            pipeTaskMeta -> {
              if (pipeTaskMeta.getExceptionMessages().iterator().hasNext()) {
                hasException.set(true);
              }
            });
    return hasException.get();
  }

  public boolean isStoppedByRuntimeException(String pipeName) {
    acquireReadLock();
    try {
      return isStoppedByRuntimeExceptionInternal(pipeName);
    } finally {
      releaseReadLock();
    }
  }

  private boolean isStoppedByRuntimeExceptionInternal(String pipeName) {
    return pipeMetaKeeper.containsPipeMeta(pipeName)
        && pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().getIsStoppedByRuntimeException();
  }

  /**
   * Clear the exceptions of, and set the isAutoStopped flag to false for a pipe locally after it
   * starts successfully.
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

    // To avoid unnecessary retries, we set the isStoppedByRuntimeException flag to false
    runtimeMeta.setIsStoppedByRuntimeException(false);

    runtimeMeta.setExceptionsClearTime(System.currentTimeMillis());

    final Map<Integer, PipeRuntimeException> exceptionMap =
        runtimeMeta.getDataNodeId2PipeRuntimeExceptionMap();
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

  public void setIsStoppedByRuntimeExceptionToFalse(String pipeName) {
    acquireWriteLock();
    try {
      setIsStoppedByRuntimeExceptionToFalseInternal(pipeName);
    } finally {
      releaseWriteLock();
    }
  }

  private void setIsStoppedByRuntimeExceptionToFalseInternal(String pipeName) {
    if (!pipeMetaKeeper.containsPipeMeta(pipeName)) {
      return;
    }

    pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().setIsStoppedByRuntimeException(false);
  }

  /**
   * Record the exceptions of all pipes locally if they encountered failure when pushing pipe meta.
   *
   * <p>If there are exceptions recorded, the related pipes will be stopped, and the exception
   * messages will then be updated to all the nodes through {@link PipeHandleMetaChangeProcedure}.
   *
   * @param respMap The responseMap after pushing pipe meta
   * @return true if there are exceptions encountered
   */
  public boolean recordPushPipeMetaExceptions(Map<Integer, TPushPipeMetaResp> respMap) {
    acquireWriteLock();
    try {
      return recordPushPipeMetaExceptionsInternal(respMap);
    } finally {
      releaseWriteLock();
    }
  }

  private boolean recordPushPipeMetaExceptionsInternal(Map<Integer, TPushPipeMetaResp> respMap) {
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
                    runtimeMeta.setIsStoppedByRuntimeException(true);

                    final Map<Integer, PipeRuntimeException> exceptionMap =
                        runtimeMeta.getDataNodeId2PipeRuntimeExceptionMap();
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
   * @return true if there are pipes need restarting
   */
  private boolean autoRestartInternal() {
    final AtomicBoolean needRestart = new AtomicBoolean(false);
    final List<String> pipeToRestart = new LinkedList<>();

    pipeMetaKeeper
        .getPipeMetaList()
        .forEach(
            pipeMeta -> {
              if (pipeMeta.getRuntimeMeta().getIsStoppedByRuntimeException()) {
                pipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);

                needRestart.set(true);
                pipeToRestart.add(pipeMeta.getStaticMeta().getPipeName());
              }
            });

    if (needRestart.get()) {
      LOGGER.info("PipeMetaSyncer is trying to restart the pipes: {}", pipeToRestart);
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
   * Clear the exceptions of, and set the isAutoStopped flag to false for the successfully restarted
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
}
