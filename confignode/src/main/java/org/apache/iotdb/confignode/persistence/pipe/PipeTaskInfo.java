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
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PipeTaskInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskInfo.class);
  private static final String SNAPSHOT_FILE_NAME = "pipe_task_info.bin";

  private final ReentrantLock pipeTaskInfoLock = new ReentrantLock();

  private final PipeMetaKeeper pipeMetaKeeper;

  public PipeTaskInfo() {
    this.pipeMetaKeeper = new PipeMetaKeeper();
  }

  /////////////////////////////// Lock ///////////////////////////////

  public void acquirePipeTaskInfoLock() {
    pipeTaskInfoLock.lock();
  }

  public void releasePipeTaskInfoLock() {
    pipeTaskInfoLock.unlock();
  }

  /////////////////////////////// Validator ///////////////////////////////

  public void checkBeforeCreatePipe(TCreatePipeReq createPipeRequest) throws PipeException {
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Check before drop pipe {}, pipe exists: {}.",
          pipeName,
          isPipeExisted(pipeName) ? "true" : "false");
    }
    // no matter whether the pipe exists, we allow the drop operation executed on all nodes to
    // ensure the consistency.
    // DO NOTHING HERE!
  }

  public boolean isPipeExisted(String pipeName) {
    return pipeMetaKeeper.containsPipeMeta(pipeName);
  }

  private PipeStatus getPipeStatus(String pipeName) {
    return pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().getStatus().get();
  }

  /////////////////////////////// Pipe Task Management ///////////////////////////////

  public TSStatus createPipe(CreatePipePlanV2 plan) {
    pipeMetaKeeper.addPipeMeta(
        plan.getPipeStaticMeta().getPipeName(),
        new PipeMeta(plan.getPipeStaticMeta(), plan.getPipeRuntimeMeta()));
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus setPipeStatus(SetPipeStatusPlanV2 plan) {
    pipeMetaKeeper
        .getPipeMeta(plan.getPipeName())
        .getRuntimeMeta()
        .getStatus()
        .set(plan.getPipeStatus());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus dropPipe(DropPipePlanV2 plan) {
    pipeMetaKeeper.removePipeMeta(plan.getPipeName());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public DataSet showPipes() {
    return new PipeTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        StreamSupport.stream(getPipeMetaList().spliterator(), false).collect(Collectors.toList()));
  }

  public Iterable<PipeMeta> getPipeMetaList() {
    return pipeMetaKeeper.getPipeMetaList();
  }

  public boolean isEmpty() {
    return pipeMetaKeeper.isEmpty();
  }

  /////////////////////////////// Pipe Runtime Management ///////////////////////////////

  /** handle the data region leader change event and update the pipe task meta accordingly */
  public TSStatus handleLeaderChange(PipeHandleLeaderChangePlan plan) {
    plan.getConsensusGroupId2NewDataRegionLeaderIdMap()
        .forEach(
            (dataRegionGroupId, newDataRegionLeader) ->
                pipeMetaKeeper
                    .getPipeMetaList()
                    .forEach(
                        pipeMeta -> {
                          final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMap =
                              pipeMeta.getRuntimeMeta().getConsensusGroupIdToTaskMetaMap();

                          if (consensusGroupIdToTaskMetaMap.containsKey(dataRegionGroupId)) {
                            // if the data region leader is -1, it means the data region is
                            // removed
                            if (newDataRegionLeader != -1) {
                              consensusGroupIdToTaskMetaMap
                                  .get(dataRegionGroupId)
                                  .setLeaderDataNodeId(newDataRegionLeader);
                            } else {
                              consensusGroupIdToTaskMetaMap.remove(dataRegionGroupId);
                            }
                          } else {
                            // if CN does not contain the data region group, it means the data
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

  public TSStatus handleMetaChanges(PipeHandleMetaChangePlan plan) {
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

  public void clearPipeMetaExceptionMessages(String pipeName) {
    pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().clearExceptionMessages();
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      pipeMetaKeeper.processTakeSnapshot(fileOutputStream);
    }
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      pipeMetaKeeper.processLoadSnapshot(fileInputStream);
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
