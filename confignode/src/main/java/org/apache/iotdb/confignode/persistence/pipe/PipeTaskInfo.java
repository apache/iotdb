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
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

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

  public boolean checkBeforeCreatePipe(TCreatePipeReq createPipeRequest) {
    if (!isPipeExisted(createPipeRequest.getPipeName())) {
      return true;
    }

    LOGGER.info(
        String.format(
            "Failed to create pipe [%s], the pipe with the same name has been created",
            createPipeRequest.getPipeName()));
    return false;
  }

  public boolean checkBeforeStartPipe(String pipeName) {
    if (!isPipeExisted(pipeName)) {
      LOGGER.info(String.format("Failed to start pipe [%s], the pipe does not exist", pipeName));
      return false;
    }

    if (getPipeStatus(pipeName) == PipeStatus.RUNNING) {
      LOGGER.info(
          String.format("Failed to start pipe [%s], the pipe is already running", pipeName));
      return false;
    }

    return true;
  }

  public boolean checkBeforeStopPipe(String pipeName) {
    if (!isPipeExisted(pipeName)) {
      LOGGER.info(String.format("Failed to stop pipe [%s], the pipe does not exist", pipeName));
      return false;
    }

    if (getPipeStatus(pipeName) == PipeStatus.STOPPED) {
      LOGGER.info(String.format("Failed to stop pipe [%s], the pipe is already stop", pipeName));
      return false;
    }

    return true;
  }

  public boolean checkBeforeDropPipe(String pipeName) {
    if (isPipeExisted(pipeName)) {
      return true;
    }

    LOGGER.info(String.format("Failed to drop pipe [%s], the pipe does not exist", pipeName));
    return false;
  }

  private boolean isPipeExisted(String pipeName) {
    return pipeMetaKeeper.containsPipeMeta(pipeName);
  }

  private PipeStatus getPipeStatus(String pipeName) {
    return pipeMetaKeeper.getPipeMeta(pipeName).getRuntimeMeta().getStatus().get();
  }

  // TODO: invoke this method by PipeTaskConnectorStage.
  public String getPipeConnectorAttributes(String pipeName) {
    return new TreeMap<>(
            pipeMetaKeeper.getPipeMeta(pipeName).getStaticMeta().getConnectorAttributes())
        .toString();
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
