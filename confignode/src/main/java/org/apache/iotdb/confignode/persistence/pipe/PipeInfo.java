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

import org.apache.iotdb.commons.pipe.meta.ConfigNodePipeMetaKeeper;
import org.apache.iotdb.commons.pipe.meta.PipeStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Objects;

public class PipeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

  private final String SNAPSHOT_FILE_NAME = "pipe_info.bin";

  private final PipePluginInfo pipePluginInfo;

  private final PipeTaskInfo pipeTaskInfo;

  private final ConfigNodePipeMetaKeeper pipeMetaKeeper = ConfigNodePipeMetaKeeper.getInstance();

  public PipeInfo() throws IOException {
    pipePluginInfo = new PipePluginInfo(pipeMetaKeeper);
    pipeTaskInfo = new PipeTaskInfo(pipeMetaKeeper);
  }

  public PipePluginInfo getPipePluginInfo() {
    return pipePluginInfo;
  }

  public PipeTaskInfo getPipeTaskInfo() {
    return pipeTaskInfo;
  }

  /** for CreatePipeProcedureV2 */
  public boolean checkPipeCreateTask(TCreatePipeReq req) {
    if (pipeTaskInfo.existPipeName(req.getPipeName())) {
      LOGGER.info(
          String.format(
              "Failed to create pipe [%s], the pipe with the same name has been created",
              req.getPipeName()));
      return false;
    }
    return pipePluginInfo.validatePluginForTask(req);
  }

  /** for DropPipeProcedureV2, StartPipeProcedureV2 & StopPipeProcedureV2 */
  public boolean checkOperatePipeTask(String pipeName, PipeTaskOperation operation) {
    switch (operation) {
      case START_PIPE:
        if (!pipeTaskInfo.existPipeName(pipeName)) {
          LOGGER.info(
              String.format("Failed to start pipe [%s], the pipe does not exist", pipeName));
          return false;
        }
        if (pipeTaskInfo.getPipeStatus(pipeName) != PipeStatus.STOPPED) {
          LOGGER.info(
              String.format("Failed to start pipe [%s], the pipe is already running", pipeName));
          return false;
        }
        return true;
      case STOP_PIPE:
        if (!pipeTaskInfo.existPipeName(pipeName)) {
          LOGGER.info(String.format("Failed to stop pipe [%s], the pipe does not exist", pipeName));
          return false;
        }
        if (pipeTaskInfo.getPipeStatus(pipeName) != PipeStatus.RUNNING) {
          LOGGER.info(
              String.format("Failed to stop pipe [%s], the pipe is already stop", pipeName));
          return false;
        }
        return true;
      case DROP_PIPE:
        if (!pipeTaskInfo.existPipeName(pipeName)) {
          LOGGER.info(String.format("Failed to drop pipe [%s], the pipe does not exist", pipeName));
          return false;
        }
        return true;
      case CREATE_PIPE: // will be checked by checkPipeCreateTask
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    pipeTaskInfo.acquirePipeTaskInfoLock();
    pipePluginInfo.acquirePipePluginInfoLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      pipeMetaKeeper.processTakeSnapshot(fileOutputStream);
    } finally {
      pipeTaskInfo.acquirePipeTaskInfoLock();
      pipePluginInfo.releasePipePluginInfoLock();
    }
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    clear();
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    pipeTaskInfo.acquirePipeTaskInfoLock();
    pipePluginInfo.acquirePipePluginInfoLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      pipeMetaKeeper.processLoadSnapshot(fileInputStream);
    } finally {
      pipeTaskInfo.acquirePipeTaskInfoLock();
      pipePluginInfo.releasePipePluginInfoLock();
    }
  }

  public void clear() {
    pipeMetaKeeper.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeInfo that = (PipeInfo) o;
    return pipeMetaKeeper.equals(that.pipeMetaKeeper);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeMetaKeeper);
  }
}
