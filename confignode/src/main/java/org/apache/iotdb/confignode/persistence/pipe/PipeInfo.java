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

import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class PipeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

  private final PipePluginInfo pipePluginInfo;

  private final PipeTaskInfo pipeTaskInfo;

  public PipeInfo() throws IOException {
    pipePluginInfo = new PipePluginInfo();
    pipeTaskInfo = new PipeTaskInfo();
  }

  public PipePluginInfo getPipePluginInfo() {
    return pipePluginInfo;
  }

  public PipeTaskInfo getPipeTaskInfo() {
    return pipeTaskInfo;
  }

  public boolean checkPipeCreateTask(TCreatePipeReq req) {
    if (pipeTaskInfo.existTaskName(req.getPipeName())) {
      LOGGER.info(
          String.format(
              "Failed to create pipe [%s], the pipe with the same name has been created",
              req.getPipeName()));
      return false;
    }
    return pipePluginInfo.validatePluginForTask(req);
  }

  public boolean checkOperatePipeTask(String pipeName, SyncOperation operation) {
    if (operation.equals(SyncOperation.START_PIPE)) {
      if (!pipeTaskInfo.existTaskName(pipeName)) {
        LOGGER.info(String.format("Failed to start pipe [%s], the pipe does not exist", pipeName));
        return false;
      }
      if (pipeTaskInfo.getPipeStatus(pipeName) != PipeStatus.STOP) {
        LOGGER.info(
            String.format("Failed to start pipe [%s], the pipe is already running", pipeName));
        return false;
      }
    } else if (operation.equals(SyncOperation.STOP_PIPE)) {
      if (!pipeTaskInfo.existTaskName(pipeName)) {
        LOGGER.info(String.format("Failed to stop pipe [%s], the pipe does not exist", pipeName));
        return false;
      }
      if (pipeTaskInfo.getPipeStatus(pipeName) != PipeStatus.RUNNING) {
        LOGGER.info(String.format("Failed to stop pipe [%s], the pipe is already stop", pipeName));
        return false;
      }
    } else {
      if (!pipeTaskInfo.existTaskName(pipeName)) {
        LOGGER.info(String.format("Failed to drop pipe [%s], the pipe does not exist", pipeName));
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return pipePluginInfo.processTakeSnapshot(snapshotDir)
        && pipeTaskInfo.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    pipePluginInfo.processLoadSnapshot(snapshotDir);
    pipeTaskInfo.processLoadSnapshot(snapshotDir);
  }
}
