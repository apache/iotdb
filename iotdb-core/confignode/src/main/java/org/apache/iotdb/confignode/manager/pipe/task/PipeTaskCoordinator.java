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

public class PipeTaskCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinator.class);

  private final ConfigManager configManager;
  private final PipeTaskInfo pipeTaskInfo;

  public PipeTaskCoordinator(ConfigManager configManager, PipeTaskInfo pipeTaskInfo) {
    this.configManager = configManager;
    this.pipeTaskInfo = pipeTaskInfo;
  }

  public PipeTaskInfo getPipeTaskInfo() {
    return pipeTaskInfo;
  }

  public TSStatus createPipe(TCreatePipeReq req) {
    return configManager.getProcedureManager().createPipe(req);
  }

  public TSStatus startPipe(String pipeName) {
    // To avoid concurrent read
    lock();
    // Whether there are exceptions to clear
    boolean hasException;
    try {
      hasException = pipeTaskInfo.hasExceptions(pipeName);
    } finally {
      unlock();
    }
    TSStatus status = configManager.getProcedureManager().startPipe(pipeName);
    if (status == RpcUtils.SUCCESS_STATUS && hasException) {
      LOGGER.info("Pipe {} has started successfully, clear its exceptions.", pipeName);
      configManager.getProcedureManager().pipeHandleMetaChange(true, true);
    }
    return status;
  }

  public TSStatus stopPipe(String pipeName) {
    return configManager.getProcedureManager().stopPipe(pipeName);
  }

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
    return ((PipeTableResp)
            configManager.getConsensusManager().read(new ShowPipePlanV2()).getDataset())
        .filter(req.whereClause, req.pipeName)
        .convertToTShowPipeResp();
  }

  public TGetAllPipeInfoResp getAllPipeInfo() {
    try {
      return ((PipeTableResp)
              configManager.getConsensusManager().read(new ShowPipePlanV2()).getDataset())
          .convertToTGetAllPipeInfoResp();
    } catch (IOException e) {
      LOGGER.error("Fail to get AllPipeInfo", e);
      return new TGetAllPipeInfoResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
