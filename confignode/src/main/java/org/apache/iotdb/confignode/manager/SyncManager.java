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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.OperatePipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.PreCreatePipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.ShowPipePlan;
import org.apache.iotdb.confignode.consensus.response.PipeResp;
import org.apache.iotdb.confignode.consensus.response.PipeSinkResp;
import org.apache.iotdb.confignode.persistence.sync.ClusterSyncInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

public class SyncManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncManager.class);

  private final IManager configManager;
  private final ClusterSyncInfo clusterSyncInfo;

  public SyncManager(IManager configManager, ClusterSyncInfo clusterSyncInfo) {
    this.configManager = configManager;
    this.clusterSyncInfo = clusterSyncInfo;
  }

  // ======================================================
  // region Implement of PipeSink
  // ======================================================

  public TSStatus createPipeSink(CreatePipeSinkPlan plan) {
    try {
      clusterSyncInfo.checkAddPipeSink(plan.getPipeSinkInfo().getPipeSinkName());
      return getConsensusManager().write(plan).getStatus();
    } catch (PipeSinkException e) {
      LOGGER.error(e.getMessage());
      return new TSStatus(TSStatusCode.PIPESINK_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TSStatus dropPipeSink(DropPipeSinkPlan plan) {
    try {
      clusterSyncInfo.checkDropPipeSink(plan.getPipeSinkName());
      return getConsensusManager().write(plan).getStatus();
    } catch (PipeSinkException e) {
      LOGGER.error(e.getMessage());
      return new TSStatus(TSStatusCode.PIPESINK_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  public TGetPipeSinkResp getPipeSink(String pipeSinkName) {
    GetPipeSinkPlan getPipeSinkPlan = new GetPipeSinkPlan(pipeSinkName);
    PipeSinkResp pipeSinkResp =
        (PipeSinkResp) getConsensusManager().read(getPipeSinkPlan).getDataset();
    TGetPipeSinkResp resp = new TGetPipeSinkResp();
    resp.setStatus(pipeSinkResp.getStatus());
    if (pipeSinkResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setPipeSinkInfoList(
          pipeSinkResp.getPipeSinkList().stream()
              .map(PipeSink::getTPipeSinkInfo)
              .collect(Collectors.toList()));
    }
    return resp;
  }

  // endregion

  // ======================================================
  // region Implement of Pipe
  // ======================================================

  public void checkAddPipe(PipeInfo pipeInfo) throws PipeException {
    clusterSyncInfo.checkAddPipe(pipeInfo);
  }

  public TSStatus preCreatePipe(PipeInfo pipeInfo) {
    pipeInfo.setStatus(PipeStatus.PREPARE_CREATE);
    return getConsensusManager().write(new PreCreatePipePlan(pipeInfo)).getStatus();
  }

  public TSStatus operatePipe(String pipeName, SyncOperation operation) {
    return getConsensusManager().write(new OperatePipePlan(pipeName, operation)).getStatus();
  }

  public TShowPipeResp showPipe(String pipeName) {
    ShowPipePlan showPipePlan = new ShowPipePlan(pipeName);
    PipeResp pipeResp = (PipeResp) getConsensusManager().read(showPipePlan).getDataset();
    TShowPipeResp resp = new TShowPipeResp();
    resp.setStatus(pipeResp.getStatus());
    if (pipeResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setPipeInfoList(
          pipeResp.getPipeInfoList().stream()
              .map(PipeInfo::getTShowPipeInfo)
              .collect(Collectors.toList()));
    }
    return resp;
  }

  // TODO....

  // endregion

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
