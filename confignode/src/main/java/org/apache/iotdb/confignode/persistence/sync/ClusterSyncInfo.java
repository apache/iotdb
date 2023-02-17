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
package org.apache.iotdb.confignode.persistence.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeNotExistException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.exception.sync.PipeSinkNotExistException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.sync.metadata.SyncMetadata;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.PreCreatePipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.RecordPipeMessagePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.SetPipeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.ShowPipePlan;
import org.apache.iotdb.confignode.consensus.response.pipe.PipeResp;
import org.apache.iotdb.confignode.consensus.response.pipe.PipeSinkResp;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ClusterSyncInfo implements SnapshotProcessor {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterSyncInfo.class);

  private final SyncMetadata syncMetadata;

  private final ReentrantLock syncMetadataLock = new ReentrantLock();

  public ClusterSyncInfo() {
    syncMetadata = new SyncMetadata();
  }
  // ======================================================
  // region Implement of PipeSink
  // ======================================================

  /**
   * Check PipeSink before create operation
   *
   * @param createPipeSinkPlan createPipeSinkPlan
   * @throws PipeSinkException if there is PipeSink with the same name exists or attributes is
   *     unsupported
   */
  public void checkAddPipeSink(CreatePipeSinkPlan createPipeSinkPlan) throws PipeSinkException {
    // check no exist
    syncMetadata.checkPipeSinkNoExist(createPipeSinkPlan.getPipeSinkInfo().getPipeSinkName());
    // check attributes
    SyncPipeUtil.parseTPipeSinkInfoAsPipeSink(createPipeSinkPlan.getPipeSinkInfo());
  }

  public TSStatus addPipeSink(CreatePipeSinkPlan plan) {
    TSStatus status = new TSStatus();
    try {
      syncMetadata.addPipeSink(SyncPipeUtil.parseTPipeSinkInfoAsPipeSink(plan.getPipeSinkInfo()));
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (PipeSinkException e) {
      LOGGER.error("failed to execute CreatePipeSinkPlan {} on ClusterSyncInfo", plan, e);
      status.setCode(TSStatusCode.CREATE_PIPE_SINK_ERROR.getStatusCode());
      LOGGER.error(e.getMessage());
    }
    return status;
  }

  /**
   * Check PipeSink before drop operation
   *
   * @param pipeSinkName name
   * @throws PipeSinkException if PipeSink is being used or does not exist
   */
  public void checkDropPipeSink(String pipeSinkName) throws PipeSinkException {
    syncMetadata.checkDropPipeSink(pipeSinkName);
  }

  public TSStatus dropPipeSink(DropPipeSinkPlan plan) {
    syncMetadata.dropPipeSink(plan.getPipeSinkName());
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public PipeSinkResp getPipeSink(GetPipeSinkPlan plan) {
    PipeSinkResp resp = new PipeSinkResp();
    if (StringUtils.isEmpty(plan.getPipeSinkName())) {
      resp.setPipeSinkList(syncMetadata.getAllPipeSink());
    } else {
      resp.setPipeSinkList(
          Collections.singletonList(syncMetadata.getPipeSink(plan.getPipeSinkName())));
    }
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return resp;
  }

  // endregion

  // ======================================================
  // region Implement of Pipe
  // ======================================================

  /**
   * Check Pipe before create operation
   *
   * @param pipeInfo pipe info
   * @throws PipeException if there is Pipe with the same name exists or PipeSink does not exist
   */
  public void checkAddPipe(PipeInfo pipeInfo) throws PipeException, PipeSinkNotExistException {
    syncMetadata.checkAddPipe(pipeInfo);
  }

  public TSStatus preCreatePipe(PreCreatePipePlan physicalPlan) {
    syncMetadata.addPipe(physicalPlan.getPipeInfo());
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus setPipeStatus(SetPipeStatusPlan physicalPlan) {
    syncMetadata.setPipeStatus(physicalPlan.getPipeName(), physicalPlan.getPipeStatus());
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus dropPipe(DropPipePlan physicalPlan) {
    syncMetadata.dropPipe(physicalPlan.getPipeName());
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus recordPipeMessage(RecordPipeMessagePlan physicalPlan) {
    syncMetadata.changePipeMessage(
        physicalPlan.getPipeName(), physicalPlan.getPipeMessage().getType());
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public PipeResp showPipe(ShowPipePlan plan) {
    PipeResp resp = new PipeResp();
    if (StringUtils.isEmpty(plan.getPipeName())) {
      // show all
      resp.setPipeInfoList(syncMetadata.getAllPipeInfos());
    } else {
      // show specific pipe
      resp.setPipeInfoList(Collections.singletonList(syncMetadata.getPipeInfo(plan.getPipeName())));
    }
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return resp;
  }

  /**
   * Get PipeInfo by pipeName. Check before start, stop and drop operation
   *
   * @param pipeName pipe name
   * @throws PipeNotExistException if there is Pipe does not exist
   */
  public PipeInfo getPipeInfo(String pipeName) throws PipeNotExistException {
    PipeInfo pipeInfo = syncMetadata.getPipeInfo(pipeName);
    if (pipeInfo == null) {
      throw new PipeNotExistException(pipeName);
    }
    return pipeInfo;
  }

  public List<PipeInfo> getAllPipeInfos() {
    return syncMetadata.getAllPipeInfos();
  }

  // endregion

  // ======================================================
  // region Implement of Lock and Unlock
  // ======================================================

  public void lockSyncMetadata() {
    LOGGER.info("Lock SyncMetadata");
    syncMetadataLock.lock();
    LOGGER.info("Acquire SyncMetadata lock");
  }

  public void unlockSyncMetadata() {
    LOGGER.info("Unlock SyncMetadata");
    syncMetadataLock.unlock();
  }

  // endregion

  // ======================================================
  // region Implement of Snapshot
  // ======================================================

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return syncMetadata.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    syncMetadata.processLoadSnapshot(snapshotDir);
  }

  // endregion
}
