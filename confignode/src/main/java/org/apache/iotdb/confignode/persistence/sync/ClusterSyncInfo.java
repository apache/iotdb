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
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.sync.metadata.SyncMetadata;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.response.PipeSinkResp;
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

public class ClusterSyncInfo implements SnapshotProcessor {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterSyncInfo.class);

  private final SyncMetadata syncMetadata;

  public ClusterSyncInfo() {
    syncMetadata = new SyncMetadata();
  }
  // ======================================================
  // region Implement of PipeSink
  // ======================================================

  /**
   * Check PipeSink before create operation
   *
   * @param pipeSinkName name
   * @throws PipeSinkException if there is PipeSink with the same name exists
   */
  public void checkAddPipeSink(String pipeSinkName) throws PipeSinkException {
    syncMetadata.checkAddPipeSink(pipeSinkName);
  }

  public TSStatus addPipeSink(CreatePipeSinkPlan plan) {
    TSStatus status = new TSStatus();
    try {
      syncMetadata.addPipeSink(SyncPipeUtil.parsePipeInfoAsPipe(plan.getPipeSinkInfo()));
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (PipeSinkException e) {
      status.setCode(TSStatusCode.PIPESINK_ERROR.getStatusCode());
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
