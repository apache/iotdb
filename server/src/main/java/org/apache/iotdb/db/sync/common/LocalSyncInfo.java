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
package org.apache.iotdb.db.sync.common;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.metadata.SyncMetadata;
import org.apache.iotdb.commons.sync.persistence.SyncLogReader;
import org.apache.iotdb.commons.sync.persistence.SyncLogWriter;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LocalSyncInfo {

  protected static final Logger LOGGER = LoggerFactory.getLogger(LocalSyncInfo.class);

  protected SyncLogWriter syncLogWriter;

  private final SyncMetadata syncMetadata;

  public LocalSyncInfo() {
    syncLogWriter = new SyncLogWriter(new File(SyncPathUtil.getSysDir()));
    syncMetadata = new SyncMetadata();
    SyncLogReader logReader = new SyncLogReader(new File(SyncPathUtil.getSysDir()));
    try {
      logReader.recover();
      syncMetadata.setPipes(logReader.getAllPipeInfos());
      syncMetadata.setPipeSinks(logReader.getAllPipeSinks());
      syncMetadata.setRunningPipe(logReader.getRunningPipeInfo());
    } catch (IOException e) {
      LOGGER.error(
          "Cannot recover ReceiverInfo because {}. Use default info values.", e.getMessage());
    }
  }

  public void close() throws IOException {
    syncLogWriter.close();
  }

  // region Implement of PipeSink

  // TODO: delete this in new-standalone version
  public void addPipeSink(CreatePipeSinkPlan plan) throws PipeSinkException, IOException {
    syncMetadata.checkAddPipeSink(plan.getPipeSinkName());
    PipeSink pipeSink = SyncPipeUtil.parseCreatePipeSinkPlan(plan);
    // should guarantee the adding pipesink is not exist.
    syncMetadata.addPipeSink(pipeSink);
    syncLogWriter.addPipeSink(pipeSink);
  }

  public void addPipeSink(CreatePipeSinkStatement createPipeSinkStatement)
      throws PipeSinkException, IOException {
    syncMetadata.checkAddPipeSink(createPipeSinkStatement.getPipeSinkName());
    PipeSink pipeSink = SyncPipeUtil.parseCreatePipeSinkStatement(createPipeSinkStatement);
    // should guarantee the adding pipesink is not exist.
    syncMetadata.addPipeSink(pipeSink);
    syncLogWriter.addPipeSink(pipeSink);
  }

  public void dropPipeSink(String name) throws PipeSinkException, IOException {
    syncMetadata.checkDropPipeSink(name);
    syncMetadata.dropPipeSink(name);
    syncLogWriter.dropPipeSink(name);
  }

  public PipeSink getPipeSink(String name) {
    return syncMetadata.getPipeSink(name);
  }

  public List<PipeSink> getAllPipeSink() {
    return syncMetadata.getAllPipeSink();
  }

  // endregion

  // region Implement of Pipe
  // TODO: delete this in new-standalone version
  public void addPipe(CreatePipePlan plan, long createTime) throws PipeException, IOException {
    if (!syncMetadata.isPipeSinkExist(plan.getPipeSinkName())) {
      throw new PipeException(String.format("Can not find pipeSink %s.", plan.getPipeSinkName()));
    }
    PipeSink pipeSink = getPipeSink(plan.getPipeSinkName());
    PipeInfo pipeInfo = SyncPipeUtil.parseCreatePipePlanAsPipeInfo(plan, pipeSink, createTime);
    syncMetadata.addPipe(pipeInfo, pipeSink);
    syncLogWriter.addPipe(pipeInfo);
  }

  public void addPipe(CreatePipeStatement createPipeStatement, long createTime)
      throws PipeException, IOException {
    if (!syncMetadata.isPipeSinkExist(createPipeStatement.getPipeSinkName())) {
      throw new PipeException(
          String.format("Can not find pipeSink %s.", createPipeStatement.getPipeSinkName()));
    }
    PipeSink pipeSink = getPipeSink(createPipeStatement.getPipeSinkName());
    PipeInfo pipeInfo =
        SyncPipeUtil.parseCreatePipePlanAsPipeInfo(createPipeStatement, pipeSink, createTime);
    syncMetadata.addPipe(pipeInfo, pipeSink);
    syncLogWriter.addPipe(pipeInfo);
  }

  public void operatePipe(String pipeName, SyncOperation syncOperation)
      throws PipeException, IOException {
    syncMetadata.operatePipe(pipeName, syncOperation);
    syncLogWriter.operatePipe(pipeName, syncOperation);
  }

  public PipeInfo getPipeInfo(String pipeName, long createTime) {
    return syncMetadata.getPipeInfo(pipeName, createTime);
  }

  public List<PipeInfo> getAllPipeInfos() {
    return syncMetadata.getAllPipeInfos();
  }

  /** @return null if no pipe has been created */
  public PipeInfo getRunningPipeInfo() {
    return syncMetadata.getRunningPipeInfo();
  }

  /**
   * Change Pipe Message. It will record the most important message about one pipe. ERROR > WARN >
   * NORMAL.
   *
   * @param pipeName name of pipe
   * @param createTime createTime of pipe
   * @param messageType pipe message type
   */
  public void changePipeMessage(
      String pipeName, long createTime, PipeMessage.PipeMessageType messageType) {
    syncMetadata.changePipeMessage(pipeName, createTime, messageType);
  }

  // endregion
}
