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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class LocalSyncInfoFetcher implements ISyncInfoFetcher {

  private static final Logger logger = LoggerFactory.getLogger(LocalSyncInfoFetcher.class);
  private SyncInfo syncInfo;

  private LocalSyncInfoFetcher() {
    syncInfo = new SyncInfo();
  }

  // region Implement of PipeSink
  @Override
  public TSStatus addPipeSink(CreatePipeSinkPlan plan) {
    try {
      syncInfo.addPipeSink(plan);
    } catch (PipeSinkException | IOException e) {
      RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus dropPipeSink(String name) {
    try {
      syncInfo.dropPipeSink(name);
    } catch (PipeSinkException | IOException e) {
      RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public PipeSink getPipeSink(String name) {
    return syncInfo.getPipeSink(name);
  }

  @Override
  public List<PipeSink> getAllPipeSinks() {
    return syncInfo.getAllPipeSink();
  }

  // endregion

  // region Implement of Pipe

  @Override
  public TSStatus addPipe(CreatePipePlan plan, long createTime) {
    try {
      syncInfo.addPipe(plan, createTime);
    } catch (PipeException | IOException e) {
      RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    try {
      syncInfo.operatePipe(pipeName, Operator.OperatorType.STOP_PIPE);
    } catch (PipeException | IOException e) {
      RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    try {
      syncInfo.operatePipe(pipeName, Operator.OperatorType.START_PIPE);
    } catch (PipeException | IOException e) {
      RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    try {
      syncInfo.operatePipe(pipeName, Operator.OperatorType.DROP_PIPE);
    } catch (PipeException | IOException e) {
      RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public List<PipeInfo> getAllPipeInfos() {
    return syncInfo.getAllPipeInfos();
  }

  @Override
  public PipeInfo getRunningPipeInfo() {
    return syncInfo.getRunningPipeInfo();
  }

  @Override
  public String getPipeMsg(String pipeName, long createTime) {
    return syncInfo.getPipeMessage(pipeName, createTime, false).getMsg();
  }

  @Override
  public TSStatus recordMsg(String pipeName, long createTime, PipeMessage pipeMessage) {
    syncInfo.writePipeMessage(pipeName, createTime, pipeMessage);
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  // endregion

  // region singleton
  private static class LocalSyncInfoFetcherHolder {
    private static final LocalSyncInfoFetcher INSTANCE = new LocalSyncInfoFetcher();

    private LocalSyncInfoFetcherHolder() {}
  }

  public static LocalSyncInfoFetcher getInstance() {
    return LocalSyncInfoFetcher.LocalSyncInfoFetcherHolder.INSTANCE;
  }
  // endregion

  @TestOnly
  public void reset() {
    syncInfo = new SyncInfo();
  }
}
