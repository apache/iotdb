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
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class LocalSyncInfoFetcher implements ISyncInfoFetcher {

  private static final Logger logger = LoggerFactory.getLogger(LocalSyncInfoFetcher.class);
  private LocalSyncInfo localSyncInfo;

  private LocalSyncInfoFetcher() {
    localSyncInfo = new LocalSyncInfo();
  }

  // region Implement of PipeSink

  @Override
  public TSStatus addPipeSink(CreatePipeSinkStatement createPipeSinkStatement) {
    try {
      localSyncInfo.addPipeSink(createPipeSinkStatement);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (PipeSinkException | IOException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @Override
  public TSStatus dropPipeSink(String name) {
    try {
      localSyncInfo.dropPipeSink(name);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (PipeSinkException | IOException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @Override
  public PipeSink getPipeSink(String name) {
    return localSyncInfo.getPipeSink(name);
  }

  @Override
  public List<PipeSink> getAllPipeSinks() {
    return localSyncInfo.getAllPipeSink();
  }

  // endregion

  // region Implement of Pipe

  @Override
  public TSStatus addPipe(PipeInfo pipeInfo) {
    try {
      localSyncInfo.addPipe(pipeInfo);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, e.getMessage());
    } catch (IOException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    } catch (PipeSinkException e) {
      return RpcUtils.getStatus(TSStatusCode.CREATE_PIPE_SINK_ERROR, e.getMessage());
    }
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    try {
      localSyncInfo.operatePipe(pipeName, SyncOperation.STOP_PIPE);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, e.getMessage());
    } catch (IOException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    try {
      localSyncInfo.operatePipe(pipeName, SyncOperation.START_PIPE);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, e.getMessage());
    } catch (IOException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    try {
      localSyncInfo.operatePipe(pipeName, SyncOperation.DROP_PIPE);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (PipeException e) {
      return RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, e.getMessage());
    } catch (IOException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @Override
  public List<PipeInfo> getAllPipeInfos() {
    return localSyncInfo.getAllPipeInfos();
  }

  @Override
  public TSStatus recordMsg(String pipeName, PipeMessage pipeMessage) {
    localSyncInfo.changePipeMessage(pipeName, pipeMessage.getType());
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
    localSyncInfo = new LocalSyncInfo();
  }

  @TestOnly
  public void close() throws IOException {
    localSyncInfo.close();
  }
}
