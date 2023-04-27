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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkResp;
import org.apache.iotdb.confignode.rpc.thrift.TRecordPipeMessageReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Only fetch read request. For write request, return SUCCESS directly. */
public class ClusterSyncInfoFetcher implements ISyncInfoFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSyncInfoFetcher.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  // region Interfaces of PipeSink

  @Override
  public TSStatus addPipeSink(CreatePipeSinkStatement createPipeSinkStatement) {
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public TSStatus dropPipeSink(String name) {
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public PipeSink getPipeSink(String name) throws PipeSinkException {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetPipeSinkReq tGetPipeSinkReq = new TGetPipeSinkReq().setPipeSinkName(name);
      TGetPipeSinkResp resp = configNodeClient.getPipeSink(tGetPipeSinkReq);
      if (resp.getPipeSinkInfoList().isEmpty()) {
        throw new PipeSinkException(
            String.format("Failed to getPipeSink [%s] because it does not exist.", name));
      }
      return SyncPipeUtil.parseTPipeSinkInfoAsPipeSink(resp.getPipeSinkInfoList().get(0));
    } catch (Exception e) {
      LOGGER.error("Get PipeSink [{}] error because {}", name, e.getMessage(), e);
      throw new PipeSinkException(e.getMessage());
    }
  }

  @Override
  public List<PipeSink> getAllPipeSinks() {
    throw new UnsupportedOperationException();
  }

  // endregion

  // region Interfaces of Pipe

  @Override
  public TSStatus addPipe(PipeInfo pipeInfo) {
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    return RpcUtils.SUCCESS_STATUS;
  }

  @Override
  public List<PipeInfo> getAllPipeInfos() {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetAllPipeInfoResp resp = configNodeClient.getAllPipeInfo();
      return resp.getAllPipeInfo().stream()
          .map(PipeInfo::deserializePipeInfo)
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOGGER.error("Get AllPipeInfos error because {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  @Override
  public TSStatus recordMsg(String pipeName, PipeMessage message) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TRecordPipeMessageReq req =
          new TRecordPipeMessageReq(pipeName, message.serializeToByteBuffer());
      return configNodeClient.recordPipeMessage(req);
    } catch (Exception e) {
      LOGGER.error("RecordMsg error because {}", e.getMessage(), e);
      return RpcUtils.getStatus(TSStatusCode.PIPE_ERROR, e.getMessage());
    }
  }

  // endregion

  // region singleton
  private static class ClusterSyncInfoFetcherHolder {
    private static final ClusterSyncInfoFetcher INSTANCE = new ClusterSyncInfoFetcher();

    private ClusterSyncInfoFetcherHolder() {}
  }

  public static ClusterSyncInfoFetcher getInstance() {
    return ClusterSyncInfoFetcher.ClusterSyncInfoFetcherHolder.INSTANCE;
  }
  // endregion
}
