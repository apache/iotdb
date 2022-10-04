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
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;

import java.util.List;

public class ClusterSyncInfoFetcher implements ISyncInfoFetcher {
  @Override
  public TSStatus addPipeSink(CreatePipeSinkPlan plan) {
    return null;
  }

  @Override
  public TSStatus addPipeSink(CreatePipeSinkStatement createPipeSinkStatement) {
    return null;
  }

  @Override
  public TSStatus dropPipeSink(String name) {
    return null;
  }

  @Override
  public PipeSink getPipeSink(String name) {
    return null;
  }

  @Override
  public List<PipeSink> getAllPipeSinks() {
    return null;
  }

  @Override
  public TSStatus addPipe(PipeInfo pipeInfo) {
    return null;
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    return null;
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    return null;
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    return null;
  }

  @Override
  public List<PipeInfo> getAllPipeInfos() {
    return null;
  }

  @Override
  public PipeInfo getRunningPipeInfo() {
    return null;
  }

  @Override
  public TSStatus recordMsg(String pipeName, long createTime, PipeMessage message) {
    return null;
  }

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
