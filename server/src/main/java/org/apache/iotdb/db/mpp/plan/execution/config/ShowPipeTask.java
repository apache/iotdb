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
package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.statement.sync.ShowPipeStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShowPipeTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowPipeTask.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final ShowPipeStatement showPipeStatement;

  public ShowPipeTask(ShowPipeStatement showPipeStatement) {
    this.showPipeStatement = showPipeStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<TPipeInfo> pipeInfoList = new ArrayList<>();
    if (config.isClusterMode()) {
      TShowPipeReq showPipeReq = new TShowPipeReq();
      showPipeReq.setPipeName(showPipeStatement.getPipeName());
      try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        TShowPipeResp resp = client.showPipe(showPipeReq);
        pipeInfoList = resp.getPipeInfoList();
      } catch (TException | IOException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      }
    } else {
      // TODO(sync）: standalone show pipe
    }
    // build TSBlock
    TsBlockBuilder builder = new TsBlockBuilder(HeaderConstant.showPipeHeader.getRespDataTypes());
    for (TPipeInfo pipeInfo : pipeInfoList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(Binary.valueOf(pipeInfo.getCreateTime()));
      builder.getColumnBuilder(1).writeBinary(Binary.valueOf(pipeInfo.getPipeName()));
      builder.getColumnBuilder(2).writeBinary(Binary.valueOf(pipeInfo.getRole()));
      builder.getColumnBuilder(3).writeBinary(Binary.valueOf(pipeInfo.getRemote()));
      builder.getColumnBuilder(4).writeBinary(Binary.valueOf(pipeInfo.getStatus()));
      builder.getColumnBuilder(5).writeBinary(Binary.valueOf(pipeInfo.getMessage()));
      builder.declarePosition();
    }
    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS, builder.build(), HeaderConstant.showPipeHeader));
    return future;
  }
}
