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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfos;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class ShowRegionTask implements IConfigTask {

  private ShowRegionStatement showRegionStatement;

  public ShowRegionTask() {}

  public ShowRegionTask(ShowRegionStatement showRegionStatement) {
    this.showRegionStatement = showRegionStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showRegion(showRegionStatement);
  }

  public static void buildTSBlock(
      TShowRegionResp showRegionResp, SettableFuture<ConfigTaskResult> future) {
    TsBlockBuilder builder = new TsBlockBuilder(HeaderConstant.showRegionHeader.getRespDataTypes());
    if (showRegionResp.getRegionInfoList() != null) {
      for (TRegionInfos tRegionInfos : showRegionResp.getRegionInfoList()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeInt(tRegionInfos.getRegionId());
        if (tRegionInfos.getRegionType() == TConsensusGroupType.SchemaRegion.ordinal()) {
          builder
              .getColumnBuilder(1)
              .writeBinary(Binary.valueOf(String.valueOf(TConsensusGroupType.SchemaRegion)));
        } else if (tRegionInfos.getRegionType() == TConsensusGroupType.DataRegion.ordinal()) {
          builder
              .getColumnBuilder(1)
              .writeBinary(Binary.valueOf(String.valueOf(TConsensusGroupType.DataRegion)));
        }
        builder
            .getColumnBuilder(2)
            .writeBinary(
                Binary.valueOf(tRegionInfos.getStatus() == null ? "" : tRegionInfos.getStatus()));
        builder.getColumnBuilder(3).writeBinary(Binary.valueOf(tRegionInfos.getStorageGroup()));
        builder.getColumnBuilder(4).writeInt(tRegionInfos.getSlots());
        builder
            .getColumnBuilder(5)
            .writeBinary(Binary.valueOf(tRegionInfos.getDataNodeId().toString()));
        builder
            .getColumnBuilder(6)
            .writeBinary(Binary.valueOf(tRegionInfos.getRpcAddresss().toString()));
        builder
            .getColumnBuilder(7)
            .writeBinary(Binary.valueOf(tRegionInfos.getRpcPort().toString()));

        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = HeaderConstant.showRegionHeader;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
