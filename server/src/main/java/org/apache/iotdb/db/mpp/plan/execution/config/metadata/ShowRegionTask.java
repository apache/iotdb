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

package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.stream.Collectors;

public class ShowRegionTask implements IConfigTask {

  private final ShowRegionStatement showRegionStatement;

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
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showRegionColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (showRegionResp.getRegionInfoList() != null) {
      for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeInt(regionInfo.getConsensusGroupId().getId());
        if (regionInfo.getConsensusGroupId().getType().ordinal()
            == TConsensusGroupType.SchemaRegion.ordinal()) {
          builder
              .getColumnBuilder(1)
              .writeBinary(Binary.valueOf(String.valueOf(TConsensusGroupType.SchemaRegion)));
        } else if (regionInfo.getConsensusGroupId().getType().ordinal()
            == TConsensusGroupType.DataRegion.ordinal()) {
          builder
              .getColumnBuilder(1)
              .writeBinary(Binary.valueOf(String.valueOf(TConsensusGroupType.DataRegion)));
        }
        builder
            .getColumnBuilder(2)
            .writeBinary(
                Binary.valueOf(regionInfo.getStatus() == null ? "" : regionInfo.getStatus()));
        builder.getColumnBuilder(3).writeBinary(Binary.valueOf(regionInfo.getDatabase()));
        builder.getColumnBuilder(4).writeInt(regionInfo.getSeriesSlots());
        builder.getColumnBuilder(5).writeLong(regionInfo.getTimeSlots());
        builder.getColumnBuilder(6).writeInt(regionInfo.getDataNodeId());
        builder.getColumnBuilder(7).writeBinary(Binary.valueOf(regionInfo.getClientRpcIp()));
        builder.getColumnBuilder(8).writeInt(regionInfo.getClientRpcPort());
        builder.getColumnBuilder(9).writeBinary(Binary.valueOf(regionInfo.getRoleType()));
        builder
            .getColumnBuilder(10)
            .writeBinary(new Binary(DateTimeUtils.convertLongToDate(regionInfo.getCreateTime())));
        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowRegionHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
