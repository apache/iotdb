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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.util.List;
import java.util.stream.Collectors;

public class ShowRegionTask implements IConfigTask {

  private final ShowRegionStatement showRegionStatement;
  private final boolean isTableModel;

  public ShowRegionTask(final ShowRegionStatement showRegionStatement, final boolean isTableModel) {
    this.showRegionStatement = showRegionStatement;
    this.isTableModel = isTableModel;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showRegion(showRegionStatement, isTableModel);
  }

  public static void buildTSBlock(
      final TShowRegionResp showRegionResp,
      final SettableFuture<ConfigTaskResult> future,
      final boolean isTableModel) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showRegionColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (showRegionResp.getRegionInfoList() != null) {
      for (final TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeInt(regionInfo.getConsensusGroupId().getId());
        if (regionInfo.getConsensusGroupId().getType().ordinal()
            == TConsensusGroupType.SchemaRegion.ordinal()) {
          builder
              .getColumnBuilder(1)
              .writeBinary(BytesUtils.valueOf(String.valueOf(TConsensusGroupType.SchemaRegion)));
        } else if (regionInfo.getConsensusGroupId().getType().ordinal()
            == TConsensusGroupType.DataRegion.ordinal()) {
          builder
              .getColumnBuilder(1)
              .writeBinary(BytesUtils.valueOf(String.valueOf(TConsensusGroupType.DataRegion)));
        }
        builder
            .getColumnBuilder(2)
            .writeBinary(
                BytesUtils.valueOf(regionInfo.getStatus() == null ? "" : regionInfo.getStatus()));
        builder
            .getColumnBuilder(3)
            .writeBinary(
                BytesUtils.valueOf(
                    isTableModel
                        ? PathUtils.unQualifyDatabaseName(regionInfo.getDatabase())
                        : regionInfo.getDatabase()));
        builder.getColumnBuilder(4).writeInt(regionInfo.getSeriesSlots());
        builder.getColumnBuilder(5).writeLong(regionInfo.getTimeSlots());
        builder.getColumnBuilder(6).writeInt(regionInfo.getDataNodeId());
        builder.getColumnBuilder(7).writeBinary(BytesUtils.valueOf(regionInfo.getClientRpcIp()));
        builder.getColumnBuilder(8).writeInt(regionInfo.getClientRpcPort());
        builder
            .getColumnBuilder(9)
            .writeBinary(BytesUtils.valueOf(regionInfo.getInternalAddress()));
        builder.getColumnBuilder(10).writeBinary(BytesUtils.valueOf(regionInfo.getRoleType()));
        builder
            .getColumnBuilder(11)
            .writeBinary(
                new Binary(
                    DateTimeUtils.convertLongToDate(regionInfo.getCreateTime()),
                    TSFileConfig.STRING_CHARSET));
        builder.declarePosition();
      }
    }
    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowRegionHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
