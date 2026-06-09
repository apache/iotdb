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
import org.apache.iotdb.confignode.rpc.thrift.TMigrationInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowMigrationsResp;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowMigrationsStatement;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.BytesUtils;

import java.util.List;
import java.util.stream.Collectors;

public class ShowMigrationsTask implements IConfigTask {

  private final ShowMigrationsStatement showMigrationsStatement;
  private final boolean isTableModel;

  public ShowMigrationsTask(
      final ShowMigrationsStatement showMigrationsStatement, final boolean isTableModel) {
    this.showMigrationsStatement = showMigrationsStatement;
    this.isTableModel = isTableModel;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showMigrations(showMigrationsStatement, isTableModel);
  }

  public static void buildTSBlock(
      final TShowMigrationsResp showMigrationsResp,
      final SettableFuture<ConfigTaskResult> future,
      final boolean isTableModel) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showMigrationsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (showMigrationsResp.getMigrationInfoList() != null) {
      for (final TMigrationInfo migrationInfo : showMigrationsResp.getMigrationInfoList()) {
        builder.getTimeColumnBuilder().writeLong(0L);
        builder.getColumnBuilder(0).writeLong(migrationInfo.getProcedureId());
        builder.getColumnBuilder(1).writeInt(migrationInfo.getRegionId());
        if (migrationInfo.getRegionType().ordinal() == TConsensusGroupType.SchemaRegion.ordinal()) {
          builder
              .getColumnBuilder(2)
              .writeBinary(BytesUtils.valueOf(String.valueOf(TConsensusGroupType.SchemaRegion)));
        } else if (migrationInfo.getRegionType().ordinal()
            == TConsensusGroupType.DataRegion.ordinal()) {
          builder
              .getColumnBuilder(2)
              .writeBinary(BytesUtils.valueOf(String.valueOf(TConsensusGroupType.DataRegion)));
        }
        builder.getColumnBuilder(3).writeInt(migrationInfo.getFromNodeId());
        builder.getColumnBuilder(4).writeInt(migrationInfo.getToNodeId());
        builder
            .getColumnBuilder(5)
            .writeBinary(BytesUtils.valueOf(migrationInfo.getCurrentState()));
        builder
            .getColumnBuilder(6)
            .writeBinary(BytesUtils.valueOf(migrationInfo.getProcedureStatus()));
        builder
            .getColumnBuilder(7)
            .writeBinary(
                BytesUtils.valueOf(
                    DateTimeUtils.convertLongToDate(migrationInfo.getSubmittedTime())));
        builder
            .getColumnBuilder(8)
            .writeBinary(
                BytesUtils.valueOf(
                    DateTimeUtils.convertLongToDate(migrationInfo.getLastUpdateTime())));
        builder.getColumnBuilder(9).writeBinary(BytesUtils.valueOf(migrationInfo.getDuration()));
        builder.declarePosition();
      }
    }
    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowMigrationsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
