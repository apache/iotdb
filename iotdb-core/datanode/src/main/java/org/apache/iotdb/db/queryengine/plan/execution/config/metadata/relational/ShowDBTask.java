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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowDBTask implements IConfigTask {

  private final ShowDB node;

  public ShowDBTask(final ShowDB node) {
    this.node = node;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showDatabases(node);
  }

  public static void buildTSBlock(
      final Map<String, TDatabaseInfo> storageGroupInfoMap,
      final SettableFuture<ConfigTaskResult> future,
      final boolean isDetails) {
    if (isDetails) {
      buildTSBlockForDetails(storageGroupInfoMap, future);
    } else {
      buildTSBlockForNonDetails(storageGroupInfoMap, future);
    }
  }

  private static void buildTSBlockForNonDetails(
      final Map<String, TDatabaseInfo> storageGroupInfoMap,
      final SettableFuture<ConfigTaskResult> future) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showDBColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final Map.Entry<String, TDatabaseInfo> entry : storageGroupInfoMap.entrySet()) {
      final String dbName = entry.getKey().substring(5);
      final TDatabaseInfo storageGroupInfo = entry.getValue();
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(dbName, TSFileConfig.STRING_CHARSET));

      if (Long.MAX_VALUE == storageGroupInfo.getTTL()) {
        builder
            .getColumnBuilder(1)
            .writeBinary(new Binary(IoTDBConstant.TTL_INFINITE, TSFileConfig.STRING_CHARSET));
      } else {
        builder
            .getColumnBuilder(1)
            .writeBinary(
                new Binary(String.valueOf(storageGroupInfo.getTTL()), TSFileConfig.STRING_CHARSET));
      }
      builder.getColumnBuilder(2).writeInt(storageGroupInfo.getSchemaReplicationFactor());
      builder.getColumnBuilder(3).writeInt(storageGroupInfo.getDataReplicationFactor());
      builder.getColumnBuilder(4).writeLong(storageGroupInfo.getTimePartitionInterval());
      builder.declarePosition();
    }

    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowDBHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  private static void buildTSBlockForDetails(
      final Map<String, TDatabaseInfo> storageGroupInfoMap,
      final SettableFuture<ConfigTaskResult> future) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showDBDetailsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final Map.Entry<String, TDatabaseInfo> entry : storageGroupInfoMap.entrySet()) {
      final String dbName = entry.getKey().substring(5);
      final TDatabaseInfo storageGroupInfo = entry.getValue();
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(dbName, TSFileConfig.STRING_CHARSET));

      if (Long.MAX_VALUE == storageGroupInfo.getTTL()) {
        builder
            .getColumnBuilder(1)
            .writeBinary(new Binary(IoTDBConstant.TTL_INFINITE, TSFileConfig.STRING_CHARSET));
      } else {
        builder
            .getColumnBuilder(1)
            .writeBinary(
                new Binary(String.valueOf(storageGroupInfo.getTTL()), TSFileConfig.STRING_CHARSET));
      }
      builder.getColumnBuilder(2).writeInt(storageGroupInfo.getSchemaReplicationFactor());
      builder.getColumnBuilder(3).writeInt(storageGroupInfo.getDataReplicationFactor());
      builder.getColumnBuilder(4).writeLong(storageGroupInfo.getTimePartitionInterval());
      builder
          .getColumnBuilder(5)
          .writeBinary(
              new Binary(
                  storageGroupInfo.isIsTableModel() ? "TABLE" : "TREE",
                  TSFileConfig.STRING_CHARSET));
      builder.declarePosition();
    }

    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowDBDetailsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
