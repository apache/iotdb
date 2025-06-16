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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.schemaengine.table.InformationSchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.InformationSchema.INFORMATION_DATABASE;

public class ShowDBTask implements IConfigTask {

  private final ShowDB node;

  // judge whether the specific database can be seen, dbName should be without `root.` prefix
  private final Predicate<String> canSeenDB;

  public ShowDBTask(final ShowDB node, final Predicate<String> canSeenDB) {
    this.node = node;
    this.canSeenDB = canSeenDB;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showDatabases(node, canSeenDB);
  }

  public static void buildTSBlock(
      final Map<String, TDatabaseInfo> databaseInfoMap,
      final SettableFuture<ConfigTaskResult> future,
      final boolean isDetails,
      final Predicate<String> canSeenDB) {
    if (isDetails) {
      buildTSBlockForDetails(databaseInfoMap, future, canSeenDB);
    } else {
      buildTSBlockForNonDetails(databaseInfoMap, future, canSeenDB);
    }
  }

  private static void buildTSBlockForNonDetails(
      final Map<String, TDatabaseInfo> databaseInfoMap,
      final SettableFuture<ConfigTaskResult> future,
      final Predicate<String> canSeenDB) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showDBColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    // Used to order by database name
    databaseInfoMap.put(INFORMATION_DATABASE, null);
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final Map.Entry<String, TDatabaseInfo> entry :
        databaseInfoMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      final String dbName = entry.getKey();
      if (Boolean.FALSE.equals(canSeenDB.test(dbName))) {
        continue;
      }
      if (dbName.equals(INFORMATION_DATABASE)) {
        InformationSchemaUtils.buildDatabaseTsBlock(builder, false, true);
        continue;
      }
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
      final Map<String, TDatabaseInfo> databaseMap,
      final SettableFuture<ConfigTaskResult> future,
      final Predicate<String> canSeenDB) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showDBDetailsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    // Used to order by database name
    databaseMap.put(INFORMATION_DATABASE, null);
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final Map.Entry<String, TDatabaseInfo> entry :
        databaseMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      final String dbName = entry.getKey();
      if (!canSeenDB.test(dbName)) {
        continue;
      }
      if (dbName.equals(INFORMATION_DATABASE)) {
        InformationSchemaUtils.buildDatabaseTsBlock(builder, true, true);
        continue;
      }
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
      builder.getColumnBuilder(5).writeInt(storageGroupInfo.getSchemaRegionNum());
      builder.getColumnBuilder(6).writeInt(storageGroupInfo.getDataRegionNum());
      builder.declarePosition();
    }

    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowDBDetailsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
