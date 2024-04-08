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

import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.relational.sql.tree.ShowDB;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowDBTask implements IConfigTask {

  private final ShowDB node;

  public ShowDBTask(ShowDB node) {
    this.node = node;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showDatabases(node);
  }

  public static void buildTSBlock(
      Map<String, TDatabaseInfo> storageGroupInfoMap, SettableFuture<ConfigTaskResult> future) {

    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showDBColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (Map.Entry<String, TDatabaseInfo> entry : storageGroupInfoMap.entrySet()) {
      String storageGroup = entry.getKey();
      TDatabaseInfo storageGroupInfo = entry.getValue();
      builder.getTimeColumnBuilder().writeLong(0L);
      builder
          .getColumnBuilder(0)
          .writeBinary(new Binary(storageGroup, TSFileConfig.STRING_CHARSET));

      builder.getColumnBuilder(1).writeInt(storageGroupInfo.getSchemaReplicationFactor());
      builder.getColumnBuilder(2).writeInt(storageGroupInfo.getDataReplicationFactor());
      builder.getColumnBuilder(3).writeLong(storageGroupInfo.getTimePartitionInterval());
      builder.declarePosition();
    }

    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowDBHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
