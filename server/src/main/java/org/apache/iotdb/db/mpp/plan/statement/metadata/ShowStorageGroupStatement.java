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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.SettableFuture;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SHOW DATABASES statement
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW DATABASES prefixPath?
 */
public class ShowStorageGroupStatement extends ShowStatement implements IConfigStatement {

  private final PartialPath pathPattern;
  private boolean isDetailed;

  public ShowStorageGroupStatement(PartialPath pathPattern) {
    super();
    this.pathPattern = pathPattern;
    this.isDetailed = false;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public boolean isDetailed() {
    return isDetailed;
  }

  public void setDetailed(boolean detailed) {
    isDetailed = detailed;
  }

  public void buildTSBlock(
      Map<String, TDatabaseInfo> storageGroupInfoMap, SettableFuture<ConfigTaskResult> future) {

    List<TSDataType> outputDataTypes =
        isDetailed
            ? ColumnHeaderConstant.showStorageGroupsDetailColumnHeaders.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList())
            : ColumnHeaderConstant.showStorageGroupsColumnHeaders.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList());

    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (Map.Entry<String, TDatabaseInfo> entry : storageGroupInfoMap.entrySet()) {
      String storageGroup = entry.getKey();
      TDatabaseInfo storageGroupInfo = entry.getValue();
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(storageGroup));

      if (Long.MAX_VALUE == storageGroupInfo.getTTL()) {
        builder.getColumnBuilder(1).appendNull();
      } else {
        builder.getColumnBuilder(1).writeLong(storageGroupInfo.getTTL());
      }
      builder.getColumnBuilder(2).writeInt(storageGroupInfo.getSchemaReplicationFactor());
      builder.getColumnBuilder(3).writeInt(storageGroupInfo.getDataReplicationFactor());
      builder.getColumnBuilder(4).writeLong(storageGroupInfo.getTimePartitionInterval());
      if (isDetailed) {
        builder.getColumnBuilder(5).writeInt(storageGroupInfo.getSchemaRegionNum());
        builder.getColumnBuilder(6).writeInt(storageGroupInfo.getMinSchemaRegionNum());
        builder.getColumnBuilder(7).writeInt(storageGroupInfo.getMaxSchemaRegionNum());
        builder.getColumnBuilder(8).writeInt(storageGroupInfo.getDataRegionNum());
        builder.getColumnBuilder(9).writeInt(storageGroupInfo.getMinDataRegionNum());
        builder.getColumnBuilder(10).writeInt(storageGroupInfo.getMaxDataRegionNum());
      }
      builder.declarePosition();
    }

    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowStorageGroupHeader(isDetailed);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowStorageGroup(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }
}
