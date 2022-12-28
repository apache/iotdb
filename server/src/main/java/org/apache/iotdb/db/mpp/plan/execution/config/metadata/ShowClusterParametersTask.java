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

import org.apache.iotdb.confignode.rpc.thrift.TBasicClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterParametersResp;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.stream.Collectors;

public class ShowClusterParametersTask implements IConfigTask {

  public ShowClusterParametersTask() {
    // Empty constructor
  }

  public static void buildTSBlock(
      TShowClusterParametersResp showClusterParametersResp,
      SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showClusterParametersColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    builder.getTimeColumnBuilder().writeLong(0L);

    TBasicClusterParameters basicClusterParameters = showClusterParametersResp.getBasicParameters();
    builder.getColumnBuilder(0).writeBinary(new Binary(basicClusterParameters.getClusterName()));
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(basicClusterParameters.getConfigNodeConsensusProtocolClass()));
    builder
        .getColumnBuilder(2)
        .writeBinary(new Binary(basicClusterParameters.getDataRegionConsensusProtocolClass()));
    builder
        .getColumnBuilder(3)
        .writeBinary(new Binary(basicClusterParameters.getSchemaRegionConsensusProtocolClass()));
    builder.getColumnBuilder(4).writeInt(basicClusterParameters.getSeriesPartitionSlotNum());
    builder
        .getColumnBuilder(5)
        .writeBinary(new Binary(basicClusterParameters.getSeriesPartitionExecutorClass()));
    builder.getColumnBuilder(6).writeLong(basicClusterParameters.getDefaultTTL());
    builder.getColumnBuilder(7).writeLong(basicClusterParameters.getTimePartitionInterval());
    builder.getColumnBuilder(8).writeInt(basicClusterParameters.getDataReplicationFactor());
    builder.getColumnBuilder(9).writeInt(basicClusterParameters.getSchemaReplicationFactor());

    builder.declarePosition();
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowClusterParametersHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showClusterParameters();
  }
}
