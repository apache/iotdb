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

import org.apache.iotdb.commons.conf.IoTDBConstant;
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

  private static void buildTSBlock(TsBlockBuilder builder, Binary parameter, Binary value) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(parameter);
    builder.getColumnBuilder(1).writeBinary(value);
    builder.declarePosition();
  }

  public static void buildTSBlock(
      TShowClusterParametersResp showClusterParametersResp,
      SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showClusterParametersColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

    TBasicClusterParameters basicClusterParameters = showClusterParametersResp.getBasicParameters();
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.CLUSTER_NAME),
        new Binary(basicClusterParameters.getClusterName()));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS),
        new Binary(basicClusterParameters.getConfigNodeConsensusProtocolClass()));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS),
        new Binary(basicClusterParameters.getDataRegionConsensusProtocolClass()));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS),
        new Binary(basicClusterParameters.getSchemaRegionConsensusProtocolClass()));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.SERIES_SLOT_NUM),
        new Binary(String.valueOf(basicClusterParameters.getSeriesPartitionSlotNum())));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.SERIES_SLOT_EXECUTOR_CLASS),
        new Binary(basicClusterParameters.getSeriesPartitionExecutorClass()));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.DEFAULT_TTL),
        new Binary(String.valueOf(basicClusterParameters.getDefaultTTL())));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.TIME_PARTITION_INTERVAL),
        new Binary(String.valueOf(basicClusterParameters.getTimePartitionInterval())));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.DATA_REPLICATION_FACTOR),
        new Binary(String.valueOf(basicClusterParameters.getDataReplicationFactor())));
    buildTSBlock(
        builder,
        new Binary(IoTDBConstant.SCHEMA_REPLICATION_FACTOR),
        new Binary(String.valueOf(basicClusterParameters.getSchemaReplicationFactor())));

    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowClusterParametersHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showClusterParameters();
  }
}
