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

import org.apache.iotdb.confignode.rpc.thrift.TClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
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

public class ShowVariablesTask implements IConfigTask {

  public ShowVariablesTask() {
    // Empty constructor
  }

  private static void buildTSBlock(TsBlockBuilder builder, Binary parameter, Binary value) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(parameter);
    builder.getColumnBuilder(1).writeBinary(value);
    builder.declarePosition();
  }

  public static void buildTSBlock(
      TShowVariablesResp showVariablesResp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showVariablesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

    TClusterParameters clusterParameters = showVariablesResp.getClusterParameters();
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.COLUMN_CLUSTER_NAME),
        new Binary(clusterParameters.getClusterName()));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DATA_REPLICATION_FACTOR),
        new Binary(String.valueOf(clusterParameters.getDataReplicationFactor())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SCHEMA_REPLICATION_FACTOR),
        new Binary(String.valueOf(clusterParameters.getSchemaReplicationFactor())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS),
        new Binary(clusterParameters.getDataRegionConsensusProtocolClass()));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS),
        new Binary(clusterParameters.getSchemaRegionConsensusProtocolClass()));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS),
        new Binary(clusterParameters.getConfigNodeConsensusProtocolClass()));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.TIME_PARTITION_INTERVAL),
        new Binary(String.valueOf(clusterParameters.getTimePartitionInterval())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DEFAULT_TTL),
        new Binary(String.valueOf(clusterParameters.getDefaultTTL())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.READ_CONSISTENCY_LEVEL),
        new Binary(String.valueOf(clusterParameters.getReadConsistencyLevel())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SCHEMA_REGION_PER_DATA_NODE),
        new Binary(String.valueOf(clusterParameters.getSchemaRegionPerDataNode())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DATA_REGION_PER_PROCESSOR),
        new Binary(String.valueOf(clusterParameters.getDataRegionPerProcessor())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SERIES_SLOT_NUM),
        new Binary(String.valueOf(clusterParameters.getSeriesPartitionSlotNum())));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SERIES_SLOT_EXECUTOR_CLASS),
        new Binary(clusterParameters.getSeriesPartitionExecutorClass()));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DISK_SPACE_WARNING_THRESHOLD),
        new Binary(String.valueOf(clusterParameters.getDiskSpaceWarningThreshold())));

    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowClusterParametersHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showClusterParameters();
  }
}
