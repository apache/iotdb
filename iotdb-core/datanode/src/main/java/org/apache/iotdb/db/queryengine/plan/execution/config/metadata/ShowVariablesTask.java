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

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

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
        new Binary(ColumnHeaderConstant.COLUMN_CLUSTER_NAME, TSFileConfig.STRING_CHARSET),
        new Binary(clusterParameters.getClusterName(), TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DATA_REPLICATION_FACTOR, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getDataReplicationFactor()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SCHEMA_REPLICATION_FACTOR, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getSchemaReplicationFactor()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(
            ColumnHeaderConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS, TSFileConfig.STRING_CHARSET),
        new Binary(
            clusterParameters.getDataRegionConsensusProtocolClass(), TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(
            ColumnHeaderConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS,
            TSFileConfig.STRING_CHARSET),
        new Binary(
            clusterParameters.getSchemaRegionConsensusProtocolClass(),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(
            ColumnHeaderConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS, TSFileConfig.STRING_CHARSET),
        new Binary(
            clusterParameters.getConfigNodeConsensusProtocolClass(), TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.TIME_PARTITION_ORIGIN, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getTimePartitionOrigin()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.TIME_PARTITION_INTERVAL, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getTimePartitionInterval()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.READ_CONSISTENCY_LEVEL, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getReadConsistencyLevel()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SCHEMA_REGION_PER_DATA_NODE, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getSchemaRegionPerDataNode()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DATA_REGION_PER_DATA_NODE, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getDataRegionPerDataNode()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SERIES_SLOT_NUM, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getSeriesPartitionSlotNum()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.SERIES_SLOT_EXECUTOR_CLASS, TSFileConfig.STRING_CHARSET),
        new Binary(
            clusterParameters.getSeriesPartitionExecutorClass(), TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.DISK_SPACE_WARNING_THRESHOLD, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getDiskSpaceWarningThreshold()),
            TSFileConfig.STRING_CHARSET));
    buildTSBlock(
        builder,
        new Binary(ColumnHeaderConstant.TIMESTAMP_PRECISION, TSFileConfig.STRING_CHARSET),
        new Binary(
            String.valueOf(clusterParameters.getTimestampPrecision()),
            TSFileConfig.STRING_CHARSET));

    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowClusterParametersHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showClusterParameters();
  }
}
