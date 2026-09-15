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
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NODE_TYPE_AI_NODE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NODE_TYPE_CONFIG_NODE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NODE_TYPE_DATA_NODE;

public class ShowClusterDetailsTask implements IConfigTask {

  private final ShowClusterStatement showClusterStatement;

  public ShowClusterDetailsTask(ShowClusterStatement showClusterStatement) {
    this.showClusterStatement = showClusterStatement;
  }

  private static void buildConfigNodesTsBlock(
      TsBlockBuilder builder,
      int nodeId,
      String nodeStatus,
      String nodeStatusReason,
      boolean hasStatusReason,
      String internalAddress,
      int internalPort,
      int configConsensusPort,
      TNodeVersionInfo versionInfo) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeInt(nodeId);
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(NODE_TYPE_CONFIG_NODE, TSFileConfig.STRING_CHARSET));
    if (nodeStatus == null) {
      builder.getColumnBuilder(2).appendNull();
    } else {
      builder.getColumnBuilder(2).writeBinary(new Binary(nodeStatus, TSFileConfig.STRING_CHARSET));
    }
    if (hasStatusReason) {
      if (nodeStatusReason == null) {
        builder.getColumnBuilder(3).appendNull();
      } else {
        builder
            .getColumnBuilder(3)
            .writeBinary(new Binary(nodeStatusReason, TSFileConfig.STRING_CHARSET));
      }
    }
    int offset = hasStatusReason ? 1 : 0;
    if (internalAddress == null) {
      builder.getColumnBuilder(3 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(3 + offset)
          .writeBinary(new Binary(internalAddress, TSFileConfig.STRING_CHARSET));
    }
    builder.getColumnBuilder(4 + offset).writeInt(internalPort);
    builder
        .getColumnBuilder(5 + offset)
        .writeBinary(
            new Binary(Integer.toString(configConsensusPort), TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(6 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(7 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(8 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(9 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(10 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    if (versionInfo == null || versionInfo.getVersion() == null) {
      builder.getColumnBuilder(11 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(11 + offset)
          .writeBinary(new Binary(versionInfo.getVersion(), TSFileConfig.STRING_CHARSET));
    }
    if (versionInfo == null || versionInfo.getBuildInfo() == null) {
      builder.getColumnBuilder(12 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(12 + offset)
          .writeBinary(new Binary(versionInfo.getBuildInfo(), TSFileConfig.STRING_CHARSET));
    }
    builder.declarePosition();
  }

  private static void buildAINodeTsBlock(
      TsBlockBuilder builder,
      int nodeId,
      String nodeStatus,
      String nodeStatusReason,
      boolean hasStatusReason,
      String internalAddress,
      int internalPort,
      TNodeVersionInfo versionInfo) {

    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeInt(nodeId);
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(NODE_TYPE_AI_NODE, TSFileConfig.STRING_CHARSET));
    if (nodeStatus == null) {
      builder.getColumnBuilder(2).appendNull();
    } else {
      builder.getColumnBuilder(2).writeBinary(new Binary(nodeStatus, TSFileConfig.STRING_CHARSET));
    }
    if (hasStatusReason) {
      if (nodeStatusReason == null) {
        builder.getColumnBuilder(3).appendNull();
      } else {
        builder
            .getColumnBuilder(3)
            .writeBinary(new Binary(nodeStatusReason, TSFileConfig.STRING_CHARSET));
      }
    }
    int offset = hasStatusReason ? 1 : 0;
    if (internalAddress == null) {
      builder.getColumnBuilder(3 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(3 + offset)
          .writeBinary(new Binary(internalAddress, TSFileConfig.STRING_CHARSET));
    }
    builder.getColumnBuilder(4 + offset).writeInt(internalPort);
    builder.getColumnBuilder(5 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(6 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(7 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(8 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(9 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(10 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    if (versionInfo == null || versionInfo.getVersion() == null) {
      builder.getColumnBuilder(11 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(11 + offset)
          .writeBinary(new Binary(versionInfo.getVersion(), TSFileConfig.STRING_CHARSET));
    }
    if (versionInfo == null || versionInfo.getBuildInfo() == null) {
      builder.getColumnBuilder(12 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(12 + offset)
          .writeBinary(new Binary(versionInfo.getBuildInfo(), TSFileConfig.STRING_CHARSET));
    }
    builder.declarePosition();
  }

  @SuppressWarnings("squid:S107")
  private static void buildDataNodesTsBlock(
      TsBlockBuilder builder,
      int nodeId,
      String nodeStatus,
      String nodeStatusReason,
      boolean hasStatusReason,
      String internalAddress,
      int internalPort,
      String rpcAddress,
      int rpcPort,
      int dataConsensusPort,
      int schemaConsensusPort,
      int mppPort,
      TNodeVersionInfo versionInfo) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeInt(nodeId);
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(NODE_TYPE_DATA_NODE, TSFileConfig.STRING_CHARSET));
    if (nodeStatus == null) {
      builder.getColumnBuilder(2).appendNull();
    } else {
      builder.getColumnBuilder(2).writeBinary(new Binary(nodeStatus, TSFileConfig.STRING_CHARSET));
    }
    if (hasStatusReason) {
      if (nodeStatusReason == null) {
        builder.getColumnBuilder(3).appendNull();
      } else {
        builder
            .getColumnBuilder(3)
            .writeBinary(new Binary(nodeStatusReason, TSFileConfig.STRING_CHARSET));
      }
    }
    int offset = hasStatusReason ? 1 : 0;
    if (internalAddress == null) {
      builder.getColumnBuilder(3 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(3 + offset)
          .writeBinary(new Binary(internalAddress, TSFileConfig.STRING_CHARSET));
    }
    builder.getColumnBuilder(4 + offset).writeInt(internalPort);
    builder.getColumnBuilder(5 + offset).writeBinary(new Binary("", TSFileConfig.STRING_CHARSET));
    if (rpcAddress == null) {
      builder.getColumnBuilder(6 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(6 + offset)
          .writeBinary(new Binary(rpcAddress, TSFileConfig.STRING_CHARSET));
    }
    builder
        .getColumnBuilder(7 + offset)
        .writeBinary(new Binary(Integer.toString(rpcPort), TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(8 + offset)
        .writeBinary(new Binary(Integer.toString(dataConsensusPort), TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(9 + offset)
        .writeBinary(
            new Binary(Integer.toString(schemaConsensusPort), TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(10 + offset)
        .writeBinary(new Binary(Integer.toString(mppPort), TSFileConfig.STRING_CHARSET));
    if (versionInfo == null || versionInfo.getVersion() == null) {
      builder.getColumnBuilder(11 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(11 + offset)
          .writeBinary(new Binary(versionInfo.getVersion(), TSFileConfig.STRING_CHARSET));
    }
    if (versionInfo == null || versionInfo.getBuildInfo() == null) {
      builder.getColumnBuilder(12 + offset).appendNull();
    } else {
      builder
          .getColumnBuilder(12 + offset)
          .writeBinary(new Binary(versionInfo.getBuildInfo(), TSFileConfig.STRING_CHARSET));
    }
    builder.declarePosition();
  }

  public static void buildTSBlock(
      TShowClusterResp clusterNodeInfos, SettableFuture<ConfigTaskResult> future) {
    boolean hasStatusReason =
        clusterNodeInfos.getNodeStatusReason() != null
            && clusterNodeInfos.getNodeStatusReason().values().stream()
                .anyMatch(reason -> reason != null && !reason.isEmpty());
    List<ColumnHeader> columnHeaders =
        new ArrayList<>(ColumnHeaderConstant.showClusterDetailsColumnHeaders);
    if (hasStatusReason) {
      // insert after the Status column
      columnHeaders.add(3, new ColumnHeader(ColumnHeaderConstant.STATUS_REASON, TSDataType.TEXT));
    }
    List<TSDataType> outputDataTypes =
        columnHeaders.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

    clusterNodeInfos
        .getConfigNodeList()
        .forEach(
            e ->
                buildConfigNodesTsBlock(
                    builder,
                    e.getConfigNodeId(),
                    clusterNodeInfos.getNodeStatus().get(e.getConfigNodeId()),
                    hasStatusReason
                        ? clusterNodeInfos.getNodeStatusReason().get(e.getConfigNodeId())
                        : null,
                    hasStatusReason,
                    e.getInternalEndPoint().getIp(),
                    e.getInternalEndPoint().getPort(),
                    e.getConsensusEndPoint().getPort(),
                    clusterNodeInfos.getNodeVersionInfo().get(e.getConfigNodeId())));

    clusterNodeInfos
        .getDataNodeList()
        .forEach(
            e ->
                buildDataNodesTsBlock(
                    builder,
                    e.getDataNodeId(),
                    clusterNodeInfos.getNodeStatus().get(e.getDataNodeId()),
                    hasStatusReason
                        ? clusterNodeInfos.getNodeStatusReason().get(e.getDataNodeId())
                        : null,
                    hasStatusReason,
                    e.getInternalEndPoint().getIp(),
                    e.getInternalEndPoint().getPort(),
                    e.getClientRpcEndPoint().getIp(),
                    e.getClientRpcEndPoint().getPort(),
                    e.getMPPDataExchangeEndPoint().getPort(),
                    e.getSchemaRegionConsensusEndPoint().getPort(),
                    e.getDataRegionConsensusEndPoint().getPort(),
                    clusterNodeInfos.getNodeVersionInfo().get(e.getDataNodeId())));
    clusterNodeInfos
        .getAiNodeList()
        .forEach(
            e ->
                buildAINodeTsBlock(
                    builder,
                    e.getAiNodeId(),
                    clusterNodeInfos.getNodeStatus().get(e.getAiNodeId()),
                    hasStatusReason
                        ? clusterNodeInfos.getNodeStatusReason().get(e.getAiNodeId())
                        : null,
                    hasStatusReason,
                    e.getInternalEndPoint().getIp(),
                    e.getInternalEndPoint().getPort(),
                    clusterNodeInfos.getNodeVersionInfo().get(e.getAiNodeId())));

    DatasetHeader datasetHeader = new DatasetHeader(columnHeaders, true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showCluster(showClusterStatement);
  }
}
