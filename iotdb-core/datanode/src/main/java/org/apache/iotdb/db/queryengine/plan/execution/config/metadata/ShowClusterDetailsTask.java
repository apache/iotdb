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

import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.NODE_TYPE_CONFIG_NODE;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.NODE_TYPE_DATA_NODE;

public class ShowClusterDetailsTask implements IConfigTask {

  private final ShowClusterStatement showClusterStatement;

  public ShowClusterDetailsTask(ShowClusterStatement showClusterStatement) {
    this.showClusterStatement = showClusterStatement;
  }

  private static void buildConfigNodesTsBlock(
      TsBlockBuilder builder,
      int nodeId,
      String nodeStatus,
      String internalAddress,
      int internalPort,
      int configConsensusPort,
      TNodeVersionInfo versionInfo) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeInt(nodeId);
    builder.getColumnBuilder(1).writeBinary(new Binary(NODE_TYPE_CONFIG_NODE));
    builder.getColumnBuilder(2).writeBinary(new Binary(nodeStatus));
    builder.getColumnBuilder(3).writeBinary(new Binary(internalAddress));
    builder.getColumnBuilder(4).writeInt(internalPort);
    builder.getColumnBuilder(5).writeBinary(new Binary(Integer.toString(configConsensusPort)));
    builder.getColumnBuilder(6).writeBinary(new Binary(""));
    builder.getColumnBuilder(7).writeBinary(new Binary(""));
    builder.getColumnBuilder(8).writeBinary(new Binary(""));
    builder.getColumnBuilder(9).writeBinary(new Binary(""));
    builder.getColumnBuilder(10).writeBinary(new Binary(""));
    builder.getColumnBuilder(11).writeBinary(new Binary(versionInfo.getVersion()));
    builder.getColumnBuilder(12).writeBinary(new Binary(versionInfo.getBuildInfo()));
    builder.declarePosition();
  }

  @SuppressWarnings("squid:S107")
  private static void buildDataNodesTsBlock(
      TsBlockBuilder builder,
      int nodeId,
      String nodeStatus,
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
    builder.getColumnBuilder(1).writeBinary(new Binary(NODE_TYPE_DATA_NODE));
    builder.getColumnBuilder(2).writeBinary(new Binary(nodeStatus));
    builder.getColumnBuilder(3).writeBinary(new Binary(internalAddress));
    builder.getColumnBuilder(4).writeInt(internalPort);
    builder.getColumnBuilder(5).writeBinary(new Binary(""));
    builder.getColumnBuilder(6).writeBinary(new Binary(rpcAddress));
    builder.getColumnBuilder(7).writeBinary(new Binary(Integer.toString(rpcPort)));
    builder.getColumnBuilder(8).writeBinary(new Binary(Integer.toString(dataConsensusPort)));
    builder.getColumnBuilder(9).writeBinary(new Binary(Integer.toString(schemaConsensusPort)));
    builder.getColumnBuilder(10).writeBinary(new Binary(Integer.toString(mppPort)));
    builder.getColumnBuilder(11).writeBinary(new Binary(versionInfo.getVersion()));
    builder.getColumnBuilder(12).writeBinary(new Binary(versionInfo.getBuildInfo()));
    builder.declarePosition();
  }

  public static void buildTSBlock(
      TShowClusterResp clusterNodeInfos, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showClusterDetailsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

    clusterNodeInfos
        .getConfigNodeList()
        .forEach(
            e ->
                buildConfigNodesTsBlock(
                    builder,
                    e.getConfigNodeId(),
                    clusterNodeInfos.getNodeStatus().get(e.getConfigNodeId()),
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
                    e.getInternalEndPoint().getIp(),
                    e.getInternalEndPoint().getPort(),
                    e.getClientRpcEndPoint().getIp(),
                    e.getClientRpcEndPoint().getPort(),
                    e.getMPPDataExchangeEndPoint().getPort(),
                    e.getSchemaRegionConsensusEndPoint().getPort(),
                    e.getDataRegionConsensusEndPoint().getPort(),
                    clusterNodeInfos.getNodeVersionInfo().get(e.getDataNodeId())));

    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowClusterDetailsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showCluster(showClusterStatement);
  }
}
