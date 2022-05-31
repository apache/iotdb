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

package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TClusterNodeInfos;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.conf.IoTDBConstant.NODE_STATUS_RUNNING;
import static org.apache.iotdb.commons.conf.IoTDBConstant.NODE_TYPE_CONFIG_NODE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.NODE_TYPE_DATA_NODE;

public class ShowClusterTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowClusterTask.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public ShowClusterTask(ShowClusterStatement showClusterStatement) {}

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TClusterNodeInfos clusterNodeInfos = new TClusterNodeInfos();

    if (config.isClusterMode()) {
      try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        clusterNodeInfos = client.getAllClusterNodeInfos();
      } catch (TException | IOException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      }
    }

    // build TSBlock
    TsBlockBuilder builder =
        new TsBlockBuilder(HeaderConstant.showClusterHeader.getRespDataTypes());

    AtomicInteger configNodeId = new AtomicInteger();
    clusterNodeInfos
        .getConfigNodeList()
        .forEach(
            e ->
                buildTsBlock(
                    builder,
                    configNodeId.getAndIncrement(),
                    NODE_TYPE_CONFIG_NODE,
                    NODE_STATUS_RUNNING,
                    e.getInternalEndPoint().getIp(),
                    e.getInternalEndPoint().getPort()));

    clusterNodeInfos
        .getDataNodeList()
        .forEach(
            e ->
                buildTsBlock(
                    builder,
                    e.getDataNodeId(),
                    NODE_TYPE_DATA_NODE,
                    NODE_STATUS_RUNNING,
                    e.getInternalEndPoint().getIp(),
                    e.getInternalEndPoint().getPort()));

    DatasetHeader datasetHeader = HeaderConstant.showClusterHeader;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
    return future;
  }

  private void buildTsBlock(
      TsBlockBuilder builder,
      int nodeId,
      String nodeType,
      String nodeStatus,
      String hostAddress,
      int port) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeInt(nodeId);
    builder.getColumnBuilder(1).writeBinary(new Binary(nodeType));
    builder.getColumnBuilder(2).writeBinary(new Binary(nodeStatus));
    builder.getColumnBuilder(3).writeBinary(new Binary(hostAddress));
    builder.getColumnBuilder(4).writeInt(port);
    builder.declarePosition();
  }
}
