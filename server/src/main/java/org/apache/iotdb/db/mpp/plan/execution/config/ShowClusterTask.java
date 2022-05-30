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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
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

public class ShowClusterTask implements IConfigTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowClusterTask.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ShowClusterStatement showClusterStatement;

  public ShowClusterTask(ShowClusterStatement showClusterStatement) {
    this.showClusterStatement = showClusterStatement;
  }

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
    } else {
      future.setException(new TException("SHOW CLUSTER not support local mode"));
    }

    // build TSBlock
    TsBlockBuilder builder =
        new TsBlockBuilder(HeaderConstant.showClusterHeader.getRespDataTypes());
    int configNodeId = 0;
    for (TConfigNodeLocation nodeLocation : clusterNodeInfos.getConfigNodeList()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeInt(configNodeId++);
      builder.getColumnBuilder(1).writeBinary(new Binary("ConfigNode"));
      builder.getColumnBuilder(2).writeBinary(new Binary("Running"));
      builder
          .getColumnBuilder(3)
          .writeBinary(new Binary(nodeLocation.getInternalEndPoint().getIp()));
      builder.getColumnBuilder(4).writeInt(nodeLocation.getInternalEndPoint().getPort());
      builder.declarePosition();
    }
    for (TDataNodeLocation nodeLocation : clusterNodeInfos.getDataNodeList()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeInt(nodeLocation.getDataNodeId());
      builder.getColumnBuilder(1).writeBinary(new Binary("DataNode"));
      builder.getColumnBuilder(2).writeBinary(new Binary("Running"));
      builder
          .getColumnBuilder(3)
          .writeBinary(new Binary(nodeLocation.getInternalEndPoint().getIp()));
      builder.getColumnBuilder(4).writeInt(nodeLocation.getInternalEndPoint().getPort());
      builder.declarePosition();
    }

    DatasetHeader datasetHeader = HeaderConstant.showClusterHeader;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
    return future;
  }
}
