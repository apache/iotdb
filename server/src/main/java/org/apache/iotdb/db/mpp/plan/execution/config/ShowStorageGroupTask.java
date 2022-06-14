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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowStorageGroupTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowStorageGroupTask.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ShowStorageGroupStatement showStorageGroupStatement;

  public ShowStorageGroupTask(ShowStorageGroupStatement showStorageGroupStatement) {
    this.showStorageGroupStatement = showStorageGroupStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    Map<String, TStorageGroupSchema> storageGroupSchemaMap = new HashMap<>();
    if (config.isClusterMode()) {
      List<String> storageGroupPathPattern =
          Arrays.asList(showStorageGroupStatement.getPathPattern().getNodes());
      try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        TStorageGroupSchemaResp resp =
            client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
        storageGroupSchemaMap = resp.getStorageGroupSchemaMap();
      } catch (TException | IOException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      }
    } else {
      try {
        LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
        List<PartialPath> partialPaths =
            localConfigNode.getMatchedStorageGroups(
                showStorageGroupStatement.getPathPattern(),
                showStorageGroupStatement.isPrefixPath());
        for (PartialPath storageGroupPath : partialPaths) {
          IStorageGroupMNode storageGroupMNode =
              localConfigNode.getStorageGroupNodeByPath(storageGroupPath);
          String storageGroup = storageGroupMNode.getFullPath();
          TStorageGroupSchema storageGroupSchema = storageGroupMNode.getStorageGroupSchema();
          storageGroupSchemaMap.put(storageGroup, storageGroupSchema);
        }
      } catch (MetadataException e) {
        future.setException(e);
      }
    }
    // build TSBlock
    TsBlockBuilder builder =
        new TsBlockBuilder(HeaderConstant.showStorageGroupHeader.getRespDataTypes());
    for (Map.Entry<String, TStorageGroupSchema> entry : storageGroupSchemaMap.entrySet()) {
      String storageGroup = entry.getKey();
      TStorageGroupSchema storageGroupSchema = entry.getValue();
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(storageGroup));
      if (Long.MAX_VALUE == storageGroupSchema.getTTL()) {
        builder.getColumnBuilder(1).appendNull();
      } else {
        builder.getColumnBuilder(1).writeLong(storageGroupSchema.getTTL());
      }
      builder.getColumnBuilder(2).writeInt(storageGroupSchema.getSchemaReplicationFactor());
      builder.getColumnBuilder(3).writeInt(storageGroupSchema.getDataReplicationFactor());
      builder.getColumnBuilder(4).writeLong(storageGroupSchema.getTimePartitionInterval());
      builder.declarePosition();
    }
    DatasetHeader datasetHeader = HeaderConstant.showStorageGroupHeader;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
    return future;
  }
}
