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

import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
<<<<<<< HEAD
<<<<<<< HEAD
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
<<<<<<< HEAD
=======
import org.apache.iotdb.db.client.DataNodeToConfigNodeClient;
=======
>>>>>>> abbe779bb7 (add interface)
>>>>>>> 8e3e7554e5 (add interface)
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
=======
>>>>>>> 5ff2b3fc1c (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.execution.config.fetcher.IConfigTaskFetcher;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ShowStorageGroupTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowStorageGroupTask.class);

  private ShowStorageGroupStatement showStorageGroupStatement;

  public ShowStorageGroupTask(ShowStorageGroupStatement showStorageGroupStatement) {
    this.showStorageGroupStatement = showStorageGroupStatement;
  }

  @Override
<<<<<<< HEAD
  public ListenableFuture<ConfigTaskResult> execute(
<<<<<<< HEAD
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
=======
<<<<<<< HEAD
      IClientManager<PartitionRegionId, DataNodeToConfigNodeClient> clientManager)
=======
          IConfigTaskFetcher configTaskFetcher)
>>>>>>> abbe779bb7 (add interface)
<<<<<<< HEAD
>>>>>>> 8e3e7554e5 (add interface)
=======
=======
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskFetcher configTaskFetcher)
>>>>>>> 5ff2b3fc1c (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
>>>>>>> e2a8c6743a (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
      throws InterruptedException {
    return configTaskFetcher.showStorageGroup(showStorageGroupStatement);
  }

<<<<<<< HEAD
    if (config.isClusterMode()) {
<<<<<<< HEAD
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
=======

>>>>>>> abbe779bb7 (add interface)
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
=======
  public static void buildTSBlock(
      Map<String, TStorageGroupSchema> storageGroupSchemaMap,
      SettableFuture<ConfigTaskResult> future) {
>>>>>>> 5ff2b3fc1c (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
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
  }
}
