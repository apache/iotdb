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
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
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

public class ShowTTLTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowTTLTask.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ShowTTLStatement showTTLStatement;

  public ShowTTLTask(ShowTTLStatement showTTLStatement) {
    this.showTTLStatement = showTTLStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<PartialPath> storageGroupPaths = showTTLStatement.getPaths();
    Map<String, Long> storageGroupToTTL = new HashMap<>();
    if (config.isClusterMode()) {
      try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        if (showTTLStatement.isAll()) {
          List<String> allStorageGroupPathPattern = Arrays.asList("root", "**");
          TStorageGroupSchemaResp resp =
              client.getMatchedStorageGroupSchemas(allStorageGroupPathPattern);
          for (Map.Entry<String, TStorageGroupSchema> entry :
              resp.getStorageGroupSchemaMap().entrySet()) {
            storageGroupToTTL.put(entry.getKey(), entry.getValue().getTTL());
          }
        } else {
          for (PartialPath storageGroupPath : storageGroupPaths) {
            List<String> storageGroupPathPattern = Arrays.asList(storageGroupPath.getNodes());
            TStorageGroupSchemaResp resp =
                client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
            for (Map.Entry<String, TStorageGroupSchema> entry :
                resp.getStorageGroupSchemaMap().entrySet()) {
              if (!storageGroupToTTL.containsKey(entry.getKey())) {
                storageGroupToTTL.put(entry.getKey(), entry.getValue().getTTL());
              }
            }
          }
        }
      } catch (TException | IOException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      }
    } else {
      try {
        Map<PartialPath, Long> allStorageGroupToTTL =
            LocalConfigNode.getInstance().getStorageGroupsTTL();
        for (PartialPath storageGroupPath : storageGroupPaths) {
          if (showTTLStatement.isAll()) {
            storageGroupToTTL.put(
                storageGroupPath.getFullPath(), allStorageGroupToTTL.get(storageGroupPath));
          } else {
            List<PartialPath> matchedStorageGroupPaths =
                LocalConfigNode.getInstance()
                    .getMatchedStorageGroups(storageGroupPath, showTTLStatement.isPrefixPath());
            for (PartialPath matchedStorageGroupPath : matchedStorageGroupPaths) {
              storageGroupToTTL.put(
                  matchedStorageGroupPath.getFullPath(),
                  allStorageGroupToTTL.get(matchedStorageGroupPath));
            }
          }
        }
      } catch (MetadataException e) {
        future.setException(e);
      }
    }
    // build TSBlock
    TsBlockBuilder builder = new TsBlockBuilder(HeaderConstant.showTTLHeader.getRespDataTypes());
    for (Map.Entry<String, Long> entry : storageGroupToTTL.entrySet()) {
      builder.getTimeColumnBuilder().writeLong(0);
      builder.getColumnBuilder(0).writeBinary(new Binary(entry.getKey()));
      if (Long.MAX_VALUE == entry.getValue()) {
        builder.getColumnBuilder(1).appendNull();
      } else {
        builder.getColumnBuilder(1).writeLong(entry.getValue());
      }
      builder.declarePosition();
    }
    DatasetHeader datasetHeader = HeaderConstant.showTTLHeader;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
    return future;
  }
}
