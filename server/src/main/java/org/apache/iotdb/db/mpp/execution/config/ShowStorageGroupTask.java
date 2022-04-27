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

package org.apache.iotdb.db.mpp.execution.config;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ShowStorageGroupTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowStorageGroupTask.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final TSDataType[] RESOURCE_TYPES = {TSDataType.TEXT};

  private ShowStorageGroupStatement showStorageGroupStatement;

  public ShowStorageGroupTask(ShowStorageGroupStatement showStorageGroupStatement) {
    this.showStorageGroupStatement = showStorageGroupStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute() throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<String> storageGroupPaths = new ArrayList<>();
    if (config.isClusterMode()) {
      List<String> storageGroupPathPattern =
          Arrays.asList(showStorageGroupStatement.getPathPattern().getNodes());
      ConfigNodeClient client = null;
      try {
        client = new ConfigNodeClient();
        TStorageGroupSchemaResp resp =
            client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
        storageGroupPaths = new ArrayList<>(resp.getStorageGroupSchemaMap().keySet());
      } catch (IoTDBConnectionException | BadNodeUrlException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      } finally {
        if (client != null) {
          client.close();
        }
      }
    } else {
      try {
        List<PartialPath> partialPaths =
            LocalConfigNode.getInstance()
                .getMatchedStorageGroups(
                    showStorageGroupStatement.getPathPattern(),
                    showStorageGroupStatement.isPrefixPath());
        for (PartialPath partialPath : partialPaths) {
          storageGroupPaths.add(partialPath.getFullPath());
        }
      } catch (MetadataException e) {
        future.setException(e);
      }
    }
    // build TSBlock
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    for (String storageGroupPath : storageGroupPaths) {
      // The Time column will be ignored by the setting of ColumnHeader.
      // So we can put a meaningless value here
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(storageGroupPath));
      builder.declarePosition();
    }
    ColumnHeader storageGroupColumnHeader =
        new ColumnHeader(IoTDBConstant.COLUMN_STORAGE_GROUP, TSDataType.TEXT);
    DatasetHeader datasetHeader =
        new DatasetHeader(Collections.singletonList(storageGroupColumnHeader), true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
    return future;
  }
}
