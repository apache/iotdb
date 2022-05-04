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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CountStorageGroupTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CountStorageGroupTask.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private CountStorageGroupStatement countStorageGroupStatement;

  public CountStorageGroupTask(CountStorageGroupStatement countStorageGroupStatement) {
    this.countStorageGroupStatement = countStorageGroupStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute() throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    int storageGroupNum = 0;
    if (config.isClusterMode()) {
      List<String> storageGroupPathPattern =
          Arrays.asList(countStorageGroupStatement.getPartialPath().getNodes());
      ConfigNodeClient client = null;
      try {
        client = new ConfigNodeClient();
        TCountStorageGroupResp resp = client.countMatchedStorageGroups(storageGroupPathPattern);
        storageGroupNum = resp.getCount();
      } catch (IoTDBConnectionException | BadNodeUrlException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      }
    } else {
      try {
        storageGroupNum =
            LocalConfigNode.getInstance()
                .getStorageGroupNum(
                    countStorageGroupStatement.getPartialPath(),
                    countStorageGroupStatement.isPrefixPath());
      } catch (MetadataException e) {
        future.setException(e);
      }
    }
    // build TSBlock
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeInt(storageGroupNum);
    builder.declarePosition();
    ColumnHeader storageGroupColumnHeader =
        new ColumnHeader(IoTDBConstant.COLUMN_COUNT, TSDataType.INT32);
    DatasetHeader datasetHeader =
        new DatasetHeader(Collections.singletonList(storageGroupColumnHeader), true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
    return future;
  }
}
