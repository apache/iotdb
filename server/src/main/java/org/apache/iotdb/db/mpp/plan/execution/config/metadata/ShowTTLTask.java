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

package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowTTLTask implements IConfigTask {

  private final ShowTTLStatement showTTLStatement;

  public ShowTTLTask(ShowTTLStatement showTTLStatement) {
    this.showTTLStatement = showTTLStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showTTL(showTTLStatement);
  }

  public static void buildTSBlock(
      Map<String, Long> storageGroupToTTL, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showTTLColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
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
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowTTLHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
