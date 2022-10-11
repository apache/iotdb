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

package org.apache.iotdb.db.mpp.plan.execution.config.sys.sync;

import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.ShowPipeSinkStatement;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.stream.Collectors;

public class ShowPipeSinkTask implements IConfigTask {

  private final ShowPipeSinkStatement showPipeSinkStatement;

  public ShowPipeSinkTask(ShowPipeSinkStatement showPipeSinkStatement) {
    this.showPipeSinkStatement = showPipeSinkStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showPipeSink(showPipeSinkStatement);
  }

  public static void buildTSBlockByPipeSink(
      List<PipeSink> pipeSinkList, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showPipeSinkColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (PipeSink pipeSink : pipeSinkList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(pipeSink.getPipeSinkName()));
      builder.getColumnBuilder(1).writeBinary(new Binary(pipeSink.getType().name()));
      builder.getColumnBuilder(2).writeBinary(new Binary(pipeSink.showAllAttributes()));
      builder.declarePosition();
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowPipeSinkHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  public static void buildTSBlockByTPipeSinkInfo(
      List<TPipeSinkInfo> pipeSinkInfoList, SettableFuture<ConfigTaskResult> future)
      throws PipeSinkException {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showPipeSinkColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (TPipeSinkInfo pipeSinkInfo : pipeSinkInfoList) {
      PipeSink pipeSink = SyncPipeUtil.parseTPipeSinkInfoAsPipeSink(pipeSinkInfo);
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(pipeSink.getPipeSinkName()));
      builder.getColumnBuilder(1).writeBinary(new Binary(pipeSink.getType().name()));
      builder.getColumnBuilder(2).writeBinary(new Binary(pipeSink.showAllAttributes()));
      builder.declarePosition();
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowPipeSinkHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
