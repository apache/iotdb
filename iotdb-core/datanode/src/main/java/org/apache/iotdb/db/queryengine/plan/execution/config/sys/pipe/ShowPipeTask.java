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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.db.pipe.metric.PipeDataNodeRemainingEventAndTimeMetrics;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.stream.Collectors;

public class ShowPipeTask implements IConfigTask {

  private final ShowPipesStatement showPipesStatement;

  public ShowPipeTask(final ShowPipesStatement showPipesStatement) {
    this.showPipesStatement = showPipesStatement;
  }

  public ShowPipeTask(ShowPipes node) {
    showPipesStatement = new ShowPipesStatement();
    showPipesStatement.setPipeName(node.getPipeName());
    showPipesStatement.setWhereClause(node.hasWhereClause());
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showPipes(showPipesStatement);
  }

  public static void buildTSBlock(
      final List<TShowPipeInfo> pipeInfoList, final SettableFuture<ConfigTaskResult> future) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showPipeColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final TShowPipeInfo tPipeInfo : pipeInfoList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder
          .getColumnBuilder(0)
          .writeBinary(new Binary(tPipeInfo.getId(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(1)
          .writeBinary(
              new Binary(
                  DateTimeUtils.convertLongToDate(tPipeInfo.getCreationTime(), "ms"),
                  TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(2)
          .writeBinary(new Binary(tPipeInfo.getState(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(3)
          .writeBinary(new Binary(tPipeInfo.getPipeExtractor(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(4)
          .writeBinary(new Binary(tPipeInfo.getPipeProcessor(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(5)
          .writeBinary(new Binary(tPipeInfo.getPipeConnector(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(6)
          .writeBinary(new Binary(tPipeInfo.getExceptionMessage(), TSFileConfig.STRING_CHARSET));

      // Optional, default 0/0.0
      long remainingEventCount = tPipeInfo.getRemainingEventCount();
      double remainingTime = tPipeInfo.getEstimatedRemainingTime();

      if (remainingEventCount == -1 && remainingTime == -1) {
        final Pair<Long, Double> remainingEventAndTime =
            PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
                .getRemainingEventAndTime(tPipeInfo.getId(), tPipeInfo.getCreationTime());
        remainingEventCount = remainingEventAndTime.getLeft();
        remainingTime = remainingEventAndTime.getRight();
      }

      builder
          .getColumnBuilder(7)
          .writeBinary(
              new Binary(
                  tPipeInfo.isSetRemainingEventCount()
                      ? String.valueOf(remainingEventCount)
                      : "Unknown",
                  TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(8)
          .writeBinary(
              new Binary(
                  tPipeInfo.isSetEstimatedRemainingTime()
                      ? String.format("%.2f", remainingTime)
                      : "Unknown",
                  TSFileConfig.STRING_CHARSET));
      builder.declarePosition();
    }
    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowPipeHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
