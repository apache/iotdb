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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.List;

public class AlignedSeriesScanOperator extends AbstractSeriesScanOperator {

  private final int valueColumnCount;
  private int maxTsBlockLineNum = -1;

  public AlignedSeriesScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      AlignedPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      boolean queryAllSensors,
      List<TSDataType> dataTypes,
      int maxTsBlockLineNum) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new AlignedSeriesScanUtil(
            seriesPath,
            scanOrder,
            seriesScanOptions,
            context.getInstanceContext(),
            queryAllSensors,
            dataTypes);
    this.valueColumnCount = seriesPath.getColumnNum();
    this.maxReturnSize =
        Math.min(
            maxReturnSize,
            (1L + valueColumnCount)
                * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.maxTsBlockLineNum = maxTsBlockLineNum;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        maxReturnSize,
        (1L + valueColumnCount)
            * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()
            * 3L);
  }

  @Override
  protected void buildResult(TsBlock tsBlock) {
    int size = tsBlock.getPositionCount();
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    for (int i = 0; i < size; i++) {
      timeColumnBuilder.writeLong(timeColumn.getLong(i));
      resultTsBlockBuilder.declarePosition();
    }
    for (int columnIndex = 0, columnSize = tsBlock.getValueColumnCount();
        columnIndex < columnSize;
        columnIndex++) {
      appendOneColumn(columnIndex, tsBlock, size);
    }
  }

  private void appendOneColumn(int columnIndex, TsBlock tsBlock, int size) {
    ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(columnIndex);
    Column column = tsBlock.getColumn(columnIndex);
    if (column.mayHaveNull()) {
      for (int i = 0; i < size; i++) {
        if (column.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, i);
        }
      }
    } else {
      for (int i = 0; i < size; i++) {
        columnBuilder.write(column, i);
      }
    }
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource(dataSource);
    resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    resultTsBlockBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
  }
}
