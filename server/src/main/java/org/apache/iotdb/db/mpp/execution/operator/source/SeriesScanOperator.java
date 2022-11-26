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
package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.statistics.QueryStatistics.SERIES_SCAN_OPERATOR;

public class SeriesScanOperator implements DataSourceOperator {

  private final OperatorContext operatorContext;
  private final SeriesScanUtil seriesScanUtil;
  private final PlanNodeId sourceId;
  private final TsBlockBuilder builder;

  private boolean finished = false;

  private final long maxReturnSize;

  public SeriesScanOperator(
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      OperatorContext context,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new SeriesScanUtil(
            seriesPath,
            allSensors,
            dataType,
            context.getInstanceContext(),
            timeFilter,
            valueFilter,
            ascending);
    this.maxReturnSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.builder = new TsBlockBuilder(seriesScanUtil.getTsDataTypeList());
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    long startTime = System.nanoTime();
    try {
      TsBlock block = builder.build();
      builder.reset();
      return block;
    } finally {
      operatorContext.addOperatorTime(SERIES_SCAN_OPERATOR, System.nanoTime() - startTime);
    }
  }

  @Override
  public boolean hasNext() {
    long startTime = System.nanoTime();
    try {

      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      // here use do-while to promise doing this at least once
      do {
        /*
         * consume page data firstly
         */
        if (readPageData()) {
          continue;
        }

        /*
         * consume chunk data secondly
         */
        if (readChunkData()) {
          continue;
        }

        /*
         * consume next file finally
         */
        if (readFileData()) {
          continue;
        }
        break;

      } while (System.nanoTime() - start < maxRuntime && !builder.isFull());

      finished = builder.isEmpty();

      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    } finally {
      operatorContext.addOperatorTime(SERIES_SCAN_OPERATOR, System.nanoTime() - startTime);
    }
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  private boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      TsBlock tsBlock = seriesScanUtil.nextPage();

      if (!isEmpty(tsBlock)) {
        appendToBuilder(tsBlock);
        return true;
      }
    }
    return false;
  }

  private void appendToBuilder(TsBlock tsBlock) {
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);
    Column column = tsBlock.getColumn(0);

    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      timeColumnBuilder.writeLong(timeColumn.getLong(i));
      columnBuilder.write(column, i);
      builder.declarePosition();
    }
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource(dataSource);
  }
}
