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
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.factory.SourceOperatorFactory;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SeriesScanOperator extends AbstractDataSourceOperator {

  public static class SeriesScanOperatorFactory implements SourceOperatorFactory {
    private final int operatorId;
    private final PlanNodeId sourceId;
    private final PartialPath seriesPath;
    private final Set<String> allSensors;
    private final Filter timeFilter;
    private final Filter valueFilter;
    private final boolean ascending;
    private boolean closed;

    public SeriesScanOperatorFactory(
        int operatorId,
        PlanNodeId sourceId,
        PartialPath seriesPath,
        Set<String> allSensors,
        Filter timeFilter,
        Filter valueFilter,
        boolean ascending) {
      this.operatorId = operatorId;
      this.sourceId = requireNonNull(sourceId, "sourceId is null");
      this.seriesPath = requireNonNull(seriesPath, "seriesPath is null");
      this.allSensors = requireNonNull(allSensors, "allSensors is null");
      this.timeFilter = timeFilter;
      this.valueFilter = valueFilter;
      this.ascending = ascending;
    }

    public int getOperatorId() {
      return operatorId;
    }

    @Override
    public PlanNodeId getSourceId() {
      return sourceId;
    }

    public PlanNodeId getPlanNodeId() {
      return sourceId;
    }

    public String getOperatorType() {
      return SeriesScanOperator.class.getSimpleName();
    }

    @Override
    public SourceOperator createOperator(DriverContext driverContext) {
      checkState(!closed, "Factory is already closed");
      OperatorContext operatorContext =
          driverContext.addOperatorContext(operatorId, sourceId, getOperatorType());
      SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
      scanOptionsBuilder.withAllSensors(allSensors);
      scanOptionsBuilder.withGlobalTimeFilter(timeFilter);
      scanOptionsBuilder.withQueryFilter(valueFilter);
      return new SeriesScanOperator(
          operatorContext,
          sourceId,
          seriesPath,
          ascending ? Ordering.ASC : Ordering.DESC,
          scanOptionsBuilder.build());
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  private final TsBlockBuilder builder;
  private boolean finished = false;

  public SeriesScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new SeriesScanUtil(seriesPath, scanOrder, seriesScanOptions, context.getInstanceContext());
    this.maxReturnSize =
        Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.builder = new TsBlockBuilder(seriesScanUtil.getTsDataTypeList());
  }

  @Override
  public TsBlock next() {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    resultTsBlock = builder.build();
    builder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  public boolean hasNext() {
    if (retainedTsBlock != null) {
      return true;
    }
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
    }
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemory() - calculateMaxReturnSize();
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

    if (column.mayHaveNull()) {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        if (column.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, i);
        }
        builder.declarePosition();
      }
    } else {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.write(column, i);
        builder.declarePosition();
      }
    }
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }
}
