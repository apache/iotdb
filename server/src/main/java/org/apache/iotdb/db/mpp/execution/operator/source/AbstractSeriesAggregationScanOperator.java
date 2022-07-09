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
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.calculateAggregationFromRawData;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;

public abstract class AbstractSeriesAggregationScanOperator implements DataSourceOperator {

  protected final PlanNodeId sourceId;
  protected final OperatorContext operatorContext;
  protected final boolean ascending;
  protected final boolean isGroupByQuery;

  protected final SeriesScanUtil seriesScanUtil;

  protected TsBlock inputTsBlock;

  protected final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  protected TimeRange curTimeRange;

  // We still think aggregator in SeriesAggregateScanOperator is a inputRaw step.
  // But in facing of statistics, it will invoke another method processStatistics()
  protected final List<Aggregator> aggregators;

  // using for building result tsBlock
  protected final TsBlockBuilder resultTsBlockBuilder;

  protected boolean finished = false;

  public AbstractSeriesAggregationScanOperator(
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Set<String> allSensors,
      OperatorContext context,
      List<Aggregator> aggregators,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.ascending = ascending;
    this.isGroupByQuery = groupByTimeParameter != null;
    this.aggregators = aggregators;

    if (seriesPath instanceof MeasurementPath) {
      this.seriesScanUtil =
          new SeriesScanUtil(
              seriesPath,
              allSensors,
              seriesPath.getSeriesType(),
              context.getInstanceContext(),
              timeFilter,
              null,
              ascending);
    } else if (seriesPath instanceof AlignedPath) {
      this.seriesScanUtil =
          new AlignedSeriesScanUtil(
              seriesPath,
              new HashSet<>(((AlignedPath) seriesPath).getMeasurementList()),
              context.getInstanceContext(),
              timeFilter,
              null,
              ascending);
    } else {
      throw new IllegalArgumentException(
          "SeriesAggregationScanOperator: The seriesPath should be MeasurementPath or AlignedPath.");
    }

    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource(dataSource);
  }

  @Override
  public boolean hasNext() {
    return timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    // reset operator state
    resultTsBlockBuilder.reset();

    while (System.nanoTime() - start < maxRuntime
        && timeRangeIterator.hasNextTimeRange()
        && !resultTsBlockBuilder.isFull()) {
      // move to next time window
      curTimeRange = timeRangeIterator.nextTimeRange();

      // clear previous aggregation result
      for (Aggregator aggregator : aggregators) {
        aggregator.updateTimeRange(curTimeRange);
      }

      // calculate aggregation result on current time window
      calculateNextAggregationResult();
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      return resultTsBlockBuilder.build();
    } else {
      return null;
    }
  }

  @Override
  public boolean isFinished() {
    return finished || (finished = !hasNext());
  }

  protected void calculateNextAggregationResult() {
    try {
      if (calcFromCachedData()) {
        updateResultTsBlock();
        return;
      }

      // read page data firstly
      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        return;
      }

      // read chunk data secondly
      if (readAndCalcFromChunk()) {
        updateResultTsBlock();
        return;
      }

      // read from file
      if (readAndCalcFromFile()) {
        updateResultTsBlock();
        return;
      }

      updateResultTsBlock();
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  protected void updateResultTsBlock() {
    appendAggregationResult(resultTsBlockBuilder, aggregators, timeRangeIterator);
  }

  protected boolean calcFromCachedData() {
    return calcFromRawData(inputTsBlock);
  }

  protected boolean calcFromRawData(TsBlock tsBlock) {
    boolean isFinishCalc =
        calculateAggregationFromRawData(tsBlock, aggregators, curTimeRange, ascending);
    // can calc for next interval
    if (tsBlock != null && !tsBlock.isEmpty()) {
      inputTsBlock = tsBlock;
    }
    return isFinishCalc;
  }

  protected abstract void calcFromStatistics(Statistics[] statistics);

  protected abstract boolean readAndCalcFromFile() throws IOException;

  protected abstract boolean readAndCalcFromChunk() throws IOException;

  protected abstract boolean readAndCalcFromPage() throws IOException;

  protected abstract boolean canUseCurrentFileStatistics() throws IOException;

  protected abstract boolean canUseCurrentChunkStatistics() throws IOException;

  protected abstract boolean canUseCurrentPageStatistics() throws IOException;
}
