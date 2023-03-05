/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.AggregationOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions.Builder;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SeriesAggregationScanTraverseOperator extends AggregationOperator
    implements DataSourceOperator {

  protected final PlanNodeId sourceId;
  protected final PartialPath seriesPath;
  protected final Ordering scanOrder;

  protected List<AbstractDataSourceOperator> scanOperatorList;
  protected final boolean isAligned;
  protected final SeriesScanOptions.Builder seriesScanOptionsBuilder;
  protected final int dop;

  public SeriesAggregationScanTraverseOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      PartialPath seriesPath,
      Ordering scanOrder,
      List<Operator> childSourceOperator,
      List<AbstractDataSourceOperator> scanOperatorList,
      Builder seriesScanOptionsBuilder,
      boolean isAligned,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      long maxReturnSize) {
    super(operatorContext, aggregators, timeRangeIterator, childSourceOperator, maxReturnSize);
    this.sourceId = sourceId;
    this.seriesPath = seriesPath;
    this.scanOrder = scanOrder;
    this.scanOperatorList = scanOperatorList;
    this.dop = childSourceOperator.size();
    this.seriesScanOptionsBuilder = seriesScanOptionsBuilder;
    this.isAligned = isAligned;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    dataSource.fillOrderIndexes(seriesPath.getDevice(), scanOrder.isAscending());
    // updated filter concerning TTL
    seriesScanOptionsBuilder.withTTL(dataSource.getDataTTL());

    List<TsFileResource> seqResources = dataSource.getSeqResources();
    List<TsFileResource> unSeqResources = dataSource.getUnseqResources();
    int[] satisfiedSeqFileIndexList = new int[seqResources.size()];
    int seqFileNum = 0;
    long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
    for (int i = 0; i < seqResources.size(); i++) {
      TsFileResource tsFileResource = seqResources.get(i);
      if (tsFileResource != null
          && tsFileResource.isSatisfied(
              seriesPath.getDevice(), getGlobalTimeFilter(), true, false)) {
        satisfiedSeqFileIndexList[seqFileNum++] = i;
        minTime = Math.min(minTime, tsFileResource.getStartTime(seriesPath.getDevice()));
        if (tsFileResource.isClosed()) {
          maxTime = Math.max(maxTime, tsFileResource.getEndTime(seriesPath.getDevice()));
        } else {
          maxTime = Long.MAX_VALUE;
        }
      }
    }
    if (seqFileNum == 0) {
      // leave one to generate empty result
      closeRedundantSourceOperator(1);
      SeriesScanUtil seriesScanUtil = createSeriesScanUtil(seriesScanOptionsBuilder.build());
      seriesScanUtil.initQueryDataSource(
          dataSource, Collections.emptyList(), dataSource.getUnSeqFileOrderIndex());
      scanOperatorList.get(0).setSeriesScanUtil(seriesScanUtil);
      updateAggregatorsInputLocation();
      return;
    }

    for (TsFileResource tsFileResource : unSeqResources) {
      if (tsFileResource != null
          && tsFileResource.isSatisfied(
              seriesPath.getDevice(), getGlobalTimeFilter(), false, false)) {
        minTime = Math.min(minTime, tsFileResource.getStartTime(seriesPath.getDevice()));
        if (tsFileResource.isClosed()) {
          maxTime = Math.max(maxTime, tsFileResource.getEndTime(seriesPath.getDevice()));
        } else {
          maxTime = Long.MAX_VALUE;
        }
      }
    }

    // Avoid split one file to more than one ScanOperator
    int splitNum = dop;
    if (seqFileNum < dop) {
      splitNum = seqFileNum;
      closeRedundantSourceOperator(splitNum);
    }
    long avgTime = (maxTime - minTime) / splitNum;
    long startTime = minTime, endTime = minTime + avgTime;
    int curSeqFile = 0;
    for (int i = 0; i < splitNum; i++) {
      if (i == splitNum - 1 && endTime < maxTime) {
        endTime = maxTime;
      }
      List<Integer> seqFileIndexList = new ArrayList<>();
      AndFilter timeRangeFilter = getCurTimeRangeFilter(startTime, endTime);
      Filter newGlobalFilter =
          getGlobalTimeFilter() == null
              ? timeRangeFilter
              : new AndFilter(getGlobalTimeFilter(), timeRangeFilter);

      // update timeFilter using timeRange in tsFileResource
      long curMinTime = startTime, curMaxTime = endTime;
      while (curSeqFile < seqFileNum
          && seqResources
              .get(satisfiedSeqFileIndexList[curSeqFile])
              .isSatisfied(seriesPath.getDevice(), newGlobalFilter, true, false)) {
        TsFileResource seqFileResource = seqResources.get(satisfiedSeqFileIndexList[curSeqFile]);
        // update time range otherwise some points may be missed
        curMinTime = Math.min(curMinTime, seqFileResource.getStartTime(seriesPath.getDevice()));
        curMaxTime = Math.max(curMaxTime, seqFileResource.getEndTime(seriesPath.getDevice()));
        // make sure one tsFile can only be processed in one ScanOperator
        seqFileIndexList.add(satisfiedSeqFileIndexList[curSeqFile++]);
      }
      // make sure at least one tsFile can be processed in one ScanOperator
      if (seqFileIndexList.isEmpty()) {
        if (curSeqFile < seqFileNum) {
          TsFileResource seqFileResource = seqResources.get(satisfiedSeqFileIndexList[curSeqFile]);
          curMinTime = Math.min(curMinTime, seqFileResource.getStartTime(seriesPath.getDevice()));
          curMaxTime = Math.max(curMaxTime, seqFileResource.getEndTime(seriesPath.getDevice()));
          seqFileIndexList.add(satisfiedSeqFileIndexList[curSeqFile++]);
          // if there is no more tsFile can be processed
        } else {
          closeRedundantSourceOperator(i);
          updateAggregatorsInputLocation();
          return;
        }
      }
      SeriesScanOptions scanOptions = seriesScanOptionsBuilder.build();
      if (curMinTime != startTime || curMaxTime != endTime) {
        timeRangeFilter.setLeft(TimeFilter.gtEq(curMinTime));
        timeRangeFilter.setRight(TimeFilter.ltEq(curMaxTime));
      }
      scanOptions.setGlobalTimeFilter(newGlobalFilter);
      SeriesScanUtil seriesScanUtil = createSeriesScanUtil(scanOptions);
      seriesScanUtil.initQueryDataSource(
          dataSource, seqFileIndexList, dataSource.getUnSeqFileOrderIndex());
      scanOperatorList.get(i).setSeriesScanUtil(seriesScanUtil);
      if (children.get(i) instanceof ExchangeOperator) {
        ((ExchangeOperator) children.get(i)).allowRunning();
      }
      // update next time range
      startTime = curMaxTime + 1;
      endTime = Math.min(startTime + avgTime, maxTime);
    }
    updateAggregatorsInputLocation();
  }

  /**
   * Aggregators and timeRangeIterators of all scanOperators were the same reference to reduce
   * memory footprint, now we reallocate aggregators and deep copy timeRangeIterator according to
   * actual used aggScanOperator.
   */
  private void updateAggregatorsInputLocation() {
    int columnIndex = 0;
    List<Aggregator> childAggregators =
        ((AbstractSeriesAggregationScanOperator) scanOperatorList.get(0)).getAggregators();
    for (int i = 0; i < aggregators.size(); i++) {
      // calculate new input location
      List<InputLocation[]> inputLocationList = new ArrayList<>();
      int partialResultLen = childAggregators.get(i).getOutputType().length;
      for (int j = 0; j < scanOperatorList.size(); j++) {
        if (partialResultLen == 1) {
          inputLocationList.add(new InputLocation[] {new InputLocation(j, columnIndex)});
        } else {
          inputLocationList.add(
              new InputLocation[] {
                new InputLocation(j, columnIndex), new InputLocation(j, columnIndex + 1)
              });
        }
      }
      columnIndex += partialResultLen;
      aggregators.get(i).setInputLocationList(inputLocationList);
    }
  }

  private Filter getGlobalTimeFilter() {
    return seriesScanOptionsBuilder.getGlobalTimeFilter();
  }

  private AndFilter getCurTimeRangeFilter(long startTime, long endTime) {
    return new AndFilter(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
  }

  /**
   * Close seriesScanOperator from @index. Since the pipeline have been submitted, and some
   * pipelines may have dependency on this pipeline, so we have to let it run. But it will finish
   * immediately that won't waste system resource.
   */
  private void closeRedundantSourceOperator(int index) {
    for (int i = index; i < children.size(); i++) {
      scanOperatorList.get(i).setFinished(true);
      ((ExchangeOperator) children.get(i)).allowRunning();
    }
    children = new ArrayList<>(children.subList(0, index));
    scanOperatorList = new ArrayList<>(scanOperatorList.subList(0, index));
    inputOperatorsCount = children.size();
  }

  private SeriesScanUtil createSeriesScanUtil(SeriesScanOptions scanOptions) {
    if (isAligned) {
      return new AlignedSeriesScanUtil(
          seriesPath, scanOrder, scanOptions, operatorContext.getInstanceContext());
    } else {
      return new SeriesScanUtil(
          seriesPath, scanOrder, scanOptions, operatorContext.getInstanceContext());
    }
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }
}
