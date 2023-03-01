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
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SeriesScanTraverseOperator extends AbstractSourceOperator
    implements DataSourceOperator {

  private final PartialPath seriesPath;
  private final Ordering scanOrder;
  private List<Operator> childSourceOperator;
  private int curChildIndex = 0;

  private final List<SeriesScanOperator> scanOperatorList;
  private final SeriesScanOptions.Builder seriesScanOptionsBuilder;
  private final int dop;

  public SeriesScanTraverseOperator(
      OperatorContext operatorContext,
      PartialPath seriesPath,
      Ordering scanOrder,
      List<Operator> childSourceOperator,
      List<SeriesScanOperator> scanOperatorList,
      SeriesScanOptions.Builder seriesScanOptionsBuilder) {
    this.operatorContext = operatorContext;
    this.seriesPath = seriesPath;
    this.scanOrder = scanOrder;
    this.childSourceOperator = childSourceOperator;
    this.scanOperatorList = scanOperatorList;
    this.dop = childSourceOperator.size();
    this.seriesScanOptionsBuilder = seriesScanOptionsBuilder;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (curChildIndex >= childSourceOperator.size()) {
      return NOT_BLOCKED;
    }
    return childSourceOperator.get(curChildIndex).isBlocked();
  }

  @Override
  public TsBlock next() {
    if (!childSourceOperator.get(curChildIndex).hasNextWithTimer()) {
      curChildIndex++;
      return null;
    }
    return childSourceOperator.get(curChildIndex).nextWithTimer();
  }

  @Override
  public boolean hasNext() {
    return curChildIndex < childSourceOperator.size();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : childSourceOperator) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    return !this.hasNextWithTimer();
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
    return 0;
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
      childSourceOperator = Collections.emptyList();
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
      childSourceOperator = childSourceOperator.subList(0, splitNum);
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
          childSourceOperator = childSourceOperator.subList(0, i);
          return;
        }
      }
      SeriesScanOptions scanOptions = seriesScanOptionsBuilder.build();
      if (curMinTime != startTime || curMaxTime != endTime) {
        timeRangeFilter.setLeft(TimeFilter.gtEq(curMinTime));
        timeRangeFilter.setRight(TimeFilter.ltEq(curMaxTime));
      }
      scanOptions.setGlobalTimeFilter(newGlobalFilter);
      SeriesScanUtil seriesScanUtil =
          new SeriesScanUtil(
              seriesPath, scanOrder, scanOptions, operatorContext.getInstanceContext());
      seriesScanUtil.initQueryDataSource(
          dataSource, seqFileIndexList, dataSource.getUnSeqFileOrderIndex());
      scanOperatorList.get(i).setSeriesScanUtils(seriesScanUtil);
      if (childSourceOperator.get(i) instanceof ExchangeOperator) {
        ((ExchangeOperator) childSourceOperator.get(i)).allowRunning();
      }
      // update next time range
      startTime = curMaxTime + 1;
      endTime = Math.min(startTime + avgTime, maxTime);
    }
  }

  Filter getGlobalTimeFilter() {
    return seriesScanOptionsBuilder.getGlobalTimeFilter();
  }

  AndFilter getCurTimeRangeFilter(long startTime, long endTime) {
    return new AndFilter(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
  }
}
