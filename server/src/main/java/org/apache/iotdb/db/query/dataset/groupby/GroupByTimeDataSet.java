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
package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.groupby.SlidingWindowGroupByExecutor;
import org.apache.iotdb.db.utils.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.utils.timerangeiterator.TimeRangeIteratorFactory;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.iotdb.db.qp.utils.DateTimeUtils.MS_TO_MONTH;

public abstract class GroupByTimeDataSet extends QueryDataSet {

  protected long queryId;
  protected long interval;
  protected long slidingStep;
  // total query [startTime, endTime)
  protected long startTime;
  protected long endTime;

  // current interval of aggregation window [curStartTime, curEndTime)
  protected TimeRange curAggrTimeRange;
  protected boolean hasCachedTimeInterval;

  // current interval of pre-aggregation window [curStartTime, curEndTime)
  protected TimeRange curPreAggrTimeRange;

  protected boolean leftCRightO;
  protected boolean isIntervalByMonth = false;
  protected boolean isSlidingStepByMonth = false;

  ITimeRangeIterator aggrWindowIterator;
  ITimeRangeIterator preAggrWindowIterator;

  protected AggregateResult[] curAggregateResults;
  protected SlidingWindowGroupByExecutor[] slidingWindowGroupByExecutors;

  public GroupByTimeDataSet() {}

  /** groupBy query. */
  public GroupByTimeDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(
        new ArrayList<>(groupByTimePlan.getDeduplicatedPaths()),
        groupByTimePlan.getDeduplicatedDataTypes(),
        groupByTimePlan.isAscending());

    // find the startTime of the first aggregation interval
    initGroupByTimeDataSetFields(context, groupByTimePlan);
  }

  protected void initGroupByTimeDataSetFields(
      QueryContext context, GroupByTimePlan groupByTimePlan) {
    this.queryId = context.getQueryId();
    this.interval = groupByTimePlan.getInterval();
    this.slidingStep = groupByTimePlan.getSlidingStep();
    if (groupByTimePlan instanceof GroupByTimeFillPlan) {
      this.startTime = ((GroupByTimeFillPlan) groupByTimePlan).getQueryStartTime();
      this.endTime = ((GroupByTimeFillPlan) groupByTimePlan).getQueryEndTime();
    } else {
      this.startTime = groupByTimePlan.getStartTime();
      this.endTime = groupByTimePlan.getEndTime();
    }
    this.leftCRightO = groupByTimePlan.isLeftCRightO();
    this.ascending = groupByTimePlan.isAscending();
    this.isIntervalByMonth = groupByTimePlan.isIntervalByMonth();
    this.isSlidingStepByMonth = groupByTimePlan.isSlidingStepByMonth();

    if (isIntervalByMonth) {
      interval = interval / MS_TO_MONTH;
    }

    if (isSlidingStepByMonth) {
      slidingStep = slidingStep / MS_TO_MONTH;
    }

    // init TimeRangeIterator
    aggrWindowIterator =
        TimeRangeIteratorFactory.getTimeRangeIterator(
            startTime,
            endTime,
            interval,
            slidingStep,
            ascending,
            isIntervalByMonth,
            isSlidingStepByMonth,
            false);

    preAggrWindowIterator =
        TimeRangeIteratorFactory.getTimeRangeIterator(
            startTime,
            endTime,
            interval,
            slidingStep,
            ascending,
            isIntervalByMonth,
            isSlidingStepByMonth,
            true);

    // find the first aggregation interval
    curAggrTimeRange = aggrWindowIterator.nextTimeRange();

    // find the first pre-aggregation interval
    curPreAggrTimeRange = preAggrWindowIterator.nextTimeRange();

    this.hasCachedTimeInterval = true;

    slidingWindowGroupByExecutors = new SlidingWindowGroupByExecutor[paths.size()];
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    // has cached
    if (hasCachedTimeInterval) {
      return true;
    }

    // find the next aggregation interval
    if (!aggrWindowIterator.hasNextTimeRange()) {
      return false;
    }
    curAggrTimeRange = aggrWindowIterator.nextTimeRange();

    hasCachedTimeInterval = true;
    return true;
  }

  @Override
  public abstract RowRecord nextWithoutConstraint() throws IOException;

  public long getStartTime() {
    return startTime;
  }

  @TestOnly
  public TimeRange nextTimePartition() {
    hasCachedTimeInterval = false;
    return curAggrTimeRange;
  }
}
