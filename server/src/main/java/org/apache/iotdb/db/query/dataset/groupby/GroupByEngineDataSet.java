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

import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

public abstract class GroupByEngineDataSet extends QueryDataSet {

  protected long queryId;
  protected long interval;
  protected long slidingStep;
  // total query [startTime, endTime)
  protected long startTime;
  protected long endTime;

  // current interval [curStartTime, curEndTime)
  protected long curStartTime;
  protected long curEndTime;
  protected boolean hasCachedTimeInterval;

  protected boolean leftCRightO;
  protected boolean isIntervalByMonth = false;
  protected boolean isSlidingStepByMonth = false;
  public static final long MS_TO_MONTH = 30 * 86400_000L;
  protected AggregateResult[] curAggregateResults;

  public GroupByEngineDataSet() {}

  /** groupBy query. */
  public GroupByEngineDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(
        new ArrayList<>(groupByTimePlan.getDeduplicatedPaths()),
        groupByTimePlan.getDeduplicatedDataTypes(),
        groupByTimePlan.isAscending());

    // find the startTime of the first aggregation interval
    initGroupByEngineDataSetFields(context, groupByTimePlan);
  }

  protected Pair<Long, Long> getFirstTimeRange() {
    long retEndTime;
    if (isIntervalByMonth) {
      // calculate interval length by natural month based on startTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime = Math.min(calcIntervalByMonth(startTime, interval), endTime);
    } else {
      retEndTime = Math.min(startTime + interval, endTime);
    }
    return new Pair<>(startTime, retEndTime);
  }

  protected Pair<Long, Long> getLastTimeRange() {
    long retStartTime;
    long retEndTime;
    long queryRange = endTime - startTime;
    long intervalNum;

    if (isSlidingStepByMonth) {
      intervalNum = (long) Math.ceil(queryRange / (double) (slidingStep * MS_TO_MONTH));
      retStartTime = calcIntervalByMonth(startTime, intervalNum * slidingStep);
      while (retStartTime >= endTime) {
        intervalNum -= 1;
        retStartTime = calcIntervalByMonth(startTime, intervalNum * slidingStep);
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
      retStartTime = slidingStep * (intervalNum - 1) + startTime;
    }

    if (isIntervalByMonth) {
      // calculate interval length by natural month based on curStartTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime = Math.min(calcIntervalByMonth(retStartTime, interval), endTime);
    } else {
      retEndTime = Math.min(retStartTime + interval, endTime);
    }

    return new Pair<>(retStartTime, retEndTime);
  }

  protected Pair<Long, Long> getNextTimeRange(
      long curStartTime, boolean isAscending, boolean isInside) {
    long retStartTime, retEndTime;

    if (isAscending) {
      if (isSlidingStepByMonth) {
        retStartTime = calcIntervalByMonth(curStartTime, (int) (slidingStep));
      } else {
        retStartTime = curStartTime + slidingStep;
      }
      // This is an open interval , [0-100)
      if (retStartTime >= endTime && isInside) {
        return null;
      }
    } else {
      if (isSlidingStepByMonth) {
        retStartTime = calcIntervalByMonth(curStartTime, (int) (-slidingStep));
      } else {
        retStartTime = curStartTime - slidingStep;
      }
      if (retStartTime < startTime && isInside) {
        return null;
      }
    }

    if (isIntervalByMonth) {
      retEndTime = calcIntervalByMonth(retStartTime, (int) (interval));
    } else {
      retEndTime = retStartTime + interval;
    }
    if (isInside) {
      retEndTime = Math.min(retEndTime, endTime);
    }

    return new Pair<>(retStartTime, retEndTime);
  }

  protected void initGroupByEngineDataSetFields(
      QueryContext context, GroupByTimePlan groupByTimePlan) {
    this.queryId = context.getQueryId();
    this.interval = groupByTimePlan.getInterval();
    this.slidingStep = groupByTimePlan.getSlidingStep();
    this.startTime = groupByTimePlan.getStartTime();
    this.endTime = groupByTimePlan.getEndTime();
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

    // find the first aggregation interval
    Pair<Long, Long> retTimeRange;
    if (ascending) {
      retTimeRange = getFirstTimeRange();
    } else {
      retTimeRange = getLastTimeRange();
    }
    curStartTime = retTimeRange.left;
    curEndTime = retTimeRange.right;

    this.hasCachedTimeInterval = true;
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    // has cached
    if (hasCachedTimeInterval) {
      return true;
    }

    // find the next aggregation interval
    Pair<Long, Long> nextTimeRange = getNextTimeRange(curStartTime, ascending, true);
    if (nextTimeRange == null) {
      return false;
    }
    curStartTime = nextTimeRange.left;
    curEndTime = nextTimeRange.right;

    hasCachedTimeInterval = true;
    return true;
  }

  /**
   * add natural months based on the startTime to avoid edge cases, ie 2/28
   *
   * @param startTime current start time
   * @param numMonths numMonths is updated in hasNextWithoutConstraint()
   * @return nextStartTime
   */
  public static long calcIntervalByMonth(long startTime, long numMonths) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(SessionManager.getInstance().getCurrSessionTimeZone());
    calendar.setTimeInMillis(startTime);
    boolean isLastDayOfMonth =
        calendar.get(Calendar.DAY_OF_MONTH) == calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    calendar.add(Calendar.MONTH, (int) (numMonths));
    if (isLastDayOfMonth) {
      calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    }
    return calendar.getTimeInMillis();
  }

  @Override
  public abstract RowRecord nextWithoutConstraint() throws IOException;

  public long getStartTime() {
    return startTime;
  }

  public AggregateResult[] getCurAggregateResults() {
    return curAggregateResults;
  }

  @TestOnly
  public Pair<Long, Long> nextTimePartition() {
    hasCachedTimeInterval = false;
    return new Pair<>(curStartTime, curEndTime);
  }
}
