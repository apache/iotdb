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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;


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
  protected boolean isGroupByMonth;
  protected int intervalTimes;

  public GroupByEngineDataSet() {
  }

  /**
   * groupBy query.
   */
  public GroupByEngineDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(new ArrayList<>(groupByTimePlan.getDeduplicatedPaths()),
        groupByTimePlan.getDeduplicatedDataTypes(), groupByTimePlan.isAscending());

    // find the startTime of the first aggregation interval
    initGroupByEngineDataSetFields(context, groupByTimePlan);
  }

  protected void initGroupByEngineDataSetFields(QueryContext context, GroupByTimePlan groupByTimePlan) {
    this.queryId = context.getQueryId();
    this.interval = groupByTimePlan.getInterval();
    this.slidingStep = groupByTimePlan.getSlidingStep();
    this.startTime = groupByTimePlan.getStartTime();
    this.endTime = groupByTimePlan.getEndTime();
    this.leftCRightO = groupByTimePlan.isLeftCRightO();
    this.ascending = groupByTimePlan.isAscending();
    this.isGroupByMonth = groupByTimePlan.isGroupByMonth();

    if (ascending) {
      curStartTime = startTime;
    } else {
      long queryRange = endTime - startTime;
      // calculate the total interval number
      long intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
      curStartTime = slidingStep * (intervalNum - 1) + startTime;
    }

    if (isGroupByMonth) {
      //interval and sliding step are calculated in ms by * 30 * 86400_000L by default
      //now converting them back to months
      interval = interval / 30 / 86400_000L;
      slidingStep = slidingStep / 30 / 86400_000L;
      curEndTime = Math.min(curStartTime + calcIntervalByMonth(interval, curStartTime), endTime);
    } else {
      curEndTime = Math.min(curStartTime + interval, endTime);
    }
    this.hasCachedTimeInterval = true;
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    long curSlidingStep = slidingStep;
    long curInterval = interval;
    // has cached
    if (hasCachedTimeInterval) {
      return true;
    }

    //gorup by natural month, given startTime recalculate interval and sliding step
    if (isGroupByMonth) {
      curInterval = calcIntervalByMonth(++intervalTimes * slidingStep + interval, startTime);
      curSlidingStep = calcIntervalByMonth(slidingStep * intervalTimes, curStartTime);
    }

    // check if the next interval out of range
    if (ascending) {
      curStartTime += curSlidingStep;
      //This is an open interval , [0-100)
      if (curStartTime >= endTime) {
        return false;
      }
    } else {
      curStartTime -= curSlidingStep;
      if (curStartTime < startTime) {
        return false;
      }
    }

    hasCachedTimeInterval = true;
    if (isGroupByMonth) {
      curEndTime = Math.min(startTime + curInterval, endTime);
    } else {
      curEndTime = Math.min(curStartTime + curInterval, endTime);
    }
    return true;
  }

  public long calcIntervalByMonth(long numMonths, long curStartTime) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(startTime);
    calendar.add(Calendar.MONTH, (int) (numMonths));
    return calendar.getTimeInMillis() - curStartTime;
  }

  @Override
  protected abstract RowRecord nextWithoutConstraint() throws IOException;

  public long getStartTime() {
    return startTime;
  }

  @TestOnly
  public Pair<Long, Long> nextTimePartition() {
    hasCachedTimeInterval = false;
    return new Pair<>(curStartTime, curEndTime);
  }

  public abstract Pair<Long, Object> peekNextNotNullValue(Path path, int i) throws IOException;
}
