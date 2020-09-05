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

import java.util.ArrayList;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;

public abstract class GroupByEngineDataSet extends QueryDataSet {

  protected long queryId;

  // current interval [curStartTime, curEndTime)
  protected long curStartTime;
  protected long curEndTime;
  protected boolean hasCachedTimeInterval;

  // cur interval index, used in descending query
  protected long curIntervalIndex;

  protected GroupByTimePlan plan;

  public GroupByEngineDataSet() {
  }

  /**
   * groupBy query.
   */
  public GroupByEngineDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(new ArrayList<>(groupByTimePlan.getDeduplicatedPaths()),
        groupByTimePlan.getDeduplicatedDataTypes(), groupByTimePlan.isAscending());
    this.queryId = context.getQueryId();
    this.plan = groupByTimePlan;
    // find the startTime of the first aggregation interval
    if (ascending) {
      curStartTime = plan.getStartTime();
    } else {
      long queryRange = plan.getEndTime() - plan.getStartTime();
      // calculate the max interval index
      curIntervalIndex =
          queryRange % plan.getSlidingStep() == 0 ? queryRange / plan.getSlidingStep()
              : queryRange / plan.getSlidingStep() + 1;
      curStartTime = plan.getSlidingStep() * (curIntervalIndex - 1) + plan.getStartTime();
    }
    curEndTime = Math.min(curStartTime + plan.getInterval(), plan.getEndTime());
    this.hasCachedTimeInterval = true;
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    // has cached
    if (hasCachedTimeInterval) {
      return true;
    }

    // check if the next interval out of range
    if (ascending) {
      curStartTime += plan.getSlidingStep();
      //This is an open interval , [0-100)
      if (curStartTime >= plan.getEndTime()) {
        return false;
      }
    } else {
      curStartTime -= plan.getSlidingStep();
      if (curStartTime < plan.getStartTime()) {
        return false;
      }
    }

    hasCachedTimeInterval = true;
    curEndTime = Math.min(curStartTime + plan.getInterval(), plan.getEndTime());
    return true;
  }

  @Override
  protected abstract RowRecord nextWithoutConstraint() throws IOException;

  public long getStartTime() {
    return plan.getStartTime();
  }

  @TestOnly
  public Pair<Long, Long> nextTimePartition() {
    hasCachedTimeInterval = false;
    return new Pair<>(curStartTime, curEndTime);
  }

  public abstract Pair<Long, Object> peekNextNotNullValue(Path path, int i) throws IOException;
}
