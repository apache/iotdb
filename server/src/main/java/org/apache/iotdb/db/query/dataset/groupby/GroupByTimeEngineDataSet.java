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
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;

public abstract class GroupByTimeEngineDataSet extends GroupByTimeDataSet {

  public GroupByTimeEngineDataSet() {}

  public GroupByTimeEngineDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(context, groupByTimePlan);
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next() in GroupByTimeEngineDataSet.");
    }
    hasCachedTimeInterval = false;
    curAggregateResults = getNextAggregateResult();
    return constructRowRecord(curAggregateResults);
  }

  protected abstract AggregateResult[] getNextAggregateResult() throws IOException;

  protected RowRecord constructRowRecord(AggregateResult[] aggregateResultList) {
    RowRecord record;
    if (leftCRightO) {
      record = new RowRecord(curStartTime);
    } else {
      record = new RowRecord(curEndTime - 1);
    }
    for (AggregateResult res : aggregateResultList) {
      if (res == null) {
        record.addField(null);
        continue;
      }
      record.addField(res.getResult(), res.getResultDataType());
    }
    return record;
  }

  protected boolean isEndCal() {
    if (curPreAggrStartTime == -1) {
      return true;
    }
    return ascending ? curPreAggrStartTime >= curEndTime : curPreAggrEndTime <= curStartTime;
  }

  // find the next pre-aggregation interval
  protected void updatePreAggrInterval() {
    Pair<Long, Long> retPerAggrTimeRange;
    retPerAggrTimeRange = preAggrWindowIterator.getNextTimeRange(curPreAggrStartTime);
    if (retPerAggrTimeRange != null) {
      curPreAggrStartTime = retPerAggrTimeRange.left;
      curPreAggrEndTime = retPerAggrTimeRange.right;
    } else {
      curPreAggrStartTime = -1;
      curPreAggrEndTime = -1;
    }
  }

  public AggregateResult[] getCurAggregateResults() {
    return curAggregateResults;
  }
}
