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
import org.apache.iotdb.tsfile.read.common.TimeRange;

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
      record = new RowRecord(curAggrTimeRange.getMin());
    } else {
      record = new RowRecord(curAggrTimeRange.getMax() - 1);
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
    if (curPreAggrTimeRange.getMin() == -1) {
      return true;
    }
    return ascending
        ? curPreAggrTimeRange.getMin() >= curAggrTimeRange.getMax()
        : curPreAggrTimeRange.getMax() <= curAggrTimeRange.getMin();
  }

  // find the next pre-aggregation interval
  protected void updatePreAggrInterval() {
    TimeRange retPerAggrTimeRange = null;
    if (preAggrWindowIterator.hasNextTimeRange()) {
      retPerAggrTimeRange = preAggrWindowIterator.nextTimeRange();
    }
    if (retPerAggrTimeRange != null) {
      curPreAggrTimeRange = retPerAggrTimeRange;
    } else {
      curPreAggrTimeRange = new TimeRange(-1, -1);
    }
  }

  public AggregateResult[] getCurAggregateResults() {
    return curAggregateResults;
  }
}
