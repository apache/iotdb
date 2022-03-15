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

package org.apache.iotdb.db.query.executor.groupby;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Deque;
import java.util.LinkedList;

public abstract class SlidingWindowGroupByExecutor {

  // current aggregate window
  protected long curStartTime;
  protected long curEndTime;

  // cached AggregateResult of pre-aggregate windows
  protected Deque<AggregateResult> deque;

  // output aggregate result
  protected AggregateResult aggregateResult;

  public SlidingWindowGroupByExecutor(TSDataType dataType, String aggrFuncName, boolean ascending) {
    this.aggregateResult =
        AggregateResultFactory.getAggrResultByName(aggrFuncName, dataType, ascending);
    this.deque = new LinkedList<>();
  }

  /** update queue and aggregateResult */
  public abstract void update(AggregateResult aggregateResult);

  /** evicting expired element in queue and reset expired aggregateResult */
  protected abstract void evictingExpiredValue();

  public AggregateResult getAggregateResult() {
    return aggregateResult;
  }

  public void setTimeRange(long curStartTime, long curEndTime) {
    this.curStartTime = curStartTime;
    this.curEndTime = curEndTime;
    evictingExpiredValue();
  }

  protected boolean inTimeRange(long curTime) {
    return curTime >= curStartTime && curTime < curEndTime;
  }
}
