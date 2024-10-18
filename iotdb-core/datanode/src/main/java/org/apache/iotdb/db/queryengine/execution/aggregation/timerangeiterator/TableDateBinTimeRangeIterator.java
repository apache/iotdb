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

package org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator;

import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;

import org.apache.tsfile.read.common.TimeRange;

public class TableDateBinTimeRangeIterator implements ITableTimeRangeIterator {

  private final DateBinFunctionColumnTransformer dateBinTransformer;

  private boolean finished = false;

  // left close, right close
  private TimeRange curTimeRange;

  public TableDateBinTimeRangeIterator(DateBinFunctionColumnTransformer dateBinTransformer) {
    this.dateBinTransformer = dateBinTransformer;
  }

  @Override
  public boolean canFinishCurrentTimeRange(long startTime) {
    if (curTimeRange == null) {
      return false;
    }

    return startTime > curTimeRange.getMax();
  }

  @Override
  public void updateCurTimeRange(long startTime) {
    long[] timeArray = dateBinTransformer.dateBinStartEnd(startTime);

    if (curTimeRange != null) {
      // meet new time range, remove old time range
      if (timeArray[0] != curTimeRange.getMin()) {
        this.curTimeRange = new TimeRange(timeArray[0], timeArray[1] - 1);
      }
    } else {
      this.curTimeRange = new TimeRange(timeArray[0], timeArray[1] - 1);
    }
  }

  @Override
  public void setFinished() {
    this.curTimeRange = null;
    this.finished = true;
  }

  @Override
  public TimeIteratorType getType() {
    return TimeIteratorType.DATE_BIN_TIME_ITERATOR;
  }

  @Override
  public boolean hasNextTimeRange() {
    return !finished;
  }

  @Override
  public boolean hasCachedTimeRange() {
    return curTimeRange != null;
  }

  public TimeRange getCurTimeRange() {
    return this.curTimeRange;
  }

  @Override
  public void resetCurTimeRange() {
    this.curTimeRange = null;
  }
}
