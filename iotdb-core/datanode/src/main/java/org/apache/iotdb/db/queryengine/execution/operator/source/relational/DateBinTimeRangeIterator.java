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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.TimeRange;

public class DateBinTimeRangeIterator implements ITimeRangeIterator {

  DateBinFunctionColumnTransformer dateBinTransformer;

  boolean finished = false;

  // total query [startTime, endTime)
  private long startTime;
  private long endTime;

  private TimeRange curTimeRange;
  private boolean hasCachedTimeRange;

  public DateBinTimeRangeIterator(DateBinFunctionColumnTransformer dateBinTransformer) {
    this.dateBinTransformer = dateBinTransformer;
  }

  public void updateCurTimeRange(Column timeColumn) {
    // long startTime = timeColumn.getLong();
    this.hasCachedTimeRange = true;
  }

  public boolean overCurrentTimeRange(long startTime) {
    if (!hasCachedTimeRange) {
      return false;
    }

    return dateBinTransformer.dateBin(startTime) > curTimeRange.getMin();
  }

  public TimeRange updateCurTimeRange(long startTime) {
    long[] timeArray = dateBinTransformer.dateBinStartEnd(startTime);

    if (hasCachedTimeRange) {
      // meet new time range, remove old time range
      if (timeArray[0] != curTimeRange.getMin()) {
        this.curTimeRange = new TimeRange(timeArray[0], timeArray[1] - 1);
      }
    } else {
      this.curTimeRange = new TimeRange(timeArray[0], timeArray[1] - 1);
      this.hasCachedTimeRange = true;
    }

    return this.curTimeRange;
  }

  public void setFinished() {
    this.finished = true;
  }

  @Override
  public TimeRange getFirstTimeRange() {
    throw new IllegalStateException(
        "Method getFirstTimeRange is not used in DateBinTimeRangeIterator");
  }

  @Override
  public boolean hasNextTimeRange() {
    return !finished;
  }

  public TimeRange getCurTimeRange() {
    return this.curTimeRange;
  }

  public void resetCurTimeRange() {
    this.curTimeRange = null;
    this.hasCachedTimeRange = false;
  }

  @Override
  public TimeRange nextTimeRange() {
    if (hasCachedTimeRange) {
      return curTimeRange;
    }
    //    hasCachedTimeRange = false;
    //    curTimeRange = null;
    return null;
  }

  @Override
  public boolean isAscending() {
    return false;
  }

  @Override
  public long currentOutputTime() {
    throw new IllegalStateException(
        "Method currentOutputTime is not used in DateBinTimeRangeIterator");
  }

  @Override
  public long getTotalIntervalNum() {
    throw new IllegalStateException(
        "Method getTotalIntervalNum is not used in DateBinTimeRangeIterator");
  }
}
