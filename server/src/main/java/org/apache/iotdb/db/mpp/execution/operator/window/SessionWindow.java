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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.Accumulator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

public class SessionWindow implements IWindow {

  private final long timeInterval;

  private final boolean ascending;

  private long timeValue;

  private long startTime;

  private long endTime;

  private long lastTsBlockTime;

  private boolean initializedTimeValue;

  public SessionWindow(long timeInterval, boolean ascending) {
    this.timeInterval = timeInterval;
    this.ascending = ascending;
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getTimeColumn();
  }

  @Override
  public boolean satisfy(Column column, int index) {
    if (!initializedTimeValue) {
      return true;
    }
    if (index == 0) {
      return Math.abs(column.getLong(index) - lastTsBlockTime) <= timeInterval;
    }
    return Math.abs(column.getLong(index) - column.getLong(index - 1)) <= timeInterval;
  }

  @Override
  public void mergeOnePoint(Column[] controlTimeAndValueColumn, int index) {
    long currentTime = controlTimeAndValueColumn[0].getLong(index);
    // judge whether we need initialize timeValue
    if (!initializedTimeValue) {
      startTime = currentTime;
      endTime = currentTime;
      lastTsBlockTime = controlTimeAndValueColumn[0].getLong(0);
      timeValue = currentTime;
      initializedTimeValue = true;
      return;
    }
    // judge whether we need update startTime
    if (startTime > currentTime) {
      startTime = currentTime;
    }
    // judge whether we need update endTime
    if (endTime < currentTime) {
      endTime = currentTime;
    }
    // update the last time of session window
    timeValue = ascending ? Math.max(timeValue, currentTime) : Math.min(timeValue, currentTime);
    setLastTsBlockTime(timeValue);
  }

  @Override
  public boolean hasFinalResult(Accumulator accumulator) {
    return accumulator.hasFinalResult();
  }

  @Override
  public boolean contains(Column column) {
    TimeColumn timeColumn = (TimeColumn) column;

    long minTime = Math.min(timeColumn.getStartTime(), timeColumn.getEndTime());
    long maxTime = Math.max(timeColumn.getStartTime(), timeColumn.getEndTime());

    boolean contains =
        Math.abs(column.getLong(0) - lastTsBlockTime) < timeInterval
            && maxTime - minTime <= timeInterval;
    if (contains) {
      if (!initializedTimeValue) {
        startTime = Long.MAX_VALUE;
        endTime = Long.MIN_VALUE;
        lastTsBlockTime = column.getLong(0);
        timeValue = ascending ? maxTime : minTime;
        initializedTimeValue = true;
      }
      timeValue = ascending ? Math.max(timeValue, maxTime) : Math.min(timeValue, minTime);
      startTime = Math.min(startTime, minTime);
      endTime = Math.max(endTime, maxTime);
    }
    return contains;
  }

  public long getTimeInterval() {
    return timeInterval;
  }

  public long getTimeValue() {
    return timeValue;
  }

  public void setTimeValue(long timeValue) {
    this.timeValue = timeValue;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setInitializedTimeValue(boolean initializedTimeValue) {
    this.initializedTimeValue = initializedTimeValue;
  }

  public void setLastTsBlockTime(long lastTsBlockTime) {
    this.lastTsBlockTime = lastTsBlockTime;
  }
}
