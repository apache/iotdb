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

  private long timeValue;

  private long startTime;

  private long endTime;

  private boolean initializedTimeValue;

  public SessionWindow(long timeInterval) {
    this.timeInterval = timeInterval;
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
    return Math.abs(column.getLong(index) - timeValue) <= timeInterval;
  }

  @Override
  public void mergeOnePoint(Column[] controlTimeAndValueColumn, int index) {
    long currentTime = controlTimeAndValueColumn[0].getLong(index);
    // judge whether we need update startTime
    if (startTime > currentTime) {
      startTime = currentTime;
    }
    // judge whether we need update endTime
    if (endTime < currentTime) {
      endTime = currentTime;
    }
    // update the last time of session window
    timeValue = currentTime;
    // judge whether we need initialize timeValue
    if (!initializedTimeValue) {
      startTime = currentTime;
      endTime = currentTime;
      initializedTimeValue = true;
    }
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

    return maxTime - minTime <= timeInterval;
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

  public boolean isInitializedTimeValue() {
    return initializedTimeValue;
  }

  public void setInitializedTimeValue(boolean initializedTimeValue) {
    this.initializedTimeValue = initializedTimeValue;
  }
}
