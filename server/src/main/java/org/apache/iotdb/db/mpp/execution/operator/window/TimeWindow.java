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
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

public class TimeWindow implements IWindow {

  private TimeRange curTimeRange;

  public TimeWindow() {}

  public TimeWindow(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
  }

  public TimeRange getCurTimeRange() {
    return curTimeRange;
  }

  public long getCurMinTime() {
    return curTimeRange.getMin();
  }

  public long getCurMaxTime() {
    return curTimeRange.getMax();
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getTimeColumn();
  }

  @Override
  public boolean satisfy(Column column, int index) {
    long curTime = column.getLong(index);
    return curTime <= getCurMaxTime() && curTime >= getCurMinTime();
  }

  @Override
  public void mergeOnePoint(Column[] controlTimeAndValueColumn, int index) {
    // do nothing
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

    return curTimeRange.contains(minTime, maxTime);
  }

  public void update(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
  }
}
