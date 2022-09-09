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

import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class TimeWindow implements IWindow {

  private TimeRange curTimeRange;

  private long curMinTime;
  private long curMaxTime;

  private int TIME_COLUMN_INDEX = 0;

  public TimeWindow() {}

  public TimeWindow(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
    this.curMinTime = curTimeRange.getMin();
    this.curMaxTime = curTimeRange.getMax();
  }

  public TimeRange getCurTimeRange() {
    return curTimeRange;
  }

  public long getCurMinTime() {
    return curMinTime;
  }

  public long getCurMaxTime() {
    return curMaxTime;
  }

  @Override
  public int getControlColumnIndex() {
    return TIME_COLUMN_INDEX;
  }

  @Override
  public boolean satisfy(Column column, int index) {
    long curTime = column.getLong(index);
    return curTime <= this.curMaxTime && curTime >= this.curMinTime;
  }

  @Override
  public boolean isTimeWindow() {
    return true;
  }

  @Override
  public void mergeOnePoint() {
    // do nothing
  }

  public void update(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
    this.curMinTime = curTimeRange.getMin();
    this.curMaxTime = curTimeRange.getMax();
  }
}
