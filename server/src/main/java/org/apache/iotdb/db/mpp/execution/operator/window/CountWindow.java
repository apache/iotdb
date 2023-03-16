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
package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class CountWindow implements IWindow {

  private final int controlColumnIndex;
  private final boolean needOutputEndTime;
  private final boolean ignoreNull;
  private final long countNumber;
  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  private long leftCount;

  public CountWindow(CountWindowParameter countWindowParameter) {
    this.controlColumnIndex = countWindowParameter.getControlColumnIndex();
    this.needOutputEndTime = countWindowParameter.isNeedOutputEndTime();
    this.countNumber = countWindowParameter.getCountNumber();
    this.ignoreNull = countWindowParameter.isIgnoreNull();
    resetCurCount();
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getColumn(controlColumnIndex);
  }

  @Override
  public boolean satisfy(Column column, int index) {
    return leftCount != 0;
  }

  @Override
  public void mergeOnePoint(Column[] timeAndValueColumn, int index) {
    long currentTime = timeAndValueColumn[1].getLong(index);
    startTime = Math.min(startTime, currentTime);
    endTime = Math.max(endTime, currentTime);
    leftCount--;
  }

  @Override
  public boolean contains(Column column) {
    return false;
  }

  public boolean isNeedOutputEndTime() {
    return needOutputEndTime;
  }

  public void resetCurCount() {
    setLeftCount(countNumber);
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getLeftCount() {
    return leftCount;
  }

  public void setLeftCount(long leftCount) {
    this.leftCount = leftCount;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public boolean isIgnoreNull() {
    return ignoreNull;
  }
}
