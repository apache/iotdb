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

public class ConditionWindow implements IWindow {

  private final int controlColumnIndex;
  private final boolean outputEndTime;
  private final boolean ignoringNull;
  private long startTime;
  private long endTime;
  private long keep;
  private boolean timeInitialized;

  public ConditionWindow(ConditionWindowParameter conditionWindowParameter) {
    this.ignoringNull = conditionWindowParameter.isIgnoringNull();
    this.controlColumnIndex = conditionWindowParameter.getControlColumnIndex();
    this.outputEndTime = conditionWindowParameter.isNeedOutputEndTime();
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getColumn(controlColumnIndex);
  }

  @Override
  public boolean satisfy(Column column, int index) {
    if (!column.isNull(index)) {
      return column.getBoolean(index);
    }
    return false;
  }

  @Override
  public void mergeOnePoint(Column[] controlTimeAndValueColumn, int index) {
    keep++;
    long currentTime = controlTimeAndValueColumn[1].getLong(index);
    if (!timeInitialized) {
      startTime = currentTime;
      endTime = currentTime;
      timeInitialized = true;
    } else {
      startTime = Math.min(startTime, currentTime);
      endTime = Math.max(endTime, currentTime);
    }
  }

  @Override
  public boolean hasFinalResult(Accumulator accumulator) {
    return accumulator.hasFinalResult();
  }

  @Override
  public boolean contains(Column column) {
    return false;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public boolean ignoringNull() {
    return ignoringNull;
  }

  public long getKeep() {
    return keep;
  }

  public void setKeep(long keep) {
    this.keep = keep;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setTimeInitialized(boolean timeInitialized) {
    this.timeInitialized = timeInitialized;
  }

  public boolean isOutputEndTime() {
    return outputEndTime;
  }
}
