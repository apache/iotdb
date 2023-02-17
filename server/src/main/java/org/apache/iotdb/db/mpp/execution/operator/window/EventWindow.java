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

public abstract class EventWindow implements IWindow {

  protected EventWindowParameter eventWindowParameter;

  protected long startTime;

  protected long endTime;

  protected boolean initializedEventValue;

  protected boolean valueIsNull = false;

  protected EventWindow(EventWindowParameter eventWindowParameter) {
    this.eventWindowParameter = eventWindowParameter;
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getColumn(eventWindowParameter.getControlColumnIndex());
  }

  @Override
  public boolean hasFinalResult(Accumulator accumulator) {
    return accumulator.hasFinalResult();
  }

  // TODO
  @Override
  public boolean contains(Column column) {
    return false;
  }

  public abstract void updatePreviousEventValue();

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

  public void setInitializedEventValue(boolean initializedEventValue) {
    this.initializedEventValue = initializedEventValue;
  }

  public boolean valueIsNull() {
    return valueIsNull;
  }

  public boolean ignoringNull() {
    return eventWindowParameter.isIgnoringNull();
  }
}
