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

  protected WindowParameter windowParameter;

  protected long startTime;

  protected boolean initializedEventValue;

  protected EventWindow(WindowParameter windowParameter) {
    this.windowParameter = windowParameter;
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getColumn(windowParameter.getControlColumnIndex());
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

  public void setInitializedEventValue(boolean initializedEventValue) {
    this.initializedEventValue = initializedEventValue;
  }
}
