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

public abstract class AbstractVariationWindow implements IWindow {

  private final double delta;
  private final int controlColumnIndex;
  private final boolean outputEndTime;
  private final boolean ignoreNull;

  protected long startTime;
  protected long endTime;
  protected boolean initializedHeadValue;
  protected boolean valueIsNull = false;

  protected AbstractVariationWindow(VariationWindowParameter variationWindowParameter) {
    this.controlColumnIndex = variationWindowParameter.getControlColumnIndex();
    this.ignoreNull = variationWindowParameter.isIgnoringNull();
    this.outputEndTime = variationWindowParameter.isNeedOutputEndTime();
    this.delta = variationWindowParameter.getDelta();
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getColumn(controlColumnIndex);
  }

  @Override
  public boolean hasFinalResult(Accumulator accumulator) {
    return accumulator.hasFinalResult();
  }

  @Override
  public boolean contains(Column column) {
    return false;
  }

  public abstract void updatePreviousValue();

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

  public void setInitializedHeadValue(boolean initializedHeadValue) {
    this.initializedHeadValue = initializedHeadValue;
  }

  public boolean ignoreNull() {
    return ignoreNull;
  }

  public boolean valueIsNull() {
    return valueIsNull;
  }

  public boolean isOutputEndTime() {
    return outputEndTime;
  }

  public double getDelta() {
    return delta;
  }
}
