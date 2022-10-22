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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class WindowParameter {
  private WindowType windowType;

  private TSDataType dataType;

  private CompareType compareType;

  private int controlColumnIndex;

  private boolean needOutputEvent;

  private boolean needOutputEndTime;

  private double delta;

  public WindowParameter(
      WindowType windowType,
      TSDataType dataType,
      CompareType compareType,
      int controlColumnIndex,
      boolean needOutputEvent,
      boolean needOutputEndTime,
      double delta) {
    this.windowType = windowType;
    this.dataType = dataType;
    this.compareType = compareType;
    this.controlColumnIndex = controlColumnIndex;
    this.needOutputEvent = needOutputEvent;
    this.needOutputEndTime = needOutputEndTime;
    this.delta = delta;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public void setWindowType(WindowType windowType) {
    this.windowType = windowType;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public CompareType getCompareType() {
    return compareType;
  }

  public void setCompareType(CompareType compareType) {
    this.compareType = compareType;
  }

  public int getControlColumnIndex() {
    return controlColumnIndex;
  }

  public void setControlColumnIndex(int controlColumnIndex) {
    this.controlColumnIndex = controlColumnIndex;
  }

  public boolean isNeedOutputEvent() {
    return needOutputEvent;
  }

  public void setNeedOutputEvent(boolean needOutputEvent) {
    this.needOutputEvent = needOutputEvent;
  }

  public boolean isNeedOutputEndTime() {
    return needOutputEndTime;
  }

  public void setNeedOutputEndTime(boolean needOutputEndTime) {
    this.needOutputEndTime = needOutputEndTime;
  }

  public double getDelta() {
    return delta;
  }

  public void setDelta(double delta) {
    this.delta = delta;
  }
}
