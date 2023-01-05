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

public class EventWindowParameter extends WindowParameter {

  private CompareType compareType;

  private boolean needOutputEvent;

  private double delta;

  public EventWindowParameter(
      TSDataType dataType,
      int controlColumnIndex,
      boolean needOutputEndTime,
      boolean needOutputEvent,
      CompareType compareType,
      double delta) {
    super(dataType, controlColumnIndex, needOutputEndTime);
    this.needOutputEvent = needOutputEvent;
    this.compareType = compareType;
    this.delta = delta;
    this.windowType = WindowType.EVENT_WINDOW;
  }

  public CompareType getCompareType() {
    return compareType;
  }

  public void setCompareType(CompareType compareType) {
    this.compareType = compareType;
  }

  public boolean isNeedOutputEvent() {
    return needOutputEvent;
  }

  public void setNeedOutputEvent(boolean needOutputEvent) {
    this.needOutputEvent = needOutputEvent;
  }

  public double getDelta() {
    return delta;
  }

  public void setDelta(double delta) {
    this.delta = delta;
  }
}
