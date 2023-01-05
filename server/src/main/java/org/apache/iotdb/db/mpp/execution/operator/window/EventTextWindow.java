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

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class EventTextWindow extends EventWindow {

  protected Binary eventValue;

  private Binary previousEventValue;

  public EventTextWindow(EventWindowParameter eventWindowParameter) {
    super(eventWindowParameter);
  }

  @Override
  public void updatePreviousEventValue() {
    previousEventValue = eventValue;
  }

  @Override
  public void mergeOnePoint(Column[] controlTimeAndValueColumn, int index) {
    long currentTime = controlTimeAndValueColumn[1].getLong(index);
    // judge whether we need update startTime
    if (startTime > currentTime) {
      startTime = currentTime;
    }
    // judge whether we need update endTime
    if (endTime < currentTime) {
      endTime = currentTime;
    }
    // judge whether we need initialize eventValue
    if (!initializedEventValue) {
      startTime = currentTime;
      endTime = currentTime;
      eventValue = controlTimeAndValueColumn[0].getBinary(index);
      initializedEventValue = true;
    }
  }

  public Binary getEventValue() {
    return eventValue;
  }

  public void setEventValue(Binary eventValue) {
    this.eventValue = eventValue;
  }

  public Binary getPreviousEventValue() {
    return previousEventValue;
  }

  public void setPreviousEventValue(Binary previousEventValue) {
    this.previousEventValue = previousEventValue;
  }
}
