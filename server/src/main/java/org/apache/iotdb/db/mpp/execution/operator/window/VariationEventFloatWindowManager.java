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

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

public class VariationEventFloatWindowManager extends EventFloatWindowManager {

  public VariationEventFloatWindowManager(
      EventWindowParameter eventWindowParameter, boolean ascending) {
    super(eventWindowParameter, ascending);
    eventWindow = new VariationEventFloatWindow(eventWindowParameter);
  }

  @Override
  public TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock) {
    if (!needSkip) {
      return inputTsBlock;
    }

    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return inputTsBlock;
    }

    Column controlColumn = inputTsBlock.getColumn(eventWindowParameter.getControlColumnIndex());
    TimeColumn timeColumn = inputTsBlock.getTimeColumn();
    int i = 0, size = inputTsBlock.getPositionCount();
    float previousEventValue = ((VariationEventFloatWindow) eventWindow).getPreviousEventValue();
    for (; i < size; i++) {
      if (!controlColumn.isNull(i)
          && Math.abs(controlColumn.getFloat(i) - previousEventValue)
              > eventWindowParameter.getDelta()) {
        break;
      }
      // judge whether we need update endTime
      long currentTime = timeColumn.getLong(i);
      if (eventWindow.getEndTime() < currentTime) {
        eventWindow.setEndTime(currentTime);
      }
    }
    // we can create a new window beginning at index i of inputTsBlock
    if (i < size) {
      needSkip = false;
    }
    return inputTsBlock.subTsBlock(i);
  }
}
