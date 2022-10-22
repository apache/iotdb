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

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.db.mpp.execution.operator.window.IWindow;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class LastValueDescAccumulator extends LastValueAccumulator {

  public LastValueDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public boolean hasFinalResult() {
    return initResult;
  }

  @Override
  public void reset() {
    super.reset();
  }

  protected int addIntInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        updateIntLastValue(column[2].getInt(i), column[1].getLong(i));
        return i;
      }
    }

    return curPositionCount;
  }

  protected int addLongInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        updateLongLastValue(column[2].getLong(i), column[1].getLong(i));
        return i;
      }
    }

    return curPositionCount;
  }

  protected int addFloatInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        updateFloatLastValue(column[2].getFloat(i), column[1].getLong(i));
        return i;
      }
    }

    return curPositionCount;
  }

  protected int addDoubleInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        updateDoubleLastValue(column[2].getDouble(i), column[1].getLong(i));
        return i;
      }
    }

    return curPositionCount;
  }

  protected int addBooleanInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        updateBooleanLastValue(column[2].getBoolean(i), column[1].getLong(i));
        return i;
      }
    }

    return curPositionCount;
  }

  protected int addBinaryInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        updateBinaryLastValue(column[2].getBinary(i), column[1].getLong(i));
        return i;
      }
    }

    return curPositionCount;
  }
}
