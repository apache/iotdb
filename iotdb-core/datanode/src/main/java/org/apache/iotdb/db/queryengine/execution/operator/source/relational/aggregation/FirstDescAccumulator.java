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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

public class FirstDescAccumulator extends FirstAccumulator {

  public FirstDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType, false);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TableAccumulator copy() {
    return new FirstDescAccumulator(seriesDataType);
  }

  @Override
  protected void addIntInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateIntFirstValue(valueColumn.getInt(i), timeColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateIntFirstValue(valueColumn.getInt(position), timeColumn.getLong(position));
        }
      }
    }
  }

  @Override
  protected void addLongInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateLongFirstValue(valueColumn.getLong(i), timeColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateLongFirstValue(valueColumn.getLong(position), timeColumn.getLong(position));
        }
      }
    }
  }

  @Override
  protected void addFloatInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateFloatFirstValue(valueColumn.getFloat(i), timeColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateFloatFirstValue(valueColumn.getFloat(position), timeColumn.getLong(position));
        }
      }
    }
  }

  @Override
  protected void addDoubleInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateDoubleFirstValue(valueColumn.getDouble(i), timeColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateDoubleFirstValue(valueColumn.getDouble(position), timeColumn.getLong(position));
        }
      }
    }
  }

  @Override
  protected void addBinaryInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateBinaryFirstValue(valueColumn.getBinary(i), timeColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateBinaryFirstValue(valueColumn.getBinary(position), timeColumn.getLong(position));
        }
      }
    }
  }

  @Override
  protected void addBooleanInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateBooleanFirstValue(valueColumn.getBoolean(i), timeColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateBooleanFirstValue(valueColumn.getBoolean(position), timeColumn.getLong(position));
        }
      }
    }
  }
}
