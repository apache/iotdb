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

public class LastByDescAccumulator extends LastByAccumulator {
  private final boolean xIsMeasurementColumn;
  private final boolean yIsMeasurementColumn;
  private final boolean canFinishAfterInit;

  public LastByDescAccumulator(
      TSDataType xDataType,
      TSDataType yDataType,
      boolean xIsTimeColumn,
      boolean yIsTimeColumn,
      boolean xIsMeasurementColumn,
      boolean yIsMeasurementColumn,
      boolean canFinishAfterInit) {
    super(xDataType, yDataType, xIsTimeColumn, yIsTimeColumn);
    this.xIsMeasurementColumn = xIsMeasurementColumn;
    this.yIsMeasurementColumn = yIsMeasurementColumn;
    this.canFinishAfterInit = canFinishAfterInit;
  }

  public boolean xIsTimeColumn() {
    return xIsTimeColumn;
  }

  public boolean yIsTimeColumn() {
    return this.yIsTimeColumn;
  }

  public boolean xIsMeasurementColumn() {
    return xIsMeasurementColumn;
  }

  public boolean yIsMeasurementColumn() {
    return yIsMeasurementColumn;
  }

  @Override
  public TableAccumulator copy() {
    return new LastByDescAccumulator(
        xDataType,
        yDataType,
        xIsTimeColumn,
        yIsTimeColumn,
        xIsMeasurementColumn,
        yIsMeasurementColumn,
        canFinishAfterInit);
  }

  @Override
  public boolean hasFinalResult() {
    return canFinishAfterInit && initResult;
  }

  @Override
  protected void addIntInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: The order time is not null. Attempt to update the xResult.
        updateIntLastValue(
            xColumn.isNull(position), xColumn.getInt(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: The order time is null. Attempt to update the xNullTimeValue.
        updateIntNullTimeValue(xColumn.isNull(position), xColumn.getInt(position));
      }
    }
  }

  @Override
  protected void addLongInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: The order time is not null. Attempt to update the xResult.
        updateLongLastValue(
            xColumn.isNull(position), xColumn.getLong(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: The order time is null. Attempt to update the xNullTimeValue.
        updateLongNullTimeValue(xColumn.isNull(position), xColumn.getLong(position));
      }
    }
  }

  @Override
  protected void addFloatInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: Valid Time
        updateFloatLastValue(
            xColumn.isNull(position), xColumn.getFloat(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: Null Time
        updateFloatNullTimeValue(xColumn.isNull(position), xColumn.getFloat(position));
      }
    }
  }

  @Override
  protected void addDoubleInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        updateDoubleLastValue(
            xColumn.isNull(position), xColumn.getDouble(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateDoubleNullTimeValue(xColumn.isNull(position), xColumn.getDouble(position));
      }
    }
  }

  @Override
  protected void addBinaryInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        updateBinaryLastValue(
            xColumn.isNull(position), xColumn.getBinary(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateBinaryNullTimeValue(xColumn.isNull(position), xColumn.getBinary(position));
      }
    }
  }

  @Override
  protected void addBooleanInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        updateBooleanLastValue(
            xColumn.isNull(position), xColumn.getBoolean(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateBooleanNullTimeValue(xColumn.isNull(position), xColumn.getBoolean(position));
      }
    }
  }
}
