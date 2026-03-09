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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

public class LastDescAccumulator extends LastAccumulator {
  private final boolean isTimeColumn;
  private final boolean isMeasurementColumn;
  private final boolean canFinishAfterInit;

  public LastDescAccumulator(
      TSDataType seriesDataType,
      boolean isTimeColumn,
      boolean isMeasurementColumn,
      boolean canFinishAfterInit) {
    super(seriesDataType);
    this.isTimeColumn = isTimeColumn;
    this.isMeasurementColumn = isMeasurementColumn;
    this.canFinishAfterInit = canFinishAfterInit;
  }

  public boolean isTimeColumn() {
    return this.isTimeColumn;
  }

  public boolean isMeasurementColumn() {
    return this.isMeasurementColumn;
  }

  @Override
  public TableAccumulator copy() {
    return new LastDescAccumulator(
        seriesDataType, isTimeColumn, isMeasurementColumn, canFinishAfterInit);
  }

  @Override
  public boolean hasFinalResult() {
    return canFinishAfterInit && initResult;
  }

  @Override
  protected void addIntInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: Time is not null. Attempt to update the value with the minimum time.
        updateIntLastValue(valueColumn.getInt(position), timeColumn.getLong(position));
        return;
      } else {
        // Case B: Time is NULL, the nullTimeValue should only be assigned once
        updateIntNullTimeValue(valueColumn.getInt(position));
      }
    }
  }

  @Override
  protected void addLongInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: Time is not null. Attempt to update the value with the minimum time.
        updateLongLastValue(valueColumn.getLong(position), timeColumn.getLong(position));
        return;
      } else {
        // Case B: Time is NULL, the nullTimeValue should only be assigned once
        updateLongNullTimeValue(valueColumn.getLong(position));
      }
    }
  }

  @Override
  protected void addFloatInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateFloatLastValue(valueColumn.getFloat(position), timeColumn.getLong(position));
        return;
      } else {
        updateFloatNullTimeValue(valueColumn.getFloat(position));
      }
    }
  }

  @Override
  protected void addDoubleInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateDoubleLastValue(valueColumn.getDouble(position), timeColumn.getLong(position));
        return;
      } else {
        updateDoubleNullTimeValue(valueColumn.getDouble(position));
      }
    }
  }

  @Override
  protected void addBinaryInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateBinaryLastValue(valueColumn.getBinary(position), timeColumn.getLong(position));
        return;
      } else {
        updateBinaryNullTimeValue(valueColumn.getBinary(position));
      }
    }
  }

  @Override
  protected void addBooleanInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateBooleanLastValue(valueColumn.getBoolean(position), timeColumn.getLong(position));
        return;
      } else {
        updateBooleanNullTimeValue(valueColumn.getBoolean(position));
      }
    }
  }
}
