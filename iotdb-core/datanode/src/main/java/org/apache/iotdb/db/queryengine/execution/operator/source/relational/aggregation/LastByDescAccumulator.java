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

  public LastByDescAccumulator(
      TSDataType xDataType, TSDataType yDataType, boolean xIsTimeColumn, boolean yIsTimeColumn) {
    super(xDataType, yDataType, xIsTimeColumn, yIsTimeColumn);
  }

  @Override
  public TableAccumulator copy() {
    return new LastByDescAccumulator(xDataType, yDataType, xIsTimeColumn, yIsTimeColumn);
  }

  @Override
  public boolean hasFinalResult() {
    return initResult;
  }

  @Override
  protected void addIntInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateIntLastValue(xColumn, i, timeColumn.getLong(i));
          return;
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateIntLastValue(xColumn, position, timeColumn.getLong(position));
          return;
        }
      }
    }
  }

  @Override
  protected void addLongInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateLongLastValue(xColumn, i, timeColumn.getLong(i));
          return;
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateLongLastValue(xColumn, position, timeColumn.getLong(position));
          return;
        }
      }
    }
  }

  @Override
  protected void addFloatInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateFloatLastValue(xColumn, i, timeColumn.getLong(i));
          return;
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateFloatLastValue(xColumn, position, timeColumn.getLong(position));
          return;
        }
      }
    }
  }

  @Override
  protected void addDoubleInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateDoubleLastValue(xColumn, i, timeColumn.getLong(i));
          return;
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateDoubleLastValue(xColumn, position, timeColumn.getLong(position));
          return;
        }
      }
    }
  }

  @Override
  protected void addBinaryInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateBinaryLastValue(xColumn, i, timeColumn.getLong(i));
          return;
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateBinaryLastValue(xColumn, position, timeColumn.getLong(position));
          return;
        }
      }
    }
  }

  @Override
  protected void addBooleanInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateBooleanLastValue(xColumn, i, timeColumn.getLong(i));
          return;
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateBooleanLastValue(xColumn, position, timeColumn.getLong(position));
          return;
        }
      }
    }
  }
}
