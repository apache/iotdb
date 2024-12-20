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

public class FirstByDescAccumulator extends FirstByAccumulator {

  public FirstByDescAccumulator(
      TSDataType xDataType, TSDataType yDataType, boolean xIsTimeColumn, boolean yIsTimeColumn) {
    super(xDataType, yDataType, xIsTimeColumn, yIsTimeColumn);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  protected void addIntInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateIntFirstValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  @Override
  protected void addLongInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateLongFirstValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  @Override
  protected void addFloatInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateFloatFirstValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  @Override
  protected void addDoubleInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateDoubleFirstValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  @Override
  protected void addBinaryInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateBinaryFirstValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  @Override
  protected void addBooleanInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateBooleanFirstValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }
}
