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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.integral;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.RamUsageEstimator;

/**
 * Accumulates the trapezoidal area in a single pass. Only used when the planner has proved that the
 * input of this group arrives with ascending timestamps.
 */
public final class OrderedIntegralAccumulator extends AbstractIntegralTableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(OrderedIntegralAccumulator.class);

  private int sampleCount;
  private long previousTime;
  private double previousValue;
  private double area;

  public OrderedIntegralAccumulator(TSDataType valueDataType) {
    super(valueDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new OrderedIntegralAccumulator(valueDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    initializeOrValidateTimeUnit(arguments[TIME_UNIT_ARGUMENT_INDEX]);
    Column valueColumn = arguments[VALUE_ARGUMENT_INDEX];
    Column timeColumn = arguments[TIME_ARGUMENT_INDEX];
    int selectedPositionCount = mask.getSelectedPositionCount();
    boolean selectAll = mask.isSelectAll();
    int[] positions = selectAll ? null : mask.getSelectedPositions();
    for (int i = 0; i < selectedPositionCount; i++) {
      int position = selectAll ? i : positions[i];
      if (valueColumn.isNull(position) || timeColumn.isNull(position)) {
        continue;
      }
      double value = readValue(valueColumn, position);
      long time = timeColumn.getLong(position);
      if (sampleCount == 0) {
        previousTime = time;
        previousValue = value;
        sampleCount = 1;
      } else if (time < previousTime) {
        throw orderedInputViolation(time, previousTime);
      } else if (time == previousTime) {
        // the later point overwrites the earlier one when timestamps are duplicated
        previousValue = value;
      } else {
        area += (previousValue + value) / 2.0 * (time - previousTime);
        previousTime = time;
        previousValue = value;
        sampleCount++;
      }
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    throw unsupportedIntermediate();
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    throw unsupportedIntermediate();
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    // fewer than two effective points have no time span, so the area is 0
    columnBuilder.writeDouble(sampleCount < 2 ? 0.0 : area / timeUnit);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    resetTimeUnit();
    sampleCount = 0;
    previousTime = 0;
    previousValue = 0.0;
    area = 0.0;
  }
}
