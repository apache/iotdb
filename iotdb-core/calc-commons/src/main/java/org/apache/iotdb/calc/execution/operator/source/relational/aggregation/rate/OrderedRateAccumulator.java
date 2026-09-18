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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.RamUsageEstimator;

public final class OrderedRateAccumulator extends AbstractRateTableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(OrderedRateAccumulator.class);

  private int sampleCount;
  private long firstTime;
  private double firstValue;
  private long lastTime;
  private double lastValue;
  private double correctedIncrease;

  public OrderedRateAccumulator(TSDataType valueDataType) {
    super(valueDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new OrderedRateAccumulator(valueDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    RateFunctionValidation.validateArgumentCount(arguments, RateFunctionType.RATE);
    int selectedCount = mask.getSelectedPositionCount();
    int[] selectedPositions = mask.isSelectAll() ? null : mask.getSelectedPositions();
    for (int index = 0; index < selectedCount; index++) {
      int position = mask.isSelectAll() ? index : selectedPositions[index];
      if (arguments[0].isNull(position)) {
        continue;
      }
      double value =
          RateFunctionValidation.readValue(
              arguments[0], position, valueDataType, RateFunctionType.RATE);
      long time =
          RateFunctionValidation.readRequiredTime(arguments[1], position, RateFunctionType.RATE, 2);
      long currentWindowStart =
          RateFunctionValidation.readRequiredTime(arguments[2], position, RateFunctionType.RATE, 3);
      long currentWindowEnd =
          RateFunctionValidation.readRequiredTime(arguments[3], position, RateFunctionType.RATE, 4);
      RateFunctionValidation.validateWindow(
          RateFunctionType.RATE, time, currentWindowStart, currentWindowEnd);
      initializeOrValidateWindow(currentWindowStart, currentWindowEnd);
      update(time, value);
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
  public void evaluateFinal(ColumnBuilder output) {
    if (sampleCount < 2) {
      output.appendNull();
      return;
    }
    output.writeDouble(
        calculateRate(sampleCount, firstTime, firstValue, lastTime, correctedIncrease));
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
    resetWindow();
    sampleCount = 0;
    firstTime = 0;
    firstValue = 0;
    lastTime = 0;
    lastValue = 0;
    correctedIncrease = 0;
  }

  private void update(long time, double value) {
    if (sampleCount == 0) {
      firstTime = time;
      firstValue = value;
      lastTime = time;
      lastValue = value;
      sampleCount = 1;
      return;
    }
    if (time == lastTime) {
      throw duplicateTimestamp(time);
    }
    if (time < lastTime) {
      throw orderedInputViolation(time, lastTime);
    }
    double increment = value >= lastValue ? value - lastValue : value;
    double newCorrectedIncrease = validateFinite(correctedIncrease + increment);
    int newSampleCount = Math.incrementExact(sampleCount);
    lastTime = time;
    lastValue = value;
    correctedIncrease = newCorrectedIncrease;
    sampleCount = newSampleCount;
  }
}
