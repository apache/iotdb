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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.RateFunctionType;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.RateFunctionValidation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

public final class GroupedOrderedIncreaseAccumulator extends AbstractGroupedIncreaseAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedOrderedIncreaseAccumulator.class);

  private final IntBigArray sampleCounts = new IntBigArray();
  private final LongBigArray firstTimes = new LongBigArray();
  private final DoubleBigArray firstValues = new DoubleBigArray();
  private final LongBigArray lastTimes = new LongBigArray();
  private final DoubleBigArray lastValues = new DoubleBigArray();
  private final DoubleBigArray correctedIncreases = new DoubleBigArray();

  public GroupedOrderedIncreaseAccumulator(TSDataType valueDataType) {
    super(valueDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + windowsSizeOf()
        + sampleCounts.sizeOf()
        + firstTimes.sizeOf()
        + firstValues.sizeOf()
        + lastTimes.sizeOf()
        + lastValues.sizeOf()
        + correctedIncreases.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    ensureWindowCapacity(groupCount);
    sampleCounts.ensureCapacity(groupCount);
    firstTimes.ensureCapacity(groupCount);
    firstValues.ensureCapacity(groupCount);
    lastTimes.ensureCapacity(groupCount);
    lastValues.ensureCapacity(groupCount);
    correctedIncreases.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    RateFunctionValidation.validateArgumentCount(arguments, RateFunctionType.INCREASE);
    int selectedCount = mask.getSelectedPositionCount();
    int[] selectedPositions = mask.isSelectAll() ? null : mask.getSelectedPositions();
    for (int index = 0; index < selectedCount; index++) {
      int position = mask.isSelectAll() ? index : selectedPositions[index];
      if (arguments[0].isNull(position)) {
        continue;
      }
      int groupId = groupIds[position];
      double value =
          RateFunctionValidation.readValue(
              arguments[0], position, valueDataType, RateFunctionType.INCREASE);
      long time =
          RateFunctionValidation.readRequiredTime(
              arguments[1], position, RateFunctionType.INCREASE, 2);
      long currentWindowStart =
          RateFunctionValidation.readRequiredTime(
              arguments[2], position, RateFunctionType.INCREASE, 3);
      long currentWindowEnd =
          RateFunctionValidation.readRequiredTime(
              arguments[3], position, RateFunctionType.INCREASE, 4);
      RateFunctionValidation.validateWindow(
          RateFunctionType.INCREASE, time, currentWindowStart, currentWindowEnd);
      initializeOrValidateWindow(groupId, currentWindowStart, currentWindowEnd);
      update(groupId, time, value);
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    throw unsupportedIntermediate();
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder output) {
    throw unsupportedIntermediate();
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder output) {
    int sampleCount = sampleCounts.get(groupId);
    if (sampleCount < 2) {
      output.appendNull();
      return;
    }
    output.writeDouble(
        calculateIncrease(
            sampleCount,
            firstTimes.get(groupId),
            firstValues.get(groupId),
            lastTimes.get(groupId),
            correctedIncreases.get(groupId),
            windowStart(groupId),
            windowEnd(groupId)));
  }

  @Override
  public void reset() {
    resetWindows();
    sampleCounts.reset();
    firstTimes.reset();
    firstValues.reset();
    lastTimes.reset();
    lastValues.reset();
    correctedIncreases.reset();
  }

  private void update(int groupId, long time, double value) {
    int sampleCount = sampleCounts.get(groupId);
    if (sampleCount == 0) {
      sampleCounts.set(groupId, 1);
      firstTimes.set(groupId, time);
      firstValues.set(groupId, value);
      lastTimes.set(groupId, time);
      lastValues.set(groupId, value);
      return;
    }
    long lastTime = lastTimes.get(groupId);
    if (time == lastTime) {
      throw duplicateTimestamp(time);
    }
    if (time < lastTime) {
      throw orderedInputViolation(time, lastTime);
    }
    double lastValue = lastValues.get(groupId);
    double increment = value >= lastValue ? value - lastValue : value;
    double correctedIncrease = validateFinite(correctedIncreases.get(groupId) + increment);
    lastTimes.set(groupId, time);
    lastValues.set(groupId, value);
    correctedIncreases.set(groupId, correctedIncrease);
    sampleCounts.set(groupId, Math.incrementExact(sampleCount));
  }
}
