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
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

public final class GroupedOrderedDeltaAccumulator extends AbstractGroupedDeltaAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedOrderedDeltaAccumulator.class);

  private final IntBigArray sampleCounts = new IntBigArray();
  private final LongBigArray firstTimes = new LongBigArray();
  private final DoubleBigArray firstValues = new DoubleBigArray();
  private final LongBigArray lastTimes = new LongBigArray();
  private final DoubleBigArray lastValues = new DoubleBigArray();
  private final MemoryReservationManager memoryReservationManager;
  private long previousSize;

  public GroupedOrderedDeltaAccumulator(
      TSDataType valueDataType, MemoryReservationManager memoryReservationManager) {
    super(valueDataType);
    this.memoryReservationManager = memoryReservationManager;
    updateMemoryReservation();
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + windowsSizeOf()
        + sampleCounts.sizeOf()
        + firstTimes.sizeOf()
        + firstValues.sizeOf()
        + lastTimes.sizeOf()
        + lastValues.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    ensureWindowCapacity(groupCount);
    sampleCounts.ensureCapacity(groupCount);
    firstTimes.ensureCapacity(groupCount);
    firstValues.ensureCapacity(groupCount);
    lastTimes.ensureCapacity(groupCount);
    lastValues.ensureCapacity(groupCount);
    updateMemoryReservation();
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    RateFunctionValidation.validateArgumentCount(arguments, RateFunctionType.DELTA);
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
              arguments[0], position, valueDataType, RateFunctionType.DELTA);
      long time =
          RateFunctionValidation.readRequiredTime(
              arguments[1], position, RateFunctionType.DELTA, 2);
      long currentWindowStart =
          RateFunctionValidation.readRequiredTime(
              arguments[2], position, RateFunctionType.DELTA, 3);
      long currentWindowEnd =
          RateFunctionValidation.readRequiredTime(
              arguments[3], position, RateFunctionType.DELTA, 4);
      RateFunctionValidation.validateWindow(
          RateFunctionType.DELTA, time, currentWindowStart, currentWindowEnd);
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
        calculateDelta(
            sampleCount,
            firstTimes.get(groupId),
            firstValues.get(groupId),
            lastTimes.get(groupId),
            lastValues.get(groupId),
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
    updateMemoryReservation();
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
    lastTimes.set(groupId, time);
    lastValues.set(groupId, value);
    sampleCounts.set(groupId, Math.incrementExact(sampleCount));
  }

  private void updateMemoryReservation() {
    long currentSize = getEstimatedSize();
    long delta = currentSize - previousSize;
    if (delta > 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else if (delta < 0) {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
    previousSize = currentSize;
  }
}
