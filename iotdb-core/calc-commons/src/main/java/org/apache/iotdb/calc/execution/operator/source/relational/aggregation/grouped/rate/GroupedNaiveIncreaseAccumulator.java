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
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.RateFunctionIntermediateStateCodec;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.RateFunctionType;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.RateFunctionValidation;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.TimeValueBuffer;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

public final class GroupedNaiveIncreaseAccumulator extends AbstractGroupedIncreaseAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedNaiveIncreaseAccumulator.class);

  private final TimeValueBufferBigArray samples = new TimeValueBufferBigArray();
  private final MemoryReservationManager memoryReservationManager;

  public GroupedNaiveIncreaseAccumulator(
      TSDataType valueDataType, MemoryReservationManager memoryReservationManager) {
    super(valueDataType);
    this.memoryReservationManager = memoryReservationManager;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + windowsSizeOf() + samples.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    ensureWindowCapacity(groupCount);
    samples.ensureCapacity(groupCount);
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
      samples.add(groupId, time, value);
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int position = 0; position < groupIds.length; position++) {
      if (argument.isNull(position)) {
        continue;
      }
      int groupId = groupIds[position];
      try (RateFunctionIntermediateStateCodec.DecodedState decoded =
          RateFunctionIntermediateStateCodec.decode(
              RateFunctionType.INCREASE, argument.getBinary(position), memoryReservationManager)) {
        initializeOrValidateWindow(groupId, decoded.getWindowStart(), decoded.getWindowEnd());
        samples.merge(groupId, decoded.getSamples());
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder output) {
    RateFunctionIntermediateStateCodec.encode(
        RateFunctionType.INCREASE,
        windowStart(groupId),
        windowEnd(groupId),
        samples.get(groupId),
        output,
        memoryReservationManager);
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder output) {
    TimeValueBuffer buffer = samples.get(groupId);
    if (buffer == null || buffer.size() < 2) {
      output.appendNull();
      return;
    }
    buffer.sortAndValidate(RateFunctionType.INCREASE.getFunctionName());
    int lastIndex = buffer.size() - 1;
    output.writeDouble(
        calculateIncrease(
            buffer.size(),
            buffer.getTime(0),
            buffer.getValue(0),
            buffer.getTime(lastIndex),
            calculateCorrectedIncrease(buffer),
            windowStart(groupId),
            windowEnd(groupId)));
  }

  @Override
  public void reset() {
    resetWindows();
    samples.reset();
  }

  private double calculateCorrectedIncrease(TimeValueBuffer buffer) {
    double result = 0.0;
    for (int index = 1; index < buffer.size(); index++) {
      double previous = buffer.getValue(index - 1);
      double current = buffer.getValue(index);
      result = validateFinite(result + (current >= previous ? current - previous : current));
    }
    return result;
  }
}
