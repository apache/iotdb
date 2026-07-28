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

public final class GroupedNaiveDeltaAccumulator extends AbstractGroupedDeltaAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedNaiveDeltaAccumulator.class);

  private final TimeValueBufferBigArray samples = new TimeValueBufferBigArray();
  private final MemoryReservationManager memoryReservationManager;
  private long previousSize;

  public GroupedNaiveDeltaAccumulator(
      TSDataType valueDataType, MemoryReservationManager memoryReservationManager) {
    super(valueDataType);
    this.memoryReservationManager = memoryReservationManager;
    updateMemoryReservation();
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + windowsSizeOf() + samples.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    ensureWindowCapacity(groupCount);
    samples.ensureCapacity(groupCount);
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
      samples.add(groupId, time, value);
    }
    updateMemoryReservation();
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int position = 0; position < groupIds.length; position++) {
      if (argument.isNull(position)) {
        continue;
      }
      int groupId = groupIds[position];
      RateFunctionIntermediateStateCodec.DecodedState decoded =
          RateFunctionIntermediateStateCodec.decode(
              RateFunctionType.DELTA, argument.getBinary(position));
      initializeOrValidateWindow(groupId, decoded.getWindowStart(), decoded.getWindowEnd());
      samples.merge(groupId, decoded.getSamples());
    }
    updateMemoryReservation();
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder output) {
    RateFunctionIntermediateStateCodec.encode(
        RateFunctionType.DELTA,
        windowStart(groupId),
        windowEnd(groupId),
        samples.get(groupId),
        output);
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder output) {
    TimeValueBuffer buffer = samples.get(groupId);
    if (buffer == null || buffer.size() < 2) {
      output.appendNull();
      return;
    }
    buffer.sortAndValidate(RateFunctionType.DELTA.getFunctionName());
    int lastIndex = buffer.size() - 1;
    output.writeDouble(
        calculateDelta(
            buffer.size(),
            buffer.getTime(0),
            buffer.getValue(0),
            buffer.getTime(lastIndex),
            buffer.getValue(lastIndex),
            windowStart(groupId),
            windowEnd(groupId)));
  }

  @Override
  public void reset() {
    resetWindows();
    samples.reset();
    updateMemoryReservation();
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
