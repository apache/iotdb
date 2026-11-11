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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.integral;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

/**
 * Accumulates the trapezoidal area of each group in a single pass. Only used when the planner has
 * proved that the input of every group arrives with ascending timestamps.
 */
public final class GroupedOrderedIntegralAccumulator extends AbstractGroupedIntegralAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedOrderedIntegralAccumulator.class);

  private final IntBigArray sampleCounts = new IntBigArray();
  // "start" is the last point whose value can no longer change, "end" is the newest point. The
  // area of the segment between them is added only once a larger timestamp proves that "end" will
  // not be overwritten by a later row of the same timestamp.
  private final LongBigArray startTimes = new LongBigArray();
  private final DoubleBigArray startValues = new DoubleBigArray();
  private final LongBigArray endTimes = new LongBigArray();
  private final DoubleBigArray endValues = new DoubleBigArray();
  private final DoubleBigArray areas = new DoubleBigArray();

  public GroupedOrderedIntegralAccumulator(TSDataType valueDataType) {
    super(valueDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + sampleCounts.sizeOf()
        + startTimes.sizeOf()
        + startValues.sizeOf()
        + endTimes.sizeOf()
        + endValues.sizeOf()
        + areas.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    sampleCounts.ensureCapacity(groupCount);
    startTimes.ensureCapacity(groupCount);
    startValues.ensureCapacity(groupCount);
    endTimes.ensureCapacity(groupCount);
    endValues.ensureCapacity(groupCount);
    areas.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
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
      update(groupIds[position], timeColumn.getLong(position), readValue(valueColumn, position));
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
    // fewer than two distinct timestamps have no time span, so the area is 0
    if (sampleCounts.get(groupId) < 2) {
      output.writeDouble(0.0);
      return;
    }
    // the newest point is final because there is no later input to overwrite it
    double area =
        areas.get(groupId)
            + (startValues.get(groupId) + endValues.get(groupId))
                / 2.0
                * (endTimes.get(groupId) - startTimes.get(groupId));
    output.writeDouble(normalizeArea(area));
  }

  @Override
  public void reset() {
    sampleCounts.reset();
    startTimes.reset();
    startValues.reset();
    endTimes.reset();
    endValues.reset();
    areas.reset();
    resetTimeUnit();
  }

  private void update(int groupId, long time, double value) {
    int sampleCount = sampleCounts.get(groupId);
    if (sampleCount == 0) {
      startTimes.set(groupId, time);
      startValues.set(groupId, value);
      endTimes.set(groupId, time);
      endValues.set(groupId, value);
      sampleCounts.set(groupId, 1);
      return;
    }
    long endTime = endTimes.get(groupId);
    if (time < endTime) {
      throw orderedInputViolation(time, endTime);
    }
    if (time == endTime) {
      // the later point overwrites the earlier one, so the segment ending at this timestamp is
      // added only after its value is final
      endValues.set(groupId, value);
      return;
    }
    if (sampleCount >= 2) {
      areas.set(
          groupId,
          areas.get(groupId)
              + (startValues.get(groupId) + endValues.get(groupId))
                  / 2.0
                  * (endTime - startTimes.get(groupId)));
    }
    startTimes.set(groupId, endTime);
    startValues.set(groupId, endValues.get(groupId));
    endTimes.set(groupId, time);
    endValues.set(groupId, value);
    sampleCounts.set(groupId, Math.incrementExact(sampleCount));
  }
}
