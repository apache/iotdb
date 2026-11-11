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
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.TimeValueBufferBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.integral.IntegralFunctionIntermediateStateCodec;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.integral.IntegralFunctionUtils;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.TimeValueBuffer;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

/** Buffers the samples of each group and sorts them when the final result is evaluated. */
public final class GroupedNaiveIntegralAccumulator extends AbstractGroupedIntegralAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedNaiveIntegralAccumulator.class);

  private final TimeValueBufferBigArray samples = new TimeValueBufferBigArray();
  private final MemoryReservationManager memoryReservationManager;

  public GroupedNaiveIntegralAccumulator(
      TSDataType valueDataType, MemoryReservationManager memoryReservationManager) {
    super(valueDataType);
    this.memoryReservationManager = memoryReservationManager;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + samples.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    samples.ensureCapacity(groupCount);
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
      samples.add(
          groupIds[position], timeColumn.getLong(position), readValue(valueColumn, position));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int position = 0; position < groupIds.length; position++) {
      if (argument.isNull(position)) {
        continue;
      }
      int groupId = groupIds[position];
      try (IntegralFunctionIntermediateStateCodec.DecodedState decoded =
          IntegralFunctionIntermediateStateCodec.decode(
              argument.getBinary(position), memoryReservationManager)) {
        initializeOrValidateTimeUnit(decoded.getTimeUnit());
        samples.merge(groupId, decoded.getSamples());
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder output) {
    IntegralFunctionIntermediateStateCodec.encode(
        timeUnit, samples.get(groupId), output, memoryReservationManager);
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder output) {
    TimeValueBuffer buffer = samples.get(groupId);
    output.writeDouble(
        normalizeArea(buffer == null ? 0.0 : IntegralFunctionUtils.calculateArea(buffer)));
  }

  @Override
  public void reset() {
    samples.reset();
    resetTimeUnit();
  }
}
