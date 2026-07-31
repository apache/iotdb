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

public final class GroupedNaiveIrateAccumulator extends AbstractGroupedIrateAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedNaiveIrateAccumulator.class);

  private final TimeValueBufferBigArray samples = new TimeValueBufferBigArray();
  private final MemoryReservationManager memoryReservationManager;

  public GroupedNaiveIrateAccumulator(
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
    RateFunctionValidation.validateArgumentCount(arguments, RateFunctionType.IRATE);
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
              arguments[0], position, valueDataType, RateFunctionType.IRATE);
      long time =
          RateFunctionValidation.readRequiredTime(
              arguments[1], position, RateFunctionType.IRATE, 2);
      samples.add(groupId, time, value);
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int position = 0; position < groupIds.length; position++) {
      if (argument.isNull(position)) {
        continue;
      }
      try (RateFunctionIntermediateStateCodec.DecodedState decoded =
          RateFunctionIntermediateStateCodec.decode(
              RateFunctionType.IRATE, argument.getBinary(position), memoryReservationManager)) {
        samples.merge(groupIds[position], decoded.getSamples());
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder output) {
    RateFunctionIntermediateStateCodec.encode(
        RateFunctionType.IRATE, 0, 0, samples.get(groupId), output, memoryReservationManager);
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder output) {
    TimeValueBuffer buffer = samples.get(groupId);
    if (buffer == null || buffer.size() < 2) {
      output.appendNull();
      return;
    }
    buffer.sortAndValidate(RateFunctionType.IRATE.getFunctionName());
    int lastIndex = buffer.size() - 1;
    output.writeDouble(
        calculateIrate(
            buffer.getTime(lastIndex - 1),
            buffer.getValue(lastIndex - 1),
            buffer.getTime(lastIndex),
            buffer.getValue(lastIndex)));
  }

  @Override
  public void reset() {
    samples.reset();
  }
}
