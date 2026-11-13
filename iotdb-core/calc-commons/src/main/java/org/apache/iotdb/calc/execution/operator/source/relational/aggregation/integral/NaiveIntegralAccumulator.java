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
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.TimeValueBuffer;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

/**
 * Buffers all samples, sorts them by time when the result is evaluated, and then accumulates the
 * trapezoidal area. It is used when the input order is unknown and is the only implementation that
 * supports intermediate states.
 */
public final class NaiveIntegralAccumulator extends AbstractIntegralTableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NaiveIntegralAccumulator.class);

  private final TimeValueBuffer samples = new TimeValueBuffer();
  private final MemoryReservationManager memoryReservationManager;
  private long previousSamplesSize;

  public NaiveIntegralAccumulator(
      TSDataType valueDataType, MemoryReservationManager memoryReservationManager) {
    super(valueDataType);
    this.memoryReservationManager = memoryReservationManager;
    updateMemoryReservation();
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + samples.getEstimatedSize();
  }

  @Override
  public TableAccumulator copy() {
    return new NaiveIntegralAccumulator(valueDataType, memoryReservationManager);
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
      samples.add(timeColumn.getLong(position), readValue(valueColumn, position));
    }
    updateMemoryReservation();
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int position = 0; position < argument.getPositionCount(); position++) {
      if (argument.isNull(position)) {
        continue;
      }
      decodeAndMerge(argument.getBinary(position));
    }
    updateMemoryReservation();
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder output) {
    IntegralFunctionIntermediateStateCodec.encode(
        timeUnit, samples, output, memoryReservationManager);
  }

  @Override
  public void evaluateFinal(ColumnBuilder output) {
    // timeUnit stays 0 when no input has ever been added
    double area = IntegralFunctionUtils.calculateArea(samples);
    output.writeDouble(timeUnit == 0 ? 0.0 : area / timeUnit);
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
    samples.reset();
    updateMemoryReservation();
  }

  private void decodeAndMerge(Binary binary) {
    try (IntegralFunctionIntermediateStateCodec.DecodedState decoded =
        IntegralFunctionIntermediateStateCodec.decode(binary, memoryReservationManager)) {
      initializeOrValidateTimeUnit(decoded.getTimeUnit());
      samples.merge(decoded.getSamples());
    }
  }

  private void updateMemoryReservation() {
    long currentSize = samples.getEstimatedSize();
    long delta = currentSize - previousSamplesSize;
    if (delta > 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else if (delta < 0) {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
    previousSamplesSize = currentSize;
  }
}
