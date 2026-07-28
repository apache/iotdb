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
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.RamUsageEstimator;

public final class NaiveRateAccumulator extends AbstractRateTableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NaiveRateAccumulator.class);

  private final TimeValueBuffer samples = new TimeValueBuffer();
  private final MemoryReservationManager memoryReservationManager;
  private long previousSamplesSize;

  public NaiveRateAccumulator(
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
    return new NaiveRateAccumulator(valueDataType, memoryReservationManager);
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
      samples.add(time, value);
    }
    updateMemoryReservation();
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int position = 0; position < argument.getPositionCount(); position++) {
      if (argument.isNull(position)) {
        continue;
      }
      RateFunctionIntermediateStateCodec.DecodedState decoded =
          RateFunctionIntermediateStateCodec.decode(
              RateFunctionType.RATE, argument.getBinary(position));
      initializeOrValidateWindow(decoded.getWindowStart(), decoded.getWindowEnd());
      samples.merge(decoded.getSamples());
    }
    updateMemoryReservation();
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder output) {
    RateFunctionIntermediateStateCodec.encode(
        RateFunctionType.RATE, windowStart, windowEnd, samples, output);
  }

  @Override
  public void evaluateFinal(ColumnBuilder output) {
    samples.sortAndValidate(RateFunctionType.RATE.getFunctionName());
    if (samples.size() < 2) {
      output.appendNull();
      return;
    }
    int lastIndex = samples.size() - 1;
    double correctedIncrease = calculateCorrectedIncrease();
    output.writeDouble(
        calculateRate(
            samples.size(),
            samples.getTime(0),
            samples.getValue(0),
            samples.getTime(lastIndex),
            correctedIncrease));
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
    samples.reset();
    updateMemoryReservation();
  }

  private double calculateCorrectedIncrease() {
    double result = 0.0;
    for (int index = 1; index < samples.size(); index++) {
      double previous = samples.getValue(index - 1);
      double current = samples.getValue(index);
      result = validateFinite(result + (current >= previous ? current - previous : current));
    }
    return result;
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
