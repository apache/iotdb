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
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.calc.utils.constant.SqlConstant.INTEGRAL;

/**
 * Buffers all samples, sorts them by time when the result is evaluated, and then accumulates the
 * trapezoidal area. It is used when the input order is unknown and is the only implementation that
 * supports intermediate states.
 */
public final class NaiveIntegralAccumulator extends AbstractIntegralTableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NaiveIntegralAccumulator.class);

  private static final int STATE_VERSION = 1;
  private static final int SAMPLE_SIZE = Long.BYTES + Double.BYTES;
  private static final int STATE_HEADER_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

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
    if (samples.isEmpty()) {
      output.appendNull();
      return;
    }
    int serializedSize =
        Math.addExact(STATE_HEADER_SIZE, Math.multiplyExact(samples.size(), SAMPLE_SIZE));
    validateSerializedSize(serializedSize);

    memoryReservationManager.reserveMemoryImmediately(serializedSize);
    try {
      ByteBuffer target = ByteBuffer.allocate(serializedSize);
      target.putInt(STATE_VERSION);
      target.putLong(timeUnit);
      target.putInt(samples.size());
      samples.writePayload(target);
      output.writeBinary(new Binary(target.array()));
    } finally {
      // The output builder owns the serialized bytes after writeBinary returns.
      memoryReservationManager.releaseMemoryCumulatively(serializedSize);
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder output) {
    // fewer than two effective points have no time span, so the area is 0
    if (samples.size() < 2) {
      output.writeDouble(0.0);
      return;
    }
    samples.sort();
    double area = 0.0;
    int effectiveSampleCount = 1;
    long previousTime = samples.getTime(0);
    double previousValue = samples.getValue(0);
    for (int index = 1; index < samples.size(); index++) {
      long time = samples.getTime(index);
      double value = samples.getValue(index);
      if (time == previousTime) {
        // the later point overwrites the earlier one when timestamps are duplicated
        previousValue = value;
        continue;
      }
      area += (previousValue + value) / 2.0 * (time - previousTime);
      previousTime = time;
      previousValue = value;
      effectiveSampleCount++;
    }
    output.writeDouble(effectiveSampleCount < 2 ? 0.0 : area / timeUnit);
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
    byte[] bytes = binary.getValues();
    if (bytes.length > TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes()) {
      throw invalidState();
    }
    try {
      ByteBuffer source = ByteBuffer.wrap(bytes);
      if (source.getInt() != STATE_VERSION) {
        throw invalidState();
      }
      long unit = source.getLong();
      int sampleCount = source.getInt();
      if (sampleCount < 0 || STATE_HEADER_SIZE + (long) sampleCount * SAMPLE_SIZE != bytes.length) {
        throw invalidState();
      }
      initializeOrValidateTimeUnit(unit);

      long transientSize = TimeValueBuffer.estimatedSizeForSampleCount(sampleCount);
      memoryReservationManager.reserveMemoryImmediately(transientSize);
      try {
        for (int index = 0; index < sampleCount; index++) {
          long time = source.getLong();
          double value = source.getDouble();
          if (!Double.isFinite(value)) {
            throw new SemanticException(
                String.format(
                    CalcMessages
                        .EXCEPTION_AGGREGATE_FUNCTION_ARG_VALUE_EXCEEDS_THE_FINITE_RANGE_OF_DOUBLE_48936427,
                    INTEGRAL,
                    value));
          }
          samples.add(time, value);
        }
        if (source.hasRemaining()) {
          throw invalidState();
        }
      } finally {
        memoryReservationManager.releaseMemoryCumulatively(transientSize);
      }
    } catch (BufferUnderflowException e) {
      throw invalidState();
    }
  }

  private void validateSerializedSize(int serializedSize) {
    int maxTsBlockSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    if (serializedSize > maxTsBlockSize) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_INTERMEDIATE_STATE_FOR_AGGREGATE_FUNCTION_ARG_EXCEEDS_THE_MAXIMUM_TSBLOCK_SIZE_OF_ARG_BYTES_D53A5546,
              INTEGRAL,
              maxTsBlockSize));
    }
  }

  private SemanticException invalidState() {
    return new SemanticException(
        String.format(
            CalcMessages.EXCEPTION_INVALID_INTERMEDIATE_STATE_FOR_AGGREGATE_FUNCTION_ARG_2999C30B,
            INTEGRAL));
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
