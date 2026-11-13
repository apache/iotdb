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

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.TimeValueBuffer;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.utils.Binary;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.calc.utils.constant.SqlConstant.INTEGRAL;

/** Serializes and merges the sampled points carried between partial and final integral(). */
public final class IntegralFunctionIntermediateStateCodec {

  private static final int STATE_VERSION = 1;
  private static final int SAMPLE_SIZE = Long.BYTES + Double.BYTES;
  private static final int STATE_HEADER_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

  private IntegralFunctionIntermediateStateCodec() {}

  public static void encode(
      long timeUnit,
      TimeValueBuffer samples,
      ColumnBuilder output,
      MemoryReservationManager memoryReservationManager) {
    if (samples == null || samples.isEmpty()) {
      output.appendNull();
      return;
    }

    long serializedSize =
        Math.addExact(STATE_HEADER_SIZE, Math.multiplyExact((long) samples.size(), SAMPLE_SIZE));
    validateSerializedSize(serializedSize);

    memoryReservationManager.reserveMemoryImmediately(serializedSize);
    try {
      ByteBuffer target = ByteBuffer.allocate(Math.toIntExact(serializedSize));
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

  public static DecodedState decode(
      Binary binary, MemoryReservationManager memoryReservationManager) {
    byte[] bytes = binary.getValues();
    if (bytes.length > TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes()) {
      throw invalidState();
    }
    try {
      ByteBuffer source = ByteBuffer.wrap(bytes);
      if (source.getInt() != STATE_VERSION) {
        throw invalidState();
      }
      long timeUnit = source.getLong();
      int sampleCount = source.getInt();
      if (sampleCount < 0
          || Math.addExact(STATE_HEADER_SIZE, Math.multiplyExact((long) sampleCount, SAMPLE_SIZE))
              != bytes.length) {
        throw invalidState();
      }

      long transientSize = TimeValueBuffer.estimatedSizeForSampleCount(sampleCount);
      memoryReservationManager.reserveMemoryImmediately(transientSize);
      boolean decoded = false;
      try {
        TimeValueBuffer samples = new TimeValueBuffer();
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
        decoded = true;
        return new DecodedState(timeUnit, samples, memoryReservationManager, transientSize);
      } finally {
        if (!decoded) {
          memoryReservationManager.releaseMemoryCumulatively(transientSize);
        }
      }
    } catch (BufferUnderflowException | ArithmeticException exception) {
      throw invalidState();
    }
  }

  private static void validateSerializedSize(long serializedSize) {
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

  private static SemanticException invalidState() {
    return new SemanticException(
        String.format(
            CalcMessages.EXCEPTION_INVALID_INTERMEDIATE_STATE_FOR_AGGREGATE_FUNCTION_ARG_2999C30B,
            INTEGRAL));
  }

  public static final class DecodedState implements AutoCloseable {

    private final long timeUnit;
    private final TimeValueBuffer samples;
    private final MemoryReservationManager memoryReservationManager;
    private final long reservedBytes;
    private boolean closed;

    private DecodedState(
        long timeUnit,
        TimeValueBuffer samples,
        MemoryReservationManager memoryReservationManager,
        long reservedBytes) {
      this.timeUnit = timeUnit;
      this.samples = samples;
      this.memoryReservationManager = memoryReservationManager;
      this.reservedBytes = reservedBytes;
    }

    public long getTimeUnit() {
      return timeUnit;
    }

    public TimeValueBuffer getSamples() {
      return samples;
    }

    @Override
    public void close() {
      if (!closed) {
        memoryReservationManager.releaseMemoryCumulatively(reservedBytes);
        closed = true;
      }
    }
  }
}
