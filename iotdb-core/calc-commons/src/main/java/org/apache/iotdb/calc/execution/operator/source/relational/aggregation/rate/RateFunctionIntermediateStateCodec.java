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

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;

import java.nio.ByteBuffer;

public final class RateFunctionIntermediateStateCodec {

  private static final int STATE_VERSION = 1;
  private static final int WINDOWED_HEADER_SIZE = 24;
  private static final int IRATE_HEADER_SIZE = 8;
  private static final int SAMPLE_SIZE = 16;

  private RateFunctionIntermediateStateCodec() {}

  public static void encode(
      RateFunctionType functionType,
      long windowStart,
      long windowEnd,
      TimeValueBuffer samples,
      ColumnBuilder output) {
    if (samples == null || samples.isEmpty()) {
      output.appendNull();
      return;
    }

    long headerSize = functionType.isWindowed() ? WINDOWED_HEADER_SIZE : IRATE_HEADER_SIZE;
    long serializedSize =
        Math.addExact(headerSize, Math.multiplyExact((long) samples.size(), SAMPLE_SIZE));
    ByteBuffer target = ByteBuffer.allocate(Math.toIntExact(serializedSize));
    target.putInt(STATE_VERSION);
    if (functionType.isWindowed()) {
      target.putLong(windowStart);
      target.putLong(windowEnd);
    }
    target.putInt(samples.size());
    samples.writePayload(target);
    output.writeBinary(new Binary(target.array()));
  }

  public static DecodedState decode(RateFunctionType functionType, Binary binary) {
    try {
      byte[] bytes = binary.getValues();
      int headerSize = functionType.isWindowed() ? WINDOWED_HEADER_SIZE : IRATE_HEADER_SIZE;
      if (bytes.length < headerSize) {
        throw invalidState(functionType);
      }
      ByteBuffer source = ByteBuffer.wrap(bytes);
      if (source.getInt() != STATE_VERSION) {
        throw invalidState(functionType);
      }

      long windowStart = 0;
      long windowEnd = 0;
      if (functionType.isWindowed()) {
        windowStart = source.getLong();
        windowEnd = source.getLong();
        if (windowStart >= windowEnd) {
          throw invalidState(functionType);
        }
      }

      int sampleCount = source.getInt();
      if (sampleCount < 0
          || Math.addExact(headerSize, Math.multiplyExact(sampleCount, SAMPLE_SIZE))
              != bytes.length) {
        throw invalidState(functionType);
      }

      TimeValueBuffer samples = new TimeValueBuffer();
      for (int index = 0; index < sampleCount; index++) {
        long time = source.getLong();
        double value = source.getDouble();
        if (!Double.isFinite(value)
            || (functionType.isCounter() && value < 0.0)
            || (functionType.isWindowed() && (time < windowStart || time >= windowEnd))) {
          throw invalidState(functionType);
        }
        samples.add(time, value);
      }
      if (source.hasRemaining()) {
        throw invalidState(functionType);
      }
      return new DecodedState(windowStart, windowEnd, samples);
    } catch (ArithmeticException | IndexOutOfBoundsException exception) {
      throw invalidState(functionType);
    }
  }

  private static SemanticException invalidState(RateFunctionType functionType) {
    return new SemanticException(
        String.format(
            CalcMessages.EXCEPTION_INVALID_INTERMEDIATE_STATE_FOR_AGGREGATE_FUNCTION_ARG_2999C30B,
            functionType.getFunctionName()));
  }

  public static final class DecodedState {
    private final long windowStart;
    private final long windowEnd;
    private final TimeValueBuffer samples;

    private DecodedState(long windowStart, long windowEnd, TimeValueBuffer samples) {
      this.windowStart = windowStart;
      this.windowEnd = windowEnd;
      this.samples = samples;
    }

    public long getWindowStart() {
      return windowStart;
    }

    public long getWindowEnd() {
      return windowEnd;
    }

    public TimeValueBuffer getSamples() {
      return samples;
    }
  }
}
