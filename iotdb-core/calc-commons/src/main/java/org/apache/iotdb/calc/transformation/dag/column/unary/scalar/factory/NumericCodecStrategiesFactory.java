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

package org.apache.iotdb.calc.transformation.dag.column.unary.scalar.factory;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * A factory for strategies that convert numeric types to/from byte arrays using various encoding
 * schemes. the decoding exceptions are wrapped into SemanticException for uniform handling in the
 * upper layer.
 */
public final class NumericCodecStrategiesFactory {

  private NumericCodecStrategiesFactory() {}

  @FunctionalInterface
  public interface IntToBytesStrategy {
    byte[] numericCodeCTransform(int input);
  }

  @FunctionalInterface
  public interface LongToBytesStrategy {
    byte[] numericCodeCTransform(long input);
  }

  @FunctionalInterface
  public interface FloatToBytesStrategy {
    byte[] numericCodeCTransform(float input);
  }

  @FunctionalInterface
  public interface DoubleToBytesStrategy {
    byte[] numericCodeCTransform(double input);
  }

  @FunctionalInterface
  public interface BytesToIntStrategy {
    int numericCodeCTransform(byte[] input);
  }

  @FunctionalInterface
  public interface BytesToLongStrategy {
    long numericCodeCTransform(byte[] input);
  }

  @FunctionalInterface
  public interface BytesToFloatStrategy {
    float numericCodeCTransform(byte[] input);
  }

  @FunctionalInterface
  public interface BytesToDoubleStrategy {
    double numericCodeCTransform(byte[] input);
  }

  // --- Strategy Implementations ---

  // for Big Endian writes, leverage ByteBuffer's default order for maximum performance

  public static final IntToBytesStrategy TO_BIG_ENDIAN_32 =
      (input) -> ByteBuffer.allocate(4).putInt(input).array();

  public static final LongToBytesStrategy TO_BIG_ENDIAN_64 =
      (input) -> ByteBuffer.allocate(8).putLong(input).array();

  public static final FloatToBytesStrategy TO_IEEE754_32_BIG_ENDIAN =
      (input) -> ByteBuffer.allocate(4).putInt(Float.floatToIntBits(input)).array();

  public static final DoubleToBytesStrategy TO_IEEE754_64_BIG_ENDIAN =
      (input) -> ByteBuffer.allocate(8).putLong(Double.doubleToLongBits(input)).array();

  // For Little Endian writes, reverse the bytes of the number first, then use the default
  // (BigEndian) writer
  public static final IntToBytesStrategy TO_LITTLE_ENDIAN_32 =
      (input) -> ByteBuffer.allocate(4).putInt(Integer.reverseBytes(input)).array();

  public static final LongToBytesStrategy TO_LITTLE_ENDIAN_64 =
      (input) -> ByteBuffer.allocate(8).putLong(Long.reverseBytes(input)).array();

  // Decoding Conversions (Bytes -> Numeric) ---
  // For reads, ByteBuffer.wrap().order() is already highly efficient as it avoids data copies.

  public static final BytesToIntStrategy FROM_BIG_ENDIAN_32 =
      (input) -> {
        // validate input length, if its length is not 4, throw exception
        if (input.length != 4) {
          throw new SemanticException(
              CalcMessages.EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_32_MUST_4_C1214BB5);
        }
        return ByteBuffer.wrap(input).order(ByteOrder.BIG_ENDIAN).getInt();
      };

  public static final BytesToLongStrategy FROM_BIG_ENDIAN_64 =
      (input) -> {
        if (input.length != 8) {
          throw new SemanticException(
              CalcMessages.EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_64_MUST_8_797F1A26);
        }
        return ByteBuffer.wrap(input).order(ByteOrder.BIG_ENDIAN).getLong();
      };

  public static final BytesToIntStrategy FROM_LITTLE_ENDIAN_32 =
      (input) -> {
        if (input.length != 4) {
          throw new SemanticException(
              CalcMessages.EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_32_MUST_4_0FBBD083);
        }
        return ByteBuffer.wrap(input).order(ByteOrder.LITTLE_ENDIAN).getInt();
      };

  public static final BytesToLongStrategy FROM_LITTLE_ENDIAN_64 =
      (input) -> {
        if (input.length != 8) {
          throw new SemanticException(
              CalcMessages.EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_64_MUST_8_771F7A96);
        }
        return ByteBuffer.wrap(input).order(ByteOrder.LITTLE_ENDIAN).getLong();
      };

  public static final BytesToFloatStrategy FROM_IEEE754_32_BIG_ENDIAN =
      (input) -> {
        if (input.length != 4) {
          throw new SemanticException(
              CalcMessages
                  .EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_32_BIG_ENDIAN_MUST_4_51AF4EF0);
        }
        return Float.intBitsToFloat(ByteBuffer.wrap(input).order(ByteOrder.BIG_ENDIAN).getInt());
      };

  public static final BytesToDoubleStrategy FROM_IEEE754_64_BIG_ENDIAN =
      (input) -> {
        if (input.length != 8) {
          throw new SemanticException(
              CalcMessages
                  .EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_64_BIG_ENDIAN_MUST_8_56F33455);
        }
        return Double.longBitsToDouble(
            ByteBuffer.wrap(input).order(ByteOrder.BIG_ENDIAN).getLong());
      };

  // for CRC32
  private static final ThreadLocal<CRC32> crc32ThreadLocal = ThreadLocal.withInitial(CRC32::new);
  public static final BytesToLongStrategy CRC32 =
      (input) -> {
        CRC32 crc32 = crc32ThreadLocal.get();
        crc32.reset();
        crc32.update(input);
        return crc32.getValue();
      };
}
