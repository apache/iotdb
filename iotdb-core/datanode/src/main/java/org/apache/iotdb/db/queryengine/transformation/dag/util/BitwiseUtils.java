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

package org.apache.iotdb.db.queryengine.transformation.dag.util;

import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.db.exception.sql.SemanticException;

public class BitwiseUtils {
  private static final long TINYINT_MASK = 0b1111_1111L;
  private static final long TINYINT_SIGNED_BIT = 0b1000_0000L;
  private static final long SMALLINT_MASK = 0b1111_1111_1111_1111L;
  private static final long SMALLINT_SIGNED_BIT = 0b1000_0000_0000_0000L;
  private static final long INTEGER_MASK = 0x00_00_00_00_ff_ff_ff_ffL;
  private static final long INTEGER_SIGNED_BIT = 0x00_00_00_00_00_80_00_00_00L;

  public static void bitCountCheck(long num, long bits) {
    bitCountCheckBits(bits);

    // set the least (bits - 1) bits
    final long lowBitsMask = (1L << (bits - 1)) - 1;
    if (num > lowBitsMask || num < ~lowBitsMask) {
      throw new SemanticException(
          String.format(
              "Argument exception, the scalar function num must be representable with the bits specified. %s cannot be represented with %s bits.",
              num, bits));
    }
  }

  public static void bitCountCheckBits(long bits) {
    if (bits <= 1 || bits > 64) {
      throw new SemanticException(
          "Argument exception, the scalar function bit_count bits must be between 2 and 64.");
    }
  }

  public static long bitCountTransform(long num, long bits) {
    if (bits == 64) {
      return Long.bitCount(num);
    }

    final long mask = (1L << bits) - 1;
    return Long.bitCount(num & mask);
  }

  public static long bitwiseTransform(long leftValue, long rightValue, String functionName) {
    TableBuiltinScalarFunction function = TableBuiltinScalarFunction.valueOf(functionName);
    switch (function) {
      case BITWISE_NOT:
        return ~leftValue;

      case BITWISE_AND:
        return leftValue & rightValue;

      case BITWISE_OR:
        return leftValue | rightValue;

      case BITWISE_XOR:
        return leftValue ^ rightValue;

      case BITWISE_LEFT_SHIFT:
        if (rightValue >= 64) {
          return 0L;
        }
        long shifted1 = (leftValue << rightValue);
        if (isTinyInt(leftValue)) {
          return preserveSign(shifted1, TINYINT_MASK, TINYINT_SIGNED_BIT);
        } else if (isSmallInt(leftValue)) {
          return preserveSign(shifted1, SMALLINT_MASK, SMALLINT_SIGNED_BIT);
        } else if (isInt32(leftValue)) {
          return preserveSign(shifted1, INTEGER_MASK, INTEGER_SIGNED_BIT);
        } else {
          return shifted1;
        }

      case BITWISE_RIGHT_SHIFT:
        if (rightValue >= 64) {
          return 0L;
        }
        if (rightValue == 0) {
          return leftValue;
        }
        if (isTinyInt(leftValue)) {
          return (leftValue & TINYINT_MASK) >>> rightValue;
        } else if (isSmallInt(leftValue)) {
          return (leftValue & SMALLINT_MASK) >>> rightValue;
        } else if (isInt32(leftValue)) {
          return (leftValue & INTEGER_MASK) >>> rightValue;
        } else {
          return leftValue >>> rightValue;
        }

      case BITWISE_RIGHT_SHIFT_ARITHMETIC:
        if (rightValue >= 64) {
          if (leftValue >= 0) {
            return 0L;
          }
          return -1L;
        }
        if (isTinyInt(leftValue)) {
          return preserveSign(leftValue, TINYINT_MASK, TINYINT_SIGNED_BIT) >> rightValue;
        } else if (isSmallInt(leftValue)) {
          return preserveSign(leftValue, SMALLINT_MASK, SMALLINT_SIGNED_BIT) >> rightValue;
        } else if (isInt32(leftValue)) {
          return preserveSign(leftValue, INTEGER_MASK, INTEGER_SIGNED_BIT) >> rightValue;
        } else {
          return leftValue >> rightValue;
        }
    }

    throw new SemanticException("Unsupported function: " + functionName);
  }

  private static long preserveSign(long shiftedValue, long mask, long signedBit) {
    // Preserve the sign in 2's complement format
    if ((shiftedValue & signedBit) != 0) {
      return shiftedValue | ~mask;
    }

    return shiftedValue & mask;
  }

  private static boolean isTinyInt(long value) {
    return ((value + 128L) & ~0xFFL) == 0L;
  }

  private static boolean isSmallInt(long value) {
    return ((value + 32_768L) & ~0xFFFFL) == 0L;
  }

  private static boolean isInt32(long value) {
    return ((value + 2_147_483_648L) & ~0xFFFF_FFFFL) == 0L;
  }
}
