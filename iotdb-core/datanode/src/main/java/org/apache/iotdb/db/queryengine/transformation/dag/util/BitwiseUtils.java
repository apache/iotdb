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

import org.apache.iotdb.db.exception.sql.SemanticException;

public class BitwiseUtils {
  private static final long TINYINT_MASK = 0b1111_1111L;
  private static final long TINYINT_SIGNED_BIT = 0b1000_0000L;
  private static final long SMALLINT_MASK = 0b1111_1111_1111_1111L;
  private static final long SMALLINT_SIGNED_BIT = 0b1000_0000_0000_0000L;
  private static final int TINYINT_MASK_IN_INT = 0b1111_1111;
  private static final int TINYINT_SIGNED_BIT_IN_INT = 0b1000_0000;
  private static final int SMALLINT_MASK_IN_INT = 0b1111_1111_1111_1111;
  private static final int SMALLINT_SIGNED_BIT_IN_INT = 0b1000_0000_0000_0000;
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
    bitCountCheck(num, bits);

    if (bits == 64) {
      return Long.bitCount(num);
    }

    final long mask = (1L << bits) - 1;
    return Long.bitCount(num & mask);
  }

  public static long bitwiseAndTransform(long leftValue, long rightValue) {
    return leftValue & rightValue;
  }

  public static long bitwiseNotTransform(long leftValue) {
    return ~leftValue;
  }

  public static long bitwiseOrTransform(long leftValue, long rightValue) {
    return leftValue | rightValue;
  }

  public static long bitwiseXorTransform(long leftValue, long rightValue) {
    return leftValue ^ rightValue;
  }

  public static int bitwiseLeftShiftTransform(int value, long shift) {
    if (shift >= 32) {
      return 0;
    }
    int shifted = (value << shift);
    if (isTinyInt(value)) {
      return preserveSign(shifted, TINYINT_MASK_IN_INT, TINYINT_SIGNED_BIT_IN_INT);
    } else if (isSmallInt(value)) {
      return preserveSign(shifted, SMALLINT_MASK_IN_INT, SMALLINT_SIGNED_BIT_IN_INT);
    } else {
      return shifted;
    }
  }

  public static long bitwiseLeftShiftTransform(long value, long shift) {
    if (shift >= 64) {
      return 0L;
    }
    long shifted = (value << shift);
    if (isTinyInt(value)) {
      return preserveSign(shifted, TINYINT_MASK, TINYINT_SIGNED_BIT);
    } else if (isSmallInt(value)) {
      return preserveSign(shifted, SMALLINT_MASK, SMALLINT_SIGNED_BIT);
    } else if (isInt32(value)) {
      return preserveSign(shifted, INTEGER_MASK, INTEGER_SIGNED_BIT);
    } else {
      return shifted;
    }
  }

  public static int bitwiseRightShiftTransform(int value, long shift) {
    if (shift >= 32) {
      return 0;
    }
    if (shift == 0) {
      return value;
    }
    if (isTinyInt(value)) {
      return (value & TINYINT_MASK_IN_INT) >>> shift;
    } else if (isSmallInt(value)) {
      return (value & SMALLINT_MASK_IN_INT) >>> shift;
    } else {
      return value >>> shift;
    }
  }

  public static long bitwiseRightShiftTransform(long value, long shift) {
    if (shift >= 64) {
      return 0L;
    }
    if (shift == 0) {
      return value;
    }
    if (isTinyInt(value)) {
      return (value & TINYINT_MASK) >>> shift;
    } else if (isSmallInt(value)) {
      return (value & SMALLINT_MASK) >>> shift;
    } else if (isInt32(value)) {
      return (value & INTEGER_MASK) >>> shift;
    } else {
      return value >>> shift;
    }
  }

  public static int bitwiseRightShiftArithmeticTransform(int value, long shift) {
    if (shift >= 32) {
      if (value >= 0) {
        return 0;
      }
      return -1;
    }
    if (isTinyInt(value)) {
      return preserveSign(value, TINYINT_MASK_IN_INT, TINYINT_SIGNED_BIT_IN_INT) >> shift;
    } else if (isSmallInt(value)) {
      return preserveSign(value, SMALLINT_MASK_IN_INT, SMALLINT_SIGNED_BIT_IN_INT) >> shift;
    } else {
      return value >> shift;
    }
  }

  public static long bitwiseRightShiftArithmeticTransform(long value, long shift) {
    if (shift >= 64) {
      if (value >= 0) {
        return 0L;
      }
      return -1L;
    }
    if (isTinyInt(value)) {
      return preserveSign(value, TINYINT_MASK, TINYINT_SIGNED_BIT) >> shift;
    } else if (isSmallInt(value)) {
      return preserveSign(value, SMALLINT_MASK, SMALLINT_SIGNED_BIT) >> shift;
    } else if (isInt32(value)) {
      return preserveSign(value, INTEGER_MASK, INTEGER_SIGNED_BIT) >> shift;
    } else {
      return value >> shift;
    }
  }

  private static long preserveSign(long shiftedValue, long mask, long signedBit) {
    // Preserve the sign in 2's complement format
    if ((shiftedValue & signedBit) != 0) {
      return shiftedValue | ~mask;
    }

    return shiftedValue & mask;
  }

  private static int preserveSign(int shiftedValue, int mask, int signedBit) {
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
