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

import org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils;
import org.apache.iotdb.commons.exception.SemanticException;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitCountCheck;
import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitwiseAndTransform;
import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitwiseLeftShiftTransform;
import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitwiseNotTransform;
import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitwiseOrTransform;
import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitwiseRightShiftArithmeticTransform;
import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitwiseRightShiftTransform;
import static org.apache.iotdb.calc.transformation.dag.util.BitwiseUtils.bitwiseXorTransform;

public class BitwiseUtilsTest {
  @Test
  public void testBitCountCheck() {
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(0, 0));
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(7, 1));
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(7, 2));
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(128, 7));
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(128, 8));
  }

  @Test
  public void testBitCountFunctions() {
    Assert.assertEquals(2, BitwiseUtils.bitCountTransform(5, 8));
    Assert.assertEquals(7, BitwiseUtils.bitCountTransform(-5, 8));
    Assert.assertEquals(2, BitwiseUtils.bitCountTransform(9, 64));
    Assert.assertEquals(2, BitwiseUtils.bitCountTransform(9, 8));
    Assert.assertEquals(62, BitwiseUtils.bitCountTransform(-7, 64));
    Assert.assertEquals(6, BitwiseUtils.bitCountTransform(-7, 8));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 9));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 10));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 11));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 12));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 32));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 64));
  }

  @Test
  public void testBitwiseAndTransform() {
    Assert.assertEquals(17, bitwiseAndTransform(19, 25));
  }

  @Test
  public void testBitwiseNotTransform() {
    Assert.assertEquals(-6, bitwiseNotTransform(5));
    Assert.assertEquals(11, bitwiseNotTransform(-12));
    Assert.assertEquals(-20, bitwiseNotTransform(19));
    Assert.assertEquals(-26, bitwiseNotTransform(25));
  }

  @Test
  public void testBitwiseOrTransform() {
    Assert.assertEquals(27, bitwiseOrTransform(19, 25));
  }

  @Test
  public void testBitwiseXorTransform() {
    Assert.assertEquals(10, bitwiseXorTransform(19, 25));
  }

  @Test
  public void testBitwiseLeftShiftTransform() {
    Assert.assertEquals(4, bitwiseLeftShiftTransform(1, 2));
    Assert.assertEquals(20, bitwiseLeftShiftTransform(5, 2));
    Assert.assertEquals(20, bitwiseLeftShiftTransform(20, 0));
    Assert.assertEquals(42, bitwiseLeftShiftTransform(42, 0));
    Assert.assertEquals(0, bitwiseLeftShiftTransform(0, 1));
    Assert.assertEquals(0, bitwiseLeftShiftTransform(0, 2));

    Assert.assertEquals(4L, bitwiseLeftShiftTransform(1L, 2));
    Assert.assertEquals(20L, bitwiseLeftShiftTransform(5L, 2));
    Assert.assertEquals(20L, bitwiseLeftShiftTransform(20L, 0));
    Assert.assertEquals(42L, bitwiseLeftShiftTransform(42L, 0));
    Assert.assertEquals(0L, bitwiseLeftShiftTransform(0L, 1));
    Assert.assertEquals(0L, bitwiseLeftShiftTransform(0L, 2));
  }

  @Test
  public void testBitwiseRightShiftTransform() {
    Assert.assertEquals(1, bitwiseRightShiftTransform(8, 3));
    Assert.assertEquals(4, bitwiseRightShiftTransform(9, 1));
    Assert.assertEquals(20, bitwiseRightShiftTransform(20, 0));
    Assert.assertEquals(42, bitwiseRightShiftTransform(42, 0));
    Assert.assertEquals(0, bitwiseRightShiftTransform(0, 1));
    Assert.assertEquals(0, bitwiseRightShiftTransform(0, 2));
    Assert.assertEquals(0, bitwiseRightShiftTransform(12, 64));
    Assert.assertEquals(0, bitwiseRightShiftTransform(-45, 64));

    Assert.assertEquals(1L, bitwiseRightShiftTransform(8L, 3));
    Assert.assertEquals(4L, bitwiseRightShiftTransform(9L, 1));
    Assert.assertEquals(20L, bitwiseRightShiftTransform(20L, 0));
    Assert.assertEquals(42L, bitwiseRightShiftTransform(42L, 0));
    Assert.assertEquals(0L, bitwiseRightShiftTransform(0L, 1));
    Assert.assertEquals(0L, bitwiseRightShiftTransform(0L, 2));
    Assert.assertEquals(0L, bitwiseRightShiftTransform(12L, 64));
    Assert.assertEquals(0L, bitwiseRightShiftTransform(-45L, 64));
  }

  /**
   * The shift width must come from the declared operand type, not from the magnitude of the value.
   * Every case below crosses the 8-bit or 16-bit boundary that the previous implementation inferred
   * from the value, which made it drop the bits that moved above that inferred width.
   */
  @Test
  public void testBitwiseLeftShiftKeepsDeclaredInt32Width() {
    Assert.assertEquals(128, bitwiseLeftShiftTransform(1, 7));
    Assert.assertEquals(256, bitwiseLeftShiftTransform(1, 8));
    Assert.assertEquals(65536, bitwiseLeftShiftTransform(1, 16));
    Assert.assertEquals(1048576, bitwiseLeftShiftTransform(1, 20));
    Assert.assertEquals(192, bitwiseLeftShiftTransform(3, 6));
    Assert.assertEquals(200, bitwiseLeftShiftTransform(100, 1));
    Assert.assertEquals(128000, bitwiseLeftShiftTransform(1000, 7));
    Assert.assertEquals(-5760, bitwiseLeftShiftTransform(-45, 7));
    Assert.assertEquals(-5760000, bitwiseLeftShiftTransform(-45000, 7));
    Assert.assertEquals(Integer.MIN_VALUE, bitwiseLeftShiftTransform(1, 31));
    // the guard keeps Java's five-bit shift count from wrapping 32 around to 0
    Assert.assertEquals(0, bitwiseLeftShiftTransform(1, 32));
    Assert.assertEquals(0, bitwiseLeftShiftTransform(1, 64));
  }

  @Test
  public void testBitwiseLeftShiftKeepsDeclaredInt64Width() {
    Assert.assertEquals(128L, bitwiseLeftShiftTransform(1L, 7));
    Assert.assertEquals(2147483648L, bitwiseLeftShiftTransform(1L, 31));
    Assert.assertEquals(4294967296L, bitwiseLeftShiftTransform(1L, 32));
    Assert.assertEquals(1099511627776L, bitwiseLeftShiftTransform(1L, 40));
    Assert.assertEquals(4611686018427387904L, bitwiseLeftShiftTransform(1L, 62));
    Assert.assertEquals(1677721600L, bitwiseLeftShiftTransform(100L, 24));
    Assert.assertEquals(-49478023249920000L, bitwiseLeftShiftTransform(-45000L, 40));
    Assert.assertEquals(Long.MIN_VALUE, bitwiseLeftShiftTransform(1L, 63));
    // the guard keeps Java's six-bit shift count from wrapping 64 around to 0
    Assert.assertEquals(0L, bitwiseLeftShiftTransform(1L, 64));
  }

  @Test
  public void testBitwiseRightShiftKeepsDeclaredInt32Width() {
    Assert.assertEquals(2147483645, bitwiseRightShiftTransform(-5, 1));
    Assert.assertEquals(2147483625, bitwiseRightShiftTransform(-45, 1));
    Assert.assertEquals(2147483647, bitwiseRightShiftTransform(-1, 1));
    Assert.assertEquals(2147483583, bitwiseRightShiftTransform(-129, 1));
    Assert.assertEquals(2147461148, bitwiseRightShiftTransform(-45000, 1));
    Assert.assertEquals(1, bitwiseRightShiftTransform(-1, 31));
  }

  @Test
  public void testBitwiseRightShiftKeepsDeclaredInt64Width() {
    Assert.assertEquals(9223372036854775805L, bitwiseRightShiftTransform(-5L, 1));
    Assert.assertEquals(9223372036854775785L, bitwiseRightShiftTransform(-45L, 1));
    Assert.assertEquals(Long.MAX_VALUE, bitwiseRightShiftTransform(-1L, 1));
    Assert.assertEquals(9223372036854753308L, bitwiseRightShiftTransform(-45000L, 1));
    Assert.assertEquals(1L, bitwiseRightShiftTransform(-1L, 63));
  }

  /**
   * The width must not vary between rows of one column: values on either side of the boundaries the
   * previous implementation inferred have to be shifted with the same semantics.
   */
  @Test
  public void testShiftWidthDoesNotDependOnTheValue() {
    for (int value : new int[] {-1, -45, -129, -32769, -45000, 0, 1, 100, 1000, 70000}) {
      for (int shift = 0; shift < 32; shift++) {
        Assert.assertEquals(value << shift, bitwiseLeftShiftTransform(value, shift));
        Assert.assertEquals(value >>> shift, bitwiseRightShiftTransform(value, shift));
      }
    }
    for (long value : new long[] {-1L, -45L, -45000L, -3000000000L, 0L, 1L, 100L, 3000000000L}) {
      for (int shift = 0; shift < 64; shift++) {
        Assert.assertEquals(value << shift, bitwiseLeftShiftTransform(value, shift));
        Assert.assertEquals(value >>> shift, bitwiseRightShiftTransform(value, shift));
      }
    }
  }

  @Test
  public void testBitwiseRightShiftArithmeticTransform() {
    Assert.assertEquals(0, bitwiseRightShiftArithmeticTransform(12, 64));
    Assert.assertEquals(-1, bitwiseRightShiftArithmeticTransform(-45, 64));
    Assert.assertEquals(-23, bitwiseRightShiftArithmeticTransform(-45, 1));
    Assert.assertEquals(-22500, bitwiseRightShiftArithmeticTransform(-45000, 1));
    Assert.assertEquals(50, bitwiseRightShiftArithmeticTransform(100, 1));

    Assert.assertEquals(0L, bitwiseRightShiftArithmeticTransform(12L, 64));
    Assert.assertEquals(-1L, bitwiseRightShiftArithmeticTransform(-45L, 64));
    Assert.assertEquals(-23L, bitwiseRightShiftArithmeticTransform(-45L, 1));
    Assert.assertEquals(-22500L, bitwiseRightShiftArithmeticTransform(-45000L, 1));
    Assert.assertEquals(-1500000000L, bitwiseRightShiftArithmeticTransform(-3000000000L, 1));
  }
}
