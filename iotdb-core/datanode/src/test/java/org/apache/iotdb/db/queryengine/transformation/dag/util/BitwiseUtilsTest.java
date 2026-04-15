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

import org.junit.Assert;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitCountCheck;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseAndTransform;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseLeftShiftTransform;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseNotTransform;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseOrTransform;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseRightShiftArithmeticTransform;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseRightShiftTransform;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseXorTransform;

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

  @Test
  public void testBitwiseRightShiftArithmeticTransform() {
    Assert.assertEquals(0, bitwiseRightShiftArithmeticTransform(12, 64));
    Assert.assertEquals(-1, bitwiseRightShiftArithmeticTransform(-45, 64));

    Assert.assertEquals(0L, bitwiseRightShiftArithmeticTransform(12L, 64));
    Assert.assertEquals(-1L, bitwiseRightShiftArithmeticTransform(-45L, 64));
  }
}
