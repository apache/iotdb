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

import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.BITWISE_AND;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.BITWISE_LEFT_SHIFT;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.BITWISE_NOT;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.BITWISE_OR;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.BITWISE_RIGHT_SHIFT;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.BITWISE_RIGHT_SHIFT_ARITHMETIC;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.BITWISE_XOR;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitCountCheck;
import static org.apache.iotdb.db.queryengine.transformation.dag.util.BitwiseUtils.bitwiseTransform;

public class BitwiseUtilsTest {
  @Test
  public void testBitCountCheck() {
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(0, 0));
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(7, 1));
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(7, 2));
    Assert.assertThrows(SemanticException.class, () -> bitCountCheck(128, 7));
  }

  @Test
  public void testBitCountFunctions() {
    Assert.assertEquals(2, BitwiseUtils.bitCountTransform(5, 8));
    Assert.assertEquals(7, BitwiseUtils.bitCountTransform(-5, 8));
    Assert.assertEquals(2, BitwiseUtils.bitCountTransform(9, 64));
    Assert.assertEquals(2, BitwiseUtils.bitCountTransform(9, 8));
    Assert.assertEquals(62, BitwiseUtils.bitCountTransform(-7, 64));
    Assert.assertEquals(6, BitwiseUtils.bitCountTransform(-7, 8));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 8));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 9));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 10));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 11));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 12));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 32));
    Assert.assertEquals(1, BitwiseUtils.bitCountTransform(128, 64));
  }

  @Test
  public void testBitwiseFunctions() {
    Assert.assertEquals(17, bitwiseTransform(19, 25, BITWISE_AND.name()));
    Assert.assertEquals(-6, bitwiseTransform(5, 0, BITWISE_NOT.name()));
    Assert.assertEquals(11, bitwiseTransform(-12, 0, BITWISE_NOT.name()));
    Assert.assertEquals(-20, bitwiseTransform(19, 0, BITWISE_NOT.name()));
    Assert.assertEquals(-26, bitwiseTransform(25, 0, BITWISE_NOT.name()));
    Assert.assertEquals(27, bitwiseTransform(19, 25, BITWISE_OR.name()));
    Assert.assertEquals(10, bitwiseTransform(19, 25, BITWISE_XOR.name()));
    Assert.assertEquals(4, bitwiseTransform(1, 2, BITWISE_LEFT_SHIFT.name()));
    Assert.assertEquals(20, bitwiseTransform(5, 2, BITWISE_LEFT_SHIFT.name()));
    Assert.assertEquals(20, bitwiseTransform(20, 0, BITWISE_LEFT_SHIFT.name()));
    Assert.assertEquals(42, bitwiseTransform(42, 0, BITWISE_LEFT_SHIFT.name()));
    Assert.assertEquals(0, bitwiseTransform(0, 1, BITWISE_LEFT_SHIFT.name()));
    Assert.assertEquals(0, bitwiseTransform(0, 2, BITWISE_LEFT_SHIFT.name()));
    Assert.assertEquals(1, bitwiseTransform(8, 3, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(4, bitwiseTransform(9, 1, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(20, bitwiseTransform(20, 0, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(42, bitwiseTransform(42, 0, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(0, bitwiseTransform(0, 1, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(0, bitwiseTransform(0, 2, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(0, bitwiseTransform(12, 64, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(0, bitwiseTransform(-45, 64, BITWISE_RIGHT_SHIFT.name()));
    Assert.assertEquals(0, bitwiseTransform(12, 64, BITWISE_RIGHT_SHIFT_ARITHMETIC.name()));
    Assert.assertEquals(-1, bitwiseTransform(-45, 64, BITWISE_RIGHT_SHIFT_ARITHMETIC.name()));
  }
}
