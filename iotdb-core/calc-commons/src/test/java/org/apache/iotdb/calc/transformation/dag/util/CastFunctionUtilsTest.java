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

package org.apache.iotdb.calc.transformation.dag.util;

import org.apache.iotdb.commons.exception.SemanticException;

import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link CastFunctionUtils}, covering both happy paths and out-of-range throws. */
public class CastFunctionUtilsTest {

  // ---- castLongToInt ----
  @Test
  public void castLongToIntInRange() {
    Assert.assertEquals(0, CastFunctionUtils.castLongToInt(0L));
    Assert.assertEquals(Integer.MAX_VALUE, CastFunctionUtils.castLongToInt(Integer.MAX_VALUE));
    Assert.assertEquals(Integer.MIN_VALUE, CastFunctionUtils.castLongToInt(Integer.MIN_VALUE));
  }

  @Test(expected = SemanticException.class)
  public void castLongToIntOverflowPositive() {
    CastFunctionUtils.castLongToInt((long) Integer.MAX_VALUE + 1L);
  }

  @Test(expected = SemanticException.class)
  public void castLongToIntOverflowNegative() {
    CastFunctionUtils.castLongToInt((long) Integer.MIN_VALUE - 1L);
  }

  // ---- castFloatToInt ----
  @Test
  public void castFloatToIntInRange() {
    Assert.assertEquals(2, CastFunctionUtils.castFloatToInt(1.5f));
    Assert.assertEquals(-1, CastFunctionUtils.castFloatToInt(-1.4f));
  }

  @Test(expected = SemanticException.class)
  public void castFloatToIntOverflow() {
    CastFunctionUtils.castFloatToInt(Float.MAX_VALUE);
  }

  // ---- castFloatToLong ----
  @Test
  public void castFloatToLongInRange() {
    Assert.assertEquals(2L, CastFunctionUtils.castFloatToLong(1.5f));
  }

  @Test(expected = SemanticException.class)
  public void castFloatToLongOverflow() {
    CastFunctionUtils.castFloatToLong(Float.MAX_VALUE);
  }

  // ---- castDoubleToInt ----
  @Test
  public void castDoubleToIntInRange() {
    Assert.assertEquals(2, CastFunctionUtils.castDoubleToInt(1.5));
  }

  @Test(expected = SemanticException.class)
  public void castDoubleToIntOverflow() {
    CastFunctionUtils.castDoubleToInt(Double.MAX_VALUE);
  }

  // ---- castDoubleToLong ----
  @Test
  public void castDoubleToLongInRange() {
    Assert.assertEquals(2L, CastFunctionUtils.castDoubleToLong(1.5));
  }

  @Test(expected = SemanticException.class)
  public void castDoubleToLongOverflow() {
    CastFunctionUtils.castDoubleToLong(Double.MAX_VALUE);
  }

  // ---- castDoubleToFloat ----
  @Test
  public void castDoubleToFloatInRange() {
    Assert.assertEquals(1.5f, CastFunctionUtils.castDoubleToFloat(1.5), 0.0f);
  }

  @Test(expected = SemanticException.class)
  public void castDoubleToFloatOverflow() {
    CastFunctionUtils.castDoubleToFloat(Double.MAX_VALUE);
  }

  // ---- castTextToFloat ----
  @Test
  public void castTextToFloatValid() {
    Assert.assertEquals(1.5f, CastFunctionUtils.castTextToFloat("1.5"), 0.0f);
    Assert.assertEquals(-2.0f, CastFunctionUtils.castTextToFloat("-2"), 0.0f);
  }

  @Test(expected = SemanticException.class)
  public void castTextToFloatOverflow() {
    // 1e40 overflows float positive infinity -> SemanticException
    CastFunctionUtils.castTextToFloat("1e40");
  }

  // ---- castTextToDouble ----
  @Test
  public void castTextToDoubleValid() {
    Assert.assertEquals(1.5, CastFunctionUtils.castTextToDouble("1.5"), 0.0);
  }

  @Test(expected = SemanticException.class)
  public void castTextToDoubleOverflow() {
    // 1e400 overflows double to positive infinity -> SemanticException
    CastFunctionUtils.castTextToDouble("1e400");
  }

  // ---- castTextToBoolean ----
  @Test
  public void castTextToBooleanValid() {
    Assert.assertTrue(CastFunctionUtils.castTextToBoolean("true"));
    Assert.assertFalse(CastFunctionUtils.castTextToBoolean("false"));
    Assert.assertTrue(CastFunctionUtils.castTextToBoolean("TRUE"));
    Assert.assertFalse(CastFunctionUtils.castTextToBoolean("False"));
  }

  @Test(expected = SemanticException.class)
  public void castTextToBooleanInvalid() {
    CastFunctionUtils.castTextToBoolean("yes");
  }
}
