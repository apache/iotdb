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

package org.apache.iotdb.calc.execution.operator.process.fill.linear;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LinearFillTest {

  @Test
  public void testInterpolationAcrossLongRange() {
    assertMidpoint(new long[] {Long.MIN_VALUE, 0, Long.MAX_VALUE});
  }

  @Test
  public void testDescendingInterpolationAcrossLongRange() {
    assertMidpoint(new long[] {Long.MAX_VALUE, 0, Long.MIN_VALUE});
  }

  @Test
  public void testSmallTimeDifferencesNearLongMaxValueRemainExact() {
    assertMidpoint(new long[] {Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, Long.MAX_VALUE});
  }

  private void assertMidpoint(long[] times) {
    Column values =
        new DoubleColumn(
            3, Optional.of(new boolean[] {false, true, false}), new double[] {0, 0, 10});
    Column filled = new DoubleLinearFill().fill(new TimeColumn(3, times), values, 0);
    assertEquals(3, filled.getPositionCount());
    for (int i = 0; i < 3; i++) {
      assertFalse(filled.isNull(i));
      assertEquals(i * 5.0, filled.getDouble(i), 1e-12);
    }
  }
}
