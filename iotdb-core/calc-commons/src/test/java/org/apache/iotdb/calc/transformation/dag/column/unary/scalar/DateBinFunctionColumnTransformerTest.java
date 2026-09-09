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

package org.apache.iotdb.calc.transformation.dag.column.unary.scalar;

import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;

import org.junit.Test;

import java.time.ZoneOffset;

import static org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.dateBin;
import static org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.nextDateBin;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DateBinFunctionColumnTransformerTest {

  @Test
  public void testSourceMinusOriginOverflows() {
    assertBin(
        Long.MAX_VALUE, Long.MIN_VALUE, 10, Long.MAX_VALUE - 5, Long.MAX_VALUE, Long.MAX_VALUE);
  }

  @Test
  public void testNegativeDifferenceRoundsDownBeforeClamping() {
    // The mathematical start is MIN_VALUE - 5; its end must be computed before clamping.
    assertBin(
        Long.MIN_VALUE, Long.MAX_VALUE, 10, Long.MIN_VALUE, Long.MIN_VALUE + 5, Long.MIN_VALUE + 4);
  }

  @Test
  public void testStepProductOverflowsButBinStartIsRepresentable() {
    assertBin(-1, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1, Long.MIN_VALUE + 2, 0, -1);
  }

  @Test
  public void testClosedEndIncludesLongMaxValue() {
    assertBin(Long.MAX_VALUE, 0, 2, Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MAX_VALUE);
  }

  @Test
  public void testNextBinSaturatesWithoutWrapping() {
    assertEquals(Long.MAX_VALUE, nextDateBin(10, Long.MAX_VALUE - 5));
    assertEquals(Long.MAX_VALUE, nextDateBin(10, Long.MAX_VALUE));
    assertEquals(Long.MIN_VALUE + 10, nextDateBin(10, Long.MIN_VALUE));
  }

  private void assertBin(
      long source, long origin, long duration, long start, long end, long closedEnd) {
    DateBinFunctionColumnTransformer transformer =
        new DateBinFunctionColumnTransformer(
            TIMESTAMP,
            0,
            duration,
            new IdentityColumnTransformer(TIMESTAMP, 0),
            origin,
            ZoneOffset.UTC);
    assertEquals(start, dateBin(source, origin, 0, duration, ZoneOffset.UTC));
    assertArrayEquals(new long[] {start, end}, transformer.dateBinStartEnd(source));
    assertArrayEquals(new long[] {start, closedEnd}, transformer.dateBinStartEndClosed(source));
  }
}
