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

package org.apache.iotdb.db.queryengine.execution.operator.process.gapfill;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.gapfill.GapFillWoGroupWoMoOperator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GapFillBoundaryTest {

  private static final List<TSDataType> DATA_TYPES =
      Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE);

  @Test(timeout = 10000)
  public void testLongMinRealRowDoesNotGeneratePrecedingGaps() throws Exception {
    Operator child = childWithTimes(Long.MIN_VALUE, Long.MIN_VALUE + 2);
    try (GapFillWoGroupWoMoOperator operator =
        new GapFillWoGroupWoMoOperator(
            mock(CommonOperatorContext.class),
            child,
            0,
            Long.MIN_VALUE,
            Long.MIN_VALUE + 2,
            DATA_TYPES,
            1)) {
      TsBlock result = operator.next();
      assertEquals(3, result.getPositionCount());
      for (int i = 0; i < 3; i++) {
        assertEquals(Long.MIN_VALUE + i, result.getColumn(0).getLong(i));
        assertEquals(i == 1, result.getColumn(1).isNull(i));
      }
      assertEquals(10.0, result.getColumn(1).getDouble(0), 0);
      assertEquals(10.0, result.getColumn(1).getDouble(2), 0);
      assertFalse(operator.hasNext());
      assertTrue(operator.isFinished());
    }
  }

  @Test(timeout = 10000)
  public void testGapFillStopsAfterLongMaxValue() throws Exception {
    Operator child = childWithTimes(Long.MAX_VALUE - 4);
    try (GapFillWoGroupWoMoOperator operator =
        new GapFillWoGroupWoMoOperator(
            mock(CommonOperatorContext.class),
            child,
            0,
            Long.MAX_VALUE - 4,
            Long.MAX_VALUE,
            DATA_TYPES,
            2)) {
      TsBlock first = operator.next();
      assertEquals(1, first.getPositionCount());
      assertEquals(Long.MAX_VALUE - 4, first.getColumn(0).getLong(0));
      assertTrue(operator.hasNext());

      TsBlock gaps = operator.next();
      assertEquals(2, gaps.getPositionCount());
      assertEquals(Long.MAX_VALUE - 2, gaps.getColumn(0).getLong(0));
      assertEquals(Long.MAX_VALUE, gaps.getColumn(0).getLong(1));
      assertTrue(gaps.getColumn(1).isNull(0));
      assertTrue(gaps.getColumn(1).isNull(1));
      assertFalse(operator.hasNext());
      assertTrue(operator.isFinished());
    }
  }

  private Operator childWithTimes(long... times) throws Exception {
    TsBlockBuilder builder = new TsBlockBuilder(DATA_TYPES);
    for (long time : times) {
      builder.getTimeColumnBuilder().writeLong(0);
      builder.getColumnBuilder(0).writeLong(time);
      builder.getColumnBuilder(1).writeDouble(10);
      builder.declarePosition();
    }
    Operator child = mock(Operator.class);
    when(child.hasNextWithTimer()).thenReturn(true, false);
    when(child.nextWithTimer()).thenReturn(builder.build());
    when(child.isFinished()).thenReturn(true);
    return child;
  }
}
