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

package org.apache.iotdb.db.queryengine.execution.operator.process.fill;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.TableLinearFillWithGroupOperator;
import org.apache.iotdb.calc.execution.operator.process.TableNextFillWithGroupOperator;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.apache.iotdb.calc.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupedFillOperatorTest {
  private static final List<TSDataType> TYPES =
      List.of(TSDataType.TIMESTAMP, TSDataType.INT32, TSDataType.DOUBLE);

  @Test
  public void testLookaheadStopsAtCachedGroupBoundary() throws Exception {
    List<TsBlock> input =
        List.of(
            block(new Object[] {1, 1, 10}, new Object[] {2, 1, null}),
            block(new Object[] {3, 1, null}),
            block(new Object[] {4, 1, null}, new Object[] {1, 2, 100}),
            block(new Object[] {2, 2, null}, new Object[] {3, 2, 300}),
            block(new Object[] {4, 2, null}, new Object[] {1, null, 7}),
            block(new Object[] {2, null, null}, new Object[] {3, null, 11}),
            block(new Object[] {4, null, null}));
    for (boolean linear : new boolean[] {false, true}) {
      assertFilled(
          linear,
          input,
          new Object[][] {
            {1, 1, 10}, {2, 1, null}, {3, 1, null}, {4, 1, null},
            {1, 2, 100}, {2, 2, linear ? 200 : 300}, {3, 2, 300}, {4, 2, null},
            {1, null, 7}, {2, null, linear ? 9 : 11}, {3, null, 11}, {4, null, null}
          });
    }
  }

  @Test
  public void testNewGroupRetainsPreparedLinearLookahead() throws Exception {
    assertFilled(
        true,
        List.of(
            block(new Object[] {1, 1, 10}, new Object[] {2, 1, null}),
            block(new Object[] {3, 1, 30}, new Object[] {1, 2, 100}, new Object[] {2, 2, null}),
            block(new Object[] {3, 2, 300})),
        new Object[][] {{1, 1, 10}, {2, 1, 20}, {3, 1, 30}, {1, 2, 100}, {2, 2, 200}, {3, 2, 300}});
  }

  @Test
  public void testNullHelperCannotBeFilledFromAnotherGroup() throws Exception {
    for (boolean linear : new boolean[] {false, true}) {
      assertFilled(
          linear,
          List.of(
              block(new Object[] {null, 1, null}),
              block(new Object[] {null, 1, null}),
              block(new Object[] {1, 2, 100})),
          new Object[][] {{null, 1, null}, {null, 1, null}, {1, 2, 100}});
    }
  }

  private static void assertFilled(boolean linear, List<TsBlock> blocks, Object[][] expected)
      throws Exception {
    Iterator<TsBlock> iterator = blocks.iterator();
    Operator input = mock(Operator.class);
    when(input.hasNextWithTimer()).thenAnswer(invocation -> iterator.hasNext());
    when(input.nextWithTimer()).thenAnswer(invocation -> iterator.next());
    when(input.isFinished()).thenAnswer(invocation -> !iterator.hasNext());
    when(input.isBlocked()).thenAnswer(invocation -> Operator.NOT_BLOCKED);
    CommonOperatorContext context = mock(CommonOperatorContext.class);
    Comparator<SortKey> comparator =
        getComparatorForTable(
            List.of(SortOrder.ASC_NULLS_LAST), List.of(1), List.of(TSDataType.INT32));
    try (Operator operator =
        linear
            ? new TableLinearFillWithGroupOperator(
                context, CommonOperatorUtils.getLinearFill(3, TYPES), input, 0, comparator, TYPES)
            : new TableNextFillWithGroupOperator(
                context,
                CommonOperatorUtils.getNextFill(3, TYPES, null, ZoneOffset.UTC),
                input,
                0,
                false,
                comparator,
                TYPES)) {
      int rows = 0;
      int calls = 0;
      while (operator.hasNext()) {
        assertTrue("Fill must make progress", ++calls < 100);
        TsBlock result = operator.next();
        if (result == null) {
          continue;
        }
        for (int i = 0; i < result.getPositionCount(); i++, rows++) {
          assertTrue(rows < expected.length);
          for (int column = 0; column < 3; column++) {
            Object value = expected[rows][column];
            if (value == null) {
              assertTrue("row " + rows + ", column " + column, result.getColumn(column).isNull(i));
            } else {
              assertFalse("row " + rows + ", column " + column, result.getColumn(column).isNull(i));
              if (column == 0) {
                assertEquals(((Number) value).longValue(), result.getColumn(column).getLong(i));
              } else if (column == 1) {
                assertEquals(((Number) value).intValue(), result.getColumn(column).getInt(i));
              } else {
                assertEquals(
                    ((Number) value).doubleValue(), result.getColumn(column).getDouble(i), 0);
              }
            }
          }
        }
      }
      assertEquals(expected.length, rows);
    }
  }

  private static TsBlock block(Object[]... rows) {
    TsBlockBuilder builder = new TsBlockBuilder(TYPES);
    for (Object[] row : rows) {
      for (int column = 0; column < 3; column++) {
        if (row[column] == null) {
          builder.getColumnBuilder(column).appendNull();
        } else if (column == 0) {
          builder.getColumnBuilder(column).writeLong(((Number) row[column]).longValue());
        } else if (column == 1) {
          builder.getColumnBuilder(column).writeInt(((Number) row[column]).intValue());
        } else {
          builder.getColumnBuilder(column).writeDouble(((Number) row[column]).doubleValue());
        }
      }
      builder.declarePosition();
    }
    return builder.build(
        new RunLengthEncodedColumn(CommonOperatorUtils.TIME_COLUMN_TEMPLATE, rows.length));
  }
}
