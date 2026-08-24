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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.calc.utils;

import org.apache.iotdb.calc.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.calc.execution.operator.process.window.utils.ColumnList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.type.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TypeServicesTest {

  @Test
  public void testNumericColumnToDoubleConversion() {
    assertEquals(
        1.0, convert(TSDataType.INT32, new IntColumn(1, Optional.empty(), new int[] {1})), 0.0);
    assertEquals(
        2.0,
        convert(
            TSDataType.DATE, new IntColumn(1, Optional.empty(), new int[] {2}, TSDataType.DATE)),
        0.0);
    assertEquals(
        3.0, convert(TSDataType.INT64, new LongColumn(1, Optional.empty(), new long[] {3L})), 0.0);
    assertEquals(
        4.0,
        convert(TSDataType.TIMESTAMP, new LongColumn(1, Optional.empty(), new long[] {4L})),
        0.0);
    assertEquals(
        5.5,
        convert(TSDataType.FLOAT, new FloatColumn(1, Optional.empty(), new float[] {5.5F})),
        0.0);
    assertEquals(
        6.5,
        convert(TSDataType.DOUBLE, new DoubleColumn(1, Optional.empty(), new double[] {6.5D})),
        0.0);
  }

  @Test
  public void testNumericColumnToDoubleConversionUsesCallerException() {
    final UnsupportedOperationException expected = new UnsupportedOperationException("expected");
    final TypeServices.ColumnToDoubleConverter converter =
        TypeServices.NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE
            .call(Type.fromTsDataType(TSDataType.BOOLEAN))
            .create(() -> expected);

    final UnsupportedOperationException actual =
        Assert.assertThrows(
            UnsupportedOperationException.class,
            () -> converter.convert(new IntColumn(1, Optional.empty(), new int[] {1}), 0));

    assertSame(expected, actual);
  }

  @Test
  public void testUnsupportedMergeSortComparatorFailsOnUse() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            TypeServices.MERGE_SORT_COMPARATOR_SERVICE
                .call(Type.fromTsDataType(TSDataType.VECTOR))
                .apply(0));
  }

  // Covers every supported RANGE-frame type and guards native integer overflow and long precision.
  @Test
  public void testRangeFrameComparatorPreservesNativeArithmetic() {
    assertRangeFrameComparator(
        TSDataType.INT32,
        new IntColumn(3, Optional.empty(), new int[] {10, 12, 8}),
        new IntColumn(3, Optional.empty(), new int[] {2, 0, 0}));
    assertRangeFrameComparator(
        TSDataType.DATE,
        new IntColumn(3, Optional.empty(), new int[] {10, 12, 8}, TSDataType.DATE),
        new IntColumn(3, Optional.empty(), new int[] {2, 0, 0}));
    assertRangeFrameComparator(
        TSDataType.INT64,
        new LongColumn(3, Optional.empty(), new long[] {10, 12, 8}),
        new LongColumn(3, Optional.empty(), new long[] {2, 0, 0}));
    assertRangeFrameComparator(
        TSDataType.TIMESTAMP,
        new LongColumn(3, Optional.empty(), new long[] {10, 12, 8}),
        new LongColumn(3, Optional.empty(), new long[] {2, 0, 0}));
    assertRangeFrameComparator(
        TSDataType.FLOAT,
        new FloatColumn(3, Optional.empty(), new float[] {10, 12, 8}),
        new FloatColumn(3, Optional.empty(), new float[] {2, 0, 0}));
    assertRangeFrameComparator(
        TSDataType.DOUBLE,
        new DoubleColumn(3, Optional.empty(), new double[] {10, 12, 8}),
        new DoubleColumn(3, Optional.empty(), new double[] {2, 0, 0}));

    assertTrue(
        compareRangeFrameValues(
            TSDataType.INT32,
            new IntColumn(2, Optional.empty(), new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE}),
            new IntColumn(2, Optional.empty(), new int[] {1, 0}),
            TypeServices.RangeFrameOffsetOperation.ADD,
            TypeServices.RangeFrameComparison.GREATER_THAN_OR_EQUAL));
    assertFalse(
        compareRangeFrameValues(
            TSDataType.INT64,
            new LongColumn(
                2, Optional.empty(), new long[] {9_007_199_254_740_992L, 9_007_199_254_740_992L}),
            new LongColumn(2, Optional.empty(), new long[] {1, 0}),
            TypeServices.RangeFrameOffsetOperation.ADD,
            TypeServices.RangeFrameComparison.GREATER_THAN_OR_EQUAL));
  }

  private static double convert(final TSDataType dataType, final Column column) {
    return TypeServices.NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .create(
            () ->
                new IllegalStateException(
                    "supported data type should not use the exception factory"))
        .convert(column, 0);
  }

  private static void assertRangeFrameComparator(
      final TSDataType dataType, final Column valueColumn, final Column offsetColumn) {
    assertTrue(
        compareRangeFrameValues(
            dataType,
            valueColumn,
            offsetColumn,
            TypeServices.RangeFrameOffsetOperation.ADD,
            TypeServices.RangeFrameComparison.GREATER_THAN_OR_EQUAL));
    assertFalse(
        compareRangeFrameValues(
            dataType,
            valueColumn,
            offsetColumn,
            TypeServices.RangeFrameOffsetOperation.ADD,
            TypeServices.RangeFrameComparison.GREATER_THAN));
    assertTrue(
        compareRangeFrameValues(
            dataType,
            valueColumn,
            offsetColumn,
            TypeServices.RangeFrameOffsetOperation.SUBTRACT,
            TypeServices.RangeFrameComparison.LESS_THAN_OR_EQUAL));
    assertFalse(
        compareRangeFrameValues(
            dataType,
            valueColumn,
            offsetColumn,
            TypeServices.RangeFrameOffsetOperation.SUBTRACT,
            TypeServices.RangeFrameComparison.LESS_THAN));
  }

  private static boolean compareRangeFrameValues(
      final TSDataType dataType,
      final Column valueColumn,
      final Column offsetColumn,
      final TypeServices.RangeFrameOffsetOperation offsetOperation,
      final TypeServices.RangeFrameComparison comparison) {
    final long[] times = new long[valueColumn.getPositionCount()];
    final TsBlock block =
        new TsBlock(
            new TimeColumn(valueColumn.getPositionCount(), times), valueColumn, offsetColumn);
    final Partition partition =
        new Partition(Collections.singletonList(block), 0, valueColumn.getPositionCount());
    final ColumnList columnList = new ColumnList(Collections.singletonList(valueColumn));
    return TypeServices.RANGE_FRAME_COMPARATOR_SERVICE
        .call(Type.fromTsDataType(dataType))
        .compare(
            columnList,
            partition,
            0,
            offsetOperation == TypeServices.RangeFrameOffsetOperation.ADD
                ? 1
                : valueColumn.getPositionCount() - 1,
            1,
            offsetOperation,
            comparison);
  }
}
