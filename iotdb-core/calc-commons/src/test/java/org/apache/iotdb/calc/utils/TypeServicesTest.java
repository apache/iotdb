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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

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

  private static double convert(final TSDataType dataType, final Column column) {
    return TypeServices.NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .create(
            () ->
                new IllegalStateException(
                    "supported data type should not use the exception factory"))
        .convert(column, 0);
  }
}
