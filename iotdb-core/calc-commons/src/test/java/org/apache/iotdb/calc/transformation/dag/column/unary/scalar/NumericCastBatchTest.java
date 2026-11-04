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
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class NumericCastBatchTest {
  private static final TSDataType[] TYPES = {
    TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
  };

  @Test
  public void testAllNumericPairsAgainstScalarIncludingOverflowAndSelections() {
    Column[] inputs = {
      new IntColumn(
          7, Optional.empty(), new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 1, 17, -17}),
      new LongColumn(
          7,
          Optional.empty(),
          new long[] {Long.MIN_VALUE, Long.MAX_VALUE, -2147483649L, 2147483648L, 0, 17, -17}),
      new FloatColumn(
          7,
          Optional.empty(),
          new float[] {
            Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, -0.0f, 0.0f, 1.5f, -1.5f
          }),
      new DoubleColumn(
          7,
          Optional.empty(),
          new double[] {
            Double.NaN,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            -0.0,
            Double.MAX_VALUE,
            1.5,
            -1.5
          })
    };
    for (int source = 0; source < TYPES.length; source++) {
      for (TSDataType targetType : TYPES) {
        Type target = Type.fromTsDataType(targetType);
        Type inputType = Type.fromTsDataType(TYPES[source]);
        CastFunctionColumnTransformer cast =
            new CastFunctionColumnTransformer(
                target, new IdentityColumnTransformer(inputType, 0), ZoneId.of("UTC"));
        TryCastFunctionColumnTransformer tryCast =
            new TryCastFunctionColumnTransformer(
                target, new IdentityColumnTransformer(inputType, 0), ZoneId.of("UTC"));
        ScalarCast scalar = new ScalarCast(inputType, target);
        // Compare one value at a time so every overflow, including those after a NaN, is exercised.
        for (int i = 0; i < 7; i++) {
          Column column = inputs[source].getRegion(i, 1);
          ColumnBuilder expected = target.createColumnBuilder(1);
          ColumnBuilder actual = target.createColumnBuilder(1);
          try {
            scalar.doTransform(column, expected);
          } catch (IoTDBRuntimeException e) {
            IoTDBRuntimeException failure =
                assertThrows(IoTDBRuntimeException.class, () -> cast.doTransform(column, actual));
            assertEquals(e.getMessage(), failure.getMessage());
            continue;
          }
          cast.doTransform(column, actual);
          assertColumnEquals(expected.build(), actual.build(), targetType);
        }
        for (Column column :
            new Column[] {
              inputs[source],
              new RunLengthEncodedColumn(inputs[source].getRegion(0, 1), 7),
              inputs[source].getRegion(1, 6)
            }) {
          for (boolean[] selection :
              new boolean[][] {null, {true, false, true, false, true, true, false}}) {
            ColumnBuilder expected = target.createColumnBuilder(7);
            ColumnBuilder actual = target.createColumnBuilder(7);
            for (int i = 0; i < column.getPositionCount(); i++) {
              if ((selection != null && !selection[i]) || column.isNull(i)) {
                expected.appendNull();
              } else {
                try {
                  scalar.castSourceValue(column, expected, i);
                } catch (IoTDBRuntimeException e) {
                  expected.appendNull();
                }
              }
            }
            if (selection == null) tryCast.doTransform(column, actual);
            else tryCast.doTransform(column, actual, selection);
            assertColumnEquals(expected.build(), actual.build(), targetType);
          }
        }
      }
    }
  }

  @Test
  public void testNullsAndDeselectedOverflows() {
    Type source = Type.fromTsDataType(TSDataType.DOUBLE);
    Type target = Type.fromTsDataType(TSDataType.INT32);
    CastFunctionColumnTransformer cast =
        new CastFunctionColumnTransformer(
            target, new IdentityColumnTransformer(source, 0), ZoneId.of("UTC"));
    Column input =
        new DoubleColumn(
            3,
            Optional.of(new boolean[] {true, false, false}),
            new double[] {Double.MAX_VALUE, Double.MAX_VALUE, 17});
    ColumnBuilder result = target.createColumnBuilder(3);
    cast.doTransform(input, result, new boolean[] {true, false, true});
    assertColumnEquals(
        new IntColumn(3, Optional.of(new boolean[] {true, true, false}), new int[] {0, 0, 17}),
        result.build(),
        TSDataType.INT32);
  }

  private static void assertColumnEquals(Column expected, Column actual, TSDataType type) {
    assertEquals(expected.getPositionCount(), actual.getPositionCount());
    for (int i = 0; i < expected.getPositionCount(); i++) {
      assertEquals(expected.isNull(i), actual.isNull(i));
      if (expected.isNull(i)) continue;
      switch (type) {
        case INT32 -> assertEquals(expected.getInt(i), actual.getInt(i));
        case INT64 -> assertEquals(expected.getLong(i), actual.getLong(i));
        case FLOAT ->
            assertEquals(
                Float.floatToIntBits(expected.getFloat(i)),
                Float.floatToIntBits(actual.getFloat(i)));
        case DOUBLE ->
            assertEquals(
                Double.doubleToLongBits(expected.getDouble(i)),
                Double.doubleToLongBits(actual.getDouble(i)));
        default -> throw new AssertionError(type);
      }
    }
  }

  // The original scalar pipeline is an independent oracle for the new typed batch loops.
  private static class ScalarCast extends AbstractCastFunctionColumnTransformer {
    ScalarCast(Type source, Type target) {
      super(target, new IdentityColumnTransformer(source, 0), ZoneId.of("UTC"));
    }

    @Override
    protected void transform(Column column, ColumnBuilder builder, int position) {
      castSourceValue(column, builder, position);
    }
  }
}
