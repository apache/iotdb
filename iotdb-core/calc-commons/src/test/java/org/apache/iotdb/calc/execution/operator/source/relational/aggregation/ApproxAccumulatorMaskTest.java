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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.HyperLogLog;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.BinaryGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.BooleanGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.DoubleGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.FloatGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedApproxPercentileAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedApproxPercentileWithWeightAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.IntGroupedApproxMostFrequentAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.LongGroupedApproxMostFrequentAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ApproxAccumulatorMaskTest {

  private static AggregationMask sparseMask() {
    // Shorter backing array than total positionCount (4); only rows 1 and 3 are selected.
    return AggregationMask.createSelectedPositions(4, new int[] {1, 3}, 2);
  }

  private static AggregationMask paddedSparseMask() {
    // Reused backing array of length 4 with trailing stale 0s beyond selectedPositionCount=2.
    return AggregationMask.createSelectedPositions(4, new int[] {1, 3, 0, 0}, 2);
  }

  @Test
  public void testApproxPercentileSparseMask() {
    Column pct = new DoubleColumn(4, Optional.empty(), new double[] {0.5, 0.5, 0.5, 0.5});

    // INT32 (short mask + padded mask so unselected row 0 value 9999 must not skew result)
    for (AggregationMask mask : new AggregationMask[] {sparseMask(), paddedSparseMask()}) {
      ApproxPercentileAccumulator acc = new ApproxPercentileAccumulator(TSDataType.INT32);
      acc.addInput(
          new Column[] {new IntColumn(4, Optional.empty(), new int[] {9999, 20, 8888, 20}), pct},
          mask);
      IntColumnBuilder out = new IntColumnBuilder(null, 1);
      acc.evaluateFinal(out);
      assertEquals(20, out.build().getInt(0));
    }

    // INT64
    ApproxPercentileAccumulator longAcc = new ApproxPercentileAccumulator(TSDataType.INT64);
    longAcc.addInput(
        new Column[] {
          new LongColumn(4, Optional.empty(), new long[] {9999L, 40L, 8888L, 40L}), pct
        },
        sparseMask());
    LongColumnBuilder longOut = new LongColumnBuilder(null, 1);
    longAcc.evaluateFinal(longOut);
    assertEquals(40L, longOut.build().getLong(0));

    // FLOAT
    ApproxPercentileAccumulator floatAcc = new ApproxPercentileAccumulator(TSDataType.FLOAT);
    floatAcc.addInput(
        new Column[] {
          new FloatColumn(4, Optional.empty(), new float[] {9999f, 12.5f, 8888f, 12.5f}), pct
        },
        sparseMask());
    FloatColumnBuilder floatOut = new FloatColumnBuilder(null, 1);
    floatAcc.evaluateFinal(floatOut);
    assertEquals(12.5f, floatOut.build().getFloat(0), 0.01f);

    // DOUBLE
    ApproxPercentileAccumulator doubleAcc = new ApproxPercentileAccumulator(TSDataType.DOUBLE);
    doubleAcc.addInput(
        new Column[] {
          new DoubleColumn(4, Optional.empty(), new double[] {9999.0, 25.0, 8888.0, 25.0}), pct
        },
        sparseMask());
    DoubleColumnBuilder doubleOut = new DoubleColumnBuilder(null, 1);
    doubleAcc.evaluateFinal(doubleOut);
    assertEquals(25.0, doubleOut.build().getDouble(0), 0.01);
  }

  @Test
  public void testApproxPercentileWithWeightSparseMask() {
    Column pct = new DoubleColumn(4, Optional.empty(), new double[] {0.5, 0.5, 0.5, 0.5});
    // Row 0 has invalid weight 0; since row 0 is NOT selected, no SemanticException should be
    // thrown.
    Column weights = new IntColumn(4, Optional.empty(), new int[] {0, 2, 0, 2});

    for (AggregationMask mask : new AggregationMask[] {sparseMask(), paddedSparseMask()}) {
      ApproxPercentileWithWeightAccumulator intAcc =
          new ApproxPercentileWithWeightAccumulator(TSDataType.INT32);
      intAcc.addInput(
          new Column[] {
            new IntColumn(4, Optional.empty(), new int[] {9999, 30, 8888, 30}), weights, pct
          },
          mask);
      IntColumnBuilder out = new IntColumnBuilder(null, 1);
      intAcc.evaluateFinal(out);
      assertEquals(30, out.build().getInt(0));
    }

    ApproxPercentileWithWeightAccumulator longAcc =
        new ApproxPercentileWithWeightAccumulator(TSDataType.INT64);
    longAcc.addInput(
        new Column[] {
          new LongColumn(4, Optional.empty(), new long[] {9999L, 50L, 8888L, 50L}), weights, pct
        },
        paddedSparseMask());
    LongColumnBuilder longOut = new LongColumnBuilder(null, 1);
    longAcc.evaluateFinal(longOut);
    assertEquals(50L, longOut.build().getLong(0));

    ApproxPercentileWithWeightAccumulator floatAcc =
        new ApproxPercentileWithWeightAccumulator(TSDataType.FLOAT);
    floatAcc.addInput(
        new Column[] {
          new FloatColumn(4, Optional.empty(), new float[] {9999f, 6.5f, 8888f, 6.5f}), weights, pct
        },
        paddedSparseMask());
    FloatColumnBuilder floatOut = new FloatColumnBuilder(null, 1);
    floatAcc.evaluateFinal(floatOut);
    assertEquals(6.5f, floatOut.build().getFloat(0), 0.01f);

    ApproxPercentileWithWeightAccumulator doubleAcc =
        new ApproxPercentileWithWeightAccumulator(TSDataType.DOUBLE);
    doubleAcc.addInput(
        new Column[] {
          new DoubleColumn(4, Optional.empty(), new double[] {9999.0, 9.5, 8888.0, 9.5}),
          weights,
          pct
        },
        paddedSparseMask());
    DoubleColumnBuilder doubleOut = new DoubleColumnBuilder(null, 1);
    doubleAcc.evaluateFinal(doubleOut);
    assertEquals(9.5, doubleOut.build().getDouble(0), 0.01);
  }

  @Test
  public void testGroupedApproxPercentileAndWeightSparseMask() {
    int[] groupIds = new int[] {0, 1, 0, 1};
    Column pct = new DoubleColumn(4, Optional.empty(), new double[] {0.5, 0.5, 0.5, 0.5});
    Column weights = new IntColumn(4, Optional.empty(), new int[] {0, 2, 0, 2});

    // GroupedApproxPercentileAccumulator across all 4 numeric types
    for (TSDataType type :
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
        }) {
      GroupedApproxPercentileAccumulator acc = new GroupedApproxPercentileAccumulator(type);
      acc.setGroupCount(2);
      Column values = numericColumn(type);
      acc.addInput(groupIds, new Column[] {values, pct}, sparseMask());
      acc.addInput(groupIds, new Column[] {values, pct}, AggregationMask.createSelectNone(4));
      assertGroupedNumericResult(acc, type, 1, 20);
    }

    // GroupedApproxPercentileWithWeightAccumulator across all 4 numeric types (with padded mask)
    for (TSDataType type :
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
        }) {
      GroupedApproxPercentileWithWeightAccumulator acc =
          new GroupedApproxPercentileWithWeightAccumulator(type);
      acc.setGroupCount(2);
      Column values = numericColumn(type);
      acc.addInput(groupIds, new Column[] {values, weights, pct}, paddedSparseMask());
      assertGroupedNumericResult(acc, type, 1, 20);
    }
  }

  @Test
  public void testApproxMostFrequentSparseMask() {
    Column k = new IntColumn(4, Optional.empty(), new int[] {2, 2, 2, 2});
    Column cap = new IntColumn(4, Optional.empty(), new int[] {10, 10, 10, 10});

    // Ungrouped: all 6 types (paddedSparseMask ensures unselected row 0 is NOT counted)
    IntApproxMostFrequentAccumulator intAcc = new IntApproxMostFrequentAccumulator();
    intAcc.addInput(
        new Column[] {new IntColumn(4, Optional.empty(), new int[] {99, 7, 88, 7}), k, cap},
        paddedSparseMask());
    assertMostFrequentContainsOnly(intAcc, "7", "99");

    LongApproxMostFrequentAccumulator longAcc = new LongApproxMostFrequentAccumulator();
    longAcc.addInput(
        new Column[] {new LongColumn(4, Optional.empty(), new long[] {99L, 7L, 88L, 7L}), k, cap},
        sparseMask());
    assertMostFrequentContainsOnly(longAcc, "7", "99");

    FloatApproxMostFrequentAccumulator floatAcc = new FloatApproxMostFrequentAccumulator();
    floatAcc.addInput(
        new Column[] {new FloatColumn(4, Optional.empty(), new float[] {99f, 7f, 88f, 7f}), k, cap},
        sparseMask());
    assertMostFrequentContainsOnly(floatAcc, "7.0", "99.0");

    DoubleApproxMostFrequentAccumulator doubleAcc = new DoubleApproxMostFrequentAccumulator();
    doubleAcc.addInput(
        new Column[] {
          new DoubleColumn(4, Optional.empty(), new double[] {99.0, 7.0, 88.0, 7.0}), k, cap
        },
        sparseMask());
    assertMostFrequentContainsOnly(doubleAcc, "7.0", "99.0");

    BooleanApproxMostFrequentAccumulator boolAcc = new BooleanApproxMostFrequentAccumulator();
    boolAcc.addInput(
        new Column[] {
          new BooleanColumn(4, Optional.empty(), new boolean[] {false, true, false, true}), k, cap
        },
        paddedSparseMask());
    assertMostFrequentContainsOnly(boolAcc, "true", "false");

    BinaryApproxMostFrequentAccumulator binAcc = new BinaryApproxMostFrequentAccumulator();
    binAcc.addInput(
        new Column[] {
          new BinaryColumn(
              4,
              Optional.empty(),
              new Binary[] {bin("drop"), bin("keep"), bin("drop2"), bin("keep")}),
          k,
          cap
        },
        paddedSparseMask());
    assertMostFrequentContainsOnly(binAcc, "keep", "drop");
  }

  @Test
  public void testGroupedApproxMostFrequentSparseMask() {
    int[] groupIds = new int[] {0, 1, 0, 1};
    Column k = new IntColumn(4, Optional.empty(), new int[] {2, 2, 2, 2});
    Column cap = new IntColumn(4, Optional.empty(), new int[] {10, 10, 10, 10});

    IntGroupedApproxMostFrequentAccumulator intAcc = new IntGroupedApproxMostFrequentAccumulator();
    intAcc.setGroupCount(2);
    intAcc.addInput(
        groupIds,
        new Column[] {new IntColumn(4, Optional.empty(), new int[] {99, 7, 88, 7}), k, cap},
        paddedSparseMask());
    assertGroupedMostFrequentContainsOnly(intAcc, 1, "7", "99");

    LongGroupedApproxMostFrequentAccumulator longAcc =
        new LongGroupedApproxMostFrequentAccumulator();
    longAcc.setGroupCount(2);
    longAcc.addInput(
        groupIds,
        new Column[] {new LongColumn(4, Optional.empty(), new long[] {99L, 7L, 88L, 7L}), k, cap},
        sparseMask());
    assertGroupedMostFrequentContainsOnly(longAcc, 1, "7", "99");

    FloatGroupedApproxMostFrequentAccumulator floatAcc =
        new FloatGroupedApproxMostFrequentAccumulator();
    floatAcc.setGroupCount(2);
    floatAcc.addInput(
        groupIds,
        new Column[] {new FloatColumn(4, Optional.empty(), new float[] {99f, 7f, 88f, 7f}), k, cap},
        sparseMask());
    assertGroupedMostFrequentContainsOnly(floatAcc, 1, "7.0", "99.0");

    DoubleGroupedApproxMostFrequentAccumulator doubleAcc =
        new DoubleGroupedApproxMostFrequentAccumulator();
    doubleAcc.setGroupCount(2);
    doubleAcc.addInput(
        groupIds,
        new Column[] {
          new DoubleColumn(4, Optional.empty(), new double[] {99.0, 7.0, 88.0, 7.0}), k, cap
        },
        sparseMask());
    assertGroupedMostFrequentContainsOnly(doubleAcc, 1, "7.0", "99.0");

    BooleanGroupedApproxMostFrequentAccumulator boolAcc =
        new BooleanGroupedApproxMostFrequentAccumulator();
    boolAcc.setGroupCount(2);
    boolAcc.addInput(
        groupIds,
        new Column[] {
          new BooleanColumn(4, Optional.empty(), new boolean[] {false, true, false, true}), k, cap
        },
        paddedSparseMask());
    assertGroupedMostFrequentContainsOnly(boolAcc, 1, "true", "false");

    BinaryGroupedApproxMostFrequentAccumulator binAcc =
        new BinaryGroupedApproxMostFrequentAccumulator();
    binAcc.setGroupCount(2);
    binAcc.addInput(
        groupIds,
        new Column[] {
          new BinaryColumn(
              4,
              Optional.empty(),
              new Binary[] {bin("drop"), bin("keep"), bin("drop2"), bin("keep")}),
          k,
          cap
        },
        paddedSparseMask());
    assertGroupedMostFrequentContainsOnly(binAcc, 1, "keep", "drop");
  }

  @Test
  public void testApproxCountDistinctTypedAddInputSparseMask() {
    ApproxCountDistinctAccumulator acc = new ApproxCountDistinctAccumulator(TSDataType.INT32);

    HyperLogLog boolHll = new HyperLogLog(HyperLogLog.DEFAULT_STANDARD_ERROR);
    acc.addBooleanInput(
        new BooleanColumn(4, Optional.empty(), new boolean[] {false, true, false, true}),
        paddedSparseMask(),
        boolHll);
    assertEquals(1L, boolHll.cardinality());

    HyperLogLog intHll = new HyperLogLog(HyperLogLog.DEFAULT_STANDARD_ERROR);
    acc.addIntInput(
        new IntColumn(4, Optional.empty(), new int[] {10, 20, 30, 20}), sparseMask(), intHll);
    assertEquals(1L, intHll.cardinality());

    HyperLogLog longHll = new HyperLogLog(HyperLogLog.DEFAULT_STANDARD_ERROR);
    acc.addLongInput(
        new LongColumn(4, Optional.empty(), new long[] {10L, 20L, 30L, 20L}),
        sparseMask(),
        longHll);
    assertEquals(1L, longHll.cardinality());

    HyperLogLog floatHll = new HyperLogLog(HyperLogLog.DEFAULT_STANDARD_ERROR);
    acc.addFloatInput(
        new FloatColumn(4, Optional.empty(), new float[] {10f, 20f, 30f, 20f}),
        sparseMask(),
        floatHll);
    assertEquals(1L, floatHll.cardinality());

    HyperLogLog doubleHll = new HyperLogLog(HyperLogLog.DEFAULT_STANDARD_ERROR);
    acc.addDoubleInput(
        new DoubleColumn(4, Optional.empty(), new double[] {10.0, 20.0, 30.0, 20.0}),
        sparseMask(),
        doubleHll);
    assertEquals(1L, doubleHll.cardinality());

    HyperLogLog binHll = new HyperLogLog(HyperLogLog.DEFAULT_STANDARD_ERROR);
    acc.addBinaryInput(
        new BinaryColumn(
            4, Optional.empty(), new Binary[] {bin("a"), bin("b"), bin("c"), bin("b")}),
        sparseMask(),
        binHll);
    assertEquals(1L, binHll.cardinality());
  }

  private static Column numericColumn(TSDataType type) {
    return switch (type) {
      case INT32 -> new IntColumn(4, Optional.empty(), new int[] {9999, 20, 8888, 20});
      case INT64 -> new LongColumn(4, Optional.empty(), new long[] {9999L, 20L, 8888L, 20L});
      case FLOAT -> new FloatColumn(4, Optional.empty(), new float[] {9999f, 20f, 8888f, 20f});
      case DOUBLE ->
          new DoubleColumn(4, Optional.empty(), new double[] {9999.0, 20.0, 8888.0, 20.0});
      default -> throw new AssertionError(type);
    };
  }

  private static void assertGroupedNumericResult(
      GroupedAccumulator acc, TSDataType type, int groupId, int expected) {
    switch (type) {
      case INT32 -> {
        IntColumnBuilder out = new IntColumnBuilder(null, 1);
        acc.evaluateFinal(groupId, out);
        assertEquals(expected, out.build().getInt(0));
      }
      case INT64 -> {
        LongColumnBuilder out = new LongColumnBuilder(null, 1);
        acc.evaluateFinal(groupId, out);
        assertEquals((long) expected, out.build().getLong(0));
      }
      case FLOAT -> {
        FloatColumnBuilder out = new FloatColumnBuilder(null, 1);
        acc.evaluateFinal(groupId, out);
        assertEquals((float) expected, out.build().getFloat(0), 0.01f);
      }
      case DOUBLE -> {
        DoubleColumnBuilder out = new DoubleColumnBuilder(null, 1);
        acc.evaluateFinal(groupId, out);
        assertEquals((double) expected, out.build().getDouble(0), 0.01);
      }
      default -> throw new AssertionError(type);
    }
  }

  private static void assertMostFrequentContainsOnly(
      TableAccumulator acc, String expected, String excluded) {
    BinaryColumnBuilder out = new BinaryColumnBuilder(null, 1);
    acc.evaluateFinal(out);
    String json = out.build().getBinary(0).getStringValue(StandardCharsets.UTF_8);
    assertTrue("Expected " + expected + " in " + json, json.contains(expected));
    assertFalse("Did not expect " + excluded + " in " + json, json.contains(excluded));
  }

  private static void assertGroupedMostFrequentContainsOnly(
      GroupedAccumulator acc, int groupId, String expected, String excluded) {
    BinaryColumnBuilder out = new BinaryColumnBuilder(null, 1);
    acc.evaluateFinal(groupId, out);
    String json = out.build().getBinary(0).getStringValue(StandardCharsets.UTF_8);
    assertTrue("Expected " + expected + " in " + json, json.contains(expected));
    assertFalse("Did not expect " + excluded + " in " + json, json.contains(excluded));
  }

  private static Binary bin(String s) {
    return new Binary(s, StandardCharsets.UTF_8);
  }
}
