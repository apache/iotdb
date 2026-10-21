/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.calc.transformation.dag.column;

import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.TimeColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.MappableUDFColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffFunctionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.commons.udf.builtin.String.UDTFLower;
import org.apache.iotdb.commons.udf.builtin.String.UDTFUpper;
import org.apache.iotdb.commons.udf.builtin.UDTFAbs;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.DictionaryColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MappableUDFColumnTransformerTest {

  @Test
  public void testAbsWithSelectedRowsAndNulls() throws Exception {
    Column values = doubles(-10.0, -20.0, null, -40.0);
    for (boolean cached : new boolean[] {false, true}) {
      assertDoubles(
          transform(
              new UDTFAbs(), DOUBLE, values, cached, new boolean[] {false, true, false, true}),
          null,
          20.0,
          null,
          40.0);
      assertDoubles(
          transform(
              new UDTFAbs(), DOUBLE, values, cached, new boolean[] {false, true, false, false}),
          null,
          20.0,
          null,
          null);
      assertDoubles(
          transform(new UDTFAbs(), DOUBLE, values, cached, new boolean[] {false, true, true, true}),
          null,
          20.0,
          null,
          40.0);
    }
  }

  @Test
  public void testAbsWithDictionaryOffsetAndRunLengthEncodedInputs() throws Exception {
    Column dictionary = DictionaryColumn.create(4, doubles(-10.0, -20.0), new int[] {1, 0, 1, 0});
    assertDoubles(
        transform(new UDTFAbs(), DOUBLE, dictionary, true, new boolean[] {false, true, true, true}),
        null,
        10.0,
        20.0,
        10.0);

    Column offset = doubles(-100.0, -10.0, -20.0, -30.0, -40.0).getRegion(1, 4);
    assertDoubles(
        transform(new UDTFAbs(), DOUBLE, offset, true, new boolean[] {false, true, true, false}),
        null,
        20.0,
        30.0,
        null);

    Column repeated = new RunLengthEncodedColumn(doubles(-7.0), 4);
    assertDoubles(
        transform(new UDTFAbs(), DOUBLE, repeated, true, new boolean[] {false, true, true, false}),
        null,
        7.0,
        7.0,
        null);
    assertDoubles(
        transform(
            new UDTFAbs(),
            DOUBLE,
            doubles(null, null, null),
            true,
            new boolean[] {true, true, true}),
        null,
        null,
        null);
  }

  @Test
  public void testStringBuiltinsWithSelectedRows() throws Exception {
    ColumnBuilder builder = TEXT.createColumnBuilder(4);
    builder.writeBinary(new Binary("FIRST", TSFileConfig.STRING_CHARSET));
    builder.writeBinary(new Binary("MiXeD", TSFileConfig.STRING_CHARSET));
    builder.appendNull();
    builder.writeBinary(new Binary("LAST", TSFileConfig.STRING_CHARSET));
    Column values = builder.build();
    for (boolean cached : new boolean[] {false, true}) {
      assertStrings(
          transform(new UDTFLower(), TEXT, values, cached, new boolean[] {false, true, true, true}),
          null,
          "mixed",
          null,
          "last");
      assertStrings(
          transform(
              new UDTFUpper(), TEXT, values, cached, new boolean[] {false, true, false, false}),
          null,
          "MIXED",
          null,
          null);
    }
  }

  @Test
  public void testAllAndNoSelectedRows() throws Exception {
    RecordingAbs udf = new RecordingAbs();
    Column values = doubles(-10.0, -20.0, -30.0);
    assertDoubles(
        transform(udf, DOUBLE, values, true, new boolean[] {false, false, false}),
        null,
        null,
        null);
    assertEquals(0, udf.invocations);

    assertDoubles(
        transform(udf, DOUBLE, values, true, new boolean[] {true, true, true}), 10.0, 20.0, 30.0);
    assertEquals(1, udf.invocations);
    assertArrayEquals(new long[] {10, 20, 30}, udf.times);
  }

  @Test
  public void testTimeColumnOnlyContainsSelectedPositions() throws Exception {
    RecordingAbs udf = new RecordingAbs();
    assertDoubles(
        transform(
            udf,
            DOUBLE,
            doubles(-10.0, -20.0, -30.0, -40.0),
            true,
            new boolean[] {false, true, false, true}),
        null,
        20.0,
        null,
        40.0);
    assertArrayEquals(new long[] {20, 40}, udf.times);

    assertDoubles(
        transform(
            udf, DOUBLE, doubles(-10.0, -20.0, -30.0), false, new boolean[] {false, false, true}),
        null,
        null,
        30.0);
    assertArrayEquals(new long[] {30}, udf.times);
  }

  @Test
  public void testNoSelectedRowsStillAdvanceDiffInput() throws Exception {
    IdentityColumnTransformer input = new IdentityColumnTransformer(DOUBLE, 0);
    input.addReferenceCount();
    TimeColumnTransformer time = new TimeColumnTransformer(INT64);
    time.addReferenceCount();
    DiffFunctionColumnTransformer diff = new DiffFunctionColumnTransformer(DOUBLE, input, true);
    diff.addReferenceCount();
    RecordingAbs udf = new RecordingAbs();
    MappableUDFColumnTransformer transformer =
        new MappableUDFColumnTransformer(
            DOUBLE, new ColumnTransformer[] {diff, time}, new TestExecutor(udf, DOUBLE));
    try {
      TsBlock first = block(doubles(-10.0, -20.0, -30.0));
      input.initFromTsBlock(first);
      time.initFromTsBlock(first);
      assertDoubles(
          transformer.getColumnWithSelection(new boolean[] {false, false, false}),
          null,
          null,
          null);
      assertEquals(0, udf.invocations);

      for (ColumnTransformer node :
          ColumnTransformer.collectTransformers(Collections.singletonList(transformer))) {
        node.clearCacheForNewInput();
      }
      TsBlock second = block(doubles(-50.0, -70.0));
      input.initFromTsBlock(second);
      time.initFromTsBlock(second);
      assertDoubles(transformer.getColumnWithSelection(new boolean[] {true, true}), 20.0, 20.0);
      assertEquals(1, udf.invocations);
    } finally {
      transformer.close();
    }
  }

  private static Column transform(
      UDTF udf, Type type, Column values, boolean cached, boolean[] selection) throws Exception {
    IdentityColumnTransformer input = new IdentityColumnTransformer(type, 0);
    input.addReferenceCount();
    TimeColumnTransformer time = new TimeColumnTransformer(INT64);
    time.addReferenceCount();
    TsBlock block = block(values);
    input.initFromTsBlock(block);
    time.initFromTsBlock(block);
    if (cached) {
      input.tryEvaluate();
      time.tryEvaluate();
    }
    MappableUDFColumnTransformer transformer =
        new MappableUDFColumnTransformer(
            type, new ColumnTransformer[] {input, time}, new TestExecutor(udf, type));
    try {
      Column result = transformer.getColumnWithSelection(selection);
      if (cached) {
        // Selected evaluation must not consume either pending full-column reference.
        assertSame(values, input.getColumn());
        assertSame(block.getTimeColumn(), time.getColumn());
      }
      return result;
    } finally {
      transformer.close();
    }
  }

  private static TsBlock block(Column values) {
    long[] times = new long[values.getPositionCount()];
    for (int i = 0; i < times.length; i++) {
      times[i] = (i + 1) * 10L;
    }
    return new TsBlock(new TimeColumn(times.length, times), values);
  }

  private static Column doubles(Double... values) {
    ColumnBuilder builder = DOUBLE.createColumnBuilder(values.length);
    for (Double value : values) {
      if (value == null) {
        builder.appendNull();
      } else {
        builder.writeDouble(value);
      }
    }
    return builder.build();
  }

  private static void assertDoubles(Column result, Double... expected) {
    assertEquals(expected.length, result.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(result.isNull(i));
      } else {
        assertFalse(result.isNull(i));
        assertEquals(expected[i], result.getDouble(i), 0.0);
      }
    }
  }

  private static void assertStrings(Column result, String... expected) {
    assertEquals(expected.length, result.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(result.isNull(i));
      } else {
        assertFalse(result.isNull(i));
        assertEquals(expected[i], result.getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
      }
    }
  }

  private static class TestExecutor extends UDTFExecutor {
    private TestExecutor(UDTF udf, Type type) throws Exception {
      super("selection_test", ZoneId.of("UTC"));
      this.udtf = udf;
      UDFParameters parameters =
          new UDFParameters(
              Collections.singletonList("input"),
              Collections.singletonList(
                  type == DOUBLE
                      ? org.apache.iotdb.udf.api.type.Type.DOUBLE
                      : org.apache.iotdb.udf.api.type.Type.TEXT),
              Collections.emptyMap());
      udf.validate(new UDFParameterValidator(parameters));
      udf.beforeStart(parameters, configurations);
      configurations.check();
    }
  }

  private static class RecordingAbs extends UDTFAbs {
    private int invocations;
    private long[] times;

    @Override
    public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
      invocations++;
      Column time = columns[columns.length - 1];
      times = Arrays.copyOf(time.getLongs(), time.getPositionCount());
      super.transform(columns, builder);
    }
  }
}
