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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.io.File;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FFTTableFunctionTest {

  private static final double DELTA = 1e-9;

  private final FFTTableFunction function = new FFTTableFunction();

  @Test
  public void testWritesFullSpectrumAndZeroPadsToSpecifiedN() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(true, 4L);
    processor.process(record(0L, 1.0), Collections.emptyList(), null);

    List<ColumnBuilder> builders = createOutputBuilders(4);
    processor.finish(builders, null);

    double intervalSeconds = TimestampPrecisionUtils.currPrecision.toNanos(1L) / 1_000_000_000.0;
    assertLongColumn(builders.get(0).build(), 0L, 1L, 2L, 3L);
    assertDoubleColumn(
        builders.get(1).build(),
        0.0,
        1.0 / (4.0 * intervalSeconds),
        -2.0 / (4.0 * intervalSeconds),
        -1.0 / (4.0 * intervalSeconds));
    assertDoubleColumn(builders.get(2).build(), 1.0, 1.0, 1.0, 1.0);
    assertDoubleColumn(builders.get(3).build(), 0.0, 0.0, 0.0, 0.0);
  }

  @Test
  public void testTruncatesInputRowsToSpecifiedN() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(true, 2L);
    processor.process(record(0L, 1.0), Collections.emptyList(), null);
    processor.process(record(1L, 2.0), Collections.emptyList(), null);
    processor.process(record(2L, 100.0), Collections.emptyList(), null);
    processor.process(record(3L, 200.0), Collections.emptyList(), null);

    List<ColumnBuilder> builders = createOutputBuilders(2);
    processor.finish(builders, null);

    assertLongColumn(builders.get(0).build(), 0L, 1L);
    assertDoubleColumn(builders.get(2).build(), 3.0, -1.0);
    assertDoubleColumn(builders.get(3).build(), 0.0, 0.0);
  }

  @Test
  public void testSupportsNonPowerOfTwoTransformLength() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(true, 3L);
    processor.process(record(0L, 1.0), Collections.emptyList(), null);
    processor.process(record(1L, 2.0), Collections.emptyList(), null);
    processor.process(record(2L, 3.0), Collections.emptyList(), null);

    List<ColumnBuilder> builders = createOutputBuilders(3);
    processor.finish(builders, null);

    double imaginaryComponent = Math.sqrt(3.0) / 2.0;
    assertLongColumn(builders.get(0).build(), 0L, 1L, 2L);
    assertDoubleColumn(builders.get(2).build(), 6.0, -1.5, -1.5);
    assertDoubleColumn(builders.get(3).build(), 0.0, imaginaryComponent, -imaginaryComponent);
  }

  @Test
  public void testSupportsFloatInputWithFloatFft() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(Type.FLOAT, true, 3L);
    processor.process(record(0L, 1.0f), Collections.emptyList(), null);
    processor.process(record(1L, 2.0f), Collections.emptyList(), null);
    processor.process(record(2L, 3.0f), Collections.emptyList(), null);

    List<ColumnBuilder> builders = createOutputBuilders(3);
    processor.finish(builders, null);

    double imaginaryComponent = Math.sqrt(3.0) / 2.0;
    assertLongColumn(builders.get(0).build(), 0L, 1L, 2L);
    assertDoubleColumnWithDelta(builders.get(2).build(), 1e-6, 6.0, -1.5, -1.5);
    assertDoubleColumnWithDelta(
        builders.get(3).build(), 1e-6, 0.0, imaginaryComponent, -imaginaryComponent);
  }

  @Test
  public void testRejectsInvalidRowsEvenWhenBeyondTruncatedN() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(true, 2L);
    processor.process(record(0L, 1.0), Collections.emptyList(), null);
    processor.process(record(1L, 2.0), Collections.emptyList(), null);

    assertSemanticException(
        () -> processor.process(nullValueRecord(2L), Collections.emptyList(), null),
        "FFT does not support null values in column [value].");
  }

  @Test
  public void testInfersSampleIntervalFromFullInputRangeBeyondSpecifiedN() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(false, 2L);
    processor.process(record(0L, 1.0), Collections.emptyList(), null);
    processor.process(record(1L, 2.0), Collections.emptyList(), null);
    processor.process(record(3L, 3.0), Collections.emptyList(), null);

    List<ColumnBuilder> builders = createOutputBuilders(2);
    processor.finish(builders, null);

    double intervalSeconds =
        1.5 * TimestampPrecisionUtils.currPrecision.toNanos(1L) / 1_000_000_000.0;
    assertLongColumn(builders.get(0).build(), 0L, 1L);
    assertDoubleColumn(builders.get(1).build(), 0.0, -1.0 / (2.0 * intervalSeconds));
    assertDoubleColumn(builders.get(2).build(), 3.0, -1.0);
    assertDoubleColumn(builders.get(3).build(), 0.0, 0.0);
  }

  @Test
  public void testRejectsDuplicateTime() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(false);
    processor.process(record(1L, 1.0), Collections.emptyList(), null);

    assertSemanticException(
        () -> processor.process(record(1L, 2.0), Collections.emptyList(), null),
        "The time column of FFT input must be strictly ascending within each partition.");
  }

  @Test
  public void testRejectsOutOfOrderTime() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(false);
    processor.process(record(2L, 1.0), Collections.emptyList(), null);

    assertSemanticException(
        () -> processor.process(record(1L, 2.0), Collections.emptyList(), null),
        "The time column of FFT input must be strictly ascending within each partition.");
  }

  @Test
  public void testRejectsSingleRowWithoutSampleInterval() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(false);
    processor.process(record(1L, 1.0), Collections.emptyList(), null);

    assertSemanticException(
        () -> processor.finish(Collections.emptyList(), null),
        "FFT requires at least two rows to infer SAMPLE_INTERVAL.");
  }

  @Test
  public void testInfersSampleIntervalFromPartitionTimeRange() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(false);
    processor.process(record(0L, 1.0), Collections.emptyList(), null);
    processor.process(record(1L, 2.0), Collections.emptyList(), null);
    processor.process(record(3L, 3.0), Collections.emptyList(), null);

    List<ColumnBuilder> builders = createOutputBuilders(3);
    processor.finish(builders, null);

    double intervalSeconds =
        1.5 * TimestampPrecisionUtils.currPrecision.toNanos(1L) / 1_000_000_000.0;
    assertLongColumn(builders.get(0).build(), 0L, 1L, 2L);
    assertDoubleColumn(
        builders.get(1).build(),
        0.0,
        1.0 / (3.0 * intervalSeconds),
        -1.0 / (3.0 * intervalSeconds));
  }

  @Test
  public void testUsesExplicitSampleIntervalWithoutGapValidation() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(true);
    processor.process(record(0L, 1.0), Collections.emptyList(), null);
    processor.process(record(2L, 2.0), Collections.emptyList(), null);

    List<ColumnBuilder> builders = createOutputBuilders(2);
    processor.finish(builders, null);

    double intervalSeconds = TimestampPrecisionUtils.currPrecision.toNanos(1L) / 1_000_000_000.0;
    assertLongColumn(builders.get(0).build(), 0L, 1L);
    assertDoubleColumn(builders.get(1).build(), 0.0, -1.0 / (2.0 * intervalSeconds));
  }

  @Test
  public void testRejectsDefaultTransformLengthAboveLimit() throws UDFException {
    TableFunctionDataProcessor processor = createProcessor(true);
    for (long time = 0; time < 65_536L; time++) {
      processor.process(record(time, 1.0), Collections.emptyList(), null);
    }

    assertSemanticException(
        () -> processor.process(record(65_536L, 1.0), Collections.emptyList(), null),
        "FFT transform length N must not exceed 65536.");
  }

  @Test
  public void testAnalyzeUsesSpecifiedTimeColumn() throws UDFException {
    Map<String, Argument> arguments = createArguments("event_time", "event_time");

    TableFunctionAnalysis analysis = function.analyze(arguments);

    assertEquals(
        Arrays.asList(0, 1),
        analysis.getRequiredColumns().get(FFTTableFunction.DATA_PARAMETER_NAME));
    assertEquals(
        "value_real", analysis.getProperColumnSchema().get().getFields().get(2).getName().get());
    assertEquals(
        "value_imag", analysis.getProperColumnSchema().get().getFields().get(3).getName().get());
  }

  @Test
  public void testAnalyzeRejectsOrderByDifferentFromSpecifiedTimeColumn() {
    assertSemanticException(
        () -> {
          try {
            function.analyze(createArguments("event_time", "time"));
          } catch (UDFException e) {
            throw new AssertionError(e);
          }
        },
        "The ORDER BY clause of the DATA argument must contain exactly the time column specified by the TIMECOL argument.");
  }

  private TableFunctionDataProcessor createProcessor(boolean sampleIntervalSpecified)
      throws UDFException {
    return createProcessor(sampleIntervalSpecified, -1L);
  }

  private TableFunctionDataProcessor createProcessor(
      boolean sampleIntervalSpecified, long transformLength) throws UDFException {
    return createProcessor(Type.DOUBLE, sampleIntervalSpecified, transformLength);
  }

  private TableFunctionDataProcessor createProcessor(
      Type valueType, boolean sampleIntervalSpecified, long transformLength) throws UDFException {
    Map<String, Argument> arguments = new HashMap<>();
    arguments.put(
        FFTTableFunction.DATA_PARAMETER_NAME,
        new TableArgument(
            Arrays.asList(Optional.of("time"), Optional.of("value")),
            Arrays.asList(Type.TIMESTAMP, valueType),
            Collections.emptyList(),
            Collections.singletonList("time"),
            false));
    arguments.put(FFTTableFunction.TIMECOL_PARAMETER_NAME, new ScalarArgument(Type.STRING, "time"));
    arguments.put(
        FFTTableFunction.SAMPLE_INTERVAL_PARAMETER_NAME,
        new ScalarArgument(Type.INT64, sampleIntervalSpecified ? 1L : Long.MIN_VALUE));
    arguments.put(
        FFTTableFunction.SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME,
        new ScalarArgument(Type.BOOLEAN, sampleIntervalSpecified));
    arguments.put(
        FFTTableFunction.N_PARAMETER_NAME, new ScalarArgument(Type.INT64, transformLength));
    arguments.put(
        FFTTableFunction.NORM_PARAMETER_NAME, new ScalarArgument(Type.STRING, "backward"));

    return function
        .getProcessorProvider(function.analyze(arguments).getTableFunctionHandle())
        .getDataProcessor();
  }

  private Map<String, Argument> createArguments(String timeColumn, String orderByColumn) {
    Map<String, Argument> arguments = new HashMap<>();
    arguments.put(
        FFTTableFunction.DATA_PARAMETER_NAME,
        new TableArgument(
            Arrays.asList(Optional.of(timeColumn), Optional.of("value")),
            Arrays.asList(Type.TIMESTAMP, Type.DOUBLE),
            Collections.emptyList(),
            Collections.singletonList(orderByColumn),
            false));
    arguments.put(
        FFTTableFunction.TIMECOL_PARAMETER_NAME, new ScalarArgument(Type.STRING, timeColumn));
    arguments.put(
        FFTTableFunction.SAMPLE_INTERVAL_PARAMETER_NAME, new ScalarArgument(Type.INT64, 1L));
    arguments.put(
        FFTTableFunction.SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME,
        new ScalarArgument(Type.BOOLEAN, true));
    arguments.put(FFTTableFunction.N_PARAMETER_NAME, new ScalarArgument(Type.INT64, -1L));
    arguments.put(
        FFTTableFunction.NORM_PARAMETER_NAME, new ScalarArgument(Type.STRING, "backward"));
    return arguments;
  }

  private Record record(long time, double value) {
    return new SimpleRecord(time, value);
  }

  private Record record(long time, float value) {
    return new SimpleRecord(time, value);
  }

  private Record nullValueRecord(long time) {
    return new SimpleRecord(time, null);
  }

  private List<ColumnBuilder> createOutputBuilders(int expectedPositionCount) {
    return Arrays.asList(
        new LongColumnBuilder(null, expectedPositionCount),
        new DoubleColumnBuilder(null, expectedPositionCount),
        new DoubleColumnBuilder(null, expectedPositionCount),
        new DoubleColumnBuilder(null, expectedPositionCount));
  }

  private void assertLongColumn(Column column, long... expected) {
    assertEquals(expected.length, column.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], column.getLong(i));
    }
  }

  private void assertDoubleColumn(Column column, double... expected) {
    assertDoubleColumnWithDelta(column, DELTA, expected);
  }

  private void assertDoubleColumnWithDelta(Column column, double delta, double... expected) {
    assertEquals(expected.length, column.getPositionCount());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], column.getDouble(i), delta);
    }
  }

  private void assertSemanticException(Runnable runnable, String message) {
    try {
      runnable.run();
      fail();
    } catch (SemanticException e) {
      assertEquals(message, e.getMessage());
    }
  }

  private static class SimpleRecord implements Record {
    private final long time;
    private final Number value;

    private SimpleRecord(long time, Number value) {
      this.time = time;
      this.value = value;
    }

    @Override
    public int getInt(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int columnIndex) {
      if (columnIndex == 0) {
        return time;
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int columnIndex) {
      if (columnIndex == 1 && value instanceof Float) {
        return value.floatValue();
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int columnIndex) {
      if (columnIndex == 1 && value instanceof Double) {
        return value.doubleValue();
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Binary getBinary(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate getLocalDate(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<File> getObjectFile(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long objectLength(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Binary readObject(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Binary readObject(int columnIndex, long offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Type getDataType(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int columnIndex) {
      if (columnIndex == 1) {
        return value == null;
      }
      return false;
    }

    @Override
    public int size() {
      return 2;
    }
  }
}
