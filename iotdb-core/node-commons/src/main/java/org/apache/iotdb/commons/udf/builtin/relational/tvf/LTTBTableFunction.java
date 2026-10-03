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
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.POSITIVE_LONG_CHECKER;

/**
 * Largest-Triangle-Three-Buckets (LTTB) downsampling as a built-in Table Model table function.
 *
 * <p>The function shares its table-argument conventions with {@link M4TableFunction}: {@code DATA}
 * has set semantics, {@code TIMECOL} names the {@code TIMESTAMP} ordering column, partition columns
 * are echoed into the proper output, and every other numeric column is a participant column that is
 * downsampled independently. Two mutually exclusive execution modes exist:
 *
 * <ul>
 *   <li><b>Target-count mode</b> ({@code N}): the whole partition is buffered and the standard LTTB
 *       algorithm reduces each participant column to exactly {@code N} points (or all points when
 *       the column has no more than {@code N} eligible points). The output uses a fixed {@code
 *       window_index = 0}.
 *   <li><b>Window/bucket mode</b> ({@code SIZE}, optional {@code SLIDE}/{@code ORIGIN}): windows
 *       are built exactly like M4 (time windows for duration literals, count windows for integers).
 *       Each window is one LTTB bucket that selects at most one point per participant column: the
 *       anchor is the previously selected point of that column, the candidates are the eligible
 *       points in the window, and the third vertex is the average point of the next window that
 *       carries data for that column (or the window's own last eligible point for the final
 *       window).
 * </ul>
 *
 * <p>{@code NULL} and non-finite participant values are ignored per column. Ties in the triangle
 * area are broken deterministically by the earliest timestamp (stable input order).
 */
public class LTTBTableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  public static final String N_PARAMETER_NAME = "N";
  public static final String SIZE_PARAMETER_NAME = "SIZE";
  public static final String SLIDE_PARAMETER_NAME = "SLIDE";
  public static final String ORIGIN_PARAMETER_NAME = "ORIGIN";

  /** Internal boolean argument injected by the analyzer: {@code true} if SIZE is a duration. */
  public static final String WINDOW_MODE_PARAMETER_NAME = "__LTTB_WINDOW_MODE";

  public static final long UNSPECIFIED_N = Long.MIN_VALUE;
  public static final long UNSPECIFIED_SIZE = Long.MIN_VALUE;
  public static final long UNSPECIFIED_SLIDE = Long.MIN_VALUE;
  public static final long MIN_TARGET_COUNT = 3L;

  /** Hard cap on eligible points buffered per participant column in one partition or window. */
  public static final int MAX_BUFFERED_POINTS = 1 << 22;

  public static final String MODE_PROPERTY = "__LTTB_MODE";
  public static final String PARTITION_TYPES_PROPERTY = "__LTTB_PARTITION_TYPES";
  public static final String PARTICIPANT_TYPES_PROPERTY = "__LTTB_PARTICIPANT_TYPES";

  private static final String OUTPUT_WINDOW_START_COLUMN = "window_start";
  private static final String OUTPUT_WINDOW_END_COLUMN = "window_end";
  private static final String OUTPUT_WINDOW_INDEX_COLUMN = "window_index";
  private static final String TIME_SUFFIX = "_time";
  private static final int INITIAL_CAPACITY = 64;
  private static final int NOT_SELECTED = -1;

  /** Execution mode persisted in the {@link MapTableFunctionHandle}. */
  public enum Mode {
    TARGET_COUNT,
    TIME_WINDOW,
    COUNT_WINDOW
  }

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(N_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_N)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SIZE_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_SIZE)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SLIDE_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_SLIDE)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(ORIGIN_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(0L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    int timeColumnIndex =
        WindowTVFUtils.checkOrderByColumn(arguments, DATA_PARAMETER_NAME, TIMECOL_PARAMETER_NAME);
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    List<Integer> partitionIndexes = WindowTVFUtils.getPartitionIndexes(tableArgument);
    Set<Integer> excludedIndexes = new HashSet<>(partitionIndexes);
    excludedIndexes.add(timeColumnIndex);

    long n = getLong(arguments, N_PARAMETER_NAME);
    long size = getLong(arguments, SIZE_PARAMETER_NAME);
    long slide = getLong(arguments, SLIDE_PARAMETER_NAME);
    boolean nSpecified = n != UNSPECIFIED_N;
    boolean sizeSpecified = size != UNSPECIFIED_SIZE;
    boolean slideSpecified = slide != UNSPECIFIED_SLIDE;
    if (nSpecified == sizeSpecified) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_EXACTLY_ONE_OF_THE_N_AND_SIZE_ARGUMENTS_MUST_BE_SPECIFIED_FOR_LTTB_54AF0733);
    }

    Mode mode;
    if (nSpecified) {
      if (slideSpecified) {
        throw new SemanticException(
            CommonMessages
                .EXCEPTION_THE_N_ARGUMENT_OF_LTTB_CANNOT_BE_COMBINED_WITH_THE_SLIDE_OR_ORIGIN_ARGUMENTS_C2FF1FE1);
      }
      if (n < MIN_TARGET_COUNT) {
        throw new SemanticException(
            CommonMessages.EXCEPTION_THE_N_ARGUMENT_OF_LTTB_MUST_BE_AT_LEAST_3_29CE2B87);
      }
      mode = Mode.TARGET_COUNT;
    } else {
      if (size <= 0) {
        throw new UDFException(
            CommonMessages
                .EXCEPTION_INVALID_SCALAR_ARGUMENT_SIZE_SHOULD_BE_A_POSITIVE_VALUE_3ECF76E0);
      }
      if (!slideSpecified) {
        slide = size;
      } else if (slide <= 0) {
        throw new UDFException(
            CommonMessages
                .EXCEPTION_INVALID_SCALAR_ARGUMENT_SLIDE_SHOULD_BE_A_POSITIVE_VALUE_F019E091);
      }
      boolean isTimeWindow =
          arguments.containsKey(WINDOW_MODE_PARAMETER_NAME)
              && (boolean) ((ScalarArgument) arguments.get(WINDOW_MODE_PARAMETER_NAME)).getValue();
      mode = isTimeWindow ? Mode.TIME_WINDOW : Mode.COUNT_WINDOW;
    }

    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();
    if (mode == Mode.TIME_WINDOW) {
      schemaBuilder
          .addField(OUTPUT_WINDOW_START_COLUMN, Type.TIMESTAMP)
          .addField(OUTPUT_WINDOW_END_COLUMN, Type.TIMESTAMP);
    } else {
      schemaBuilder.addField(OUTPUT_WINDOW_INDEX_COLUMN, Type.INT64);
    }

    List<Type> partitionTypes = new ArrayList<>();
    for (int partitionIndex : partitionIndexes) {
      Type type = tableArgument.getFieldTypes().get(partitionIndex);
      partitionTypes.add(type);
      schemaBuilder.addField(tableArgument.getFieldNames().get(partitionIndex).get(), type);
    }

    // Rejects every non-numeric participant column with the shared "not allowed columns" message.
    List<Integer> participantIndexes =
        WindowTVFUtils.getCalculationIndexes(tableArgument, excludedIndexes, null);
    if (participantIndexes.isEmpty()) {
      throw new SemanticException(CommonMessages.EXCEPTION_NO_CALCULATE_COLUMNS);
    }
    List<Type> participantTypes = new ArrayList<>();
    for (int participantIndex : participantIndexes) {
      Type type = tableArgument.getFieldTypes().get(participantIndex);
      String columnName = tableArgument.getFieldNames().get(participantIndex).get();
      participantTypes.add(type);
      schemaBuilder.addField(columnName + TIME_SUFFIX, Type.TIMESTAMP);
      schemaBuilder.addField(columnName, type);
    }

    MapTableFunctionHandle.Builder handleBuilder =
        new MapTableFunctionHandle.Builder()
            .addProperty(MODE_PROPERTY, mode.name())
            .addProperty(PARTITION_TYPES_PROPERTY, WindowTVFUtils.joinTypes(partitionTypes))
            .addProperty(PARTICIPANT_TYPES_PROPERTY, WindowTVFUtils.joinTypes(participantTypes));
    if (mode == Mode.TARGET_COUNT) {
      handleBuilder.addProperty(N_PARAMETER_NAME, n);
    } else {
      handleBuilder.addProperty(SIZE_PARAMETER_NAME, size).addProperty(SLIDE_PARAMETER_NAME, slide);
      if (mode == Mode.TIME_WINDOW) {
        handleBuilder.addProperty(ORIGIN_PARAMETER_NAME, getLong(arguments, ORIGIN_PARAMETER_NAME));
      }
    }

    List<Integer> requiredColumns = new ArrayList<>();
    requiredColumns.add(timeColumnIndex);
    requiredColumns.addAll(partitionIndexes);
    requiredColumns.addAll(participantIndexes);

    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, requiredColumns)
        .handle(handleBuilder.build())
        .build();
  }

  private static long getLong(Map<String, Argument> arguments, String name) {
    return (long) ((ScalarArgument) arguments.get(name)).getValue();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
    Mode mode = Mode.valueOf((String) handle.getProperty(MODE_PROPERTY));
    Type[] partitionTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(PARTITION_TYPES_PROPERTY));
    Type[] participantTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(PARTICIPANT_TYPES_PROPERTY));

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        ParticipantColumn[] participantColumns =
            createParticipantColumns(participantTypes, partitionTypes.length + 1);
        switch (mode) {
          case TARGET_COUNT:
            return new TargetCountLTTBDataProcessor(
                (long) handle.getProperty(N_PARAMETER_NAME), partitionTypes, participantColumns);
          case TIME_WINDOW:
            return new TimeWindowLTTBDataProcessor(
                (long) handle.getProperty(SIZE_PARAMETER_NAME),
                (long) handle.getProperty(SLIDE_PARAMETER_NAME),
                (long) handle.getProperty(ORIGIN_PARAMETER_NAME),
                partitionTypes,
                participantColumns);
          case COUNT_WINDOW:
            return new CountWindowLTTBDataProcessor(
                (long) handle.getProperty(SIZE_PARAMETER_NAME),
                (long) handle.getProperty(SLIDE_PARAMETER_NAME),
                partitionTypes,
                participantColumns);
          default:
            throw new IllegalStateException(String.valueOf(mode));
        }
      }
    };
  }

  private static ParticipantColumn[] createParticipantColumns(Type[] types, int firstInputIndex) {
    ParticipantColumn[] columns = new ParticipantColumn[types.length];
    for (int i = 0; i < types.length; i++) {
      columns[i] = new ParticipantColumn(firstInputIndex + i, types[i]);
    }
    return columns;
  }

  // ---------------------------------------------------------------------------------------------
  // LTTB core
  // ---------------------------------------------------------------------------------------------

  /**
   * Standard LTTB selection over an ordered point sequence.
   *
   * <p>When {@code count <= n} every index is returned. Otherwise the first and last points are
   * retained and the {@code count - 2} intermediate points are divided into {@code n - 2} buckets;
   * bucket {@code i} covers {@code [floor(i*(count-2)/(n-2))+1, floor((i+1)*(count-2)/(n-2))+1)}.
   * For every bucket the candidate forming the largest triangle with the previously selected point
   * and the average point of the next bucket wins (the last bucket uses the final point). Ties are
   * broken by the earliest timestamp.
   *
   * @return ascending indexes of the selected points
   */
  static int[] selectTargetCount(long[] times, double[] values, int count, long n) {
    if (count <= n) {
      int[] all = new int[count];
      for (int i = 0; i < count; i++) {
        all[i] = i;
      }
      return all;
    }

    int[] selected = new int[(int) n];
    int selectedCount = 0;
    selected[selectedCount++] = 0;
    long bucketCount = n - 2;
    long intermediate = count - 2L;
    int anchor = 0;
    for (long bucket = 0; bucket < bucketCount; bucket++) {
      int currentStart = (int) (bucket * intermediate / bucketCount) + 1;
      int currentEnd = (int) ((bucket + 1) * intermediate / bucketCount) + 1;

      double avgTime;
      double avgValue;
      if (bucket == bucketCount - 1) {
        avgTime = times[count - 1];
        avgValue = values[count - 1];
      } else {
        int nextEnd = (int) ((bucket + 2) * intermediate / bucketCount) + 1;
        double sumTime = 0;
        double sumValue = 0;
        for (int i = currentEnd; i < nextEnd; i++) {
          sumTime += times[i];
          sumValue += values[i];
        }
        int nextSize = nextEnd - currentEnd;
        avgTime = sumTime / nextSize;
        avgValue = sumValue / nextSize;
      }

      int pick =
          argMaxArea(
              times,
              values,
              currentStart,
              currentEnd,
              times[anchor],
              values[anchor],
              avgTime,
              avgValue);
      selected[selectedCount++] = pick;
      anchor = pick;
    }
    selected[selectedCount] = count - 1;
    return selected;
  }

  /** Returns the index in {@code [start, end)} maximizing the triangle area; earliest wins ties. */
  static int argMaxArea(
      long[] times,
      double[] values,
      int start,
      int end,
      double anchorTime,
      double anchorValue,
      double nextTime,
      double nextValue) {
    int pick = start;
    double maxArea = -1;
    for (int i = start; i < end; i++) {
      double area =
          Math.abs(
              (anchorTime - nextTime) * (values[i] - anchorValue)
                  - (anchorTime - times[i]) * (nextValue - anchorValue));
      if (area > maxArea) {
        maxArea = area;
        pick = i;
      }
    }
    return pick;
  }

  // ---------------------------------------------------------------------------------------------
  // Column access and buffering
  // ---------------------------------------------------------------------------------------------

  private static final class ParticipantColumn {
    private final int inputIndex;
    private final Type type;

    private ParticipantColumn(int inputIndex, Type type) {
      this.inputIndex = inputIndex;
      this.type = type;
    }

    private boolean isIntegral() {
      return type == Type.INT32 || type == Type.INT64;
    }

    /** Appends the record's value to {@code buffer} if it is a non-null finite number. */
    private void collect(Record record, long time, PointBuffer buffer) {
      if (record.isNull(inputIndex)) {
        return;
      }
      switch (type) {
        case INT32:
          buffer.addIntegral(time, record.getInt(inputIndex));
          break;
        case INT64:
          buffer.addIntegral(time, record.getLong(inputIndex));
          break;
        case FLOAT:
          float floatValue = record.getFloat(inputIndex);
          if (Float.isFinite(floatValue)) {
            buffer.addFloating(time, floatValue);
          }
          break;
        case DOUBLE:
          double doubleValue = record.getDouble(inputIndex);
          if (Double.isFinite(doubleValue)) {
            buffer.addFloating(time, doubleValue);
          }
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  CommonMessages.EXCEPTION_UNSUPPORTED_LTTB_VALUE_TYPE_ARG_3E60F6FE, type));
      }
    }

    private void write(
        ColumnBuilder timeBuilder, ColumnBuilder valueBuilder, PointBuffer buffer, int index) {
      timeBuilder.writeLong(buffer.times[index]);
      switch (type) {
        case INT32:
          valueBuilder.writeInt((int) buffer.integralValues[index]);
          break;
        case INT64:
          valueBuilder.writeLong(buffer.integralValues[index]);
          break;
        case FLOAT:
          valueBuilder.writeFloat((float) buffer.values[index]);
          break;
        case DOUBLE:
          valueBuilder.writeDouble(buffer.values[index]);
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  CommonMessages.EXCEPTION_UNSUPPORTED_LTTB_VALUE_TYPE_ARG_3E60F6FE, type));
      }
    }
  }

  /**
   * Growable buffer of eligible points for one participant column. Integral columns additionally
   * keep the exact {@code long} value so INT64 output is not rounded through {@code double}.
   */
  static final class PointBuffer {
    private long[] times = new long[INITIAL_CAPACITY];
    private double[] values = new double[INITIAL_CAPACITY];
    private long[] integralValues;
    private int size;

    PointBuffer(boolean integral) {
      if (integral) {
        integralValues = new long[INITIAL_CAPACITY];
      }
    }

    void addFloating(long time, double value) {
      ensureCapacity();
      times[size] = time;
      values[size] = value;
      size++;
    }

    void addIntegral(long time, long value) {
      ensureCapacity();
      times[size] = time;
      values[size] = value;
      integralValues[size] = value;
      size++;
    }

    private void ensureCapacity() {
      if (size < times.length) {
        return;
      }
      if (size >= MAX_BUFFERED_POINTS) {
        throw new SemanticException(
            String.format(
                CommonMessages
                    .EXCEPTION_LTTB_BUFFERS_AT_MOST_ARG_ELIGIBLE_POINTS_PER_COLUMN_IN_ONE_PARTITION_OR_WINDOW_4C746D82,
                MAX_BUFFERED_POINTS));
      }
      int newCapacity = (int) Math.min((long) times.length * 2, MAX_BUFFERED_POINTS);
      times = Arrays.copyOf(times, newCapacity);
      values = Arrays.copyOf(values, newCapacity);
      if (integralValues != null) {
        integralValues = Arrays.copyOf(integralValues, newCapacity);
      }
    }

    int size() {
      return size;
    }

    boolean isEmpty() {
      return size == 0;
    }

    long time(int index) {
      return times[index];
    }

    double value(int index) {
      return values[index];
    }

    double averageTime() {
      double sum = 0;
      for (int i = 0; i < size; i++) {
        sum += times[i];
      }
      return sum / size;
    }

    double averageValue() {
      double sum = 0;
      for (int i = 0; i < size; i++) {
        sum += values[i];
      }
      return sum / size;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Processors
  // ---------------------------------------------------------------------------------------------

  private abstract static class AbstractLTTBDataProcessor implements TableFunctionDataProcessor {
    protected final Type[] partitionTypes;
    protected final Object[] partitionValues;
    protected final ParticipantColumn[] participantColumns;

    protected AbstractLTTBDataProcessor(
        Type[] partitionTypes, ParticipantColumn[] participantColumns) {
      this.partitionTypes = partitionTypes;
      this.partitionValues = new Object[partitionTypes.length];
      this.participantColumns = participantColumns;
    }

    protected final PointBuffer[] newBuffers() {
      PointBuffer[] buffers = new PointBuffer[participantColumns.length];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = new PointBuffer(participantColumns[i].isIntegral());
      }
      return buffers;
    }

    /** Partition columns are constant within a partition; keep the first non-null value. */
    protected final void capturePartitionValues(Record input) {
      for (int i = 0; i < partitionTypes.length; i++) {
        int inputIndex = i + 1;
        if (partitionValues[i] == null && !input.isNull(inputIndex)) {
          partitionValues[i] = WindowTVFUtils.readValue(input, inputIndex, partitionTypes[i]);
        }
      }
    }

    protected final void collect(Record input, long time, PointBuffer[] buffers) {
      for (int i = 0; i < participantColumns.length; i++) {
        participantColumns[i].collect(input, time, buffers[i]);
      }
    }

    /**
     * Writes one output row. {@code windowColumnCount} leading builders were already filled by the
     * caller; {@code selectedIndexes[i]} is {@link #NOT_SELECTED} when column {@code i} has no
     * point.
     */
    protected final void writeRow(
        List<ColumnBuilder> builders,
        int windowColumnCount,
        PointBuffer[] buffers,
        int[] selectedIndexes) {
      int builderIndex = windowColumnCount;
      for (int i = 0; i < partitionTypes.length; i++) {
        WindowTVFUtils.writeValue(
            builders.get(builderIndex++), partitionValues[i], partitionTypes[i]);
      }
      for (int i = 0; i < participantColumns.length; i++) {
        ColumnBuilder timeBuilder = builders.get(builderIndex++);
        ColumnBuilder valueBuilder = builders.get(builderIndex++);
        if (selectedIndexes[i] == NOT_SELECTED) {
          timeBuilder.appendNull();
          valueBuilder.appendNull();
        } else {
          participantColumns[i].write(timeBuilder, valueBuilder, buffers[i], selectedIndexes[i]);
        }
      }
    }
  }

  /** Buffers the whole partition and applies the standard LTTB algorithm in {@link #finish}. */
  private static final class TargetCountLTTBDataProcessor extends AbstractLTTBDataProcessor {
    private final long n;
    private final PointBuffer[] buffers;
    private boolean hasRows;

    private TargetCountLTTBDataProcessor(
        long n, Type[] partitionTypes, ParticipantColumn[] participantColumns) {
      super(partitionTypes, participantColumns);
      this.n = n;
      this.buffers = newBuffers();
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      hasRows = true;
      capturePartitionValues(input);
      collect(input, input.getLong(0), buffers);
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      if (!hasRows) {
        return;
      }
      int[][] selections = new int[participantColumns.length][];
      int rowCount = 1;
      for (int i = 0; i < participantColumns.length; i++) {
        PointBuffer buffer = buffers[i];
        selections[i] = selectTargetCount(buffer.times, buffer.values, buffer.size, n);
        rowCount = Math.max(rowCount, selections[i].length);
      }

      int[] selectedIndexes = new int[participantColumns.length];
      for (int row = 0; row < rowCount; row++) {
        properColumnBuilders.get(0).writeLong(0L);
        for (int i = 0; i < participantColumns.length; i++) {
          selectedIndexes[i] = row < selections[i].length ? selections[i][row] : NOT_SELECTED;
        }
        writeRow(properColumnBuilders, 1, buffers, selectedIndexes);
      }
    }
  }

  /** One window (= one LTTB bucket) worth of eligible points. */
  private abstract static class WindowState {
    protected final PointBuffer[] buffers;
    protected final long endExclusive;

    private WindowState(PointBuffer[] buffers, long endExclusive) {
      this.buffers = buffers;
      this.endExclusive = endExclusive;
    }

    protected abstract void writeWindowColumns(List<ColumnBuilder> builders);

    protected abstract int getWindowColumnCount();
  }

  private static final class TimeWindowState extends WindowState {
    private final long windowStart;

    private TimeWindowState(PointBuffer[] buffers, long windowStart, long endExclusive) {
      super(buffers, endExclusive);
      this.windowStart = windowStart;
    }

    @Override
    protected void writeWindowColumns(List<ColumnBuilder> builders) {
      builders.get(0).writeLong(windowStart);
      builders.get(1).writeLong(endExclusive);
    }

    @Override
    protected int getWindowColumnCount() {
      return 2;
    }
  }

  private static final class CountWindowState extends WindowState {
    private final long windowIndex;

    private CountWindowState(PointBuffer[] buffers, long endExclusive, long windowIndex) {
      super(buffers, endExclusive);
      this.windowIndex = windowIndex;
    }

    @Override
    protected void writeWindowColumns(List<ColumnBuilder> builders) {
      builders.get(0).writeLong(windowIndex);
    }

    @Override
    protected int getWindowColumnCount() {
      return 1;
    }
  }

  /**
   * Window/bucket mode. Windows are constructed like M4 and emitted in window order; a window is
   * emitted only once the following window with data is closed (or in {@link #finish}), because the
   * average point of that following window is the third vertex of the LTTB triangle. Anchors are
   * chained per participant column across the windows of the partition; windows without data for a
   * column leave its anchor untouched.
   */
  private abstract static class AbstractWindowLTTBDataProcessor<W extends WindowState>
      extends AbstractLTTBDataProcessor {
    protected final long size;
    protected final long slide;
    protected final Deque<W> activeWindows = new ArrayDeque<>();
    private final boolean[] hasAnchor;
    private final long[] anchorTimes;
    private final double[] anchorValues;

    protected AbstractWindowLTTBDataProcessor(
        long size, long slide, Type[] partitionTypes, ParticipantColumn[] participantColumns) {
      super(partitionTypes, participantColumns);
      this.size = size;
      this.slide = slide;
      this.hasAnchor = new boolean[participantColumns.length];
      this.anchorTimes = new long[participantColumns.length];
      this.anchorValues = new double[participantColumns.length];
    }

    @Override
    public final void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      capturePartitionValues(input);
      processRecord(input, input.getLong(0), properColumnBuilders);
    }

    protected abstract void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders);

    /** Emits every window whose successor is already closed at {@code position}. */
    protected final void emitClosedWindows(long position, List<ColumnBuilder> builders) {
      while (activeWindows.size() >= 2) {
        W current = activeWindows.peekFirst();
        W next = nextOf(current);
        if (next.endExclusive > position) {
          return;
        }
        activeWindows.removeFirst();
        emitWindow(current, next, builders);
      }
    }

    private W nextOf(W first) {
      Iterator<W> iterator = activeWindows.iterator();
      iterator.next();
      return iterator.next();
    }

    protected final long getWindowEnd(long windowStart) {
      return windowStart + size;
    }

    @Override
    public final void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      while (!activeWindows.isEmpty()) {
        W current = activeWindows.removeFirst();
        emitWindow(current, activeWindows.peekFirst(), properColumnBuilders);
      }
    }

    private void emitWindow(W current, W next, List<ColumnBuilder> builders) {
      int[] selectedIndexes = new int[participantColumns.length];
      for (int i = 0; i < participantColumns.length; i++) {
        PointBuffer bucket = current.buffers[i];
        if (bucket.isEmpty()) {
          selectedIndexes[i] = NOT_SELECTED;
          continue;
        }
        if (!hasAnchor[i]) {
          // The first eligible point of the partition seeds the anchor, as in standard LTTB.
          hasAnchor[i] = true;
          anchorTimes[i] = bucket.time(0);
          anchorValues[i] = bucket.value(0);
        }
        double nextTime;
        double nextValue;
        if (next != null && !next.buffers[i].isEmpty()) {
          nextTime = next.buffers[i].averageTime();
          nextValue = next.buffers[i].averageValue();
        } else {
          int last = bucket.size() - 1;
          nextTime = bucket.time(last);
          nextValue = bucket.value(last);
        }
        int pick =
            argMaxArea(
                bucket.times,
                bucket.values,
                0,
                bucket.size(),
                anchorTimes[i],
                anchorValues[i],
                nextTime,
                nextValue);
        selectedIndexes[i] = pick;
        anchorTimes[i] = bucket.time(pick);
        anchorValues[i] = bucket.value(pick);
      }
      current.writeWindowColumns(builders);
      writeRow(builders, current.getWindowColumnCount(), current.buffers, selectedIndexes);
    }
  }

  private static final class TimeWindowLTTBDataProcessor
      extends AbstractWindowLTTBDataProcessor<TimeWindowState> {
    private final long origin;
    private boolean nextWindowStartInitialized = false;
    private long nextWindowStart;

    private TimeWindowLTTBDataProcessor(
        long size,
        long slide,
        long origin,
        Type[] partitionTypes,
        ParticipantColumn[] participantColumns) {
      super(size, slide, partitionTypes, participantColumns);
      this.origin = origin;
    }

    @Override
    protected void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders) {
      emitClosedWindows(time, properColumnBuilders);

      long firstCandidateStart =
          origin + Math.floorDiv(time - origin - size, slide) * slide + slide;
      while (getWindowEnd(firstCandidateStart) <= time) {
        firstCandidateStart += slide;
      }
      if (!nextWindowStartInitialized) {
        nextWindowStart = firstCandidateStart;
        nextWindowStartInitialized = true;
      } else if (nextWindowStart < firstCandidateStart) {
        nextWindowStart = firstCandidateStart;
      }

      while (nextWindowStart <= time && getWindowEnd(nextWindowStart) > time) {
        activeWindows.addLast(
            new TimeWindowState(newBuffers(), nextWindowStart, getWindowEnd(nextWindowStart)));
        nextWindowStart += slide;
      }

      for (TimeWindowState window : activeWindows) {
        // Closed windows waiting for their successor stay in the deque but must not absorb rows.
        if (time < window.endExclusive) {
          collect(input, time, window.buffers);
        }
      }
    }
  }

  private static final class CountWindowLTTBDataProcessor
      extends AbstractWindowLTTBDataProcessor<CountWindowState> {
    private long rowCount = 0;
    private long nextWindowStart = 0;
    private long nextWindowIndex = 0;

    private CountWindowLTTBDataProcessor(
        long size, long slide, Type[] partitionTypes, ParticipantColumn[] participantColumns) {
      super(size, slide, partitionTypes, participantColumns);
    }

    @Override
    protected void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders) {
      emitClosedWindows(rowCount, properColumnBuilders);

      while (getWindowEnd(nextWindowStart) <= rowCount) {
        nextWindowStart += slide;
      }
      while (nextWindowStart <= rowCount) {
        activeWindows.addLast(
            new CountWindowState(newBuffers(), getWindowEnd(nextWindowStart), nextWindowIndex++));
        nextWindowStart += slide;
      }

      for (CountWindowState window : activeWindows) {
        if (rowCount < window.endExclusive) {
          collect(input, time, window.buffers);
        }
      }
      rowCount++;
    }
  }
}
