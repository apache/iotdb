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

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;
import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.POSITIVE_LONG_CHECKER;

public class M4TableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  public static final String VALUECOL_PARAMETER_NAME = "VALUECOL";
  public static final String SIZE_PARAMETER_NAME = "SIZE";
  public static final String SLIDE_PARAMETER_NAME = "SLIDE";
  public static final String DISPLAYBEGIN_PARAMETER_NAME = "DISPLAYBEGIN";
  public static final String DISPLAYEND_PARAMETER_NAME = "DISPLAYEND";
  public static final String WINDOW_MODE_PARAMETER_NAME = "__M4_WINDOW_MODE";

  private static final String OUTPUT_WINDOW_START_COLUMN = "window_start";
  private static final String OUTPUT_WINDOW_END_COLUMN = "window_end";
  private static final String OUTPUT_TIME_COLUMN = "m4_time";
  private static final String OUTPUT_VALUE_COLUMN = "m4_value";
  private static final String VALUE_TYPE_PROPERTY = "__M4_VALUE_TYPE";
  private static final long UNSPECIFIED_SLIDE = Long.MIN_VALUE;
  private static final long UNSPECIFIED_DISPLAY_BEGIN = Long.MIN_VALUE;
  private static final long INVALID_INDEX = -1;

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .setSemantics()
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(VALUECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SIZE_PARAMETER_NAME)
            .type(Type.INT64)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SLIDE_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_SLIDE)
            .build(),
        ScalarParameterSpecification.builder()
            .name(DISPLAYBEGIN_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(UNSPECIFIED_DISPLAY_BEGIN)
            .build(),
        ScalarParameterSpecification.builder()
            .name(DISPLAYEND_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(Long.MAX_VALUE)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    if (tableArgument.getOrderBy().isEmpty()) {
      throw new SemanticException("Table argument with set semantics requires an ORDER BY clause.");
    }

    String timeColumn =
        (String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue();
    String valueColumn =
        (String) ((ScalarArgument) arguments.get(VALUECOL_PARAMETER_NAME)).getValue();
    int timeColumnIndex =
        findColumnIndex(tableArgument, timeColumn, Collections.singleton(Type.TIMESTAMP));
    int valueColumnIndex =
        findColumnIndex(tableArgument, valueColumn, ImmutableSet.copyOf(Type.numericTypes()));

    long size = (long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue();
    long slide = (long) ((ScalarArgument) arguments.get(SLIDE_PARAMETER_NAME)).getValue();
    if (slide == UNSPECIFIED_SLIDE) {
      slide = size;
    } else if (slide <= 0) {
      throw new UDFException("Invalid scalar argument SLIDE, should be a positive value");
    }

    Type valueType = tableArgument.getFieldTypes().get(valueColumnIndex);
    boolean isTimeWindow =
        arguments.containsKey(WINDOW_MODE_PARAMETER_NAME)
            && (boolean) ((ScalarArgument) arguments.get(WINDOW_MODE_PARAMETER_NAME)).getValue();

    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(WINDOW_MODE_PARAMETER_NAME, isTimeWindow)
            .addProperty(SIZE_PARAMETER_NAME, size)
            .addProperty(SLIDE_PARAMETER_NAME, slide)
            .addProperty(
                DISPLAYBEGIN_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(DISPLAYBEGIN_PARAMETER_NAME)).getValue())
            .addProperty(
                DISPLAYEND_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(DISPLAYEND_PARAMETER_NAME)).getValue())
            .addProperty(VALUE_TYPE_PROPERTY, valueType.name())
            .build();

    return TableFunctionAnalysis.builder()
        .properColumnSchema(
            new DescribedSchema.Builder()
                .addField(OUTPUT_WINDOW_START_COLUMN, Type.TIMESTAMP)
                .addField(OUTPUT_WINDOW_END_COLUMN, Type.TIMESTAMP)
                .addField(OUTPUT_TIME_COLUMN, Type.TIMESTAMP)
                .addField(OUTPUT_VALUE_COLUMN, valueType)
                .build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, Arrays.asList(timeColumnIndex, valueColumnIndex))
        .handle(handle)
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
    boolean isTimeWindow = (boolean) handle.getProperty(WINDOW_MODE_PARAMETER_NAME);
    long size = (long) handle.getProperty(SIZE_PARAMETER_NAME);
    long slide = (long) handle.getProperty(SLIDE_PARAMETER_NAME);
    long displayBegin = (long) handle.getProperty(DISPLAYBEGIN_PARAMETER_NAME);
    long displayEnd = (long) handle.getProperty(DISPLAYEND_PARAMETER_NAME);
    ValueOperator valueOperator =
        ValueOperator.fromType(Type.valueOf((String) handle.getProperty(VALUE_TYPE_PROPERTY)));

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return isTimeWindow
            ? new TimeWindowM4DataProcessor(valueOperator, size, slide, displayBegin, displayEnd)
            : new CountWindowM4DataProcessor(valueOperator, size, slide, displayBegin, displayEnd);
      }
    };
  }

  private enum ValueOperator {
    INT32(Type.INT32) {
      @Override
      Object read(Record record) {
        return record.getInt(1);
      }

      @Override
      int compare(Object left, Object right) {
        return Integer.compare((Integer) left, (Integer) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeInt((Integer) value);
      }
    },
    INT64(Type.INT64) {
      @Override
      Object read(Record record) {
        return record.getLong(1);
      }

      @Override
      int compare(Object left, Object right) {
        return Long.compare((Long) left, (Long) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeLong((Long) value);
      }
    },
    FLOAT(Type.FLOAT) {
      @Override
      Object read(Record record) {
        return record.getFloat(1);
      }

      @Override
      int compare(Object left, Object right) {
        return Float.compare((Float) left, (Float) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeFloat((Float) value);
      }
    },
    DOUBLE(Type.DOUBLE) {
      @Override
      Object read(Record record) {
        return record.getDouble(1);
      }

      @Override
      int compare(Object left, Object right) {
        return Double.compare((Double) left, (Double) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeDouble((Double) value);
      }
    };

    private final Type type;

    ValueOperator(Type type) {
      this.type = type;
    }

    abstract Object read(Record record);

    abstract int compare(Object left, Object right);

    abstract void write(ColumnBuilder builder, Object value);

    static ValueOperator fromType(Type type) {
      for (ValueOperator valueOperator : values()) {
        if (valueOperator.type == type) {
          return valueOperator;
        }
      }
      throw new IllegalArgumentException("Unsupported M4 value type: " + type);
    }
  }

  private static class Candidate {
    private long index = INVALID_INDEX;
    private long time;
    private Object value;

    private void set(long index, long time, Object value) {
      this.index = index;
      this.time = time;
      this.value = value;
    }
  }

  private abstract static class WindowState {
    protected final Candidate first = new Candidate();
    protected final Candidate last = new Candidate();
    protected final Candidate bottom = new Candidate();
    protected final Candidate top = new Candidate();

    private void update(long rowIndex, long time, Object value, ValueOperator valueOperator) {
      if (first.index == INVALID_INDEX) {
        first.set(rowIndex, time, value);
        last.set(rowIndex, time, value);
        bottom.set(rowIndex, time, value);
        top.set(rowIndex, time, value);
        return;
      }

      last.set(rowIndex, time, value);
      if (valueOperator.compare(value, bottom.value) < 0) {
        bottom.set(rowIndex, time, value);
      }
      if (valueOperator.compare(value, top.value) > 0) {
        top.set(rowIndex, time, value);
      }
    }

    private boolean hasOutput() {
      return first.index != INVALID_INDEX;
    }

    protected abstract long getOutputWindowStart();

    protected abstract long getOutputWindowEnd();
  }

  private static class TimeWindowState extends WindowState {
    private final long windowStart;
    private final long endExclusive;

    private TimeWindowState(long windowStart, long endExclusive) {
      this.windowStart = windowStart;
      this.endExclusive = endExclusive;
    }

    @Override
    protected long getOutputWindowStart() {
      return windowStart;
    }

    @Override
    protected long getOutputWindowEnd() {
      return endExclusive;
    }
  }

  private static class CountWindowState extends WindowState {
    private final long endExclusive;
    private long windowStart = Long.MIN_VALUE;
    private long windowEnd = Long.MIN_VALUE;

    private CountWindowState(long endExclusive) {
      this.endExclusive = endExclusive;
    }

    @Override
    protected long getOutputWindowStart() {
      return windowStart;
    }

    @Override
    protected long getOutputWindowEnd() {
      return windowEnd;
    }
  }

  private abstract static class AbstractM4DataProcessor implements TableFunctionDataProcessor {
    protected final ValueOperator valueOperator;
    protected final long size;
    protected final long slide;
    protected final long displayBegin;
    protected final long displayEnd;

    protected long curIndex = 0;
    protected boolean reachedDisplayEnd = false;

    protected AbstractM4DataProcessor(
        ValueOperator valueOperator, long size, long slide, long displayBegin, long displayEnd) {
      this.valueOperator = valueOperator;
      this.size = size;
      this.slide = slide;
      this.displayBegin = displayBegin;
      this.displayEnd = displayEnd;
    }

    @Override
    public final void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      long time = input.getLong(0);
      if (reachedDisplayEnd || time >= displayEnd) {
        reachedDisplayEnd = true;
        curIndex++;
        return;
      }
      if (displayBegin != UNSPECIFIED_DISPLAY_BEGIN && time < displayBegin) {
        curIndex++;
        return;
      }

      processFilteredRecord(input, time, properColumnBuilders, passThroughIndexBuilder);
      curIndex++;
    }

    protected abstract void processFilteredRecord(
        Record input,
        long time,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder);

    protected final void updateWindow(WindowState windowState, Record input, long time) {
      if (input.isNull(1)) {
        return;
      }
      windowState.update(curIndex, time, valueOperator.read(input), valueOperator);
    }

    protected final void outputWindow(
        WindowState windowState,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if (!windowState.hasOutput()) {
        return;
      }

      Candidate[] candidates =
          new Candidate[] {
            windowState.first, windowState.bottom, windowState.top, windowState.last
          };
      Arrays.sort(candidates, Comparator.comparingLong(candidate -> candidate.index));

      Set<Long> emittedTimestamps = new LinkedHashSet<>();
      for (Candidate candidate : candidates) {
        if (!emittedTimestamps.add(candidate.time)) {
          continue;
        }
        properColumnBuilders.get(0).writeLong(windowState.getOutputWindowStart());
        properColumnBuilders.get(1).writeLong(windowState.getOutputWindowEnd());
        properColumnBuilders.get(2).writeLong(candidate.time);
        valueOperator.write(properColumnBuilders.get(3), candidate.value);
        passThroughIndexBuilder.writeLong(candidate.index);
      }
    }

    protected final long alignWindowStart(long time) {
      return Math.floorDiv(time, slide) * slide;
    }

    protected final long getWindowEnd(long windowStart) {
      return windowStart + size;
    }
  }

  private static class TimeWindowM4DataProcessor extends AbstractM4DataProcessor {
    private final Deque<TimeWindowState> activeWindows = new ArrayDeque<>();

    private boolean nextWindowStartInitialized = false;
    private long nextWindowStart;

    private TimeWindowM4DataProcessor(
        ValueOperator valueOperator, long size, long slide, long displayBegin, long displayEnd) {
      super(valueOperator, size, slide, displayBegin, displayEnd);
    }

    @Override
    protected void processFilteredRecord(
        Record input,
        long time,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if (!nextWindowStartInitialized) {
        nextWindowStart =
            displayBegin == UNSPECIFIED_DISPLAY_BEGIN ? alignWindowStart(time) : displayBegin;
        nextWindowStartInitialized = true;
      }

      while (!activeWindows.isEmpty() && activeWindows.peekFirst().endExclusive <= time) {
        outputWindow(activeWindows.removeFirst(), properColumnBuilders, passThroughIndexBuilder);
      }

      if (activeWindows.isEmpty() && getWindowEnd(nextWindowStart) <= time) {
        long skipBase = time - size - nextWindowStart;
        long skipCount = Math.floorDiv(skipBase, slide) + 1;
        nextWindowStart += skipCount * slide;
      }

      while (nextWindowStart <= time && nextWindowStart < displayEnd) {
        activeWindows.addLast(new TimeWindowState(nextWindowStart, getWindowEnd(nextWindowStart)));
        nextWindowStart += slide;
      }

      for (TimeWindowState activeWindow : activeWindows) {
        updateWindow(activeWindow, input, time);
      }
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      while (!activeWindows.isEmpty()) {
        outputWindow(activeWindows.removeFirst(), properColumnBuilders, passThroughIndexBuilder);
      }
    }
  }

  private static class CountWindowM4DataProcessor extends AbstractM4DataProcessor {
    private final Deque<CountWindowState> activeWindows = new ArrayDeque<>();

    private long filteredRowCount = 0;
    private long nextWindowStart = 0;

    private CountWindowM4DataProcessor(
        ValueOperator valueOperator, long size, long slide, long displayBegin, long displayEnd) {
      super(valueOperator, size, slide, displayBegin, displayEnd);
    }

    @Override
    protected void processFilteredRecord(
        Record input,
        long time,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      while (!activeWindows.isEmpty()
          && activeWindows.peekFirst().endExclusive <= filteredRowCount) {
        outputWindow(activeWindows.removeFirst(), properColumnBuilders, passThroughIndexBuilder);
      }

      if (activeWindows.isEmpty() && getWindowEnd(nextWindowStart) <= filteredRowCount) {
        long skipBase = filteredRowCount - size - nextWindowStart;
        long skipCount = Math.floorDiv(skipBase, slide) + 1;
        nextWindowStart += skipCount * slide;
      }

      while (nextWindowStart <= filteredRowCount) {
        activeWindows.addLast(new CountWindowState(getWindowEnd(nextWindowStart)));
        nextWindowStart += slide;
      }

      for (CountWindowState activeWindow : activeWindows) {
        updateCountWindow(activeWindow, input, time);
      }
      filteredRowCount++;
    }

    private void updateCountWindow(CountWindowState windowState, Record input, long time) {
      if (windowState.windowStart == Long.MIN_VALUE) {
        windowState.windowStart = time;
      }
      windowState.windowEnd = time + 1;
      if (input.isNull(1)) {
        return;
      }
      updateWindow(windowState, input, time);
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      while (!activeWindows.isEmpty()) {
        outputWindow(activeWindows.removeFirst(), properColumnBuilders, passThroughIndexBuilder);
      }
    }
  }
}
