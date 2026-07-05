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
import org.apache.tsfile.utils.Binary;

import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;
import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.POSITIVE_LONG_CHECKER;

public class M4TableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  public static final String SIZE_PARAMETER_NAME = "SIZE";
  public static final String SLIDE_PARAMETER_NAME = "SLIDE";
  public static final String ORIGIN_PARAMETER_NAME = "ORIGIN";
  public static final String WINDOW_MODE_PARAMETER_NAME = "__M4_WINDOW_MODE";

  private static final String OUTPUT_WINDOW_START_COLUMN = "window_start";
  private static final String OUTPUT_WINDOW_END_COLUMN = "window_end";
  private static final String OUTPUT_WINDOW_INDEX_COLUMN = "window_index";
  private static final String PARTITION_TYPES_PROPERTY = "__M4_PARTITION_TYPES";
  private static final String PARTICIPANT_TYPES_PROPERTY = "__M4_PARTICIPANT_TYPES";
  private static final long UNSPECIFIED_SLIDE = Long.MIN_VALUE;
  private static final long INVALID_INDEX = -1;
  private static final Set<Type> SUPPORTED_PARTITION_TYPES =
      new HashSet<>(
          Arrays.asList(
              Type.BOOLEAN,
              Type.INT32,
              Type.INT64,
              Type.FLOAT,
              Type.DOUBLE,
              Type.TEXT,
              Type.TIMESTAMP,
              Type.DATE,
              Type.BLOB,
              Type.STRING));

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
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
            .name(ORIGIN_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(0L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    if (tableArgument.getOrderBy().isEmpty()) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_TABLE_ARGUMENT_WITH_SET_SEMANTICS_REQUIRES_AN_ORDER_BY_CLAUSE_10C986D9);
    }

    String timeColumn =
        (String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue();
    int timeColumnIndex =
        findColumnIndex(tableArgument, timeColumn, Collections.singleton(Type.TIMESTAMP));
    validateOrderBy(tableArgument, timeColumn);

    List<Integer> partitionIndexes = getPartitionIndexes(tableArgument);
    Set<Integer> excludedIndexes = new HashSet<>(partitionIndexes);
    excludedIndexes.add(timeColumnIndex);

    boolean isTimeWindow =
        arguments.containsKey(WINDOW_MODE_PARAMETER_NAME)
            && (boolean) ((ScalarArgument) arguments.get(WINDOW_MODE_PARAMETER_NAME)).getValue();

    List<Integer> participantIndexes = new ArrayList<>();
    List<Type> partitionTypes = new ArrayList<>();
    List<Type> participantTypes = new ArrayList<>();
    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();
    if (isTimeWindow) {
      schemaBuilder
          .addField(OUTPUT_WINDOW_START_COLUMN, Type.TIMESTAMP)
          .addField(OUTPUT_WINDOW_END_COLUMN, Type.TIMESTAMP);
    } else {
      schemaBuilder.addField(OUTPUT_WINDOW_INDEX_COLUMN, Type.INT64);
    }
    for (int partitionIndex : partitionIndexes) {
      Type type = tableArgument.getFieldTypes().get(partitionIndex);
      partitionTypes.add(type);
      schemaBuilder.addField(tableArgument.getFieldNames().get(partitionIndex).get(), type);
    }

    for (int i = 0; i < tableArgument.getFieldTypes().size(); i++) {
      if (excludedIndexes.contains(i)) {
        continue;
      }
      Type type = tableArgument.getFieldTypes().get(i);
      String columnName = tableArgument.getFieldNames().get(i).get();
      if (!isComparableType(type)) {
        throw new SemanticException(
            String.format(
                CommonMessages.EXCEPTION_THE_TYPE_OF_THE_COLUMN_ARG_IS_NOT_COMPARABLE_E3098096,
                columnName));
      }
      participantIndexes.add(i);
      participantTypes.add(type);
      schemaBuilder.addField(columnName + "_time", Type.TIMESTAMP);
      schemaBuilder.addField(columnName, type);
    }

    if (participantIndexes.isEmpty()) {
      throw new SemanticException(
          CommonMessages.EXCEPTION_NO_COMPARABLE_COLUMNS_FOUND_FOR_M4_CALCULATION_4E5A3092);
    }

    long size = (long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue();
    long slide = (long) ((ScalarArgument) arguments.get(SLIDE_PARAMETER_NAME)).getValue();
    if (slide == UNSPECIFIED_SLIDE) {
      slide = size;
    } else if (slide <= 0) {
      throw new UDFException(
          CommonMessages
              .EXCEPTION_INVALID_SCALAR_ARGUMENT_SLIDE_SHOULD_BE_A_POSITIVE_VALUE_F019E091);
    }

    MapTableFunctionHandle.Builder handleBuilder =
        new MapTableFunctionHandle.Builder()
            .addProperty(WINDOW_MODE_PARAMETER_NAME, isTimeWindow)
            .addProperty(SIZE_PARAMETER_NAME, size)
            .addProperty(SLIDE_PARAMETER_NAME, slide)
            .addProperty(PARTITION_TYPES_PROPERTY, joinTypes(partitionTypes))
            .addProperty(PARTICIPANT_TYPES_PROPERTY, joinTypes(participantTypes));
    if (isTimeWindow) {
      handleBuilder.addProperty(
          ORIGIN_PARAMETER_NAME,
          ((ScalarArgument) arguments.get(ORIGIN_PARAMETER_NAME)).getValue());
    }
    MapTableFunctionHandle handle = handleBuilder.build();

    List<Integer> requiredColumns = new ArrayList<>();
    requiredColumns.add(timeColumnIndex);
    requiredColumns.addAll(partitionIndexes);
    requiredColumns.addAll(participantIndexes);

    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, requiredColumns)
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
    long origin = isTimeWindow ? (long) handle.getProperty(ORIGIN_PARAMETER_NAME) : 0L;
    Type[] partitionTypes = parseTypes((String) handle.getProperty(PARTITION_TYPES_PROPERTY));
    Type[] participantTypes = parseTypes((String) handle.getProperty(PARTICIPANT_TYPES_PROPERTY));

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        M4Column[] partitionColumns = createColumns(partitionTypes, 1);
        M4Column[] participantColumns = createColumns(participantTypes, partitionTypes.length + 1);
        return isTimeWindow
            ? new TimeWindowM4DataProcessor(
                size, slide, origin, partitionColumns, participantColumns)
            : new CountWindowM4DataProcessor(size, slide, partitionColumns, participantColumns);
      }
    };
  }

  private static void validateOrderBy(TableArgument tableArgument, String timeColumn) {
    if (tableArgument.getOrderBy().size() != 1
        || !tableArgument.getOrderBy().get(0).equalsIgnoreCase(timeColumn)) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_THE_ORDER_BY_CLAUSE_OF_THE_DATA_ARGUMENT_MUST_CONTAIN_EXACTLY_THE_TIME_COLUMN_SPECIFIED_BY_THE_TIMECOL_ARGUMENT_4375BAE9);
    }
  }

  private static List<Integer> getPartitionIndexes(TableArgument tableArgument) {
    List<Integer> indexes = new ArrayList<>();
    for (String partitionColumn : tableArgument.getPartitionBy()) {
      indexes.add(findColumnIndex(tableArgument, partitionColumn, SUPPORTED_PARTITION_TYPES));
    }
    return indexes;
  }

  // BLOB can be used as a partition column because M4 only needs to read/write it there
  private static boolean isComparableType(Type type) {
    return type != Type.BLOB && type != Type.OBJECT;
  }

  private static String joinTypes(List<Type> types) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < types.size(); i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(types.get(i).name());
    }
    return builder.toString();
  }

  private static Type[] parseTypes(String value) {
    if (value.isEmpty()) {
      return new Type[0];
    }
    String[] values = value.split(",");
    Type[] types = new Type[values.length];
    for (int i = 0; i < values.length; i++) {
      types[i] = Type.valueOf(values[i]);
    }
    return types;
  }

  private static M4Column[] createColumns(Type[] types, int firstInputIndex) {
    M4Column[] columns = new M4Column[types.length];
    for (int i = 0; i < types.length; i++) {
      columns[i] = new M4Column(firstInputIndex + i, ValueOperator.fromType(types[i]));
    }
    return columns;
  }

  private enum ValueOperator {
    BOOLEAN(Type.BOOLEAN) {
      @Override
      Object read(Record record, int index) {
        return record.getBoolean(index);
      }

      @Override
      int compare(Object left, Object right) {
        return Boolean.compare((Boolean) left, (Boolean) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeBoolean((Boolean) value);
      }
    },
    INT32(Type.INT32) {
      @Override
      Object read(Record record, int index) {
        return record.getInt(index);
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
      Object read(Record record, int index) {
        return record.getLong(index);
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
      Object read(Record record, int index) {
        return record.getFloat(index);
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
      Object read(Record record, int index) {
        return record.getDouble(index);
      }

      @Override
      int compare(Object left, Object right) {
        return Double.compare((Double) left, (Double) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeDouble((Double) value);
      }
    },
    TEXT(Type.TEXT) {
      @Override
      Object read(Record record, int index) {
        return record.getBinary(index);
      }

      @Override
      int compare(Object left, Object right) {
        return ((Binary) left).compareTo((Binary) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeBinary((Binary) value);
      }
    },
    BLOB(Type.BLOB) {
      @Override
      Object read(Record record, int index) {
        return record.getBinary(index);
      }

      @Override
      int compare(Object left, Object right) {
        return ((Binary) left).compareTo((Binary) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeBinary((Binary) value);
      }
    },
    TIMESTAMP(Type.TIMESTAMP) {
      @Override
      Object read(Record record, int index) {
        return record.getLong(index);
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
    DATE(Type.DATE) {
      @Override
      Object read(Record record, int index) {
        return record.getLocalDate(index);
      }

      @Override
      int compare(Object left, Object right) {
        return ((LocalDate) left).compareTo((LocalDate) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeObject(value);
      }
    },
    STRING(Type.STRING) {
      @Override
      Object read(Record record, int index) {
        return record.getBinary(index);
      }

      @Override
      int compare(Object left, Object right) {
        return ((Binary) left).compareTo((Binary) right);
      }

      @Override
      void write(ColumnBuilder builder, Object value) {
        builder.writeBinary((Binary) value);
      }
    };

    private final Type type;

    ValueOperator(Type type) {
      this.type = type;
    }

    abstract Object read(Record record, int index);

    abstract int compare(Object left, Object right);

    abstract void write(ColumnBuilder builder, Object value);

    static ValueOperator fromType(Type type) {
      for (ValueOperator valueOperator : values()) {
        if (valueOperator.type == type) {
          return valueOperator;
        }
      }
      throw new IllegalArgumentException(
          CommonMessages.EXCEPTION_UNSUPPORTED_M4_VALUE_TYPE_AF0EF286 + type);
    }
  }

  private static class M4Column {
    private final int inputIndex;
    private final ValueOperator valueOperator;

    private M4Column(int inputIndex, ValueOperator valueOperator) {
      this.inputIndex = inputIndex;
      this.valueOperator = valueOperator;
    }

    private boolean isNull(Record record) {
      return record.isNull(inputIndex);
    }

    private Object read(Record record) {
      return valueOperator.read(record, inputIndex);
    }

    private void write(ColumnBuilder builder, Object value) {
      valueOperator.write(builder, value);
    }

    private int compare(Object left, Object right) {
      return valueOperator.compare(left, right);
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

  private static class ColumnWindowState {
    private final Candidate first = new Candidate();
    private final Candidate last = new Candidate();
    private final Candidate bottom = new Candidate();
    private final Candidate top = new Candidate();

    private void update(long rowIndex, long time, Object value, M4Column column) {
      if (first.index == INVALID_INDEX) {
        first.set(rowIndex, time, value);
        last.set(rowIndex, time, value);
        bottom.set(rowIndex, time, value);
        top.set(rowIndex, time, value);
        return;
      }

      last.set(rowIndex, time, value);
      if (column.compare(value, bottom.value) < 0) {
        bottom.set(rowIndex, time, value);
      }
      if (column.compare(value, top.value) > 0) {
        top.set(rowIndex, time, value);
      }
    }

    private boolean hasOutput() {
      return first.index != INVALID_INDEX;
    }

    private List<Candidate> getSortedCandidates() {
      if (!hasOutput()) {
        return Collections.emptyList();
      }
      List<Candidate> candidates = new ArrayList<>(Arrays.asList(first, last, bottom, top));
      Set<Long> emittedTimestamps = new HashSet<>();
      candidates.removeIf(candidate -> !emittedTimestamps.add(candidate.time));
      candidates.sort(Comparator.comparingLong(candidate -> candidate.time));
      return candidates;
    }
  }

  private abstract static class WindowState {
    protected final ColumnWindowState[] columnStates;
    protected final Object[] partitionValues;

    private WindowState(int partitionColumnCount, int participantColumnCount) {
      partitionValues = new Object[partitionColumnCount];
      columnStates = new ColumnWindowState[participantColumnCount];
      for (int i = 0; i < participantColumnCount; i++) {
        columnStates[i] = new ColumnWindowState();
      }
    }

    protected abstract void writeWindowColumns(List<ColumnBuilder> properColumnBuilders);

    protected abstract int getWindowColumnCount();
  }

  private static class TimeWindowState extends WindowState {
    private final long windowStart;
    private final long endExclusive;

    private TimeWindowState(
        long windowStart, long endExclusive, int partitionColumnCount, int participantColumnCount) {
      super(partitionColumnCount, participantColumnCount);
      this.windowStart = windowStart;
      this.endExclusive = endExclusive;
    }

    @Override
    protected void writeWindowColumns(List<ColumnBuilder> properColumnBuilders) {
      properColumnBuilders.get(0).writeLong(windowStart);
      properColumnBuilders.get(1).writeLong(endExclusive);
    }

    @Override
    protected int getWindowColumnCount() {
      return 2;
    }
  }

  private static class CountWindowState extends WindowState {
    private final long endExclusive;
    private final long windowIndex;

    private CountWindowState(
        long endExclusive, long windowIndex, int partitionColumnCount, int participantColumnCount) {
      super(partitionColumnCount, participantColumnCount);
      this.endExclusive = endExclusive;
      this.windowIndex = windowIndex;
    }

    @Override
    protected void writeWindowColumns(List<ColumnBuilder> properColumnBuilders) {
      properColumnBuilders.get(0).writeLong(windowIndex);
    }

    @Override
    protected int getWindowColumnCount() {
      return 1;
    }
  }

  private abstract static class AbstractM4DataProcessor implements TableFunctionDataProcessor {
    protected final long size;
    protected final long slide;
    protected final M4Column[] partitionColumns;
    protected final M4Column[] participantColumns;
    protected long curIndex = 0;

    protected AbstractM4DataProcessor(
        long size, long slide, M4Column[] partitionColumns, M4Column[] participantColumns) {
      this.size = size;
      this.slide = slide;
      this.partitionColumns = partitionColumns;
      this.participantColumns = participantColumns;
    }

    @Override
    public final void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      processRecord(input, input.getLong(0), properColumnBuilders);
      curIndex++;
    }

    protected abstract void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders);

    protected final void updateWindow(WindowState windowState, Record input, long time) {
      for (int i = 0; i < partitionColumns.length; i++) {
        if (windowState.partitionValues[i] == null && !partitionColumns[i].isNull(input)) {
          windowState.partitionValues[i] = partitionColumns[i].read(input);
        }
      }
      for (int i = 0; i < participantColumns.length; i++) {
        if (!participantColumns[i].isNull(input)) {
          windowState.columnStates[i].update(
              curIndex, time, participantColumns[i].read(input), participantColumns[i]);
        }
      }
    }

    protected final void outputWindow(
        WindowState windowState, List<ColumnBuilder> properColumnBuilders) {
      List<List<Candidate>> candidatesByColumn = new ArrayList<>();
      int rowCount = 0;
      for (ColumnWindowState columnState : windowState.columnStates) {
        List<Candidate> candidates = columnState.getSortedCandidates();
        candidatesByColumn.add(candidates);
        rowCount = Math.max(rowCount, candidates.size());
      }
      if (rowCount == 0) {
        return;
      }

      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        windowState.writeWindowColumns(properColumnBuilders);
        int outputColumnIndex = windowState.getWindowColumnCount();
        for (int columnIndex = 0; columnIndex < partitionColumns.length; columnIndex++) {
          Object value = windowState.partitionValues[columnIndex];
          if (value == null) {
            properColumnBuilders.get(outputColumnIndex++).appendNull();
          } else {
            partitionColumns[columnIndex].write(
                properColumnBuilders.get(outputColumnIndex++), value);
          }
        }
        for (int columnIndex = 0; columnIndex < participantColumns.length; columnIndex++) {
          List<Candidate> candidates = candidatesByColumn.get(columnIndex);
          if (rowIndex < candidates.size()) {
            Candidate candidate = candidates.get(rowIndex);
            properColumnBuilders.get(outputColumnIndex++).writeLong(candidate.time);
            participantColumns[columnIndex].write(
                properColumnBuilders.get(outputColumnIndex++), candidate.value);
          } else {
            properColumnBuilders.get(outputColumnIndex++).appendNull();
            properColumnBuilders.get(outputColumnIndex++).appendNull();
          }
        }
      }
    }

    protected final long getWindowEnd(long windowStart) {
      return windowStart + size;
    }
  }

  private static class TimeWindowM4DataProcessor extends AbstractM4DataProcessor {
    private final Deque<TimeWindowState> activeWindows = new ArrayDeque<>();
    private final long origin;
    private boolean nextWindowStartInitialized = false;
    private long nextWindowStart;

    private TimeWindowM4DataProcessor(
        long size,
        long slide,
        long origin,
        M4Column[] partitionColumns,
        M4Column[] participantColumns) {
      super(size, slide, partitionColumns, participantColumns);
      this.origin = origin;
    }

    @Override
    protected void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders) {
      while (!activeWindows.isEmpty() && activeWindows.peekFirst().endExclusive <= time) {
        outputWindow(activeWindows.removeFirst(), properColumnBuilders);
      }

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
            new TimeWindowState(
                nextWindowStart,
                getWindowEnd(nextWindowStart),
                partitionColumns.length,
                participantColumns.length));
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
        outputWindow(activeWindows.removeFirst(), properColumnBuilders);
      }
    }
  }

  private static class CountWindowM4DataProcessor extends AbstractM4DataProcessor {
    private final Deque<CountWindowState> activeWindows = new ArrayDeque<>();
    private long rowCount = 0;
    private long nextWindowStart = 0;
    private long nextWindowIndex = 0;

    private CountWindowM4DataProcessor(
        long size, long slide, M4Column[] partitionColumns, M4Column[] participantColumns) {
      super(size, slide, partitionColumns, participantColumns);
    }

    @Override
    protected void processRecord(
        Record input, long time, List<ColumnBuilder> properColumnBuilders) {
      while (!activeWindows.isEmpty() && activeWindows.peekFirst().endExclusive <= rowCount) {
        outputWindow(activeWindows.removeFirst(), properColumnBuilders);
      }

      while (getWindowEnd(nextWindowStart) <= rowCount) {
        nextWindowStart += slide;
      }

      while (nextWindowStart <= rowCount) {
        activeWindows.addLast(
            new CountWindowState(
                getWindowEnd(nextWindowStart),
                nextWindowIndex++,
                partitionColumns.length,
                participantColumns.length));
        nextWindowStart += slide;
      }

      for (CountWindowState activeWindow : activeWindows) {
        updateWindow(activeWindow, input, time);
      }
      rowCount++;
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      while (!activeWindows.isEmpty()) {
        outputWindow(activeWindows.removeFirst(), properColumnBuilders);
      }
    }
  }
}
