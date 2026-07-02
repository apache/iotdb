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
import org.jtransforms.fft.DoubleFFT_1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;
import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.POSITIVE_LONG_CHECKER;

public class FFTTableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String SAMPLE_INTERVAL_PARAMETER_NAME = "SAMPLE_INTERVAL";
  public static final String N_PARAMETER_NAME = "N";
  public static final String NORM_PARAMETER_NAME = "NORM";
  public static final String SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME =
      "__FFT_SAMPLE_INTERVAL_SPECIFIED";

  private static final String TIME_COLUMN_NAME = "time";
  private static final String OUTPUT_FREQUENCY_INDEX_COLUMN = "frequency_index";
  private static final String OUTPUT_FREQUENCY_COLUMN = "frequency";
  private static final String PARTITION_TYPES_PROPERTY = "__FFT_PARTITION_TYPES";
  private static final String VALUE_TYPES_PROPERTY = "__FFT_VALUE_TYPES";
  private static final String VALUE_NAMES_PROPERTY = "__FFT_VALUE_NAMES";
  private static final long UNSPECIFIED_SAMPLE_INTERVAL = Long.MIN_VALUE;
  private static final long UNSPECIFIED_N = -1L;
  private static final long MAX_TRANSFORM_LENGTH = 65_536L;
  private static final long MAX_SPECTRUM_DOUBLE_VALUES = 16_777_216L;
  private static final String NORM_BACKWARD = "backward";
  private static final String NORM_FORWARD = "forward";
  private static final String NORM_ORTHO = "ortho";
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
  private static final Set<Type> SUPPORTED_VALUE_TYPES =
      new HashSet<>(Arrays.asList(Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE));

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(SAMPLE_INTERVAL_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_SAMPLE_INTERVAL)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(N_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(UNSPECIFIED_N)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(NORM_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(NORM_BACKWARD)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    if (tableArgument.getOrderBy().isEmpty()) {
      throw new SemanticException("Table argument with set semantics requires an ORDER BY clause.");
    }

    int timeColumnIndex =
        findColumnIndex(tableArgument, TIME_COLUMN_NAME, Collections.singleton(Type.TIMESTAMP));
    validateOrderBy(tableArgument);

    List<Integer> partitionIndexes = getPartitionIndexes(tableArgument);
    Set<Integer> excludedIndexes = new HashSet<>(partitionIndexes);
    excludedIndexes.add(timeColumnIndex);

    List<Integer> valueIndexes = new ArrayList<>();
    List<String> valueNames = new ArrayList<>();
    List<Type> valueTypes = new ArrayList<>();
    List<Type> partitionTypes = new ArrayList<>();
    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();

    for (int partitionIndex : partitionIndexes) {
      Type type = tableArgument.getFieldTypes().get(partitionIndex);
      partitionTypes.add(type);
      schemaBuilder.addField(tableArgument.getFieldNames().get(partitionIndex).get(), type);
    }
    schemaBuilder
        .addField(OUTPUT_FREQUENCY_INDEX_COLUMN, Type.INT64)
        .addField(OUTPUT_FREQUENCY_COLUMN, Type.DOUBLE);

    for (int i = 0; i < tableArgument.getFieldTypes().size(); i++) {
      if (excludedIndexes.contains(i)) {
        continue;
      }
      Type type = tableArgument.getFieldTypes().get(i);
      if (!SUPPORTED_VALUE_TYPES.contains(type)) {
        continue;
      }
      String columnName =
          tableArgument
              .getFieldNames()
              .get(i)
              .orElseThrow(
                  () -> new SemanticException("FFT requires named numeric input columns."));
      valueIndexes.add(i);
      valueNames.add(columnName);
      valueTypes.add(type);
      schemaBuilder.addField(columnName + "_real", Type.DOUBLE);
      schemaBuilder.addField(columnName + "_imag", Type.DOUBLE);
    }

    if (valueIndexes.isEmpty()) {
      throw new SemanticException("No numeric columns found for FFT calculation.");
    }

    long transformLength = (long) ((ScalarArgument) arguments.get(N_PARAMETER_NAME)).getValue();
    validateTransformLength(transformLength, valueIndexes.size());
    String norm =
        ((String) ((ScalarArgument) arguments.get(NORM_PARAMETER_NAME)).getValue())
            .toLowerCase(Locale.ROOT);
    validateNorm(norm);

    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                SAMPLE_INTERVAL_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(SAMPLE_INTERVAL_PARAMETER_NAME)).getValue())
            .addProperty(
                SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME,
                (boolean)
                    ((ScalarArgument) arguments.get(SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME))
                        .getValue())
            .addProperty(N_PARAMETER_NAME, transformLength)
            .addProperty(NORM_PARAMETER_NAME, norm)
            .addProperty(PARTITION_TYPES_PROPERTY, joinTypes(partitionTypes))
            .addProperty(VALUE_TYPES_PROPERTY, joinTypes(valueTypes))
            .addProperty(VALUE_NAMES_PROPERTY, joinStrings(valueNames))
            .build();

    List<Integer> requiredColumns = new ArrayList<>();
    requiredColumns.add(timeColumnIndex);
    requiredColumns.addAll(partitionIndexes);
    requiredColumns.addAll(valueIndexes);

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
    boolean sampleIntervalSpecified =
        (boolean) handle.getProperty(SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME);
    long sampleInterval = (long) handle.getProperty(SAMPLE_INTERVAL_PARAMETER_NAME);
    long transformLength = (long) handle.getProperty(N_PARAMETER_NAME);
    String norm = (String) handle.getProperty(NORM_PARAMETER_NAME);
    Type[] partitionTypes = parseTypes((String) handle.getProperty(PARTITION_TYPES_PROPERTY));
    Type[] valueTypes = parseTypes((String) handle.getProperty(VALUE_TYPES_PROPERTY));
    String[] valueNames = splitStrings((String) handle.getProperty(VALUE_NAMES_PROPERTY));

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new FFTDataProcessor(
            sampleIntervalSpecified,
            sampleInterval,
            transformLength,
            norm,
            createColumns(partitionTypes, 1),
            createNumericColumns(valueTypes, valueNames, partitionTypes.length + 1));
      }
    };
  }

  private static void validateOrderBy(TableArgument tableArgument) {
    if (tableArgument.getOrderBy().size() != 1
        || !tableArgument.getOrderBy().get(0).equalsIgnoreCase(TIME_COLUMN_NAME)) {
      throw new SemanticException(
          "The ORDER BY clause of the DATA argument must contain exactly the time column.");
    }
  }

  private static List<Integer> getPartitionIndexes(TableArgument tableArgument)
      throws UDFException {
    List<Integer> indexes = new ArrayList<>();
    for (String partitionColumn : tableArgument.getPartitionBy()) {
      indexes.add(findColumnIndex(tableArgument, partitionColumn, SUPPORTED_PARTITION_TYPES));
    }
    return indexes;
  }

  public static void validateTransformLength(long transformLength, int valueColumnCount) {
    if (transformLength == UNSPECIFIED_N) {
      return;
    }
    if (transformLength > MAX_TRANSFORM_LENGTH) {
      throw new SemanticException(
          String.format("FFT transform length N must not exceed %d.", MAX_TRANSFORM_LENGTH));
    }
    long spectrumDoubleValues;
    try {
      spectrumDoubleValues =
          Math.multiplyExact(Math.multiplyExact(transformLength, 2L), valueColumnCount);
    } catch (ArithmeticException e) {
      throw new SemanticException(
          "FFT spectrum buffer is too large. Reduce N or the number of numeric columns.");
    }
    if (spectrumDoubleValues > MAX_SPECTRUM_DOUBLE_VALUES) {
      throw new SemanticException(
          "FFT spectrum buffer is too large. Reduce N or the number of numeric columns.");
    }
  }

  private static void validateNorm(String norm) {
    if (!NORM_BACKWARD.equals(norm) && !NORM_FORWARD.equals(norm) && !NORM_ORTHO.equals(norm)) {
      throw new SemanticException(
          "Invalid NORM value for FFT. Supported values are backward, forward and ortho.");
    }
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

  private static String joinStrings(List<String> values) {
    return String.join(",", values);
  }

  private static String[] splitStrings(String value) {
    if (value.isEmpty()) {
      return new String[0];
    }
    return value.split(",");
  }

  private static ValueColumn[] createColumns(Type[] types, int firstInputIndex) {
    ValueColumn[] columns = new ValueColumn[types.length];
    for (int i = 0; i < types.length; i++) {
      columns[i] = new ValueColumn(firstInputIndex + i, ValueOperator.fromType(types[i]));
    }
    return columns;
  }

  private static NumericColumn[] createNumericColumns(
      Type[] types, String[] names, int firstInputIndex) {
    NumericColumn[] columns = new NumericColumn[types.length];
    for (int i = 0; i < types.length; i++) {
      columns[i] =
          new NumericColumn(firstInputIndex + i, names[i], NumericOperator.fromType(types[i]));
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
      void write(ColumnBuilder builder, Object value) {
        builder.writeBinary((Binary) value);
      }
    };

    private final Type type;

    ValueOperator(Type type) {
      this.type = type;
    }

    abstract Object read(Record record, int index);

    abstract void write(ColumnBuilder builder, Object value);

    static ValueOperator fromType(Type type) {
      for (ValueOperator valueOperator : values()) {
        if (valueOperator.type == type) {
          return valueOperator;
        }
      }
      throw new IllegalArgumentException("Unsupported FFT partition type: " + type);
    }
  }

  private enum NumericOperator {
    INT32(Type.INT32) {
      @Override
      double read(Record record, int index) {
        return record.getInt(index);
      }
    },
    INT64(Type.INT64) {
      @Override
      double read(Record record, int index) {
        return record.getLong(index);
      }
    },
    FLOAT(Type.FLOAT) {
      @Override
      double read(Record record, int index) {
        return record.getFloat(index);
      }
    },
    DOUBLE(Type.DOUBLE) {
      @Override
      double read(Record record, int index) {
        return record.getDouble(index);
      }
    };

    private final Type type;

    NumericOperator(Type type) {
      this.type = type;
    }

    abstract double read(Record record, int index);

    static NumericOperator fromType(Type type) {
      for (NumericOperator numericOperator : values()) {
        if (numericOperator.type == type) {
          return numericOperator;
        }
      }
      throw new IllegalArgumentException("Unsupported FFT value type: " + type);
    }
  }

  private static class ValueColumn {
    private final int inputIndex;
    private final ValueOperator valueOperator;

    private ValueColumn(int inputIndex, ValueOperator valueOperator) {
      this.inputIndex = inputIndex;
      this.valueOperator = valueOperator;
    }

    private Object read(Record record) {
      return valueOperator.read(record, inputIndex);
    }

    private void write(ColumnBuilder builder, Object value) {
      valueOperator.write(builder, value);
    }
  }

  private static class NumericColumn {
    private final int inputIndex;
    private final String name;
    private final NumericOperator numericOperator;

    private NumericColumn(int inputIndex, String name, NumericOperator numericOperator) {
      this.inputIndex = inputIndex;
      this.name = name;
      this.numericOperator = numericOperator;
    }

    private double read(Record record) {
      return numericOperator.read(record, inputIndex);
    }
  }

  private static class FFTDataProcessor implements TableFunctionDataProcessor {
    private final boolean sampleIntervalSpecified;
    private final long sampleInterval;
    private final long specifiedTransformLength;
    private final String norm;
    private final ValueColumn[] partitionColumns;
    private final NumericColumn[] valueColumns;
    private final Object[] partitionValues;
    private final boolean[] partitionValueIsNull;
    private final List<double[]> rows = new ArrayList<>();
    private long inputRowCount;
    private long previousTime;
    private long inferredSampleInterval = UNSPECIFIED_SAMPLE_INTERVAL;
    private boolean initialized;

    private FFTDataProcessor(
        boolean sampleIntervalSpecified,
        long sampleInterval,
        long specifiedTransformLength,
        String norm,
        ValueColumn[] partitionColumns,
        NumericColumn[] valueColumns) {
      this.sampleIntervalSpecified = sampleIntervalSpecified;
      this.sampleInterval = sampleInterval;
      this.specifiedTransformLength = specifiedTransformLength;
      this.norm = norm;
      this.partitionColumns = partitionColumns;
      this.valueColumns = valueColumns;
      this.partitionValues = new Object[partitionColumns.length];
      this.partitionValueIsNull = new boolean[partitionColumns.length];
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      long currentTime = input.getLong(0);
      if (!initialized) {
        capturePartitionValues(input);
        initialized = true;
      } else if (currentTime <= previousTime) {
        throw new SemanticException(
            "The time column of FFT input must be strictly ascending within each partition.");
      } else {
        validateSampleInterval(currentTime - previousTime);
      }
      previousTime = currentTime;

      if (specifiedTransformLength == UNSPECIFIED_N && inputRowCount >= MAX_TRANSFORM_LENGTH) {
        throw new SemanticException(
            String.format("FFT transform length N must not exceed %d.", MAX_TRANSFORM_LENGTH));
      }

      boolean shouldCacheRow =
          specifiedTransformLength == UNSPECIFIED_N || rows.size() < specifiedTransformLength;
      double[] row = shouldCacheRow ? new double[valueColumns.length] : null;
      for (int i = 0; i < valueColumns.length; i++) {
        NumericColumn valueColumn = valueColumns[i];
        if (input.isNull(valueColumn.inputIndex)) {
          throw new SemanticException(
              String.format("FFT does not support null values in column [%s].", valueColumn.name));
        }
        if (shouldCacheRow) {
          row[i] = valueColumn.read(input);
        }
      }
      inputRowCount++;
      if (shouldCacheRow) {
        rows.add(row);
      }
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      if (inputRowCount == 0) {
        return;
      }

      int transformLength = getTransformLength();
      double sampleIntervalSeconds = getSampleIntervalSeconds();
      double scaleFactor = getScaleFactor(transformLength);
      double[][] spectra = new double[valueColumns.length][2 * transformLength];

      int copiedRows = Math.min(rows.size(), transformLength);
      DoubleFFT_1D fft = new DoubleFFT_1D(transformLength);
      for (int columnIndex = 0; columnIndex < valueColumns.length; columnIndex++) {
        double[] spectrum = spectra[columnIndex];
        for (int rowIndex = 0; rowIndex < copiedRows; rowIndex++) {
          spectrum[2 * rowIndex] = rows.get(rowIndex)[columnIndex];
        }
        fft.complexForward(spectrum);
      }

      for (int frequencyIndex = 0; frequencyIndex < transformLength; frequencyIndex++) {
        int outputColumnIndex = 0;
        for (int partitionIndex = 0; partitionIndex < partitionColumns.length; partitionIndex++) {
          if (partitionValueIsNull[partitionIndex]) {
            properColumnBuilders.get(outputColumnIndex++).appendNull();
          } else {
            partitionColumns[partitionIndex].write(
                properColumnBuilders.get(outputColumnIndex++), partitionValues[partitionIndex]);
          }
        }
        properColumnBuilders.get(outputColumnIndex++).writeLong(frequencyIndex);
        properColumnBuilders
            .get(outputColumnIndex++)
            .writeDouble(
                calculateFrequency(frequencyIndex, transformLength, sampleIntervalSeconds));
        for (int columnIndex = 0; columnIndex < valueColumns.length; columnIndex++) {
          double[] spectrum = spectra[columnIndex];
          properColumnBuilders
              .get(outputColumnIndex++)
              .writeDouble(spectrum[2 * frequencyIndex] * scaleFactor);
          properColumnBuilders
              .get(outputColumnIndex++)
              .writeDouble(spectrum[2 * frequencyIndex + 1] * scaleFactor);
        }
      }
    }

    private void capturePartitionValues(Record input) {
      for (int i = 0; i < partitionColumns.length; i++) {
        if (input.isNull(partitionColumns[i].inputIndex)) {
          partitionValueIsNull[i] = true;
        } else {
          partitionValues[i] = partitionColumns[i].read(input);
        }
      }
    }

    private int getTransformLength() {
      long transformLength =
          specifiedTransformLength == UNSPECIFIED_N ? inputRowCount : specifiedTransformLength;
      validateTransformLength(transformLength, valueColumns.length);
      return (int) transformLength;
    }

    private double getSampleIntervalSeconds() {
      long interval;
      if (sampleIntervalSpecified) {
        interval = sampleInterval;
      } else {
        if (inputRowCount < 2) {
          throw new SemanticException("FFT requires at least two rows to infer SAMPLE_INTERVAL.");
        }
        interval = inferredSampleInterval;
      }
      double intervalSeconds =
          interval * TimestampPrecisionUtils.currPrecision.toNanos(1L) / 1_000_000_000.0;
      if (intervalSeconds <= 0) {
        throw new SemanticException("FFT SAMPLE_INTERVAL must be positive.");
      }
      return intervalSeconds;
    }

    private void validateSampleInterval(long currentInterval) {
      if (sampleIntervalSpecified) {
        if (currentInterval != sampleInterval) {
          throw new SemanticException(
              "FFT input time interval must match the specified SAMPLE_INTERVAL.");
        }
        return;
      }

      if (inferredSampleInterval == UNSPECIFIED_SAMPLE_INTERVAL) {
        inferredSampleInterval = currentInterval;
      } else if (currentInterval != inferredSampleInterval) {
        throw new SemanticException(
            "FFT requires evenly spaced input time values when SAMPLE_INTERVAL is not specified.");
      }
    }

    private double getScaleFactor(int transformLength) {
      if (NORM_FORWARD.equals(norm)) {
        return 1.0 / transformLength;
      }
      if (NORM_ORTHO.equals(norm)) {
        return 1.0 / Math.sqrt(transformLength);
      }
      return 1.0;
    }

    private double calculateFrequency(
        int frequencyIndex, int transformLength, double sampleIntervalSeconds) {
      int positiveFrequencyCount = (transformLength + 1) / 2;
      int signedIndex =
          frequencyIndex < positiveFrequencyCount
              ? frequencyIndex
              : frequencyIndex - transformLength;
      return signedIndex / (transformLength * sampleIntervalSeconds);
    }
  }
}
