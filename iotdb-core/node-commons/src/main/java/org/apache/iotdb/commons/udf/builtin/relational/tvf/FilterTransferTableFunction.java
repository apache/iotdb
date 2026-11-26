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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.i18n.CommonMessages.EXCEPTION_FILTER_FUNCTION_WPASS_VALIDATION;

public abstract class FilterTransferTableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  public static final String WPASS = "WPASS";

  protected static final String PARTITION_TYPES_PROPERTY = "PARTITION_TYPES";
  protected static final String CALCULATION_COLUMN_COUNT_PROPERTY = "CALCULATION_COLUMN_COUNT";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .build(),
        ScalarParameterSpecification.builder()
            .name(WPASS)
            .type(Type.DOUBLE)
            .addChecker(
                object -> {
                  if (object instanceof Number) {
                    double value = ((Number) object).doubleValue();
                    if (value > 0 && value < 1) {
                      return null;
                    }
                  }
                  return EXCEPTION_FILTER_FUNCTION_WPASS_VALIDATION;
                })
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {

    // order by column must only be the time column
    int timeColumnIndex =
        WindowTVFUtils.checkOrderByColumn(arguments, DATA_PARAMETER_NAME, TIMECOL_PARAMETER_NAME);
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);

    List<Integer> partitionIndexes = WindowTVFUtils.getPartitionIndexes(tableArgument);
    Set<Integer> excludedIndexes = new HashSet<>(partitionIndexes);
    excludedIndexes.add(timeColumnIndex);

    List<Type> partitionTypes = new ArrayList<>();
    List<Integer> calculationIndexes = new ArrayList<>();
    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();

    // record the partition columns
    for (int partitionIndex : partitionIndexes) {
      Type type = tableArgument.getFieldTypes().get(partitionIndex);
      partitionTypes.add(type);
      schemaBuilder.addField(tableArgument.getFieldNames().get(partitionIndex).get(), type);
    }

    // record the time column
    schemaBuilder.addField(tableArgument.getFieldNames().get(timeColumnIndex), Type.TIMESTAMP);
    // record the calculation columns, only double, float, int32, and int64 are allowed
    calculationIndexes.addAll(
        WindowTVFUtils.getCalculationIndexes(
            tableArgument,
            excludedIndexes,
            columnName -> schemaBuilder.addField(convertColumnName(columnName), Type.DOUBLE)));

    if (calculationIndexes.isEmpty()) {
      throw new SemanticException(CommonMessages.EXCEPTION_NO_CALCULATE_COLUMNS);
    }

    MapTableFunctionHandle.Builder handleBuilder =
        new MapTableFunctionHandle.Builder()
            .addProperty(PARTITION_TYPES_PROPERTY, WindowTVFUtils.joinTypes(partitionTypes))
            .addProperty(CALCULATION_COLUMN_COUNT_PROPERTY, calculationIndexes.size())
            .addProperty(WPASS, ((ScalarArgument) arguments.get(WPASS)).getValue());
    List<Integer> requiredColumns = new ArrayList<>(partitionIndexes);
    requiredColumns.add(timeColumnIndex);
    requiredColumns.addAll(calculationIndexes);

    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, requiredColumns)
        .handle(handleBuilder.build())
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public abstract TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle);

  protected abstract String convertColumnName(String columnName);

  /**
   * Processes one complete partition (which may contain multiple calculation columns).
   *
   * <p>FFT-based filters cannot operate on null values. We therefore collect only finite, non-null
   * values for each calculation column and keep their original row positions. The compact sequence
   * is passed to the filter, then the transformed values are expanded back to the partition's
   * original row layout. Rows that had a null or non-finite input remain null in the output; they
   * do not participate in the FFT calculation.
   */
  protected abstract static class FilterTransferDataProcessor
      implements TableFunctionDataProcessor {

    protected static final int INITIAL_CAPACITY = 512;
    protected static final int MAX_COUNT_IN_ONE_PARTITION = 65536;

    private final double wpass;
    private int partitionRowCount;

    private final int partitionColumnCount;
    private final int timeColumnIndex;
    private final int calculationColumnStartIndex;
    private final Type[] partitionTypes;
    private final Object[] partitionValues;

    // CalculationColumnContainer collect the all value of a column in one partition
    private long[] partitionTimestamps;
    private final CalculationColumnContainer[] calculationColumnContainers;

    protected FilterTransferDataProcessor(
        double wpass, Type[] partitionTypes, int calculationColumnCount) {
      this.wpass = wpass;
      this.partitionColumnCount = partitionTypes.length;
      this.timeColumnIndex = partitionColumnCount;
      this.partitionRowCount = 0;
      this.calculationColumnStartIndex = timeColumnIndex + 1;
      this.partitionTypes = partitionTypes;
      this.partitionValues = new Object[partitionTypes.length];
      this.partitionTimestamps = new long[INITIAL_CAPACITY];
      this.calculationColumnContainers = new CalculationColumnContainer[calculationColumnCount];
      for (int i = 0; i < calculationColumnCount; i++) {
        calculationColumnContainers[i] = new CalculationColumnContainer();
      }
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if (partitionRowCount >= MAX_COUNT_IN_ONE_PARTITION) {
        throw new SemanticException(
            CommonMessages.EXCEPTION_FILTER_FUNCTION_ROW_INDEX_EXCEED_MAXIMUM);
      }
      if (partitionRowCount == 0) {
        capturePartitionValues(input);
      }
      collectTimeColumnValue(input);
      collectCalculationValues(input, partitionRowCount);
      partitionRowCount++;
    }

    private void capturePartitionValues(Record input) {
      for (int i = 0; i < partitionColumnCount; i++) {
        partitionValues[i] =
            input.isNull(i) ? null : WindowTVFUtils.readValue(input, i, partitionTypes[i]);
      }
    }

    private void collectTimeColumnValue(Record input) {
      if (partitionRowCount >= partitionTimestamps.length) {
        int newCapacity = partitionTimestamps.length + (partitionTimestamps.length >> 2);
        partitionTimestamps = Arrays.copyOf(partitionTimestamps, newCapacity);
      }
      partitionTimestamps[partitionRowCount] = input.getLong(timeColumnIndex);
    }

    private void collectCalculationValues(Record input, int partitionRowIndex) {
      for (int i = 0; i < calculationColumnContainers.length; i++) {
        // Missing and non-finite values are intentionally excluded from the compact FFT input.
        if (!input.isNull(calculationColumnStartIndex + i)) {
          double aDouble = input.getDouble(calculationColumnStartIndex + i);
          if (Double.isFinite(aDouble)) {
            calculationColumnContainers[i].add(partitionRowIndex, aDouble);
          }
          ;
        }
      }
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {

      // collect the partition columns
      for (int columnIndex = 0; columnIndex < partitionColumnCount; columnIndex++) {
        ColumnBuilder partitionColumnBuilder = properColumnBuilders.get(columnIndex);
        Object partitionValue = partitionValues[columnIndex];
        Type partitionType = partitionTypes[columnIndex];
        for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++) {
          WindowTVFUtils.writeValue(partitionColumnBuilder, partitionValue, partitionType);
        }
      }

      // collect the time column
      ColumnBuilder timeColumnBuilder = properColumnBuilders.get(timeColumnIndex);
      for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++) {
        timeColumnBuilder.writeLong(partitionTimestamps[rowIndex]);
      }

      // collect the calculation column
      for (int i = 0; i < calculationColumnContainers.length; i++) {
        transformSingleColumn(
            calculationColumnContainers[i],
            wpass,
            properColumnBuilders.get(calculationColumnStartIndex + i));
      }
    }

    private void transformSingleColumn(
        CalculationColumnContainer columnContainer,
        double wpass,
        ColumnBuilder properColumnBuilder) {
      int size = columnContainer.validValueCount;
      if (size == 0) {
        for (int rowIndex = 0; rowIndex < partitionRowCount; rowIndex++) {
          properColumnBuilder.appendNull();
        }
        return;
      }
      double[] temp = filterTransform(columnContainer, size, wpass);
      // Restore the transformed values to their original rows; excluded rows stay null.
      int validValueIndex = 0;
      for (int i = 0; i < partitionRowCount; i++) {
        if (columnContainer.validRows.get(i)) {
          properColumnBuilder.writeDouble(temp[2 * validValueIndex]);
          validValueIndex++;
        } else {
          properColumnBuilder.appendNull();
        }
      }
    }

    protected abstract double[] filterTransform(
        CalculationColumnContainer columnContainer, int size, double wpass);
  }

  protected static class CalculationColumnContainer {
    protected double[] validValues = new double[FilterTransferDataProcessor.INITIAL_CAPACITY];
    private int validValueCount = 0;
    private final BitSet validRows = new BitSet();

    public void add(int rowIndex, double value) {
      ensureCapacity(validValueCount + 1);
      validValues[validValueCount++] = value;
      validRows.set(rowIndex);
    }

    private void ensureCapacity(int requiredCapacity) {
      if (requiredCapacity <= validValues.length) {
        return;
      }
      int newCapacity = validValues.length + (validValues.length >> 1);
      validValues = Arrays.copyOf(validValues, newCapacity);
    }
  }
}
