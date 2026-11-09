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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DerivativeTableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String UNIT_PARAMETER_NAME = "UNIT";

  // duration literals are evaluated in the current timestamp precision, so the 1s default has to
  // be expressed in that precision as well
  private static final long DEFAULT_UNIT =
      TimestampPrecisionUtils.currPrecision.convert(1, TimeUnit.SECONDS);
  private static final String OUTPUT_TYPES_PROPERTY = "OUTPUT_TYPES";
  private static final String PARTITION_COLUMN_COUNT_PROPERTY = "PARTITION_COLUMN_COUNT";
  private static final String UNIT_PROPERTY = "UNIT";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(UNIT_PARAMETER_NAME)
            .type(Type.INT64)
            .defaultValue(DEFAULT_UNIT)
            .addChecker(
                object -> {
                  // a duration smaller than the smallest unit of the current timestamp precision
                  // has already been truncated to 0 when the argument was evaluated, so explain
                  // this case explicitly
                  if (object instanceof Long && (Long) object > 0) {
                    return null;
                  }
                  return String.format(
                      CommonMessages
                          .EXCEPTION_DERIVATIVE_FUNCTION_UNIT_SHOULD_BE_A_POSITIVE_DURATION_OF_AT_LEAST_ONE_43D975B7,
                      TimestampPrecisionUtils.TIMESTAMP_PRECISION);
                })
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);

    // ORDER BY is mandatory here and must contain exactly one TIMESTAMP column: the derivative is
    // taken with respect to this column, which is also emitted as the time of each output row.
    List<String> orderBy = checkAndGetOrderBy(tableArgument);
    int timeColumnIndex =
        WindowTVFUtils.findColumnIndex(
            tableArgument, orderBy.get(0), Collections.singleton(Type.TIMESTAMP));

    List<Integer> partitionIndexes = WindowTVFUtils.getPartitionIndexes(tableArgument);
    Set<Integer> excludedIndexes = new HashSet<>(partitionIndexes);
    excludedIndexes.add(timeColumnIndex);

    List<Type> outputTypes = new ArrayList<>();
    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();

    // keep the partition columns with their original names and types, in PARTITION BY order
    for (int partitionIndex : partitionIndexes) {
      String columnName =
          tableArgument
              .getFieldNames()
              .get(partitionIndex)
              .orElseThrow(
                  () -> new SemanticException(CommonMessages.EXCEPTION_COLUMN_LACK_OF_NAME));
      Type columnType = tableArgument.getFieldTypes().get(partitionIndex);
      outputTypes.add(columnType);
      schemaBuilder.addField(columnName, columnType);
    }

    // each output row corresponds to a pair of adjacent points and carries the time of the second
    // point of the pair, so the time column is emitted as is.
    String timeColumnName =
        tableArgument
            .getFieldNames()
            .get(timeColumnIndex)
            .orElseThrow(() -> new SemanticException(CommonMessages.EXCEPTION_COLUMN_LACK_OF_NAME));
    outputTypes.add(Type.TIMESTAMP);
    schemaBuilder.addField(timeColumnName, Type.TIMESTAMP);
    List<Integer> calculationIndexes =
        WindowTVFUtils.getCalculationIndexes(
            tableArgument,
            excludedIndexes,
            columnName -> {
              outputTypes.add(Type.DOUBLE);
              schemaBuilder.addField(convertColumnName(columnName), Type.DOUBLE);
            });
    if (calculationIndexes.isEmpty()) {
      throw new SemanticException(CommonMessages.EXCEPTION_NO_CALCULATE_COLUMNS);
    }

    // the output columns are in the same order as the required input columns: partition columns,
    // the time column and one DOUBLE column per calculation column. The processor therefore only
    // needs the output types and the number of partition columns to walk both of them.
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(OUTPUT_TYPES_PROPERTY, WindowTVFUtils.joinTypes(outputTypes))
            .addProperty(PARTITION_COLUMN_COUNT_PROPERTY, partitionIndexes.size())
            .addProperty(
                UNIT_PROPERTY,
                ((ScalarArgument) arguments.get(UNIT_PARAMETER_NAME)).getValue())
            .build();

    List<Integer> requiredColumns = new ArrayList<>(partitionIndexes);
    requiredColumns.add(timeColumnIndex);
    requiredColumns.addAll(calculationIndexes);

    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, requiredColumns)
        .handle(handle)
        .build();
  }

  private static List<String> checkAndGetOrderBy(TableArgument tableArgument) {
    List<String> orderBy = tableArgument.getOrderBy();
    if (orderBy.isEmpty()) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_TABLE_ARGUMENT_WITH_SET_SEMANTICS_REQUIRES_AN_ORDER_BY_CLAUSE_10C986D9);
    }
    if (orderBy.size() != 1) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_THE_ORDER_BY_CLAUSE_OF_THE_DATA_ARGUMENT_MUST_CONTAIN_EXACTLY_ONE_TIMESTAMP_COLUMN_6B40C59B);
    }
    return orderBy;
  }

  private static String convertColumnName(String columnName) {
    return String.format("derivative(%s)", columnName);
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
    Type[] outputTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(OUTPUT_TYPES_PROPERTY));
    int partitionColumnCount = (Integer) handle.getProperty(PARTITION_COLUMN_COUNT_PROPERTY);
    long unit = (Long) handle.getProperty(UNIT_PROPERTY);
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new DerivativeDataProcessor(partitionColumnCount, outputTypes, unit);
      }
    };
  }

  /**
   * Streaming processor over one partition. Each row is paired with the previous one, so the
   * output row is produced as soon as the pair is complete and {@code finish} is left empty.
   *
   * <p>The required input columns are the partition columns, the time column (the single ORDER BY
   * column of DATA) and the calculation columns, in that order, which is also the order of the
   * output columns.
   */
  private static class DerivativeDataProcessor implements TableFunctionDataProcessor {

    private final Type[] outputTypes;
    private final int partitionColumnCount;
    private final int timeColumnIndex;
    private final int calculationColumnCount;
    private final long unit;
    private final LastFieldValues lastFieldValues;

    private DerivativeDataProcessor(int partitionColumnCount, Type[] outputTypes, long unit) {
      this.outputTypes = outputTypes;
      this.partitionColumnCount = partitionColumnCount;
      this.timeColumnIndex = partitionColumnCount;
      this.calculationColumnCount = outputTypes.length - partitionColumnCount - 1;
      this.unit = unit;
      this.lastFieldValues = new LastFieldValues(calculationColumnCount);
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      // the ORDER BY column of DATA is not necessarily the implicit time column and may be null;
      // such a row cannot be paired with anything and is dropped.
      if (input.isNull(timeColumnIndex)) {
        return;
      }
      long timestamp = input.getLong(timeColumnIndex);

      if (!lastFieldValues.present) {
        lastFieldValues.update(input, timestamp, timeColumnIndex);
        return;
      }
      if (timestamp < lastFieldValues.timestamp) {
        throw new SemanticException(
            CommonMessages
                .EXCEPTION_THE_TIME_COLUMN_OF_THE_DATA_ARGUMENT_MUST_BE_MONOTONICALLY_INCREASING_WITHIN_EACH_PARTITION_68B8C2B7);
      }
      if (timestamp == lastFieldValues.timestamp) {
        // the same timestamp may appear across devices: the later row overwrites the earlier one
        // and this pair produces no output.
        lastFieldValues.update(input, timestamp, timeColumnIndex);
        return;
      }

      emitDerivative(input, timestamp, properColumnBuilders);
      lastFieldValues.update(input, timestamp, timeColumnIndex);
    }

    private void emitDerivative(
        Record input, long timestamp, List<ColumnBuilder> properColumnBuilders) {
      // all rows of a partition share the same partition values, so they are copied from the
      // current row instead of being cached.
      for (int i = 0; i < partitionColumnCount; i++) {
        ColumnBuilder builder = properColumnBuilders.get(i);
        if (input.isNull(i)) {
          builder.appendNull();
        } else {
          WindowTVFUtils.writeValue(
              builder, WindowTVFUtils.readValue(input, i, outputTypes[i]), outputTypes[i]);
        }
      }
      properColumnBuilders.get(timeColumnIndex).writeLong(timestamp);

      long timeDelta = timestamp - lastFieldValues.timestamp;
      for (int i = 0; i < calculationColumnCount; i++) {
        int columnIndex = timeColumnIndex + 1 + i;
        ColumnBuilder builder = properColumnBuilders.get(columnIndex);
        Object lastValue = lastFieldValues.values[i];
        // a null on either side only makes this column null for this pair; pairing and time still
        // advance normally.
        if (lastValue == null || input.isNull(columnIndex)) {
          builder.appendNull();
        } else {
          double valueDelta = input.getDouble(columnIndex) - ((Number) lastValue).doubleValue();
          builder.writeDouble(valueDelta * unit / timeDelta);
        }
      }
    }

    private static class LastFieldValues {

      private boolean present;
      private long timestamp;
      // native boxed values of the calculation columns, null when the value is null
      private final Object[] values;

      private LastFieldValues(int fieldCount) {
        this.values = new Object[fieldCount];
      }

      private void update(Record input, long timestamp, int timeColumnIndex) {
        this.present = true;
        this.timestamp = timestamp;
        for (int i = 0; i < values.length; i++) {
          int columnIndex = timeColumnIndex + 1 + i;
          values[i] = input.isNull(columnIndex) ? null : input.getObject(columnIndex);
        }
      }
    }
  }
}
