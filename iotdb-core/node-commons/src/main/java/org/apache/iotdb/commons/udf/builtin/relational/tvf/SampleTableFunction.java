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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.iotdb.commons.i18n.CommonMessages.EXCEPTION_SAMPLE_FUNCTION_N_VALIDATION;

public class SampleTableFunction implements TableFunction {

  public static final String DATA_PARAMETER_NAME = "DATA";
  public static final String SAMPLE_SIZE_PARAMETER_NAME = "N";

  private static final String OUTPUT_TYPES_PROPERTY = "OUTPUT_TYPES";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).setSemantics().build(),
        ScalarParameterSpecification.builder()
            .name(SAMPLE_SIZE_PARAMETER_NAME)
            .type(Type.INT32)
            .addChecker(
                object -> {
                  if (object instanceof Number) {
                    int value = ((Number) object).intValue();
                    if (value > 0) {
                      return null;
                    }
                  }
                  return EXCEPTION_SAMPLE_FUNCTION_N_VALIDATION;
                })
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);

    // Sampling is row based: every row is a candidate, regardless of which column it comes from.
    // All columns are therefore required from the source and kept in the output, with the
    // partition columns moved to the front in PARTITION BY order; the remaining columns keep their
    // original relative order.
    List<Integer> partitionIndexes = WindowTVFUtils.getPartitionIndexes(tableArgument);
    Set<Integer> partitionIndexSet = new HashSet<>(partitionIndexes);

    List<Integer> outputIndexes = new ArrayList<>(tableArgument.size());
    List<Type> outputTypes = new ArrayList<>(tableArgument.size());
    DescribedSchema.Builder schemaBuilder = new DescribedSchema.Builder();

    for (int partitionIndex : partitionIndexes) {
      String columnName =
          tableArgument
              .getFieldNames()
              .get(partitionIndex)
              .orElseThrow(
                  () -> new SemanticException(CommonMessages.EXCEPTION_COLUMN_LACK_OF_NAME));
      Type columnType = tableArgument.getFieldTypes().get(partitionIndex);
      outputIndexes.add(partitionIndex);
      outputTypes.add(columnType);
      schemaBuilder.addField(columnName, columnType);
    }

    for (int i = 0; i < tableArgument.size(); i++) {
      if (partitionIndexSet.contains(i)) {
        continue;
      }
      Type columnType = tableArgument.getFieldTypes().get(i);
      if (columnType == Type.OBJECT) {
        throw new SemanticException(CommonMessages.OBJECT_TYPE_COLUMN_NOT_SUPPORTED);
      }
      outputIndexes.add(i);
      outputTypes.add(columnType);
      schemaBuilder.addField(tableArgument.getFieldNames().get(i), columnType);
    }

    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                SAMPLE_SIZE_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(SAMPLE_SIZE_PARAMETER_NAME)).getValue())
            .addProperty(OUTPUT_TYPES_PROPERTY, WindowTVFUtils.joinTypes(outputTypes))
            .build();

    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, outputIndexes)
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
    int sampleSize = (Integer) handle.getProperty(SAMPLE_SIZE_PARAMETER_NAME);
    Type[] outputTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(OUTPUT_TYPES_PROPERTY));
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new SampleDataProcessor(sampleSize, outputTypes);
      }
    };
  }

  /**
   * Reservoir sampling over whole rows. All rows of a partition are candidates: the first N rows
   * fill the reservoir, and each following row replaces a uniformly chosen slot with probability N
   * / (rows seen so far). Rows are buffered until the partition ends because the final sample is
   * not known before then.
   */
  private static class SampleDataProcessor implements TableFunctionDataProcessor {

    private final int sampleSize;
    private final Type[] outputTypes;
    private final List<SampledRow> reservoir = new ArrayList<>();
    private long rowCount = 0;

    private SampleDataProcessor(int sampleSize, Type[] outputTypes) {
      this.sampleSize = sampleSize;
      this.outputTypes = outputTypes;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      int slot = pickSlot();
      if (slot >= 0) {
        SampledRow row = new SampledRow(rowCount, captureRow(input));
        if (slot == reservoir.size()) {
          reservoir.add(row);
        } else {
          reservoir.set(slot, row);
        }
      }
      rowCount++;
    }

    /** Returns the reservoir slot the current row occupies, or -1 if the row is not kept. */
    private int pickSlot() {
      if (rowCount < sampleSize) {
        return (int) rowCount;
      }
      long candidate = ThreadLocalRandom.current().nextLong(rowCount + 1);
      return candidate < sampleSize ? (int) candidate : -1;
    }

    private Object[] captureRow(Record input) {
      Object[] values = new Object[outputTypes.length];
      for (int i = 0; i < outputTypes.length; i++) {
        values[i] = input.isNull(i) ? null : WindowTVFUtils.readValue(input, i, outputTypes[i]);
      }
      return values;
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      // emit in input order so the ORDER BY of DATA still
      // decides the relative order of the sampled rows.
      reservoir.sort((left, right) -> Long.compare(left.position, right.position));
      for (SampledRow row : reservoir) {
        for (int i = 0; i < outputTypes.length; i++) {
          WindowTVFUtils.writeValue(properColumnBuilders.get(i), row.values[i], outputTypes[i]);
        }
      }
    }

    private static class SampledRow {
      private final long position;
      private final Object[] values;

      private SampledRow(long position, Object[] values) {
        this.position = position;
        this.values = values;
      }
    }
  }
}
